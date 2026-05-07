import argparse
import json
import logging
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from supabase import create_client


SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"
MIGRATION_VERSION = "normativa_drive_structure_v1"
REPORTS_DIR = Path("reports")
DRY_RUN_REPORT_PATH = REPORTS_DIR / "normativa_drive_migration_dry_run.md"
RESULT_REPORT_PATH = REPORTS_DIR / "normativa_drive_migration_result.md"
RESULT_JSON_PATH = REPORTS_DIR / "normativa_drive_migration_result.json"
SUBFOLDERS = [
    "00_ORIGINAL",
    "01_TEXTO",
    "02_PAGINAS_RENDER",
    "03_IMAGENES_EXTRAIDAS",
    "04_TABLAS",
    "05_ESTRUCTURADO",
    "06_IA",
    "99_MANIFEST",
]
SUPABASE_SELECT_FIELDS = (
    "id, document_key, source_type, source_section, tipo_norma, numero, anio, "
    "titulo, fecha_publicacion, fecha_promulgacion, entidad_emisora, "
    "fuente_oficial, source_url, pdf_url, file_name, mime_type, has_file, "
    "drive_file_id, drive_file_url, drive_folder_id, drive_structure, raw, "
    "process_status, updated_at"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_drive_service():
    client_path_raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON_PATH")
    token_path_raw = os.getenv("GOOGLE_OAUTH_TOKEN_PATH")

    if not client_path_raw:
        raise ValueError("Falta GOOGLE_OAUTH_CLIENT_JSON_PATH")
    if not token_path_raw:
        raise ValueError("Falta GOOGLE_OAUTH_TOKEN_PATH")

    client_path = Path(client_path_raw)
    token_path = Path(token_path_raw)

    if not client_path.exists():
        raise FileNotFoundError(f"No existe GOOGLE_OAUTH_CLIENT_JSON_PATH: {client_path}")
    if not token_path.exists():
        raise FileNotFoundError(f"No existe GOOGLE_OAUTH_TOKEN_PATH: {token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise ValueError("El token OAuth no es valido o requiere reautorizacion")

    return build("drive", "v3", credentials=creds)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--document-key")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--pending-only", action="store_true")
    args = parser.parse_args()

    if args.apply:
        args.mode = "apply"
    else:
        args.mode = "dry-run"
        args.dry_run = True

    return args


def normalize_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def escape_drive_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def iso_date(value) -> str | None:
    if not value:
        return None
    return str(value)[:10]


def normalize_document_folder_name(document_key: str) -> str:
    return normalize_text(document_key).replace("/", "-").replace("\\", "-")


def make_pdf_copy_name(row: dict, document_folder_name: str) -> str:
    published = iso_date(row.get("fecha_publicacion")) or "sin-fecha"
    return f"{document_folder_name}__{published}__original.pdf"


def build_drive_folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def build_document_paths(document_folder_name: str) -> dict[str, str]:
    document_path = f"DIGEMID/02_NORMATIVA/{document_folder_name}"
    return {
        "planned_document_path": document_path,
        "planned_original_folder_path": f"{document_path}/00_ORIGINAL",
        "planned_manifest_path": f"{document_path}/99_MANIFEST/manifest.json",
    }


def has_drive_structure(row: dict) -> bool:
    drive_structure = row.get("drive_structure")
    return isinstance(drive_structure, dict) and bool(drive_structure)


def should_process_pending(row: dict) -> bool:
    return (not has_drive_structure(row)) or (not normalize_text(row.get("drive_folder_id")))


def drive_get_file(service, file_id: str) -> dict:
    return (
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, size, parents, webViewLink, webContentLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def drive_find_child(service, parent_id: str, name: str, mime_type: str | None = None) -> dict | None:
    query_parts = [
        f"'{escape_drive_query_value(parent_id)}' in parents",
        f"name = '{escape_drive_query_value(name)}'",
        "trashed = false",
    ]
    if mime_type:
        query_parts.append(f"mimeType = '{escape_drive_query_value(mime_type)}'")

    response = (
        service.files()
        .list(
            q=" and ".join(query_parts),
            fields="files(id, name, mimeType, size, parents, webViewLink, webContentLink)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = response.get("files", [])
    return files[0] if files else None


def drive_ensure_folder(
    service,
    parent_id: str | None,
    folder_name: str,
    apply_changes: bool,
    operations: list[dict],
    planned_path: str | None = None,
) -> dict:
    if not parent_id:
        operations.append(
            {
                "action": "create_folder",
                "folder_name": folder_name,
                "parent_id": None,
                "planned_path": planned_path,
            }
        )
        return {
            "id": None,
            "name": folder_name,
            "mimeType": DRIVE_FOLDER_MIME,
            "webViewLink": None,
            "pending_create": True,
        }

    existing = drive_find_child(service, parent_id, folder_name, DRIVE_FOLDER_MIME)
    if existing:
        operations.append(
            {
                "action": "reuse_folder",
                "folder_name": folder_name,
                "folder_id": existing.get("id"),
                "parent_id": parent_id,
                "planned_path": planned_path,
            }
        )
        return existing

    operations.append(
        {
            "action": "create_folder",
            "folder_name": folder_name,
            "parent_id": parent_id,
            "planned_path": planned_path,
        }
    )
    if not apply_changes:
        return {
            "id": None,
            "name": folder_name,
            "mimeType": DRIVE_FOLDER_MIME,
            "webViewLink": None,
            "pending_create": True,
        }

    return (
        service.files()
        .create(
            body={
                "name": folder_name,
                "mimeType": DRIVE_FOLDER_MIME,
                "parents": [parent_id],
            },
            fields="id, name, mimeType, parents, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def ensure_folder_path(service, root_folder_id: str, document_folder_name: str, apply_changes: bool, operations: list[dict], planned_paths: dict[str, str]) -> tuple[dict, dict]:
    normativa_folder = drive_ensure_folder(
        service,
        root_folder_id,
        "02_NORMATIVA",
        apply_changes,
        operations,
        planned_path="DIGEMID/02_NORMATIVA",
    )
    document_folder = drive_ensure_folder(
        service,
        normativa_folder["id"],
        document_folder_name,
        apply_changes,
        operations,
        planned_path=planned_paths["planned_document_path"],
    )

    subfolders = {}
    current_parent = document_folder["id"]
    for subfolder_name in SUBFOLDERS:
        if subfolder_name == "00_ORIGINAL":
            planned_subfolder_path = planned_paths["planned_original_folder_path"]
        elif subfolder_name == "99_MANIFEST":
            planned_subfolder_path = planned_paths["planned_manifest_path"].rsplit("/", 1)[0]
        else:
            planned_subfolder_path = f"{planned_paths['planned_document_path']}/{subfolder_name}"
        subfolders[subfolder_name] = drive_ensure_folder(
            service,
            current_parent,
            subfolder_name,
            apply_changes,
            operations,
            planned_path=planned_subfolder_path,
        )
        current_parent = document_folder["id"]

    return document_folder, subfolders


def drive_copy_file(
    service,
    source_file_id: str,
    target_folder_id: str | None,
    target_name: str,
    apply_changes: bool,
    operations: list[dict],
    planned_path: str | None = None,
) -> dict:
    if not target_folder_id:
        operations.append(
            {
                "action": "copy_file",
                "source_file_id": source_file_id,
                "target_folder_id": None,
                "file_name": target_name,
                "planned_path": planned_path,
            }
        )
        return {
            "id": None,
            "name": target_name,
            "mimeType": "application/pdf",
            "webViewLink": None,
            "webContentLink": None,
            "size": None,
            "pending_copy": True,
        }

    existing = drive_find_child(service, target_folder_id, target_name)
    if existing:
        operations.append(
            {
                "action": "reuse_file_copy",
                "source_file_id": source_file_id,
                "target_folder_id": target_folder_id,
                "file_name": target_name,
                "file_id": existing.get("id"),
                "planned_path": planned_path,
            }
        )
        return existing

    operations.append(
        {
            "action": "copy_file",
            "source_file_id": source_file_id,
            "target_folder_id": target_folder_id,
            "file_name": target_name,
            "planned_path": planned_path,
        }
    )
    if not apply_changes:
        return {
            "id": None,
            "name": target_name,
            "mimeType": "application/pdf",
            "webViewLink": None,
            "webContentLink": None,
            "size": None,
            "pending_copy": True,
        }

    return (
        service.files()
        .copy(
            fileId=source_file_id,
            body={"name": target_name, "parents": [target_folder_id]},
            fields="id, name, mimeType, size, parents, webViewLink, webContentLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def drive_upsert_json_file(
    service,
    folder_id: str | None,
    file_name: str,
    payload: dict,
    apply_changes: bool,
    operations: list[dict],
    planned_path: str | None = None,
) -> dict:
    content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    if not folder_id:
        operations.append(
            {
                "action": "create_manifest",
                "target_folder_id": None,
                "file_name": file_name,
                "planned_path": planned_path,
            }
        )
        return {
            "id": None,
            "name": file_name,
            "mimeType": "application/json",
            "webViewLink": None,
            "webContentLink": None,
            "size": len(content),
            "pending_create": True,
        }

    existing = drive_find_child(service, folder_id, file_name)
    if existing:
        operations.append(
            {
                "action": "update_manifest",
                "target_folder_id": folder_id,
                "file_name": file_name,
                "file_id": existing.get("id"),
                "planned_path": planned_path,
            }
        )
        if not apply_changes:
            return {
                **existing,
                "pending_update": True,
            }

        media = MediaInMemoryUpload(content, mimetype="application/json", resumable=False)
        return (
            service.files()
            .update(
                fileId=existing["id"],
                media_body=media,
                fields="id, name, mimeType, size, parents, webViewLink, webContentLink",
                supportsAllDrives=True,
            )
            .execute()
        )

    operations.append(
        {
            "action": "create_manifest",
            "target_folder_id": folder_id,
            "file_name": file_name,
            "planned_path": planned_path,
        }
    )
    if not apply_changes:
        return {
            "id": None,
            "name": file_name,
            "mimeType": "application/json",
            "webViewLink": None,
            "webContentLink": None,
            "size": len(content),
            "pending_create": True,
        }

    media = MediaInMemoryUpload(content, mimetype="application/json", resumable=False)
    return (
        service.files()
        .create(
            body={"name": file_name, "parents": [folder_id]},
            media_body=media,
            fields="id, name, mimeType, size, parents, webViewLink, webContentLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def get_normas(supabase, limit: int | None, document_key: str | None, pending_only: bool) -> list[dict]:
    query = (
        supabase.table("digemid_normas")
        .select(SUPABASE_SELECT_FIELDS)
        .eq("source_type", "norma")
        .order("fecha_publicacion", desc=False)
    )
    if document_key:
        query = query.eq("document_key", document_key)
    if limit and not (pending_only and not document_key):
        query = query.limit(limit)

    response = query.execute()
    rows = response.data or []

    if pending_only:
        total_candidates = len(rows)
        rows = [row for row in rows if should_process_pending(row)]
        logger.info(
            "Pending-only activo: %s normas pendientes de %s normas candidatas.",
            len(rows),
            total_candidates,
        )
        if limit:
            rows = rows[:limit]

    return rows


def get_existing_assets(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table("digemid_norma_assets")
        .select("id, asset_tipo, asset_subtipo, drive_file_id, file_name, metadata")
        .eq("norma_id", norma_id)
        .execute()
    )
    return response.data or []


def find_matching_asset(existing_assets: list[dict], drive_file_id: str | None, asset_tipo: str, asset_subtipo: str, file_name: str | None) -> dict | None:
    for item in existing_assets:
        if file_name and item.get("file_name") == file_name and item.get("asset_tipo") == asset_tipo and item.get("asset_subtipo") == asset_subtipo:
            return item
        if drive_file_id and item.get("drive_file_id") == drive_file_id and item.get("asset_tipo") == asset_tipo and item.get("asset_subtipo") == asset_subtipo:
            return item
    return None


def insert_asset_if_needed(supabase, existing_assets: list[dict], payload: dict, apply_changes: bool, operations: list[dict]):
    existing = find_matching_asset(
        existing_assets,
        payload.get("drive_file_id"),
        payload.get("asset_tipo"),
        payload.get("asset_subtipo"),
        payload.get("file_name"),
    )
    if existing:
        operations.append(
            {
                "action": "reuse_asset",
                "asset_tipo": payload.get("asset_tipo"),
                "asset_subtipo": payload.get("asset_subtipo"),
                "drive_file_id": payload.get("drive_file_id"),
                "file_name": payload.get("file_name"),
                "asset_id": existing.get("id"),
            }
        )
        return existing

    operations.append(
        {
            "action": "insert_asset",
            "asset_tipo": payload.get("asset_tipo"),
            "asset_subtipo": payload.get("asset_subtipo"),
            "drive_file_id": payload.get("drive_file_id"),
            "file_name": payload.get("file_name"),
        }
    )
    if not apply_changes:
        return None

    response = supabase.table("digemid_norma_assets").insert(payload).execute()
    inserted = (response.data or [None])[0]
    if inserted:
        existing_assets.append(inserted)
    return inserted


def deep_merge_dicts(base: dict, patch: dict) -> dict:
    result = deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def update_norma_record(supabase, row: dict, payload: dict, apply_changes: bool, operations: list[dict]):
    operations.append(
        {
            "action": "update_norma",
            "norma_id": row["id"],
            "document_key": row["document_key"],
            "drive_folder_id": payload.get("drive_folder_id"),
        }
    )
    if not apply_changes:
        return None

    return supabase.table("digemid_normas").update(payload).eq("id", row["id"]).execute()


def build_manifest(row: dict, document_folder: dict, subfolders: dict[str, dict], copied_pdf: dict | None, created_at_iso: str) -> dict:
    return {
        "norma_id": row["id"],
        "document_key": row["document_key"],
        "source_type": row.get("source_type"),
        "source_section": row.get("source_section"),
        "tipo_norma": row.get("tipo_norma"),
        "numero": row.get("numero"),
        "anio": row.get("anio"),
        "titulo": row.get("titulo"),
        "fecha_publicacion": iso_date(row.get("fecha_publicacion")),
        "fecha_promulgacion": iso_date(row.get("fecha_promulgacion")),
        "entidad_emisora": row.get("entidad_emisora"),
        "fuente_oficial": row.get("fuente_oficial"),
        "source_url": row.get("source_url"),
        "pdf_url": row.get("pdf_url"),
        "original_drive_file_id": row.get("drive_file_id"),
        "copied_pdf_drive_file_id": copied_pdf.get("id") if copied_pdf else None,
        "document_folder_id": document_folder.get("id"),
        "subfolder_ids": {name: data.get("id") for name, data in subfolders.items()},
        "created_at": created_at_iso,
        "migration_version": MIGRATION_VERSION,
    }


def make_pdf_asset_payload(row: dict, document_folder_id: str, original_drive_file: dict, copied_pdf: dict) -> dict:
    return {
        "norma_id": row["id"],
        "page_id": None,
        "asset_tipo": "pdf_original",
        "asset_subtipo": "copied_to_document_folder",
        "storage_backend": "google_drive",
        "storage_path": None,
        "drive_file_id": copied_pdf.get("id"),
        "source_url": copied_pdf.get("webViewLink"),
        "mime_type": "application/pdf",
        "file_name": copied_pdf.get("name"),
        "file_ext": "pdf",
        "file_size_bytes": int(copied_pdf["size"]) if copied_pdf.get("size") else None,
        "page_number": None,
        "bbox": {},
        "text_hint": None,
        "metadata": {
            "original_drive_file_id": row.get("drive_file_id"),
            "document_folder_id": document_folder_id,
            "original_drive_file_url": original_drive_file.get("webViewLink"),
            "copied_drive_file_url": copied_pdf.get("webViewLink"),
            "migration_version": MIGRATION_VERSION,
        },
    }


def make_manifest_asset_payload(row: dict, manifest_file: dict) -> dict:
    return {
        "norma_id": row["id"],
        "page_id": None,
        "asset_tipo": "manifest",
        "asset_subtipo": "document_manifest",
        "storage_backend": "google_drive",
        "storage_path": None,
        "drive_file_id": manifest_file.get("id"),
        "source_url": manifest_file.get("webViewLink"),
        "mime_type": "application/json",
        "file_name": "manifest.json",
        "file_ext": "json",
        "file_size_bytes": int(manifest_file["size"]) if manifest_file.get("size") else None,
        "page_number": None,
        "bbox": {},
        "text_hint": None,
        "metadata": {
            "migration_version": MIGRATION_VERSION,
        },
    }


def build_norma_update_payload(row: dict, document_folder: dict, subfolders: dict[str, dict], copied_pdf: dict | None, manifest_file: dict, migrated_at_iso: str) -> dict:
    existing_structure = row.get("drive_structure") if isinstance(row.get("drive_structure"), dict) else {}
    document_folder_id = document_folder.get("id")
    drive_structure_patch = {
        "migration_version": MIGRATION_VERSION,
        "document_folder_id": document_folder_id,
        "document_folder_url": build_drive_folder_url(document_folder_id) if document_folder_id else None,
        "subfolders": {
            name: {
                "id": value.get("id"),
                "url": build_drive_folder_url(value.get("id")) if value.get("id") else None,
            }
            for name, value in subfolders.items()
        },
        "original_drive_file_id": row.get("drive_file_id"),
        "copied_pdf_drive_file_id": copied_pdf.get("id") if copied_pdf else None,
        "manifest_file_id": manifest_file.get("id"),
        "migrated_at": migrated_at_iso,
    }
    merged_structure = deep_merge_dicts(existing_structure, drive_structure_patch)
    payload = {
        "drive_folder_id": document_folder_id or row.get("drive_folder_id"),
        "drive_structure": merged_structure,
        "updated_at": migrated_at_iso,
    }
    if not normalize_text(row.get("drive_file_id")):
        payload["process_status"] = "drive_structured"
        payload["has_file"] = False
    return payload


def ensure_reports_dir():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def write_report_files(mode: str, report_payload: dict):
    ensure_reports_dir()
    RESULT_JSON_PATH.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = report_payload["summary"]
    lines = [
        f"# DIGEMID Normativa Drive Migration - {mode}",
        "",
        f"- Mode: `{mode}`",
        f"- Pending only: `{report_payload['pending_only']}`",
        f"- Normas consideradas: **{summary['documents_considered']}**",
        f"- Normas procesadas: **{summary['documents_processed']}**",
        f"- Normas omitidas: **{summary['documents_skipped']}**",
        f"- Errores: **{summary['errors']}**",
        f"- Carpetas crear/reusar: **{summary['folders_created_or_planned']} / {summary['folders_reused']}**",
        f"- PDFs copiar/reusar: **{summary['pdf_copies_created_or_planned']} / {summary['pdf_copies_reused']}**",
        f"- Manifests crear/actualizar: **{summary['manifests_created_or_updated']}**",
        f"- Assets insertar: **{summary['assets_inserted_or_planned']}**",
        f"- Normas actualizar: **{summary['documents_updated_or_planned']}**",
        "",
        "## Normas",
        "",
    ]

    for item in report_payload["documents"]:
        lines.append(f"### {item['document_key']}")
        lines.append(f"- Status: `{item['status']}`")
        lines.append(f"- Title: `{item.get('title') or ''}`")
        lines.append(f"- Planned document path: `{item.get('planned_document_path') or ''}`")
        lines.append(f"- Planned manifest path: `{item.get('planned_manifest_path') or ''}`")
        lines.append(f"- Planned PDF status: `{item.get('planned_pdf_status') or ''}`")
        lines.append(f"- Drive folder id: `{item.get('document_folder_id') or 'pending/dry-run'}`")
        if mode == "dry-run":
            lines.append(
                "- Nota: En dry-run los IDs reales de carpetas nuevas pueden aparecer como pending/null porque no se crean carpetas."
            )
        if item.get("error"):
            lines.append(f"- Error: `{item['error']}`")
        lines.append("- Planned operations:")
        for op in item.get("operations", []):
            op_parts = [op.get("action", "unknown")]
            for key in ("folder_name", "file_name", "asset_tipo", "asset_subtipo", "drive_file_id", "planned_path"):
                if op.get(key):
                    op_parts.append(f"{key}={op[key]}")
            lines.append(f"  - {'; '.join(op_parts)}")
        lines.append("")

    target_path = DRY_RUN_REPORT_PATH if mode == "dry-run" else RESULT_REPORT_PATH
    target_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    other_path = RESULT_REPORT_PATH if mode == "dry-run" else DRY_RUN_REPORT_PATH
    if not other_path.exists():
        other_path.write_text(
            f"# DIGEMID Normativa Drive Migration - {mode}\n\nEste reporte no fue generado en esta ejecucion.\n",
            encoding="utf-8",
        )


def process_norma(service, supabase, root_folder_id: str, row: dict, apply_changes: bool) -> dict:
    operations: list[dict] = []
    document_key = row["document_key"]
    document_folder_name = normalize_document_folder_name(document_key)
    planned_paths = build_document_paths(document_folder_name)
    copied_pdf_name = make_pdf_copy_name(row, document_folder_name)
    migrated_at_iso = datetime.now(timezone.utc).isoformat()

    document_folder, subfolders = ensure_folder_path(
        service,
        root_folder_id,
        document_folder_name,
        apply_changes,
        operations,
        planned_paths,
    )

    copied_pdf = None
    original_drive_file = None
    planned_pdf_status = "PDF pendiente"
    if normalize_text(row.get("drive_file_id")):
        original_drive_file = drive_get_file(service, row["drive_file_id"])
        copied_pdf = drive_copy_file(
            service,
            row["drive_file_id"],
            subfolders["00_ORIGINAL"]["id"],
            copied_pdf_name,
            apply_changes,
            operations,
            planned_path=f"{planned_paths['planned_original_folder_path']}/{copied_pdf_name}",
        )
        planned_pdf_status = (
            "PDF copiado o reutilizado" if apply_changes else "PDF disponible; se copiaria a 00_ORIGINAL"
        )

    manifest_payload = build_manifest(
        row,
        document_folder,
        subfolders,
        copied_pdf,
        migrated_at_iso,
    )
    manifest_file = drive_upsert_json_file(
        service,
        subfolders["99_MANIFEST"]["id"],
        "manifest.json",
        manifest_payload,
        apply_changes,
        operations,
        planned_path=planned_paths["planned_manifest_path"],
    )

    existing_assets = get_existing_assets(supabase, row["id"])
    if copied_pdf and original_drive_file:
        insert_asset_if_needed(
            supabase,
            existing_assets,
            make_pdf_asset_payload(row, document_folder.get("id"), original_drive_file, copied_pdf),
            apply_changes,
            operations,
        )

    insert_asset_if_needed(
        supabase,
        existing_assets,
        make_manifest_asset_payload(row, manifest_file),
        apply_changes,
        operations,
    )

    update_norma_record(
        supabase,
        row,
        build_norma_update_payload(
            row,
            document_folder,
            subfolders,
            copied_pdf,
            manifest_file,
            migrated_at_iso,
        ),
        apply_changes,
        operations,
    )

    return {
        "norma_id": row["id"],
        "document_key": document_key,
        "status": "applied" if apply_changes else "planned",
        "title": row.get("titulo"),
        "planned_document_path": planned_paths["planned_document_path"],
        "planned_manifest_path": planned_paths["planned_manifest_path"],
        "planned_pdf_status": planned_pdf_status,
        "document_folder_id": document_folder.get("id"),
        "copied_pdf_drive_file_id": copied_pdf.get("id") if copied_pdf else None,
        "manifest_file_id": manifest_file.get("id"),
        "operations": operations,
    }


def summarize_documents(items: list[dict]) -> dict:
    summary = {
        "documents_considered": len(items),
        "documents_processed": 0,
        "documents_skipped": 0,
        "errors": 0,
        "folders_created_or_planned": 0,
        "folders_reused": 0,
        "pdf_copies_created_or_planned": 0,
        "pdf_copies_reused": 0,
        "manifests_created_or_updated": 0,
        "assets_inserted_or_planned": 0,
        "documents_updated_or_planned": 0,
    }
    for item in items:
        if item.get("status") in {"planned", "applied"}:
            summary["documents_processed"] += 1
        elif item.get("status") == "skipped":
            summary["documents_skipped"] += 1
        elif item.get("status") == "error":
            summary["errors"] += 1

        for op in item.get("operations", []):
            action = op.get("action")
            if action == "create_folder":
                summary["folders_created_or_planned"] += 1
            elif action == "reuse_folder":
                summary["folders_reused"] += 1
            elif action == "copy_file":
                summary["pdf_copies_created_or_planned"] += 1
            elif action == "reuse_file_copy":
                summary["pdf_copies_reused"] += 1
            elif action in {"create_manifest", "update_manifest"}:
                summary["manifests_created_or_updated"] += 1
            elif action == "insert_asset":
                summary["assets_inserted_or_planned"] += 1
            elif action == "update_norma":
                summary["documents_updated_or_planned"] += 1

    return summary


def main():
    args = parse_args()
    load_env()

    root_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not root_folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID")

    supabase = get_supabase()
    drive_service = get_drive_service()

    rows = get_normas(
        supabase=supabase,
        limit=args.limit,
        document_key=args.document_key,
        pending_only=args.pending_only,
    )
    logger.info("Normas encontradas para migracion: %s", len(rows))

    results = []
    for row in rows:
        document_key = row.get("document_key")
        try:
            logger.info("Procesando norma %s", document_key)
            result = process_norma(
                service=drive_service,
                supabase=supabase,
                root_folder_id=root_folder_id,
                row=row,
                apply_changes=args.mode == "apply",
            )
        except Exception as exc:
            logger.exception("Error migrando norma %s", document_key)
            result = {
                "norma_id": row.get("id"),
                "document_key": document_key,
                "status": "error",
                "title": row.get("titulo"),
                "planned_document_path": build_document_paths(normalize_document_folder_name(document_key))["planned_document_path"],
                "planned_manifest_path": build_document_paths(normalize_document_folder_name(document_key))["planned_manifest_path"],
                "planned_pdf_status": "PDF pendiente" if not normalize_text(row.get("drive_file_id")) else "PDF disponible",
                "error": str(exc),
                "operations": [],
            }
        results.append(result)

    report_payload = {
        "mode": args.mode,
        "document_key_filter": args.document_key,
        "pending_only": args.pending_only,
        "limit": args.limit,
        "migration_version": MIGRATION_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summarize_documents(results),
        "documents": results,
    }
    write_report_files(args.mode, report_payload)
    logger.info("Reportes generados en %s", REPORTS_DIR)


if __name__ == "__main__":
    main()
