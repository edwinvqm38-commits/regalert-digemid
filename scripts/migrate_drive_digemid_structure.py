import argparse
import json
import logging
import os
import re
import tempfile
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
MIGRATION_VERSION = "drive_structure_v1"
REPORTS_DIR = Path("reports")
DRY_RUN_REPORT_PATH = REPORTS_DIR / "drive_migration_dry_run.md"
RESULT_REPORT_PATH = REPORTS_DIR / "drive_migration_result.md"
RESULT_JSON_PATH = REPORTS_DIR / "drive_migration_result.json"
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
    "id, source_type, source_section, document_key, title, published_date, "
    "file_url, file_name, drive_file_id, drive_file_url, drive_folder_id, "
    "mime_type, file_size_bytes, raw"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def env_json_to_file(env_name: str, fallback_path_env: str | None = None) -> tuple[Path, bool]:
    raw_value = os.getenv(env_name)
    if raw_value:
        stripped = raw_value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            json.loads(stripped)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                delete=False,
            ) as temp_file:
                temp_file.write(stripped)
                return Path(temp_file.name), True

        candidate = Path(raw_value)
        if candidate.exists():
            return candidate, False

        raise ValueError(f"{env_name} no parece JSON valido ni una ruta existente")

    if fallback_path_env:
        fallback = os.getenv(fallback_path_env)
        if fallback:
            candidate = Path(fallback)
            if candidate.exists():
                return candidate, False

    raise ValueError(
        f"Falta {env_name}" + (f" o {fallback_path_env}" if fallback_path_env else "")
    )


def cleanup_temp_paths(paths: list[Path]):
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning("No se pudo eliminar archivo temporal: %s", path)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_drive_service():
    client_path, client_is_temp = env_json_to_file(
        "GOOGLE_OAUTH_CLIENT_JSON",
        fallback_path_env="GOOGLE_OAUTH_CLIENT_JSON_PATH",
    )
    token_path, token_is_temp = env_json_to_file(
        "GOOGLE_OAUTH_TOKEN_JSON",
        fallback_path_env="GOOGLE_OAUTH_TOKEN_PATH",
    )

    if not client_path.exists():
        raise FileNotFoundError(f"No existe el client json OAuth: {client_path}")
    if not token_path.exists():
        raise FileNotFoundError(f"No existe el token OAuth: {token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise ValueError("El token OAuth no es valido o requiere reautorizacion")

    temp_paths = [
        path
        for path, is_temp in ((client_path, client_is_temp), (token_path, token_is_temp))
        if is_temp
    ]
    return build("drive", "v3", credentials=creds), temp_paths


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--source-type", default="alerta", choices=["alerta"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--document-key")
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


def infer_year(row: dict) -> int:
    published_date = row.get("published_date")
    if published_date:
        return int(str(published_date)[:4])

    match = re.search(r"(\d{4})", row.get("document_key") or "")
    if match:
        return int(match.group(1))

    raise ValueError(
        f"No se pudo inferir el año para document_key={row.get('document_key')}"
    )


def iso_date(value) -> str | None:
    if not value:
        return None
    return str(value)[:10]


def make_document_folder_name(document_key: str) -> str:
    safe_key = normalize_text(document_key).replace("/", "-").replace("\\", "-")
    return f"ALERTA-{safe_key}"


def make_pdf_copy_name(row: dict) -> str:
    published = iso_date(row.get("published_date")) or "sin-fecha"
    return f"{make_document_folder_name(row['document_key'])}__{published}__original.pdf"


def build_drive_folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def build_document_paths(year: int, document_folder_name: str) -> dict[str, str]:
    document_path = f"DIGEMID/01_ALERTAS/{year}/{document_folder_name}"
    return {
        "planned_document_path": document_path,
        "planned_original_folder_path": f"{document_path}/00_ORIGINAL",
        "planned_manifest_path": f"{document_path}/99_MANIFEST/manifest.json",
    }


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
    parent_id: str,
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


def ensure_folder_path(
    service,
    root_folder_id: str,
    year: int,
    document_folder_name: str,
    apply_changes: bool,
    operations: list[dict],
    planned_paths: dict[str, str],
) -> tuple[dict, dict]:
    alertas_folder = drive_ensure_folder(
        service,
        root_folder_id,
        "01_ALERTAS",
        apply_changes,
        operations,
        planned_path="DIGEMID/01_ALERTAS",
    )
    year_folder = drive_ensure_folder(
        service,
        alertas_folder["id"],
        str(year),
        apply_changes,
        operations,
        planned_path=f"DIGEMID/01_ALERTAS/{year}",
    )
    document_folder = drive_ensure_folder(
        service,
        year_folder["id"],
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
    target_folder_id: str,
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
    folder_id: str,
    file_name: str,
    payload: dict,
    apply_changes: bool,
    operations: list[dict],
    planned_path: str | None = None,
) -> dict:
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
            "size": len(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")),
            "pending_create": True,
        }

    existing = drive_find_child(service, folder_id, file_name)
    content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

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


def get_documents(supabase, source_type: str, limit: int | None, document_key: str | None) -> list[dict]:
    query = (
        supabase.table("digemid_documentos")
        .select(SUPABASE_SELECT_FIELDS)
        .eq("source_type", source_type)
        .eq("has_file", True)
        .not_.is_("drive_file_id", "null")
        .order("published_date", desc=False)
    )
    if document_key:
        query = query.eq("document_key", document_key)
    if limit:
        query = query.limit(limit)
    response = query.execute()
    return response.data or []


def get_existing_assets(supabase, document_id: str) -> list[dict]:
    response = (
        supabase.table("digemid_documento_assets")
        .select("id, asset_tipo, asset_subtipo, drive_file_id, file_name, metadatos")
        .eq("document_id", document_id)
        .execute()
    )
    return response.data or []


def find_matching_asset(existing_assets: list[dict], drive_file_id: str | None, asset_tipo: str, asset_subtipo: str) -> dict | None:
    for item in existing_assets:
        if (
            item.get("drive_file_id") == drive_file_id
            and item.get("asset_tipo") == asset_tipo
            and item.get("asset_subtipo") == asset_subtipo
        ):
            return item
    return None


def insert_asset_if_needed(
    supabase,
    existing_assets: list[dict],
    payload: dict,
    apply_changes: bool,
    operations: list[dict],
):
    existing = find_matching_asset(
        existing_assets,
        payload.get("drive_file_id"),
        payload.get("asset_tipo"),
        payload.get("asset_subtipo"),
    )
    if existing:
        operations.append(
            {
                "action": "reuse_asset",
                "asset_tipo": payload.get("asset_tipo"),
                "asset_subtipo": payload.get("asset_subtipo"),
                "drive_file_id": payload.get("drive_file_id"),
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

    response = supabase.table("digemid_documento_assets").insert(payload).execute()
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


def update_document_record(
    supabase,
    row: dict,
    payload: dict,
    apply_changes: bool,
    operations: list[dict],
):
    operations.append(
        {
            "action": "update_document",
            "document_id": row["id"],
            "document_key": row["document_key"],
            "drive_folder_id": payload.get("drive_folder_id"),
        }
    )
    if not apply_changes:
        return None

    return (
        supabase.table("digemid_documentos")
        .update(payload)
        .eq("id", row["id"])
        .execute()
    )


def build_manifest(
    row: dict,
    document_folder: dict,
    subfolders: dict[str, dict],
    original_drive_file: dict,
    copied_pdf: dict,
    created_at_iso: str,
) -> dict:
    return {
        "document_id": row["id"],
        "document_key": row["document_key"],
        "source_type": row.get("source_type"),
        "source_section": row.get("source_section"),
        "title": row.get("title"),
        "published_date": iso_date(row.get("published_date")),
        "file_url": row.get("file_url"),
        "original_drive_file_id": row.get("drive_file_id"),
        "copied_pdf_drive_file_id": copied_pdf.get("id"),
        "document_folder_id": document_folder.get("id"),
        "subfolder_ids": {name: data.get("id") for name, data in subfolders.items()},
        "created_at": created_at_iso,
        "migration_version": MIGRATION_VERSION,
    }


def make_pdf_asset_payload(
    row: dict,
    document_folder_id: str,
    original_drive_file: dict,
    copied_pdf: dict,
) -> dict:
    return {
        "document_id": row["id"],
        "page_id": None,
        "block_id": None,
        "asset_tipo": "pdf_original",
        "asset_subtipo": "copied_to_document_folder",
        "storage_backend": "google_drive",
        "storage_path": None,
        "drive_file_id": copied_pdf.get("id"),
        "source_url": copied_pdf.get("webViewLink"),
        "mime_type": "application/pdf",
        "file_name": copied_pdf.get("name"),
        "sha256": None,
        "size_bytes": int(copied_pdf["size"]) if copied_pdf.get("size") else None,
        "width_px": None,
        "height_px": None,
        "page_number": None,
        "bbox": None,
        "metadatos": {
            "original_drive_file_id": row.get("drive_file_id"),
            "document_folder_id": document_folder_id,
            "original_drive_file_url": original_drive_file.get("webViewLink"),
            "copied_drive_file_url": copied_pdf.get("webViewLink"),
            "migration_version": MIGRATION_VERSION,
        },
    }


def make_manifest_asset_payload(row: dict, manifest_file: dict) -> dict:
    return {
        "document_id": row["id"],
        "page_id": None,
        "block_id": None,
        "asset_tipo": "manifest",
        "asset_subtipo": "document_manifest",
        "storage_backend": "google_drive",
        "storage_path": None,
        "drive_file_id": manifest_file.get("id"),
        "source_url": manifest_file.get("webViewLink"),
        "mime_type": "application/json",
        "file_name": "manifest.json",
        "sha256": None,
        "size_bytes": int(manifest_file["size"]) if manifest_file.get("size") else None,
        "width_px": None,
        "height_px": None,
        "page_number": None,
        "bbox": None,
        "metadatos": {
            "migration_version": MIGRATION_VERSION,
        },
    }


def build_document_update_payload(
    row: dict,
    document_folder: dict,
    subfolders: dict[str, dict],
    copied_pdf: dict,
    manifest_file: dict,
    migrated_at_iso: str,
) -> dict:
    existing_raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    document_folder_id = document_folder.get("id")
    drive_structure_patch = {
        "drive_structure": {
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
            "copied_pdf_drive_file_id": copied_pdf.get("id"),
            "manifest_file_id": manifest_file.get("id"),
            "migrated_at": migrated_at_iso,
        }
    }
    merged_raw = deep_merge_dicts(existing_raw, drive_structure_patch)
    return {
        "drive_folder_id": document_folder_id or row.get("drive_folder_id"),
        "raw": merged_raw,
        "updated_at": migrated_at_iso,
    }


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
        f"# DIGEMID Drive Migration - {mode}",
        "",
        f"- Mode: `{mode}`",
        f"- Source type: `{report_payload['source_type']}`",
        f"- Documents considered: **{summary['documents_considered']}**",
        f"- Documents processed: **{summary['documents_processed']}**",
        f"- Documents skipped: **{summary['documents_skipped']}**",
        f"- Errors: **{summary['errors']}**",
        f"- Folders to create/reused: **{summary['folders_created_or_planned']} / {summary['folders_reused']}**",
        f"- Files to copy/reused: **{summary['pdf_copies_created_or_planned']} / {summary['pdf_copies_reused']}**",
        f"- Manifest files to create/update: **{summary['manifests_created_or_updated']}**",
        f"- Assets to insert: **{summary['assets_inserted_or_planned']}**",
        f"- Documents to update: **{summary['documents_updated_or_planned']}**",
        "",
        "## Documentos",
        "",
    ]

    for item in report_payload["documents"]:
        lines.append(f"### {item['document_key']}")
        lines.append(f"- Status: `{item['status']}`")
        lines.append(f"- Year: `{item.get('year')}`")
        lines.append(f"- Title: `{item.get('title') or ''}`")
        lines.append(f"- Original drive_file_id: `{item.get('original_drive_file_id')}`")
        lines.append(f"- Planned document path: `{item.get('planned_document_path') or ''}`")
        lines.append(f"- Planned PDF name: `{item.get('planned_pdf_name') or ''}`")
        lines.append(f"- Planned manifest path: `{item.get('planned_manifest_path') or ''}`")
        lines.append(f"- Document folder id: `{item.get('document_folder_id') or 'pending/dry-run'}`")
        if mode == "dry-run":
            lines.append(
                "- Nota: En dry-run los IDs reales de carpetas nuevas pueden aparecer como pending/null porque no se crean carpetas."
            )
        if item.get("error"):
            lines.append(f"- Error: `{item['error']}`")
        lines.append("- Planned operations:")
        for op in item.get("operations", []):
            op_parts = [op.get("action", "unknown")]
            for key in (
                "folder_name",
                "file_name",
                "asset_tipo",
                "asset_subtipo",
                "drive_file_id",
                "planned_path",
            ):
                if op.get(key):
                    op_parts.append(f"{key}={op[key]}")
            lines.append(f"  - {'; '.join(op_parts)}")
        lines.append("")

    target_path = DRY_RUN_REPORT_PATH if mode == "dry-run" else RESULT_REPORT_PATH
    target_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    other_path = RESULT_REPORT_PATH if mode == "dry-run" else DRY_RUN_REPORT_PATH
    if not other_path.exists():
        other_path.write_text(
            f"# DIGEMID Drive Migration - {mode}\n\nEste reporte no fue generado en esta ejecucion.\n",
            encoding="utf-8",
        )


def process_document(service, supabase, root_folder_id: str, row: dict, apply_changes: bool) -> dict:
    operations: list[dict] = []
    document_key = row["document_key"]
    year = infer_year(row)
    document_folder_name = make_document_folder_name(document_key)
    copied_pdf_name = make_pdf_copy_name(row)
    planned_paths = build_document_paths(year, document_folder_name)
    migrated_at_iso = datetime.now(timezone.utc).isoformat()

    original_drive_file = drive_get_file(service, row["drive_file_id"])
    document_folder, subfolders = ensure_folder_path(
        service,
        root_folder_id,
        year,
        document_folder_name,
        apply_changes,
        operations,
        planned_paths,
    )
    original_subfolder = subfolders["00_ORIGINAL"]
    manifest_subfolder = subfolders["99_MANIFEST"]

    copied_pdf = drive_copy_file(
        service,
        row["drive_file_id"],
        original_subfolder["id"],
        copied_pdf_name,
        apply_changes,
        operations,
        planned_path=f"{planned_paths['planned_original_folder_path']}/{copied_pdf_name}",
    )
    manifest_payload = build_manifest(
        row,
        document_folder,
        subfolders,
        original_drive_file,
        copied_pdf,
        migrated_at_iso,
    )
    manifest_file = drive_upsert_json_file(
        service,
        manifest_subfolder["id"],
        "manifest.json",
        manifest_payload,
        apply_changes,
        operations,
        planned_path=planned_paths["planned_manifest_path"],
    )

    existing_assets = get_existing_assets(supabase, row["id"])
    pdf_asset_payload = make_pdf_asset_payload(
        row,
        document_folder.get("id"),
        original_drive_file,
        copied_pdf,
    )
    manifest_asset_payload = make_manifest_asset_payload(row, manifest_file)
    insert_asset_if_needed(
        supabase,
        existing_assets,
        pdf_asset_payload,
        apply_changes,
        operations,
    )
    insert_asset_if_needed(
        supabase,
        existing_assets,
        manifest_asset_payload,
        apply_changes,
        operations,
    )

    document_update_payload = build_document_update_payload(
        row,
        document_folder,
        subfolders,
        copied_pdf,
        manifest_file,
        migrated_at_iso,
    )
    update_document_record(
        supabase,
        row,
        document_update_payload,
        apply_changes,
        operations,
    )

    return {
        "document_id": row["id"],
        "document_key": document_key,
        "year": year,
        "status": "applied" if apply_changes else "planned",
        "title": row.get("title"),
        "original_drive_file_id": row.get("drive_file_id"),
        "planned_document_path": planned_paths["planned_document_path"],
        "planned_original_folder_path": planned_paths["planned_original_folder_path"],
        "planned_manifest_path": planned_paths["planned_manifest_path"],
        "planned_pdf_name": copied_pdf_name,
        "document_folder_id": document_folder.get("id"),
        "copied_pdf_drive_file_id": copied_pdf.get("id"),
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
            elif action == "update_document":
                summary["documents_updated_or_planned"] += 1

    return summary


def main():
    args = parse_args()
    load_env()

    root_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not root_folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID")

    supabase = get_supabase()
    drive_service, temp_paths = get_drive_service()

    try:
        rows = get_documents(
            supabase=supabase,
            source_type=args.source_type,
            limit=args.limit,
            document_key=args.document_key,
        )
        logger.info("Documentos encontrados para migracion: %s", len(rows))

        results = []
        for row in rows:
            document_key = row.get("document_key")
            try:
                logger.info("Procesando %s", document_key)
                result = process_document(
                    service=drive_service,
                    supabase=supabase,
                    root_folder_id=root_folder_id,
                    row=row,
                    apply_changes=args.mode == "apply",
                )
            except Exception as exc:
                logger.exception("Error migrando %s", document_key)
                result = {
                    "document_id": row.get("id"),
                    "document_key": document_key,
                    "status": "error",
                    "error": str(exc),
                    "operations": [],
                }
            results.append(result)

        report_payload = {
            "mode": args.mode,
            "source_type": args.source_type,
            "document_key_filter": args.document_key,
            "limit": args.limit,
            "migration_version": MIGRATION_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": summarize_documents(results),
            "documents": results,
        }
        write_report_files(args.mode, report_payload)
        logger.info("Reportes generados en %s", REPORTS_DIR)
    finally:
        cleanup_temp_paths(temp_paths)


if __name__ == "__main__":
    main()
