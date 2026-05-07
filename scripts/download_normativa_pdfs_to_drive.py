import argparse
import hashlib
import json
import logging
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from supabase import create_client


SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"
MIGRATION_VERSION = "normativa_pdf_download_v1"
REPORTS_DIR = Path("reports")
DRY_RUN_REPORT_PATH = REPORTS_DIR / "normativa_pdf_download_dry_run.md"
RESULT_REPORT_PATH = REPORTS_DIR / "normativa_pdf_download_result.md"
RESULT_JSON_PATH = REPORTS_DIR / "normativa_pdf_download_result.json"
MIN_PDF_BYTES = 10 * 1024
SUPABASE_SELECT_FIELDS = (
    "id, document_key, titulo, source_url, has_file, drive_folder_id, "
    "drive_structure, raw, process_status, updated_at"
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
    parser.add_argument("--document-key", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.dry_run and args.apply:
        raise ValueError("No puedes usar --dry-run y --apply al mismo tiempo")

    if args.apply:
        args.mode = "apply"
    else:
        args.mode = "dry-run"
        args.dry_run = True

    return args


def normalize_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def escape_drive_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_drive_folder_url(folder_id: str | None) -> str | None:
    if not folder_id:
        return None
    return f"https://drive.google.com/drive/folders/{folder_id}"


def deep_merge_dicts(base: dict, patch: dict) -> dict:
    result = deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


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
            fields="files(id, name, mimeType, size, webViewLink, webContentLink, parents)",
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
) -> dict:
    existing = drive_find_child(service, parent_id, folder_name, DRIVE_FOLDER_MIME)
    if existing:
        operations.append(
            {
                "action": "reuse_folder",
                "folder_name": folder_name,
                "folder_id": existing.get("id"),
                "parent_id": parent_id,
            }
        )
        return existing

    operations.append(
        {
            "action": "create_folder",
            "folder_name": folder_name,
            "parent_id": parent_id,
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
            body={"name": folder_name, "mimeType": DRIVE_FOLDER_MIME, "parents": [parent_id]},
            fields="id, name, mimeType, parents, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def resolve_original_folder(
    service,
    row: dict,
    root_folder_id: str | None,
    apply_changes: bool,
    operations: list[dict],
) -> dict:
    drive_structure = row.get("drive_structure") if isinstance(row.get("drive_structure"), dict) else {}
    subfolders = drive_structure.get("subfolders") if isinstance(drive_structure.get("subfolders"), dict) else {}
    original_subfolder = subfolders.get("00_ORIGINAL") if isinstance(subfolders.get("00_ORIGINAL"), dict) else {}
    original_id = normalize_text(original_subfolder.get("id"))

    if original_id:
        operations.append(
            {
                "action": "reuse_folder",
                "folder_name": "00_ORIGINAL",
                "folder_id": original_id,
                "source": "drive_structure.subfolders.00_ORIGINAL.id",
            }
        )
        return {
            "id": original_id,
            "name": "00_ORIGINAL",
            "mimeType": DRIVE_FOLDER_MIME,
            "webViewLink": build_drive_folder_url(original_id),
        }

    document_folder_id = normalize_text(row.get("drive_folder_id")) or normalize_text(
        drive_structure.get("document_folder_id")
    )
    if not document_folder_id:
        if root_folder_id:
            operations.append(
                {
                    "action": "missing_document_folder_id",
                    "document_key": row.get("document_key"),
                    "root_folder_id": root_folder_id,
                }
            )
        raise ValueError(
            f"No se pudo resolver carpeta padre para 00_ORIGINAL en {row.get('document_key')}"
        )

    return drive_ensure_folder(
        service=service,
        parent_id=document_folder_id,
        folder_name="00_ORIGINAL",
        apply_changes=apply_changes,
        operations=operations,
    )


def is_pdf_content_type(content_type: str) -> bool:
    cleaned = content_type.split(";")[0].strip().lower()
    if not cleaned:
        return False
    return cleaned in {
        "application/pdf",
        "application/x-pdf",
        "application/octet-stream",
        "binary/octet-stream",
    } or cleaned.endswith("/pdf")


def download_and_validate_pdf(url: str) -> dict:
    response = requests.get(
        url,
        timeout=120,
        headers={"User-Agent": "RegAlert-DIGEMID-NormativaPDFDownloader/1.0"},
    )

    if response.status_code != 200:
        raise ValueError(f"HTTP inesperado: {response.status_code}")

    content_type = response.headers.get("Content-Type", "")
    if not is_pdf_content_type(content_type):
        raise ValueError(f"Content-Type no compatible con PDF: {content_type or 'vacio'}")

    content = response.content or b""
    size_bytes = len(content)
    if size_bytes <= MIN_PDF_BYTES:
        raise ValueError(f"PDF demasiado pequeno: {size_bytes} bytes")

    if not content.startswith(b"%PDF"):
        raise ValueError("Magic bytes invalidos: no inicia con %PDF")

    return {
        "content": content,
        "content_type": content_type,
        "size_bytes": size_bytes,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def drive_upsert_pdf(
    service,
    folder_id: str | None,
    file_name: str,
    file_bytes: bytes | None,
    apply_changes: bool,
    force: bool,
    operations: list[dict],
) -> dict:
    if not folder_id:
        raise ValueError("No se pudo resolver ID de carpeta 00_ORIGINAL")

    existing = drive_find_child(service, folder_id, file_name)
    if existing and not force:
        operations.append(
            {
                "action": "reuse_drive_file",
                "folder_id": folder_id,
                "drive_file_id": existing.get("id"),
                "file_name": file_name,
            }
        )
        return {**existing, "already_exists": True, "updated": False}

    if existing and force:
        operations.append(
            {
                "action": "update_drive_file",
                "folder_id": folder_id,
                "drive_file_id": existing.get("id"),
                "file_name": file_name,
            }
        )
        if not apply_changes:
            return {**existing, "already_exists": True, "updated": True, "pending_update": True}
        if file_bytes is None:
            raise ValueError("No hay contenido para actualizar archivo en Drive")

        media = MediaInMemoryUpload(file_bytes, mimetype="application/pdf", resumable=False)
        updated = (
            service.files()
            .update(
                fileId=existing["id"],
                media_body=media,
                fields="id, name, mimeType, size, webViewLink, webContentLink, parents",
                supportsAllDrives=True,
            )
            .execute()
        )
        return {**updated, "already_exists": True, "updated": True}

    operations.append(
        {
            "action": "upload_drive_file",
            "folder_id": folder_id,
            "file_name": file_name,
        }
    )
    if not apply_changes:
        return {
            "id": None,
            "name": file_name,
            "mimeType": "application/pdf",
            "size": None,
            "webViewLink": None,
            "webContentLink": None,
            "already_exists": False,
            "updated": False,
            "pending_upload": True,
        }
    if file_bytes is None:
        raise ValueError("No hay contenido para subir archivo a Drive")

    media = MediaInMemoryUpload(file_bytes, mimetype="application/pdf", resumable=False)
    created = (
        service.files()
        .create(
            body={"name": file_name, "parents": [folder_id]},
            media_body=media,
            fields="id, name, mimeType, size, webViewLink, webContentLink, parents",
            supportsAllDrives=True,
        )
        .execute()
    )
    return {**created, "already_exists": False, "updated": False}


def get_norma_by_document_key(supabase, document_key: str) -> dict:
    response = (
        supabase.table("digemid_normas")
        .select(SUPABASE_SELECT_FIELDS)
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_existing_assets(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table("digemid_norma_assets")
        .select("id, norma_id, asset_tipo, asset_subtipo, drive_file_id, file_name")
        .eq("norma_id", norma_id)
        .execute()
    )
    return response.data or []


def find_matching_asset(existing_assets: list[dict], norma_id: str, asset_tipo: str, asset_subtipo: str | None, file_name: str | None) -> dict | None:
    for item in existing_assets:
        if (
            item.get("norma_id") == norma_id
            and item.get("asset_tipo") == asset_tipo
            and (item.get("asset_subtipo") or None) == (asset_subtipo or None)
            and item.get("file_name") == file_name
        ):
            return item
    return None


def upsert_asset(
    supabase,
    existing_assets: list[dict],
    payload: dict,
    apply_changes: bool,
    force: bool,
    operations: list[dict],
) -> dict | None:
    existing = find_matching_asset(
        existing_assets=existing_assets,
        norma_id=payload.get("norma_id"),
        asset_tipo=payload.get("asset_tipo"),
        asset_subtipo=payload.get("asset_subtipo"),
        file_name=payload.get("file_name"),
    )
    if existing:
        operations.append(
            {
                "action": "reuse_asset",
                "asset_id": existing.get("id"),
                "asset_tipo": payload.get("asset_tipo"),
                "asset_subtipo": payload.get("asset_subtipo"),
                "file_name": payload.get("file_name"),
            }
        )
        if apply_changes and force:
            update_payload = {
                "drive_file_id": payload.get("drive_file_id"),
                "source_url": payload.get("source_url"),
                "mime_type": payload.get("mime_type"),
                "file_ext": payload.get("file_ext"),
                "file_size_bytes": payload.get("file_size_bytes"),
                "bbox": payload.get("bbox"),
                "text_hint": payload.get("text_hint"),
                "metadata": payload.get("metadata"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            supabase.table("digemid_norma_assets").update(update_payload).eq("id", existing["id"]).execute()
            return {**existing, **update_payload}
        return existing

    operations.append(
        {
            "action": "insert_asset",
            "asset_tipo": payload.get("asset_tipo"),
            "asset_subtipo": payload.get("asset_subtipo"),
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


def normalize_official_files(raw_value) -> list[dict]:
    if not isinstance(raw_value, dict):
        return []
    official_files = raw_value.get("official_files")
    if not isinstance(official_files, list):
        return []
    normalized: list[dict] = []
    for item in official_files:
        normalized.append(item if isinstance(item, dict) else {"raw_value": item})
    return normalized


def make_asset_payload(norma_row: dict, official_item: dict, drive_file: dict, downloaded: dict, now_iso: str) -> dict:
    source_url = normalize_text(official_item.get("url")) or normalize_text(official_item.get("source_url"))
    file_name = normalize_text(official_item.get("file_name")) or normalize_text(drive_file.get("name"))
    asset_tipo = normalize_text(official_item.get("asset_tipo")) or "pdf_original"
    asset_subtipo = normalize_text(official_item.get("asset_subtipo")) or None
    source_page = official_item.get("source_page") or norma_row.get("source_url")
    source = official_item.get("source") or "digemid_normativa_official_files"
    title = official_item.get("title") or norma_row.get("titulo")

    return {
        "norma_id": norma_row["id"],
        "page_id": None,
        "asset_tipo": asset_tipo,
        "asset_subtipo": asset_subtipo,
        "storage_backend": "google_drive",
        "storage_path": None,
        "drive_file_id": drive_file.get("id"),
        "source_url": source_url,
        "mime_type": "application/pdf",
        "file_name": file_name,
        "file_ext": "pdf",
        "file_size_bytes": downloaded.get("size_bytes"),
        "page_number": None,
        "bbox": {},
        "text_hint": None,
        "metadata": {
            "source_page": source_page,
            "source": source,
            "title": title,
            "downloaded_at": now_iso,
            "sha256": downloaded.get("sha256"),
            "migration_version": MIGRATION_VERSION,
        },
    }


def ensure_reports_dir():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def write_report_files(mode: str, report_payload: dict):
    ensure_reports_dir()
    RESULT_JSON_PATH.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = report_payload.get("summary", {})
    lines = [
        f"# DIGEMID Normativa PDF Download - {mode}",
        "",
        f"- Mode: `{mode}`",
        f"- Document key: `{report_payload.get('document_key')}`",
        f"- Force: `{report_payload.get('force')}`",
        f"- Files total: **{summary.get('files_total', 0)}**",
        f"- Files selected: **{summary.get('files_selected', 0)}**",
        f"- Files downloaded: **{summary.get('files_downloaded', 0)}**",
        f"- Drive reused: **{summary.get('drive_reused', 0)}**",
        f"- Drive uploaded/updated: **{summary.get('drive_written', 0)}**",
        f"- Assets inserted/reused: **{summary.get('assets_ok', 0)}**",
        f"- Errors: **{summary.get('errors', 0)}**",
        "",
        "## Archivos oficiales",
        "",
    ]

    for item in report_payload.get("files", []):
        lines.append(f"### {item.get('file_name') or '(sin nombre)'}")
        lines.append(f"- Status in: `{item.get('status_in')}`")
        lines.append(f"- Status out: `{item.get('status_out')}`")
        lines.append(f"- Selected: `{item.get('selected')}`")
        lines.append(f"- URL: `{item.get('url')}`")
        lines.append(f"- Drive file id: `{item.get('drive_file_id')}`")
        lines.append(f"- Size bytes: `{item.get('file_size_bytes')}`")
        lines.append(f"- SHA256: `{item.get('sha256')}`")
        if item.get("error"):
            lines.append(f"- Error: `{item.get('error')}`")
        lines.append("")

    lines.append("## Operaciones")
    lines.append("")
    for op in report_payload.get("operations", []):
        op_parts = [op.get("action", "unknown")]
        for key in (
            "folder_name",
            "folder_id",
            "file_name",
            "drive_file_id",
            "asset_tipo",
            "asset_subtipo",
        ):
            if op.get(key):
                op_parts.append(f"{key}={op[key]}")
        lines.append(f"- {'; '.join(op_parts)}")

    target_path = DRY_RUN_REPORT_PATH if mode == "dry-run" else RESULT_REPORT_PATH
    target_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    other_path = RESULT_REPORT_PATH if mode == "dry-run" else DRY_RUN_REPORT_PATH
    if not other_path.exists():
        other_path.write_text(
            f"# DIGEMID Normativa PDF Download - {mode}\n\nEste reporte no fue generado en esta ejecucion.\n",
            encoding="utf-8",
        )


def summarize(file_results: list[dict], operations: list[dict]) -> dict:
    summary = {
        "files_total": len(file_results),
        "files_selected": 0,
        "files_downloaded": 0,
        "drive_reused": 0,
        "drive_written": 0,
        "assets_ok": 0,
        "errors": 0,
    }
    for item in file_results:
        if item.get("selected"):
            summary["files_selected"] += 1
        if item.get("status_out") == "downloaded":
            summary["files_downloaded"] += 1
        if item.get("asset_registered"):
            summary["assets_ok"] += 1
        if item.get("error"):
            summary["errors"] += 1

    for op in operations:
        action = op.get("action")
        if action == "reuse_drive_file":
            summary["drive_reused"] += 1
        elif action in {"upload_drive_file", "update_drive_file"}:
            summary["drive_written"] += 1

    return summary


def process_norma(
    service,
    supabase,
    root_folder_id: str | None,
    row: dict,
    apply_changes: bool,
    force: bool,
) -> dict:
    operations: list[dict] = []
    file_results: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    official_files = normalize_official_files(raw)
    updated_official_files = deepcopy(official_files)
    existing_assets = get_existing_assets(supabase, row["id"])
    original_folder = resolve_original_folder(
        service=service,
        row=row,
        root_folder_id=root_folder_id,
        apply_changes=apply_changes,
        operations=operations,
    )

    assets_registered_count = 0
    for index, original_item in enumerate(official_files):
        item = original_item if isinstance(original_item, dict) else {}
        status_in = normalize_text(item.get("status")) or "pending_download"
        selected = force or status_in == "pending_download"
        file_name = normalize_text(item.get("file_name"))
        url = normalize_text(item.get("url"))
        asset_tipo = normalize_text(item.get("asset_tipo")) or "pdf_original"
        asset_subtipo = normalize_text(item.get("asset_subtipo")) or None

        item_result = {
            "index": index,
            "file_name": file_name or None,
            "url": url or None,
            "status_in": status_in,
            "status_out": status_in,
            "selected": selected,
            "drive_file_id": item.get("drive_file_id"),
            "file_size_bytes": item.get("file_size_bytes"),
            "sha256": item.get("sha256"),
            "asset_registered": False,
            "error": None,
        }

        if not selected:
            file_results.append(item_result)
            continue

        if not url:
            item_result["status_out"] = "download_error"
            item_result["error"] = "official_files.url vacio"
            updated_official_files[index]["status"] = "download_error"
            updated_official_files[index]["last_error"] = item_result["error"]
            file_results.append(item_result)
            continue

        if not file_name:
            file_name = f"{row.get('document_key')}_official_{index + 1}.pdf"
            item_result["file_name"] = file_name

        try:
            downloaded = None
            if apply_changes:
                downloaded = download_and_validate_pdf(url)

            drive_file = drive_upsert_pdf(
                service=service,
                folder_id=original_folder.get("id"),
                file_name=file_name,
                file_bytes=downloaded.get("content") if downloaded else None,
                apply_changes=apply_changes,
                force=force,
                operations=operations,
            )

            if apply_changes:
                asset_payload = make_asset_payload(
                    norma_row=row,
                    official_item={
                        **item,
                        "file_name": file_name,
                        "asset_tipo": asset_tipo,
                        "asset_subtipo": asset_subtipo,
                        "url": url,
                    },
                    drive_file=drive_file,
                    downloaded=downloaded,
                    now_iso=now_iso,
                )
                asset = upsert_asset(
                    supabase=supabase,
                    existing_assets=existing_assets,
                    payload=asset_payload,
                    apply_changes=True,
                    force=force,
                    operations=operations,
                )
                if asset:
                    assets_registered_count += 1
                    item_result["asset_registered"] = True

                updated_official_files[index]["status"] = "downloaded"
                updated_official_files[index]["drive_file_id"] = drive_file.get("id")
                updated_official_files[index]["file_size_bytes"] = downloaded.get("size_bytes")
                updated_official_files[index]["sha256"] = downloaded.get("sha256")
                updated_official_files[index]["downloaded_at"] = now_iso
                updated_official_files[index].pop("last_error", None)

                item_result["status_out"] = "downloaded"
                item_result["drive_file_id"] = drive_file.get("id")
                item_result["file_size_bytes"] = downloaded.get("size_bytes")
                item_result["sha256"] = downloaded.get("sha256")
            else:
                planned_status = "would_download"
                if force:
                    planned_status = "would_force_download"
                item_result["status_out"] = planned_status
                item_result["drive_file_id"] = drive_file.get("id")
        except Exception as exc:
            item_result["status_out"] = "download_error"
            item_result["error"] = str(exc)
            if apply_changes:
                updated_official_files[index]["status"] = "download_error"
                updated_official_files[index]["last_error"] = str(exc)

        file_results.append(item_result)

    all_downloaded = bool(updated_official_files) and all(
        normalize_text(item.get("status")) == "downloaded" for item in updated_official_files
    )
    has_registered_pdf = assets_registered_count > 0
    new_drive_structure = deep_merge_dicts(
        row.get("drive_structure") if isinstance(row.get("drive_structure"), dict) else {},
        {
            "migration_version": MIGRATION_VERSION,
            "subfolders": {
                "00_ORIGINAL": {
                    "id": original_folder.get("id"),
                    "url": build_drive_folder_url(original_folder.get("id")),
                }
            },
            "official_files": updated_official_files if apply_changes else official_files,
            "official_files_updated_at": now_iso,
        },
    )

    norma_update_payload = {
        "drive_structure": new_drive_structure,
        "updated_at": now_iso,
    }
    if apply_changes and (row.get("has_file") or has_registered_pdf):
        norma_update_payload["has_file"] = True
    if apply_changes and all_downloaded:
        norma_update_payload["process_status"] = "pdf_downloaded"

    operations.append(
        {
            "action": "update_norma",
            "norma_id": row["id"],
            "document_key": row.get("document_key"),
            "has_file": norma_update_payload.get("has_file"),
            "process_status": norma_update_payload.get("process_status"),
        }
    )

    if apply_changes:
        supabase.table("digemid_normas").update(norma_update_payload).eq("id", row["id"]).execute()

    return {
        "norma_id": row["id"],
        "document_key": row.get("document_key"),
        "title": row.get("titulo"),
        "mode": "apply" if apply_changes else "dry-run",
        "force": force,
        "original_folder_id": original_folder.get("id"),
        "original_folder_url": build_drive_folder_url(original_folder.get("id")),
        "summary": summarize(file_results, operations),
        "files": file_results,
        "operations": operations,
    }


def main():
    args = parse_args()
    load_env()

    root_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not root_folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID")

    supabase = get_supabase()
    drive_service = get_drive_service()
    row = get_norma_by_document_key(supabase, args.document_key)

    result = process_norma(
        service=drive_service,
        supabase=supabase,
        root_folder_id=root_folder_id,
        row=row,
        apply_changes=args.mode == "apply",
        force=args.force,
    )

    report_payload = {
        "mode": args.mode,
        "document_key": args.document_key,
        "force": args.force,
        "migration_version": MIGRATION_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    write_report_files(args.mode, report_payload)
    logger.info("Reportes generados en %s", REPORTS_DIR)


if __name__ == "__main__":
    main()
