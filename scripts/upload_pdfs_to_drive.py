import argparse
import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = ["https://www.googleapis.com/auth/drive"]

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
    token_path = os.getenv("GOOGLE_OAUTH_TOKEN_PATH")

    if not token_path:
        raise ValueError("Falta GOOGLE_OAUTH_TOKEN_PATH en .env")

    token_path = Path(token_path)

    if not token_path.exists():
        raise FileNotFoundError(
            f"No existe el token OAuth: {token_path}. "
            "Primero ejecuta scripts/test_drive_oauth_upload.py"
        )

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise ValueError("El token OAuth no es válido. Vuelve a autorizar con test_drive_oauth_upload.py")

    return build("drive", "v3", credentials=creds)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            h.update(chunk)

    return h.hexdigest()


def build_drive_download_url(file_id: str) -> str:
    return f"https://drive.google.com/uc?id={file_id}&export=download"


def make_drive_file_name(row: dict) -> str:
    document_key = row.get("document_key") or "SIN-CODIGO"
    safe_key = document_key.replace("/", "-").replace("\\", "-").strip()
    return f"DIGEMID_ALERTA_{safe_key}.pdf"


def find_file_in_drive(service, folder_id: str, file_name: str):
    safe_name = file_name.replace("'", "\\'")

    query = (
        f"name = '{safe_name}' "
        f"and '{folder_id}' in parents "
        f"and trashed = false"
    )

    result = (
        service.files()
        .list(
            q=query,
            fields="files(id, name, webViewLink, webContentLink)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    files = result.get("files", [])

    return files[0] if files else None


def get_pending_pdfs(supabase, limit: int):
    response = (
        supabase
        .table("digemid_documentos")
        .select(
            "id, document_key, title, file_url, file_name, file_ext, "
            "mime_type, drive_file_id, drive_file_url, drive_download_url, "
            "drive_folder_id, has_file, process_status"
        )
        .eq("source_type", "alerta")
        .eq("has_file", True)
        .not_.is_("file_url", "null")
        .is_("drive_file_id", "null")
        .order("published_date", desc=True)
        .limit(limit)
        .execute()
    )

    return response.data or []


def download_pdf(file_url: str, file_name: str, temp_dir: Path) -> Path:
    temp_dir.mkdir(parents=True, exist_ok=True)

    local_path = temp_dir / file_name

    logger.info("Descargando PDF: %s", file_url)

    response = requests.get(
        file_url,
        timeout=60,
        headers={
            "User-Agent": "RegAlert-DIGEMID-PDFDownloader/1.0",
        },
    )

    response.raise_for_status()
    local_path.write_bytes(response.content)

    return local_path


def upload_to_drive(service, folder_id: str, local_path: Path, drive_file_name: str, mime_type: str):
    existing = find_file_in_drive(service, folder_id, drive_file_name)

    if existing:
        existing["already_exists"] = True
        return existing

    metadata = {
        "name": drive_file_name,
        "parents": [folder_id],
    }

    media = MediaFileUpload(
        str(local_path),
        mimetype=mime_type,
        resumable=False,
    )

    uploaded = (
        service.files()
        .create(
            body=metadata,
            media_body=media,
            fields="id, name, webViewLink, webContentLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    uploaded["already_exists"] = False

    return uploaded


def update_supabase_after_upload(supabase, row: dict, uploaded: dict, local_path: Path, folder_id: str):
    now = datetime.now(timezone.utc).isoformat()
    file_id = uploaded.get("id")

    payload = {
        "drive_file_id": file_id,
        "drive_file_url": uploaded.get("webViewLink"),
        "drive_download_url": uploaded.get("webContentLink") or build_drive_download_url(file_id),
        "drive_folder_id": folder_id,
        "uploaded_to_drive_at": now,
        "updated_at": now,
        "file_size_bytes": local_path.stat().st_size,
        "file_hash": sha256_file(local_path),
        "process_status": "pdf_uploaded_to_drive",
        "process_message": (
            "PDF ya existía en Drive y fue vinculado a Supabase"
            if uploaded.get("already_exists")
            else "PDF descargado y subido a Drive"
        ),
    }

    (
        supabase
        .table("digemid_documentos")
        .update(payload)
        .eq("id", row["id"])
        .execute()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID en .env")

    supabase = get_supabase()
    drive_service = get_drive_service()

    rows = get_pending_pdfs(supabase, args.limit)

    logger.info("PDFs pendientes de subir a Drive: %s", len(rows))

    temp_dir = Path("tmp") / "pdfs"

    uploaded_count = 0
    skipped_count = 0
    error_count = 0

    for row in rows:
        document_key = row.get("document_key")
        file_url = row.get("file_url")
        source_file_name = row.get("file_name") or f"{document_key}.pdf"
        drive_file_name = make_drive_file_name(row)
        mime_type = row.get("mime_type") or "application/pdf"

        try:
            logger.info("Procesando %s", document_key)
            logger.info("Nombre Drive objetivo: %s", drive_file_name)

            if row.get("drive_file_id"):
                logger.info("Saltado %s: ya tiene drive_file_id", document_key)
                skipped_count += 1
                continue

            if not file_url:
                logger.info("Saltado %s: no tiene file_url", document_key)
                skipped_count += 1
                continue

            if args.dry_run:
                logger.info("DRY RUN subiría %s desde %s", drive_file_name, file_url)
                continue

            local_path = download_pdf(file_url, source_file_name, temp_dir)

            uploaded = upload_to_drive(
                service=drive_service,
                folder_id=folder_id,
                local_path=local_path,
                drive_file_name=drive_file_name,
                mime_type=mime_type,
            )

            update_supabase_after_upload(
                supabase=supabase,
                row=row,
                uploaded=uploaded,
                local_path=local_path,
                folder_id=folder_id,
            )

            uploaded_count += 1

            logger.info(
                "Registrado en Supabase %s | Drive ID: %s | Existía: %s",
                document_key,
                uploaded.get("id"),
                uploaded.get("already_exists"),
            )

        except Exception as error:
            error_count += 1
            logger.exception("Error procesando %s: %s", document_key, error)

            if not args.dry_run:
                now = datetime.now(timezone.utc).isoformat()

                (
                    supabase
                    .table("digemid_documentos")
                    .update({
                        "process_status": "pdf_drive_error",
                        "process_error": str(error),
                        "updated_at": now,
                    })
                    .eq("id", row["id"])
                    .execute()
                )

    logger.info(
        "Finalizado. Subidos/registrados: %s | Saltados: %s | Errores: %s",
        uploaded_count,
        skipped_count,
        error_count,
    )


if __name__ == "__main__":
    main()
