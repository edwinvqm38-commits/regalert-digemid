import argparse
import logging
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client
from storage3.types import CreateOrUpdateBucketOptions


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

BUCKET_NAME = "digemid-documentos"
TABLE_NAME = "digemid_documentos"
DELAY_BETWEEN_DESCARGAS_SEGUNDOS = 2.0
MAX_REINTENTOS_429 = 3


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def ensure_bucket(supabase) -> None:
    """Crea el bucket privado de respaldo si aún no existe."""
    existing = {bucket.id for bucket in supabase.storage.list_buckets()}

    if BUCKET_NAME in existing:
        return

    supabase.storage.create_bucket(
        BUCKET_NAME,
        options=CreateOrUpdateBucketOptions(
            public=False,
            allowed_mime_types=["application/pdf"],
        ),
    )
    logger.info("Bucket de respaldo creado: %s", BUCKET_NAME)


def get_pending_documents(supabase, limit: int) -> list[dict]:
    response = (
        supabase
        .table(TABLE_NAME)
        .select("id, document_key, file_url, file_name")
        .eq("source_type", "alerta")
        .eq("has_file", True)
        .not_.is_("file_url", "null")
        .is_("file_storage_path", "null")
        .order("published_date", desc=True)
        .limit(limit)
        .execute()
    )

    return response.data or []


def sanitize_file_name(document_key: str | None, file_name: str | None) -> str:
    base_name = file_name or f"{document_key or 'documento'}.pdf"
    safe_name = base_name.replace("/", "-").replace("\\", "-").strip()

    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"

    return safe_name


def download_pdf(file_url: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    for intento in range(1, MAX_REINTENTOS_429 + 1):
        response = requests.get(
            file_url,
            timeout=90,
            headers={"User-Agent": "RegAlert-DIGEMID-StorageBackup/1.0"},
        )

        if response.status_code == 429 and intento < MAX_REINTENTOS_429:
            espera = float(response.headers.get("Retry-After", 10 * intento))
            logger.warning(
                "429 al descargar %s (intento %s/%s). Esperando %.1fs antes de reintentar.",
                file_url,
                intento,
                MAX_REINTENTOS_429,
                espera,
            )
            time.sleep(espera)
            continue

        response.raise_for_status()
        local_path.write_bytes(response.content)
        return local_path

    raise RuntimeError(f"No se pudo descargar {file_url} tras {MAX_REINTENTOS_429} intentos (429)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    if not args.dry_run:
        ensure_bucket(supabase)

    documents = get_pending_documents(supabase, args.limit)
    logger.info("Documentos pendientes de respaldo: %s", len(documents))

    backed_up = 0
    errors = 0

    for index, doc in enumerate(documents):
        if index > 0:
            time.sleep(DELAY_BETWEEN_DESCARGAS_SEGUNDOS)

        document_key = doc.get("document_key")
        file_url = doc.get("file_url")
        file_name = sanitize_file_name(document_key, doc.get("file_name"))
        object_path = f"alertas/{document_key}/{file_name}"

        if args.dry_run:
            logger.info("[dry-run] Respaldaría %s -> %s", document_key, object_path)
            continue

        local_path = Path("tmp") / "storage_backup" / file_name

        try:
            download_pdf(file_url, local_path)

            with local_path.open("rb") as file_obj:
                supabase.storage.from_(BUCKET_NAME).upload(
                    object_path,
                    file_obj,
                    file_options={"content-type": "application/pdf", "upsert": "true"},
                )

            supabase.table(TABLE_NAME).update(
                {"file_storage_path": object_path}
            ).eq("id", doc["id"]).execute()

            logger.info("Respaldado %s -> %s", document_key, object_path)
            backed_up += 1
        except Exception:
            logger.exception("Error respaldando %s", document_key)
            errors += 1
        finally:
            local_path.unlink(missing_ok=True)

    logger.info("Finalizado. Respaldados: %s | Errores: %s", backed_up, errors)


if __name__ == "__main__":
    main()
