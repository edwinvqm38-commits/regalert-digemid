import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client

# Permite importar agentes desde la raíz del proyecto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.agent_detail import DetailAgent


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


BAD_TITLES = {
    "",
    "read more...",
    "leer más",
    "leer mas",
    "sin dato",
    "sin título",
    "sin titulo",
    "none",
    "null",
}


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env")

    return create_client(url, key)


def is_bad_title(title: str | None) -> bool:
    if title is None:
        return True

    return title.strip().lower() in BAD_TITLES


def is_incomplete(row: dict) -> bool:
    """
    Un registro está incompleto si le falta título real, fecha o PDF.
    """
    return (
        is_bad_title(row.get("title"))
        or not row.get("published_date")
        or not row.get("published_date_display")
        or not row.get("file_url")
        or row.get("has_file") is not True
    )


def get_alerts_to_enrich(supabase, limit: int) -> list[dict]:
    """
    Trae alertas recientes desde Supabase y filtra localmente las incompletas.
    No crea registros nuevos. Solo trabaja con las alertas ya existentes.
    """
    response = (
        supabase
        .table("digemid_documentos")
        .select(
            "id, source_type, source_section, document_key, title, detail_url, "
            "published_date, published_date_display, file_url, file_name, "
            "file_ext, has_file, mime_type, raw, process_status, created_at"
        )
        .eq("source_type", "alerta")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )

    rows = response.data or []

    rows = [
        row for row in rows
        if row.get("detail_url") and is_incomplete(row)
    ]

    return rows


def build_update_payload(row: dict, detail_data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    payload = {
        "last_seen_at": now,
        "updated_at": now,
        "process_status": "metadata_enriched",
        "process_message": "Metadata enriquecida desde detail_url",
        "source_site": "DIGEMID",
        "url_canonica": row.get("detail_url"),
    }

    current_title = row.get("title")
    detected_title = detail_data.get("title")

    if detected_title and is_bad_title(current_title):
        payload["title"] = detected_title

    if detail_data.get("published_date"):
        payload["published_date"] = detail_data["published_date"]

    if detail_data.get("published_date_display"):
        payload["published_date_display"] = detail_data["published_date_display"]

    if detail_data.get("file_url"):
        payload["file_url"] = detail_data["file_url"]
        payload["file_name"] = detail_data.get("file_name")
        payload["file_ext"] = detail_data.get("file_ext") or "pdf"
        payload["has_file"] = True
        payload["mime_type"] = detail_data.get("mime_type") or "application/pdf"

    old_raw = row.get("raw") or {}

    payload["raw"] = {
        **old_raw,
        "detail_enrichment": detail_data.get("raw", {}),
    }

    return payload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()

    supabase = get_supabase()
    detail_agent = DetailAgent()

    rows = get_alerts_to_enrich(supabase, args.limit)

    logger.info("Registros incompletos encontrados: %s", len(rows))

    updated_count = 0
    error_count = 0

    for row in rows:
        document_key = row.get("document_key")
        detail_url = row.get("detail_url")

        try:
            logger.info("Procesando alerta %s", document_key)

            detail_data = detail_agent.extract(detail_url)
            payload = build_update_payload(row, detail_data)

            if args.dry_run:
                logger.info("DRY RUN %s", document_key)
                logger.info("Payload propuesto: %s", payload)
                continue

            (
                supabase
                .table("digemid_documentos")
                .update(payload)
                .eq("id", row["id"])
                .execute()
            )

            updated_count += 1
            logger.info("Actualizado %s", document_key)

        except Exception as error:
            error_count += 1
            logger.exception("Error procesando %s: %s", document_key, error)

            if not args.dry_run:
                now = datetime.now(timezone.utc).isoformat()

                (
                    supabase
                    .table("digemid_documentos")
                    .update({
                        "process_status": "metadata_error",
                        "process_error": str(error),
                        "updated_at": now,
                    })
                    .eq("id", row["id"])
                    .execute()
                )

    logger.info(
        "Finalizado. Actualizados: %s | Errores: %s",
        updated_count,
        error_count,
    )


if __name__ == "__main__":
    main()