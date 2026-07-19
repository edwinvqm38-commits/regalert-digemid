import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

TABLE_NAME = "digemid_documentos"


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def get_candidates(supabase, cutoff_date: date, limit: int) -> list[dict]:
    response = (
        supabase
        .table(TABLE_NAME)
        .select("id, document_key, published_date")
        .eq("source_type", "alerta")
        .is_("archived_at", "null")
        .lt("published_date", cutoff_date.isoformat())
        .order("published_date", desc=False)
        .limit(limit)
        .execute()
    )

    return response.data or []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--older-than-months", type=int, default=12)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    cutoff_date = date.today() - timedelta(days=args.older_than_months * 30)
    logger.info("Archivando alertas publicadas antes de: %s", cutoff_date.isoformat())

    candidates = get_candidates(supabase, cutoff_date, args.limit)
    logger.info("Alertas candidatas a archivar: %s", len(candidates))

    if not args.apply:
        for doc in candidates:
            logger.info(
                "[dry-run] Archivaría %s (publicada %s)",
                doc.get("document_key"),
                doc.get("published_date"),
            )
        logger.info("Dry-run finalizado. Usa --apply para archivar de verdad.")
        return

    archived = 0
    archived_at = datetime.now(timezone.utc).isoformat()

    for doc in candidates:
        supabase.table(TABLE_NAME).update(
            {"archived_at": archived_at}
        ).eq("id", doc["id"]).execute()
        archived += 1

    logger.info("Finalizado. Alertas archivadas: %s", archived)


if __name__ == "__main__":
    main()
