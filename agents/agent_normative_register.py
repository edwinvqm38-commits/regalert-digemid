import logging
import os

from supabase import Client, create_client

from agents.agent_utils import utc_now_iso

logger = logging.getLogger(__name__)


class NormativeRegisterAgent:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError(
                "Faltan variables de entorno SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY"
            )

        self.supabase: Client = create_client(url, key)
        self.table_name = "digemid_documentos"
        self.allowed_fields = {
            "source_type",
            "source_section",
            "source_page",
            "source_site",
            "document_key",
            "title",
            "document_slug",
            "published_date",
            "published_date_display",
            "detail_url",
            "has_file",
            "file_url",
            "file_name",
            "file_ext",
            "discovery_mode",
            "process_status",
            "process_message",
            "first_seen_at",
            "last_seen_at",
            "updated_at",
            "raw",
        }

    def _fetch_existing_documents(self, keys: list[str]) -> dict[str, dict]:
        if not keys:
            return {}

        response = (
            self.supabase
            .table(self.table_name)
            .select(
                "document_key, first_seen_at, detail_url, file_url, file_name, file_ext, raw"
            )
            .eq("source_type", "normativa")
            .in_("document_key", keys)
            .execute()
        )

        rows = response.data or []
        return {
            row["document_key"]: row
            for row in rows
            if row.get("document_key")
        }

    def _build_payload(self, docs: list[dict], existing_by_key: dict[str, dict]) -> list[dict]:
        now = utc_now_iso()
        payloads: list[dict] = []

        for doc in docs:
            key = doc["document_key"]
            existing = existing_by_key.get(key) or {}

            merged_raw = dict(existing.get("raw") or {})
            merged_raw.update(doc.get("raw") or {})

            payload = dict(doc)
            payload["first_seen_at"] = existing.get("first_seen_at") or now
            payload["last_seen_at"] = now
            payload["updated_at"] = now
            payload["detail_url"] = doc.get("detail_url") or existing.get("detail_url")
            payload["file_url"] = doc.get("file_url") or existing.get("file_url")
            payload["file_name"] = doc.get("file_name") or existing.get("file_name")
            payload["file_ext"] = doc.get("file_ext") or existing.get("file_ext")
            payload["raw"] = merged_raw

            clean_payload = {
                field: value
                for field, value in payload.items()
                if field in self.allowed_fields
            }
            payloads.append(clean_payload)

        return payloads

    def process_and_save(self, docs: list[dict]) -> dict:
        if not docs:
            logger.info("No hay documentos normativos para registrar.")
            return {
                "found": 0,
                "new": 0,
                "updated": 0,
                "saved": 0,
            }

        keys = [doc["document_key"] for doc in docs if doc.get("document_key")]
        existing_by_key = self._fetch_existing_documents(keys)
        payloads = self._build_payload(docs, existing_by_key)

        response = (
            self.supabase
            .table(self.table_name)
            .upsert(payloads, on_conflict="source_type,document_key")
            .execute()
        )

        saved_rows = response.data or payloads
        existing_keys = set(existing_by_key.keys())
        new_count = sum(1 for doc in docs if doc["document_key"] not in existing_keys)
        updated_count = len(docs) - new_count

        logger.info("Registros encontrados: %s", len(docs))
        logger.info("Registros nuevos: %s", new_count)
        logger.info("Registros actualizados: %s", updated_count)
        logger.info("Registros guardados en Supabase: %s", len(saved_rows))

        return {
            "found": len(docs),
            "new": new_count,
            "updated": updated_count,
            "saved": len(saved_rows),
        }
