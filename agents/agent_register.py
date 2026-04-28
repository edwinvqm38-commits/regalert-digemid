import logging
import os

from supabase import Client, create_client

logger = logging.getLogger(__name__)


class RegisterAgent:
    """Agente responsable de deduplicar y registrar documentos en Supabase."""

    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError(
                "Faltan variables de entorno SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY"
            )

        self.supabase: Client = create_client(url, key)
        self.table_name = "digemid_documentos"

    def filter_new_documents(self, docs: list[dict]) -> list[dict]:
        """Consulta Supabase en lote y retorna solo documentos nuevos."""
        if not docs:
            return []

        keys = [doc["document_key"] for doc in docs if doc.get("document_key")]

        if not keys:
            return []

        response = (
            self.supabase
            .table(self.table_name)
            .select("document_key")
            .in_("document_key", keys)
            .execute()
        )

        existing_keys = {row["document_key"] for row in (response.data or [])}

        new_docs = [
            doc for doc in docs
            if doc.get("document_key") not in existing_keys
        ]

        logger.info("Documentos existentes en Supabase: %s", len(existing_keys))
        logger.info("Documentos nuevos para registrar: %s", len(new_docs))

        return new_docs

    def process_and_save(self, docs: list[dict]) -> list[dict]:
        """Filtra documentos existentes y registra solo novedades."""
        new_docs = self.filter_new_documents(docs)

        if not new_docs:
            return []

        allowed_fields = {
            "source_type",
            "source_section",
            "document_key",
            "title",
            "document_slug",
            "detail_url",
            "file_url",
            "file_name",
            "published_date",
            "published_date_display",
            "has_file",
            "process_status",
            "raw",
        }

        clean_docs = [
            {key: value for key, value in doc.items() if key in allowed_fields}
            for doc in new_docs
        ]

        response = (
            self.supabase
            .table(self.table_name)
            .upsert(clean_docs, on_conflict="document_key")
            .execute()
        )

        logger.info("Documentos registrados en Supabase: %s", len(clean_docs))

        return response.data or clean_docs