import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


BASE_DIR = Path(__file__).resolve().parents[1]
SEED_PATH = BASE_DIR / "data" / "normativa_seed_minima.json"


SAFE_UPDATE_FIELDS = [
    "source_type",
    "source_section",
    "tipo_norma",
    "numero",
    "anio",
    "titulo",
    "fecha_publicacion",
    "entidad_emisora",
    "fuente_oficial",
    "source_url",
    "pdf_url",
    "botica_relevance",
]


def get_supabase():
    load_dotenv(BASE_DIR / ".env")

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")

    if not url:
        raise ValueError("Falta SUPABASE_URL")
    if not key:
        raise ValueError("Falta SUPABASE_SERVICE_ROLE_KEY o SUPABASE_KEY")

    return create_client(url, key)


def load_seed_records() -> list[dict]:
    if not SEED_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo de semilla: {SEED_PATH}")

    records = json.loads(SEED_PATH.read_text(encoding="utf-8"))

    if not isinstance(records, list):
        raise ValueError("La semilla debe ser una lista JSON")

    for item in records:
        if not item.get("document_key"):
            raise ValueError("Hay un registro sin document_key")
        if not item.get("titulo"):
            raise ValueError(f"Registro sin titulo: {item.get('document_key')}")

    return records


def merge_raw(existing_raw, seed_raw, document_key: str):
    base = existing_raw if isinstance(existing_raw, dict) else {}
    seed = seed_raw if isinstance(seed_raw, dict) else {}

    base["seed"] = {
        "name": "normativa_seed_minima",
        "document_key": document_key,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "status": seed.get("seed_status", "metadata_inicial"),
        "pendiente": seed.get("pendiente"),
    }

    return base


def main():
    supabase = get_supabase()
    records = load_seed_records()

    inserted = 0
    updated = 0

    for record in records:
        document_key = record["document_key"]

        existing_response = (
            supabase.table("digemid_normas")
            .select("id, document_key, raw, process_status, has_file, drive_file_id, drive_folder_id")
            .eq("document_key", document_key)
            .limit(1)
            .execute()
        )

        existing_rows = existing_response.data or []

        if existing_rows:
            existing = existing_rows[0]

            payload = {
                field: record.get(field)
                for field in SAFE_UPDATE_FIELDS
                if field in record
            }
            payload["raw"] = merge_raw(existing.get("raw"), record.get("raw"), document_key)
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()

            (
                supabase.table("digemid_normas")
                .update(payload)
                .eq("id", existing["id"])
                .execute()
            )
            updated += 1
            print(f"Actualizado: {document_key}")
        else:
            payload = {
                "document_key": document_key,
                "source_type": record.get("source_type", "norma"),
                "source_section": record.get("source_section"),
                "tipo_norma": record.get("tipo_norma"),
                "numero": record.get("numero"),
                "anio": record.get("anio"),
                "titulo": record["titulo"],
                "fecha_publicacion": record.get("fecha_publicacion"),
                "entidad_emisora": record.get("entidad_emisora"),
                "fuente_oficial": record.get("fuente_oficial"),
                "source_url": record.get("source_url"),
                "pdf_url": record.get("pdf_url"),
                "has_file": False,
                "process_status": "registered",
                "botica_relevance": record.get("botica_relevance") or {},
                "raw": merge_raw({}, record.get("raw"), document_key),
            }

            supabase.table("digemid_normas").insert(payload).execute()
            inserted += 1
            print(f"Insertado: {document_key}")

    print(f"Finalizado. Insertados: {inserted} | Actualizados: {updated}")


if __name__ == "__main__":
    main()
