import argparse
import csv
import json
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from supabase import create_client


DEFAULT_INPUT = Path("reports") / "digemid_normativa_inventory.json"
DEFAULT_OUTPUT_DIR = Path("reports")
TABLE_NAME = "digemid_normas"

DRY_RUN_MD = "digemid_normativa_import_dry_run.md"
DRY_RUN_JSON = "digemid_normativa_import_dry_run.json"
DRY_RUN_CSV = "digemid_normativa_import_dry_run.csv"

SOURCE_TYPE = "digemid_normativa_inventory"
SOURCE_SECTION = "normas_legales"
FUENTE_OFICIAL = "DIGEMID"
PROCESS_STATUS = "inventory_imported"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YEAR_RE = re.compile(r"^(19|20)\d{2}$")
CRITICAL_KEYS = [
    "RM-554-2022",
    "RM-810-2024",
    "DS-8-2025",
    "DS-14-2011",
    "DS-16-2011",
    "LEY-29459",
    "LEY-32033-2024",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--only-new", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    if args.dry_run and args.apply:
        raise ValueError("No puedes usar --dry-run y --apply al mismo tiempo")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit debe ser mayor que cero")

    if not args.apply:
        args.dry_run = True
    return args


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_url(value):
    if not value:
        return ""
    parsed = urlparse(str(value).strip())
    normalized_path = re.sub(r"/{2,}", "/", (parsed.path or "/").rstrip("/"))
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{normalized_path}"


def normalize_title(value):
    text = clean_text(value).lower()
    text = text.replace("n°", "n").replace("nº", "n")
    text = re.sub(r"[^a-z0-9áéíóúüñ ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def strip_accents(value: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
    )


def normalize_tipo_norma(value) -> str | None:
    raw = strip_accents(clean_text(value)).upper()
    raw = re.sub(r"[^A-Z0-9 ]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return None
    if raw in {"RM", "RD", "DS", "DU", "DL", "LEY", "RS"}:
        return raw
    if "RESOLUCION MINISTERIAL" in raw:
        return "RM"
    if "RESOLUCION DIRECTORAL" in raw:
        return "RD"
    if "RESOLUCION SUPREMA" in raw:
        return "RS"
    if "DECRETO SUPREMO" in raw:
        return "DS"
    if "DECRETO DE URGENCIA" in raw or "DECRETO URGENCIA" in raw:
        return "DU"
    if "DECRETO LEGISLATIVO" in raw or "DECRETO LEY" in raw:
        return "DL"
    if "LEY" in raw:
        return "LEY"
    return raw


def normalize_numero(value) -> str | None:
    raw = clean_text(value)
    if not raw:
        return None
    match = re.search(r"\d+", raw)
    if not match:
        return None
    normalized = str(int(match.group(0)))
    return normalized


def normalize_anio(value) -> str | None:
    raw = clean_text(value)
    if YEAR_RE.match(raw):
        return raw
    match = re.search(r"(19|20)\d{2}", raw)
    return match.group(0) if match else None


def parse_document_key_parts(document_key: str) -> tuple[str | None, str | None, str | None]:
    key = clean_text(document_key).upper()
    if not key:
        return None, None, None
    tokens = [token for token in key.split("-") if token]
    if not tokens:
        return None, None, None

    tipo = normalize_tipo_norma(tokens[0])
    number = None
    year = None

    for token in tokens[1:]:
        digits = re.sub(r"\D+", "", token)
        if not digits:
            continue
        if year is None and YEAR_RE.match(digits):
            year = digits
            continue
        if number is None:
            number = str(int(digits))

    if year is None:
        for token in tokens[1:]:
            if YEAR_RE.match(token):
                year = token
                break
    return tipo, number, year


def build_normalized_document_key(tipo: str | None, numero: str | None, anio: str | None) -> str | None:
    if tipo and numero and anio:
        return f"{tipo}-{numero}-{anio}"
    if tipo and numero:
        return f"{tipo}-{numero}"
    return None


def get_normalized_identity(tipo_value, numero_value, anio_value, doc_key_value):
    key_tipo, key_numero, key_anio = parse_document_key_parts(doc_key_value)
    tipo = normalize_tipo_norma(tipo_value) or key_tipo
    numero = normalize_numero(numero_value) or key_numero
    anio = normalize_anio(anio_value) or key_anio
    normalized_key = build_normalized_document_key(tipo, numero, anio)
    return {"tipo": tipo, "numero": numero, "anio": anio, "normalized_key": normalized_key}


def safe_date(value):
    if not value:
        return None
    value = clean_text(value)
    if DATE_RE.match(value):
        return value
    return None


def basename_from_url(value):
    if not value:
        return None
    path = urlparse(value).path or ""
    name = Path(path).name
    return name or None


def read_inventory(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el inventario: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("El inventario no contiene una lista en payload['items']")
    return items


def fetch_existing_rows(supabase) -> list[dict]:
    fields = ",".join(
        [
            "id",
            "document_key",
            "tipo_norma",
            "numero",
            "anio",
            "titulo",
            "source_url",
            "pdf_url",
            "process_status",
            "drive_file_id",
            "drive_file_url",
            "drive_folder_id",
        ]
    )
    rows: list[dict] = []
    offset = 0
    batch_size = 1000
    while True:
        response = (
            supabase.table(TABLE_NAME)
            .select(fields)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        data = response.data or []
        rows.extend(data)
        if len(data) < batch_size:
            break
        offset += batch_size
    return rows


def has_sufficient_data(item: dict) -> bool:
    document_key = clean_text(item.get("document_key"))
    title = clean_text(item.get("title"))
    source_url = clean_text(item.get("source_url"))
    identity = get_normalized_identity(
        item.get("tipo_norma_probable"),
        item.get("numero"),
        item.get("anio"),
        item.get("document_key"),
    )
    tipo = identity.get("tipo")
    numero = identity.get("numero")
    anio = identity.get("anio")
    if not document_key or not title or not source_url:
        return False
    if document_key.startswith("NORM-"):
        return False
    if tipo and numero and anio:
        return True
    if tipo == "LEY" and numero:
        return True
    return False


def build_insert_payload(item: dict, available_columns: set[str] | None):
    tipo = clean_text(item.get("tipo_norma_probable")) or None
    numero = clean_text(item.get("numero")) or None
    anio = clean_text(item.get("anio")) or None
    title = clean_text(item.get("title")) or None
    source_url = clean_text(item.get("source_url")) or None
    pdf_url = clean_text(item.get("pdf_url")) or None
    pdf_urls = item.get("pdf_urls") if isinstance(item.get("pdf_urls"), list) else []
    if not pdf_url and pdf_urls:
        pdf_url = clean_text(pdf_urls[0]) or None

    payload = {
        "document_key": clean_text(item.get("document_key")),
        "source_type": SOURCE_TYPE,
        "source_section": SOURCE_SECTION,
        "tipo_norma": tipo,
        "numero": numero,
        "anio": anio,
        "titulo": title,
        "fecha_publicacion": safe_date(item.get("fecha_publicacion")),
        "fuente_oficial": FUENTE_OFICIAL,
        "source_url": source_url,
        "pdf_url": pdf_url,
        "file_name": basename_from_url(pdf_url) if pdf_url else None,
        "mime_type": "application/pdf" if pdf_url else None,
        "has_file": bool(pdf_url or pdf_urls),
        "raw": item,
        "process_status": PROCESS_STATUS,
    }

    if available_columns:
        payload = {k: v for k, v in payload.items() if k in available_columns}
    return payload


def classify_items(items: list[dict], existing_rows: list[dict]):
    by_doc_key: dict[str, list[dict]] = {}
    by_normalized_doc_key: dict[str, list[dict]] = {}
    by_type_number_year: dict[tuple[str, str, str], list[dict]] = {}
    by_type_number: dict[tuple[str, str], list[dict]] = {}
    by_source_url: dict[str, list[dict]] = {}

    for row in existing_rows:
        doc_key = clean_text(row.get("document_key")).upper()
        if doc_key:
            by_doc_key.setdefault(doc_key, []).append(row)

        identity = get_normalized_identity(
            row.get("tipo_norma"),
            row.get("numero"),
            row.get("anio"),
            row.get("document_key"),
        )
        tipo = identity.get("tipo")
        numero = identity.get("numero")
        anio = identity.get("anio")
        normalized_key = identity.get("normalized_key")

        if normalized_key:
            by_normalized_doc_key.setdefault(normalized_key, []).append(row)
        if tipo and numero and anio:
            by_type_number_year.setdefault((tipo, numero, anio), []).append(row)
        if tipo and numero:
            by_type_number.setdefault((tipo, numero), []).append(row)

        url = normalize_url(row.get("source_url"))
        if url:
            by_source_url.setdefault(url, []).append(row)

    actions = []
    for item in items:
        doc_key = clean_text(item.get("document_key")).upper()
        identity = get_normalized_identity(
            item.get("tipo_norma_probable"),
            item.get("numero"),
            item.get("anio"),
            item.get("document_key"),
        )
        tipo = identity.get("tipo")
        numero = identity.get("numero")
        anio = identity.get("anio")
        normalized_key = identity.get("normalized_key")
        title = clean_text(item.get("title"))
        source_url = clean_text(item.get("source_url"))
        n_source_url = normalize_url(source_url)
        n_title = normalize_title(title)

        exact_matches = by_doc_key.get(doc_key, [])
        normalized_key_matches = by_normalized_doc_key.get(normalized_key, []) if normalized_key else []
        type_number_year_matches = (
            by_type_number_year.get((tipo, numero, anio), []) if tipo and numero and anio else []
        )
        type_number_no_year_matches = (
            by_type_number.get((tipo, numero), []) if tipo == "LEY" and numero else []
        )
        url_matches = by_source_url.get(n_source_url, []) if n_source_url else []

        title_similar_matches = []
        if n_title:
            for row in existing_rows:
                row_title = normalize_title(row.get("titulo"))
                row_identity = get_normalized_identity(
                    row.get("tipo_norma"),
                    row.get("numero"),
                    row.get("anio"),
                    row.get("document_key"),
                )
                row_anio = row_identity.get("anio")
                row_tipo = row_identity.get("tipo")
                if row_title and row_title == n_title and (not anio or not row_anio or row_anio == anio):
                    if tipo and row_tipo and tipo != row_tipo:
                        continue
                    title_similar_matches.append(row)
                    if len(title_similar_matches) >= 5:
                        break

        reasons = []
        if exact_matches:
            reasons.append("exact_document_key")
        if normalized_key_matches:
            reasons.append("normalized_key")
        if type_number_year_matches:
            reasons.append("type_number_year")
        if type_number_no_year_matches:
            reasons.append("type_number_no_year")
        if url_matches:
            reasons.append("source_url")
        if title_similar_matches:
            reasons.append("title_similarity")

        strong_reasons = [reason for reason in reasons if reason != "title_similarity"]

        if exact_matches:
            action = "skipped_exact_existing"
        elif strong_reasons:
            action = "skipped_possible_duplicate"
        elif not has_sufficient_data(item):
            action = "skipped_insufficient_data"
        else:
            action = "new_candidate"

        actions.append(
            {
                "action": action,
                "document_key": doc_key or None,
                "tipo_norma": tipo or None,
                "numero": numero or None,
                "anio": anio or None,
                "normalized_document_key": normalized_key,
                "title": title or None,
                "source_url": source_url or None,
                "has_pdf": bool(clean_text(item.get("pdf_url")) or (item.get("pdf_urls") or [])),
                "match_reasons": reasons,
                "existing_exact_count": len(exact_matches),
                "existing_normalized_key_count": len(normalized_key_matches),
                "existing_type_number_year_count": len(type_number_year_matches),
                "existing_type_number_no_year_count": len(type_number_no_year_matches),
                "existing_url_count": len(url_matches),
                "existing_title_count": len(title_similar_matches),
                "item": item,
            }
        )
    return actions


def summarize(actions: list[dict]) -> dict:
    total = len(actions)
    existing_exact = sum(1 for x in actions if x["action"] == "skipped_exact_existing")
    possible_matches = sum(1 for x in actions if x["action"] == "skipped_possible_duplicate")
    new_candidates = sum(1 for x in actions if x["action"] == "new_candidate")
    omitted = sum(1 for x in actions if x["action"] == "skipped_insufficient_data")
    with_pdfs = sum(1 for x in actions if x["has_pdf"])

    return {
        "total_inventory_read": total,
        "total_existing_exact_by_document_key": existing_exact,
        "total_possible_matches_tipo_numero_anio_or_url_or_title": possible_matches,
        "total_new_candidates": new_candidates,
        "total_skipped_insufficient_data": omitted,
        "total_with_associated_pdfs": with_pdfs,
    }


def write_dry_run_reports(output_dir: Path, summary: dict, actions: list[dict], args):
    output_dir.mkdir(parents=True, exist_ok=True)

    proposed_30 = actions[:30]
    action_by_doc_key = {clean_text(row.get("document_key")).upper(): row for row in actions}
    action_by_normalized_key = {
        clean_text(row.get("normalized_document_key")).upper(): row
        for row in actions
        if clean_text(row.get("normalized_document_key"))
    }
    critical_rows = []
    for key in CRITICAL_KEYS:
        key_upper = key.upper()
        target_identity = get_normalized_identity(None, None, None, key_upper)
        target_normalized_key = clean_text(target_identity.get("normalized_key")).upper()
        row = action_by_doc_key.get(key_upper) or action_by_normalized_key.get(target_normalized_key)
        critical_rows.append(
            {
                "requested_key": key,
                "action": row.get("action") if row else "not_found_in_inventory",
                "document_key": row.get("document_key") if row else None,
                "tipo_norma": row.get("tipo_norma") if row else target_identity.get("tipo"),
                "numero": row.get("numero") if row else target_identity.get("numero"),
                "anio": row.get("anio") if row else target_identity.get("anio"),
                "match_reasons": row.get("match_reasons") if row else [],
            }
        )

    json_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "dry-run",
        "input": str(args.input),
        "limit": args.limit,
        "only_new": bool(args.only_new),
        "summary": summary,
        "first_30_actions": [
            {
                "action": x["action"],
                "document_key": x["document_key"],
                "tipo_norma": x["tipo_norma"],
                "numero": x["numero"],
                "anio": x["anio"],
                "source_url": x["source_url"],
                "has_pdf": x["has_pdf"],
                "match_reasons": x["match_reasons"],
            }
            for x in proposed_30
        ],
        "critical_reviews": critical_rows,
    }
    (output_dir / DRY_RUN_JSON).write_text(
        json.dumps(json_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    csv_path = output_dir / DRY_RUN_CSV
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "action",
                "document_key",
                "tipo_norma",
                "numero",
                "anio",
                "has_pdf",
                "match_reasons",
                "source_url",
                "title",
            ],
        )
        writer.writeheader()
        for row in actions:
            writer.writerow(
                {
                    "action": row["action"],
                    "document_key": row["document_key"],
                    "tipo_norma": row["tipo_norma"],
                    "numero": row["numero"],
                    "anio": row["anio"],
                    "has_pdf": row["has_pdf"],
                    "match_reasons": ",".join(row["match_reasons"]),
                    "source_url": row["source_url"],
                    "title": row["title"],
                }
            )

    md_lines = [
        "# DIGEMID Normativa Import Dry Run",
        "",
        f"- generated_at: `{json_payload['generated_at']}`",
        f"- input: `{args.input}`",
        f"- limit: `{args.limit}`",
        f"- only_new: `{args.only_new}`",
        "",
        "## Resumen",
        "",
        f"- total registros leídos del inventario: **{summary['total_inventory_read']}**",
        f"- total existentes exactos por document_key: **{summary['total_existing_exact_by_document_key']}**",
        f"- total posibles coincidencias por tipo_norma + numero + anio: **{summary['total_possible_matches_tipo_numero_anio_or_url_or_title']}**",
        f"- total nuevos candidatos: **{summary['total_new_candidates']}**",
        f"- total omitidos por datos insuficientes: **{summary['total_skipped_insufficient_data']}**",
        f"- total con PDFs asociados: **{summary['total_with_associated_pdfs']}**",
        "",
        "## Primeras 30 acciones propuestas",
        "",
        "| action | document_key | tipo | numero | anio | has_pdf | match_reasons |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in proposed_30:
        md_lines.append(
            "| {action} | {document_key} | {tipo} | {numero} | {anio} | {has_pdf} | {reasons} |".format(
                action=row["action"],
                document_key=row["document_key"] or "",
                tipo=row["tipo_norma"] or "",
                numero=row["numero"] or "",
                anio=row["anio"] or "",
                has_pdf="yes" if row["has_pdf"] else "no",
                reasons=", ".join(row["match_reasons"]),
            )
        )
    md_lines.extend(
        [
            "",
            "## Normas críticas revisadas",
            "",
            "| requested_key | action | document_key_detected | tipo | numero | anio | match_reasons |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in critical_rows:
        md_lines.append(
            "| {requested_key} | {action} | {document_key} | {tipo} | {numero} | {anio} | {reasons} |".format(
                requested_key=row.get("requested_key") or "",
                action=row.get("action") or "",
                document_key=row.get("document_key") or "",
                tipo=row.get("tipo_norma") or "",
                numero=row.get("numero") or "",
                anio=row.get("anio") or "",
                reasons=", ".join(row.get("match_reasons") or []),
            )
        )
    (output_dir / DRY_RUN_MD).write_text("\n".join(md_lines).strip() + "\n", encoding="utf-8")


def apply_inserts(supabase, actions: list[dict], available_columns: set[str] | None, only_new: bool):
    insertables = [x for x in actions if x["action"] == "new_candidate"]
    if only_new:
        target_actions = insertables
    else:
        target_actions = insertables

    inserted = 0
    failed = 0
    for entry in target_actions:
        payload = build_insert_payload(entry["item"], available_columns)
        try:
            supabase.table(TABLE_NAME).insert(payload).execute()
            inserted += 1
        except Exception as exc:
            failed += 1
            logger.warning(
                "No se pudo insertar %s: %s",
                entry.get("document_key"),
                exc,
            )
    return {"inserted": inserted, "failed": failed, "attempted": len(target_actions)}


def main():
    args = parse_args()
    load_env()
    supabase = get_supabase()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    inventory_items = read_inventory(input_path)
    if args.limit:
        inventory_items = inventory_items[: args.limit]

    existing_rows = fetch_existing_rows(supabase)
    available_columns = set(existing_rows[0].keys()) if existing_rows else None

    actions = classify_items(inventory_items, existing_rows)
    if args.only_new:
        actions = [x for x in actions if x["action"] == "new_candidate"]
    summary = summarize(actions)

    if args.dry_run:
        write_dry_run_reports(output_dir, summary, actions, args)
        logger.info("Dry-run completado.")
        logger.info("Total leídos: %s", summary["total_inventory_read"])
        logger.info("Exactos existentes: %s", summary["total_existing_exact_by_document_key"])
        logger.info(
            "Posibles coincidencias: %s",
            summary["total_possible_matches_tipo_numero_anio_or_url_or_title"],
        )
        logger.info("Nuevos candidatos: %s", summary["total_new_candidates"])
        logger.info("Omitidos: %s", summary["total_skipped_insufficient_data"])
        return

    result = apply_inserts(supabase, actions, available_columns, args.only_new)
    logger.info("Apply completado. Intentados=%s Insertados=%s Fallidos=%s", result["attempted"], result["inserted"], result["failed"])


if __name__ == "__main__":
    main()
