import argparse
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

PRODUCT_TABLE = "digemid_alerta_productos"
PAGE_TABLE = "digemid_documento_paginas"
EXTRACTION_METHOD = "rule_based_v1"

HEADER_ALIASES = {
    "product_name": [
        "NOMBRE DEL PRODUCTO",
        "PRODUCTO",
    ],
    "lot_number": [
        "Nº DE LOTE",
        "N° DE LOTE",
        "NO DE LOTE",
        "NRO DE LOTE",
        "NUMERO DE LOTE",
        "LOTE",
    ],
    "sanitary_registration": [
        "REGISTRO SANITARIO",
        "R.S.",
        "R S",
    ],
    "manufacturer": [
        "FABRICANTE",
    ],
    "manufacturer_country": [
        "PAIS",
        "PAÍS",
    ],
    "registration_holder": [
        "TITULAR DEL REGISTRO SANITARIO",
    ],
    "analytical_result": [
        "RESULTADOS ANALITICOS",
        "RESULTADOS ANALÍTICOS",
        "RESULTADO ANALITICO",
        "RESULTADO ANALÍTICO",
    ],
    "expiry_date": [
        "FECHA DE VENCIMIENTO",
        "VENCIMIENTO",
    ],
    "department": [
        "DEPARTAMENTO",
    ],
    "intervention_address": [
        "DIRECCION DE INCAUTACION",
        "DIRECCIÓN DE INCAUTACIÓN",
        "DIRECCION DE INTERVENCION",
        "DIRECCIÓN DE INTERVENCIÓN",
    ],
}

FIELD_ORDER = [
    "product_name",
    "lot_number",
    "sanitary_registration",
    "manufacturer",
    "manufacturer_country",
    "registration_holder",
    "analytical_result",
    "expiry_date",
    "department",
    "intervention_address",
]

SECTION_BREAK_PATTERNS = [
    "RECOMENDACIONES",
    "A LOS PROFESIONALES",
    "A LA POBLACION",
    "A LA POBLACIÓN",
    "ACCIONES ADOPTADAS",
    "MEDIDAS ADOPTADAS",
    "NOTA",
]


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""

    normalized = value.replace("\xa0", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    return normalized.strip()


def normalize_header(value: str) -> str:
    upper = normalize_text(value).upper()
    upper = (
        upper.replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ñ", "N")
        .replace("°", "º")
    )
    return upper


def get_documents_to_process(supabase, limit: int) -> list[dict]:
    response = (
        supabase
        .table("digemid_documentos")
        .select("id, document_key, title, process_status")
        .eq("source_type", "alerta")
        .eq("process_status", "text_extracted")
        .order("published_date", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data or []


def fetch_document_pages(supabase, document_id: str) -> tuple[list[dict], str]:
    try:
        response = (
            supabase
            .table(PAGE_TABLE)
            .select("page_number, text_content")
            .eq("document_id", document_id)
            .order("page_number")
            .execute()
        )
        return response.data or [], "target"
    except Exception:
        response = (
            supabase
            .table(PAGE_TABLE)
            .select("page_number, page_text_clean, page_text_raw")
            .eq("document_id", document_id)
            .order("page_number")
            .execute()
        )
        return response.data or [], "legacy"


def build_full_text(pages: list[dict], storage_mode: str) -> str:
    content = []

    for page in pages:
        if storage_mode == "target":
            text = page.get("text_content") or ""
        else:
            text = page.get("page_text_clean") or page.get("page_text_raw") or ""

        text = text.strip()
        if text:
            content.append(text)

    return "\n\n".join(content)


def detect_alert_number(title: str | None, full_text: str, document_key: str | None) -> str | None:
    candidates = [title or "", full_text[:2000], document_key or ""]
    pattern = re.compile(
        r"ALERTA\s+DIGEMID\s*N[º°O]?\s*([0-9]{1,3}\s*-\s*[0-9]{4})",
        re.IGNORECASE,
    )

    for candidate in candidates:
        match = pattern.search(candidate)
        if match:
            return normalize_text(match.group(1)).replace(" ", "")

    return document_key


def detect_alert_type(title: str | None) -> str:
    normalized_title = normalize_header(title or "")

    if "RETIRO DEL MERCADO" in normalized_title:
        return "retiro_mercado_control_calidad"
    if "PRODUCTO FARMACEUTICO FALSIFICADO" in normalized_title:
        return "producto_falsificado"
    if "PRODUCTOS FARMACEUTICOS FALSIFICADOS" in normalized_title:
        return "productos_falsificados"
    if "RECOMENDACIONES" in normalized_title:
        return "recomendacion_seguridad"
    return "otro"


def split_table_cells(line: str) -> list[str]:
    raw_parts = re.split(r"\s{2,}|\t+|\s+\|\s+", line.strip())
    parts = [normalize_text(part) for part in raw_parts if normalize_text(part)]

    if len(parts) <= 1:
        pipe_parts = [normalize_text(part) for part in line.split("|") if normalize_text(part)]
        if len(pipe_parts) > len(parts):
            return pipe_parts

    return parts


def resolve_header_field(header_cell: str) -> str | None:
    normalized_cell = normalize_header(header_cell)

    for field, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            if alias in normalized_cell:
                return field

    return None


def detect_header_map(lines: list[str], start_index: int) -> tuple[dict[int, str], int] | tuple[None, None]:
    for index in range(start_index, len(lines)):
        cells = split_table_cells(lines[index])
        if len(cells) < 2:
            continue

        header_map: dict[int, str] = {}
        for position, cell in enumerate(cells):
            field = resolve_header_field(cell)
            if field:
                header_map[position] = field

        if "product_name" in header_map.values() and len(header_map) >= 2:
            return header_map, index

    return None, None


def is_section_break(line: str) -> bool:
    normalized_line = normalize_header(line)
    return any(pattern in normalized_line for pattern in SECTION_BREAK_PATTERNS)


def looks_like_new_row(cells: list[str], header_map: dict[int, str]) -> bool:
    if not cells:
        return False

    product_position = next(
        (index for index, field in header_map.items() if field == "product_name"),
        None,
    )
    if product_position is None or product_position >= len(cells):
        return False

    product_cell = normalize_text(cells[product_position])
    if not product_cell:
        return False

    return len(cells) >= max(2, len(header_map) - 1)


def append_continuation(product: dict, line: str) -> None:
    text = normalize_text(line)
    if not text:
        return

    target_fields = [
        "analytical_result",
        "intervention_address",
        "registration_holder",
        "manufacturer",
        "product_name",
    ]

    for field in target_fields:
        if product.get(field):
            product[field] = normalize_text(f"{product[field]} {text}")
            return

    product["raw_block"] = normalize_text(f"{product.get('raw_block', '')}\n{text}")


def row_to_product(
    document: dict,
    alert_number: str | None,
    alert_type: str,
    header_map: dict[int, str],
    cells: list[str],
    raw_line: str,
    row_index: int,
) -> dict:
    product = {
        "document_id": document["id"],
        "document_key": document.get("document_key"),
        "alert_number": alert_number,
        "alert_type": alert_type,
        "product_name": None,
        "lot_number": None,
        "sanitary_registration": None,
        "manufacturer": None,
        "manufacturer_country": None,
        "registration_holder": None,
        "analytical_result": None,
        "expiry_date": None,
        "department": None,
        "intervention_address": None,
        "raw_block": raw_line.strip(),
        "extraction_method": EXTRACTION_METHOD,
        "confidence": 0.78,
        "metadata": {
            "row_index": row_index,
            "header_fields": [field for _, field in sorted(header_map.items())],
            "source": "page_text_concat",
        },
    }

    for position, field in header_map.items():
        if position < len(cells):
            product[field] = normalize_text(cells[position])

    if product["expiry_date"]:
        product["expiry_date"] = normalize_text(product["expiry_date"])

    return product


def is_valid_product(product: dict) -> bool:
    populated = [
        field for field in FIELD_ORDER
        if normalize_text(product.get(field))
    ]
    return bool(product.get("product_name")) and len(populated) >= 2


def extract_products_from_text(document: dict, full_text: str) -> tuple[list[dict], dict]:
    lines = [line.rstrip() for line in full_text.splitlines()]
    title = document.get("title")
    alert_number = detect_alert_number(title, full_text, document.get("document_key"))
    alert_type = detect_alert_type(title)

    header_map, header_index = detect_header_map(lines, 0)
    if not header_map or header_index is None:
        return [], {
            "alert_number": alert_number,
            "alert_type": alert_type,
            "table_detected": False,
        }

    products: list[dict] = []
    current_product: dict | None = None

    for index in range(header_index + 1, len(lines)):
        raw_line = lines[index]
        line = normalize_text(raw_line)

        if not line:
            continue

        if is_section_break(line):
            break

        cells = split_table_cells(raw_line)
        resolved_headers = [resolve_header_field(cell) for cell in cells]
        if "product_name" in resolved_headers:
            continue

        if looks_like_new_row(cells, header_map):
            if current_product and is_valid_product(current_product):
                products.append(current_product)

            current_product = row_to_product(
                document=document,
                alert_number=alert_number,
                alert_type=alert_type,
                header_map=header_map,
                cells=cells,
                raw_line=raw_line,
                row_index=index,
            )
            continue

        if current_product:
            append_continuation(current_product, raw_line)

    if current_product and is_valid_product(current_product):
        products.append(current_product)

    return products, {
        "alert_number": alert_number,
        "alert_type": alert_type,
        "table_detected": True,
    }


def replace_products_for_document(supabase, document_id: str, products: list[dict]) -> None:
    (
        supabase
        .table(PRODUCT_TABLE)
        .delete()
        .eq("document_id", document_id)
        .execute()
    )

    if products:
        (
            supabase
            .table(PRODUCT_TABLE)
            .insert(products)
            .execute()
        )


def update_document_status(
    supabase,
    document_id: str,
    process_status: str,
    process_message: str,
) -> None:
    now = datetime.now(timezone.utc).isoformat()

    (
        supabase
        .table("digemid_documentos")
        .update({
            "process_status": process_status,
            "process_message": process_message,
            "processed_at": now,
            "updated_at": now,
        })
        .eq("id", document_id)
        .execute()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()

    supabase = get_supabase()
    documents = get_documents_to_process(supabase, args.limit)

    logger.info("Documentos encontrados para extraer productos: %s", len(documents))

    processed_count = 0
    docs_without_products = 0
    total_products = 0
    error_count = 0

    for document in documents:
        document_id = document["id"]
        document_key = document.get("document_key") or str(document_id)

        try:
            logger.info("Procesando documento: %s", document_key)

            pages, page_storage_mode = fetch_document_pages(supabase, document_id)
            full_text = build_full_text(pages, page_storage_mode)

            products, summary = extract_products_from_text(document, full_text)

            logger.info(
                "Documento %s | modo paginas: %s | productos extraidos: %s",
                document_key,
                page_storage_mode,
                len(products),
            )

            if args.dry_run:
                processed_count += 1
                total_products += len(products)
                if not products:
                    docs_without_products += 1
                logger.info("DRY RUN %s: no se escribira en Supabase", document_key)
                continue

            replace_products_for_document(supabase, document_id, products)

            if products:
                update_document_status(
                    supabase,
                    document_id,
                    "structured_extracted",
                    (
                        f"Extraccion estructurada completada con {EXTRACTION_METHOD}. "
                        f"Productos: {len(products)}. "
                        f"Tipo: {summary['alert_type']}."
                    ),
                )
            else:
                update_document_status(
                    supabase,
                    document_id,
                    "text_extracted_no_products",
                    (
                        f"No se detectaron tablas de productos con {EXTRACTION_METHOD}. "
                        f"Tipo: {summary['alert_type']}."
                    ),
                )
                docs_without_products += 1

            processed_count += 1
            total_products += len(products)

        except Exception as error:
            error_count += 1
            logger.exception("Error procesando %s: %s", document_key, error)

            if not args.dry_run:
                update_document_status(
                    supabase,
                    document_id,
                    "structured_extraction_error",
                    str(error),
                )

    logger.info(
        "Finalizado. Documentos procesados: %s | Productos extraidos: %s | Documentos sin productos: %s | Errores: %s",
        processed_count,
        total_products,
        docs_without_products,
        error_count,
    )


if __name__ == "__main__":
    main()
