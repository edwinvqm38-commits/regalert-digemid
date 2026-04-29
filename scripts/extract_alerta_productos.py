import argparse
import logging
import os
import re
import unicodedata
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
LAYOUT_PAGE_TABLE = "digemid_documento_layout_paginas"
EXTRACTION_METHOD = "rule_based_v1"
LAYOUT_EXTRACTION_METHOD = "layout_rule_based_v1"
DEFAULT_STATUSES = [
    "text_extracted",
    "text_extracted_no_products",
]
FORCE_EXTRA_STATUSES = [
    "structured_extracted",
    "structured_extraction_error",
]

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

CONTROL_QUALITY_BREAK_PATTERNS = [
    "EXISTIENDO LA POSIBILIDAD",
    "PARA MAYOR INFORMACION",
    "PARA MAYOR INFORMACIÓN",
    "LIMA,",
    "DEBIDO AL RIESGO",
]

FALSIFIED_BREAK_PATTERNS = CONTROL_QUALITY_BREAK_PATTERNS + [
    "ACCIONES REALIZADAS",
    "SE EXHORTA",
]

COUNTRY_NAMES = {
    "ALEMANIA",
    "ARGENTINA",
    "BRASIL",
    "CANADA",
    "CANADÁ",
    "CHILE",
    "CHINA",
    "COLOMBIA",
    "ECUADOR",
    "ESPANA",
    "ESPAÑA",
    "ESTADOS UNIDOS",
    "FRANCIA",
    "INDIA",
    "ITALIA",
    "JAPON",
    "JAPÓN",
    "MEXICO",
    "MÉXICO",
    "PANAMA",
    "PANAMÁ",
    "PERU",
    "PERÚ",
    "REINO UNIDO",
    "SUIZA",
    "URUGUAY",
    "VENEZUELA",
}

PERU_DEPARTMENTS = {
    "AMAZONAS",
    "ANCASH",
    "APURIMAC",
    "AREQUIPA",
    "AYACUCHO",
    "CAJAMARCA",
    "CUSCO",
    "HUANCAVELICA",
    "HUANUCO",
    "ICA",
    "JUNIN",
    "LA LIBERTAD",
    "LAMBAYEQUE",
    "LIMA",
    "LORETO",
    "MADRE DE DIOS",
    "MOQUEGUA",
    "PASCO",
    "PIURA",
    "PUNO",
    "SAN MARTIN",
    "TACNA",
    "TUMBES",
    "UCAYALI",
}

CONTROL_QUALITY_HEADER_PATTERNS = [
    "NOMBRE DEL PRODUCTO",
    "Nº DE LOTE",
    "N° DE LOTE",
    "LOTE",
    "REGISTRO",
    "SANITARIO",
    "FABRICANTE",
    "PAIS",
    "PAÍS",
    "RESULTADOS ANALITICOS",
    "RESULTADOS ANALÍTICOS",
]

FALSIFIED_HEADER_LABELS = {
    "product_name": ["NOMBRE", "NOMBRE DEL PRODUCTO"],
    "lot_number": ["LOTE", "Nº DE LOTE", "N° DE LOTE"],
    "expiry_date": ["FECHA DE VENCIMIENTO", "VENCIMIENTO"],
    "manufacturer_country": ["FABRICANTE/PAIS", "FABRICANTE/PAÍS"],
    "intervention_address": [
        "DIRECCION DE INCAUTACION / INTERVENCION",
        "DIRECCIÓN DE INCAUTACIÓN / INTERVENCIÓN",
        "DIRECCION DE INCAUTACION",
        "DIRECCIÓN DE INCAUTACIÓN",
    ],
    "department": ["DEPARTAMENTO"],
}


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


def normalize_for_matching(value: str | None) -> str:
    if value is None:
        return ""

    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.upper()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def get_documents_to_process(supabase, limit: int, force: bool) -> list[dict]:
    statuses = DEFAULT_STATUSES + (FORCE_EXTRA_STATUSES if force else [])
    response = (
        supabase
        .table("digemid_documentos")
        .select("id, document_key, title, process_status")
        .eq("source_type", "alerta")
        .in_("process_status", statuses)
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


def fetch_document_layout_pages(supabase, document_id: str) -> list[dict]:
    response = (
        supabase
        .table(LAYOUT_PAGE_TABLE)
        .select("page_number, words_json")
        .eq("document_id", document_id)
        .order("page_number")
        .execute()
    )
    return response.data or []


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


def detect_alert_type(title: str | None, full_text: str) -> str:
    combined_text = normalize_for_matching(f"{title or ''} {full_text or ''}")

    if "RETIRO DEL MERCADO" in combined_text and "CONTROL DE CALIDAD" in combined_text:
        return "retiro_mercado_control_calidad"
    if (
        "PRODUCTO FARMACEUTICO FALSIFICADO" in combined_text
        or "PRODUCTOS FARMACEUTICOS FALSIFICADOS" in combined_text
    ):
        return "producto_falsificado"
    if "PRODUCTOS SANITARIOS" in combined_text and "FALSIFICADOS" in combined_text:
        return "producto_sanitario_falsificado"
    if "PRODUCTOS COSMETICOS FALSIFICADOS" in combined_text:
        return "producto_cosmetico_falsificado"
    if "RECOMENDACIONES" in combined_text:
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


def is_country_line(line: str) -> bool:
    normalized_line = normalize_header(line)
    if normalized_line in COUNTRY_NAMES:
        return True
    if re.fullmatch(r"[A-ZÁÉÍÓÚÑ ]{4,}", line.strip()) and normalized_line in COUNTRY_NAMES:
        return True
    return False


def looks_like_lot(line: str) -> bool:
    normalized_line = normalize_text(line)
    if len(normalized_line) < 3 or len(normalized_line) > 40:
        return False
    if not re.search(r"\d", normalized_line):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9./()\-]+", normalized_line))


def looks_like_sanitary_registration(line: str) -> bool:
    normalized_line = normalize_text(line).upper()
    if len(normalized_line) < 4 or len(normalized_line) > 40:
        return False
    return bool(re.fullmatch(r"[A-Z]{1,4}-[A-Z0-9\-]+", normalized_line))


def normalize_lines(full_text: str) -> list[str]:
    return [
        normalize_text(line)
        for line in full_text.splitlines()
        if normalize_text(line)
    ]


def find_control_quality_header_start(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        normalized_line = normalize_header(line)
        if "NOMBRE DEL PRODUCTO" not in normalized_line:
            continue

        window = lines[index:index + 12]
        normalized_window = [normalize_header(item) for item in window]
        score = sum(
            1 for pattern in CONTROL_QUALITY_HEADER_PATTERNS
            if any(pattern in candidate for candidate in normalized_window)
        )
        if score >= 7:
            return index

    return None


def has_control_quality_markers(lines: list[str]) -> bool:
    normalized_lines = [normalize_for_matching(line) for line in lines]
    markers = [
        "NOMBRE DEL PRODUCTO",
        "LOTE",
        "REGISTRO SANITARIO",
        "RESULTADOS ANALITICOS",
    ]
    return all(any(marker in line for line in normalized_lines) for marker in markers)


def find_falsified_header_start(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        normalized_line = normalize_header(line)
        if (
            "DATOS DEL PRODUCTO FARMACEUTICO FALSIFICADO" in normalized_line
            or "DATOS DE PRODUCTOS FARMACEUTICOS FALSIFICADOS" in normalized_line
        ):
            return index

    for index, line in enumerate(lines):
        normalized_line = normalize_header(line)
        if normalized_line == "NOMBRE":
            window = lines[index:index + 10]
            normalized_window = [normalize_header(item) for item in window]
            if any("LOTE" == candidate or "Nº DE LOTE" in candidate or "N° DE LOTE" in candidate for candidate in normalized_window):
                return index

    return None


def has_falsified_markers(lines: list[str]) -> bool:
    normalized_lines = [normalize_for_matching(line) for line in lines]
    has_header = any(
        "DATOS DEL PRODUCTO" in line
        or "DATOS DE PRODUCTOS FARMACEUTICOS FALSIFICADOS" in line
        for line in normalized_lines
    )
    has_name = any("NOMBRE" == line or "NOMBRE DEL PRODUCTO" in line for line in normalized_lines)
    has_lot = any("LOTE" == line or "Nº DE LOTE" in line or "N° DE LOTE" in line for line in normalized_lines)
    has_manufacturer_country = any(
        "FABRICANTE/PAIS" in line or "FABRICANTE / PAIS" in line
        for line in normalized_lines
    )
    return has_header and has_name and has_lot and has_manufacturer_country


def select_extractor(alert_type: str, lines: list[str]) -> str:
    if alert_type == "retiro_mercado_control_calidad" or has_control_quality_markers(lines):
        return "retiro_mercado"

    if (
        alert_type in {
            "producto_falsificado",
            "producto_sanitario_falsificado",
            "producto_cosmetico_falsificado",
        }
        or has_falsified_markers(lines)
    ):
        return "falsificados"

    return "ninguno"


def get_preview_lines(lines: list[str], start_index: int | None, limit: int = 20) -> list[str]:
    if start_index is None:
        return lines[:limit]
    return lines[start_index:start_index + limit]


def collect_until(lines: list[str], start_index: int, stop_predicate) -> tuple[list[str], int]:
    collected: list[str] = []
    index = start_index

    while index < len(lines):
        line = lines[index]
        if stop_predicate(line):
            break
        collected.append(line)
        index += 1

    return collected, index


def join_lines(lines: list[str]) -> str | None:
    filtered_lines = [normalize_text(line) for line in lines if normalize_text(line)]
    value = normalize_text(" ".join(filtered_lines))
    return value or None


def split_manufacturer_country(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None

    parts = [normalize_text(part) for part in re.split(r"\s*/\s*", value) if normalize_text(part)]
    if len(parts) >= 2 and is_country_line(parts[-1]):
        return join_lines(parts[:-1]), parts[-1]

    if is_country_line(value):
        return None, value

    return value, None


def group_words_by_layout_line(layout_pages: list[dict]) -> list[dict]:
    grouped: dict[tuple[int, int], list[dict]] = {}

    for page in layout_pages:
        page_number = page.get("page_number")
        words = page.get("words_json") or []

        if not isinstance(words, list):
            continue

        for word in words:
            if not isinstance(word, dict):
                continue

            text = normalize_text(word.get("text"))
            x0 = word.get("x0")
            y0 = word.get("y0")

            if not text or x0 is None or y0 is None:
                continue

            try:
                y_group = round(float(y0) / 3) * 3
                key = (int(page_number), int(y_group))
                grouped.setdefault(key, []).append({
                    "text": text,
                    "x0": float(x0),
                    "y0": float(y0),
                })
            except (TypeError, ValueError):
                continue

    lines: list[dict] = []
    for (page_number, y_group), words in sorted(grouped.items()):
        words_sorted = sorted(words, key=lambda item: item["x0"])
        full_text = join_lines([word["text"] for word in words_sorted]) or ""
        columns = {
            "col_producto": [],
            "col_lote": [],
            "col_fabricante_pais": [],
            "col_intervencion": [],
            "col_departamento": [],
        }

        for word in words_sorted:
            x0 = word["x0"]
            text = word["text"]

            if x0 < 270:
                columns["col_producto"].append(text)
            elif x0 < 350:
                columns["col_lote"].append(text)
            elif x0 < 480:
                columns["col_fabricante_pais"].append(text)
            elif x0 < 650:
                columns["col_intervencion"].append(text)
            else:
                columns["col_departamento"].append(text)

        lines.append({
            "page_number": page_number,
            "y_group": y_group,
            "full_text": full_text,
            "col_producto": join_lines(columns["col_producto"]),
            "col_lote": join_lines(columns["col_lote"]),
            "col_fabricante_pais": join_lines(columns["col_fabricante_pais"]),
            "col_intervencion": join_lines(columns["col_intervencion"]),
            "col_departamento": join_lines(columns["col_departamento"]),
        })

    return lines


def find_layout_table_zone(lines: list[dict]) -> tuple[list[dict], int | None, int | None]:
    start_markers = [
        "DATOS DEL PRODUCTO",
        "DATOS DE PRODUCTOS",
        "PRODUCTOS COSMETICOS FALSIFICADOS",
        "NOMBRE",
        "LOTE",
        "FABRICANTE",
    ]
    end_markers = [
        "DEBIDO AL RIESGO",
        "LIMA,",
        "(*)",
        "PARA MAYOR INFORMACION",
        "EL TITULAR DEL REGISTRO SANITARIO",
    ]

    start_index = None
    for index, line in enumerate(lines):
        haystack = normalize_for_matching(" ".join(filter(None, [
            line.get("full_text"),
            line.get("col_producto"),
            line.get("col_lote"),
            line.get("col_fabricante_pais"),
            line.get("col_intervencion"),
            line.get("col_departamento"),
        ])))
        if any(marker in haystack for marker in start_markers):
            start_index = index
            break

    if start_index is None:
        return [], None, None

    end_index = len(lines)
    for index in range(start_index + 1, len(lines)):
        haystack = normalize_for_matching(lines[index].get("full_text"))
        if any(marker in haystack for marker in end_markers):
            end_index = index
            break

    return lines[start_index:end_index], start_index, end_index


def is_layout_header_value(value: str | None) -> bool:
    normalized = normalize_for_matching(value)
    if not normalized:
        return True

    header_tokens = [
        "NOMBRE",
        "NOMBRE DEL PRODUCTO",
        "LOTE",
        "FECHA DE",
        "VENCIMIENTO",
        "FABRICANTE",
        "PAIS",
        "FABRICANTE/PAIS",
        "FABRICANTE / PAIS",
        "INTERVENCION",
        "DIRECCION DE INCAUTACION",
        "DEPARTAMENTO",
        "DATOS DEL PRODUCTO",
        "DATOS DE PRODUCTOS",
        "PRODUCTO FARMACEUTICO INCAUTADO",
        "PRODUCTOS FARMACEUTICOS FALSIFICADOS",
    ]
    return any(token == normalized or token in normalized for token in header_tokens)


def first_non_header_value(values: list[str], reject_prefixes: list[str] | None = None) -> str | None:
    reject_prefixes = reject_prefixes or []

    for value in values:
        normalized = normalize_for_matching(value)
        if is_layout_header_value(value):
            continue
        if any(normalized.startswith(prefix) for prefix in reject_prefixes):
            continue
        return value

    return None


def extract_valid_layout_lot(values: list[str]) -> str | None:
    lot_candidates: list[str] = []

    for value in values:
        normalized_value = normalize_text(value)
        if not normalized_value:
            continue

        lot_candidates.append(normalized_value)

        if is_valid_lot_candidate(normalized_value):
            return normalized_value

        tokens = [normalize_text(token) for token in re.split(r"\s+", normalized_value) if normalize_text(token)]
        for token in tokens:
            if is_valid_lot_candidate(token):
                return token

    return None


def is_valid_lot_candidate(value: str | None) -> bool:
    normalized = normalize_for_matching(value)
    if not normalized:
        return False

    invalid_values = {
        "LOTE",
        "NOMBRE",
        "PRODUCTO",
        "PRODUCTOS",
        "FALSIFICADO",
        "FALSIFICADOS",
        "FABRICANTE",
        "PAIS",
        "INTERVENCION",
        "DEPARTAMENTO",
        "FECHA",
        "VENCIMIENTO",
    }

    if normalized in invalid_values:
        return False

    if is_expiry_date_candidate(value):
        return False

    if " " in normalize_text(value) and len(normalize_text(value)) > 20:
        return False

    return looks_like_lot(normalize_text(value))


def is_expiry_date_candidate(value: str | None) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False

    return bool(re.fullmatch(r"(0[1-9]|1[0-2])[-/](19|20)\d{2}\*?", normalized))


def is_low_quality_product(product: dict) -> bool:
    product_name = normalize_text(product.get("product_name"))
    lot_number = normalize_text(product.get("lot_number"))
    manufacturer = normalize_text(product.get("manufacturer"))
    manufacturer_country = normalize_text(product.get("manufacturer_country"))
    confidence = product.get("confidence")
    normalized_lot = normalize_for_matching(lot_number)

    invalid_lot_tokens = [
        "FECHA DE",
        "VENCIMIENTO",
        "FABRICANTE",
        "PAIS",
        "NOMBRE",
        "LOTE",
        "PRODUCTO",
    ]

    if not product_name or not lot_number:
        return True
    if any(token in normalized_lot for token in invalid_lot_tokens):
        return True
    if len(lot_number) > 40:
        return True
    if len(manufacturer) > 250 or len(manufacturer_country) > 250:
        return True
    if isinstance(confidence, (int, float)) and float(confidence) <= 0.65:
        return True

    return False


def detect_layout_table_profile(zone_lines: list[dict]) -> str:
    zone_text = normalize_for_matching(" ".join(
        line.get("full_text") or ""
        for line in zone_lines
    ))

    if any(marker in zone_text for marker in ["FECHA", "VENCIMIENTO", "FECHA DE VENCIMIENTO"]):
        return "with_expiry"

    lot_rows = sum(
        1 for line in zone_lines
        if extract_valid_layout_lot([line.get("col_lote")]) is not None
        or (
            is_valid_lot_candidate(line.get("col_producto"))
            and is_expiry_date_candidate(line.get("col_lote"))
        )
    )
    if lot_rows > 1:
        return "multiproduct"

    return "simple"


def select_layout_department(values: list[str]) -> str | None:
    invalid_values = {"", "/", "EN", "DE", "LA", "EL", "NULL"}

    logger.info("department candidates: %s", values)

    normalized_pairs = [
        (value, normalize_for_matching(value))
        for value in values
        if normalize_text(value)
    ]

    for value, normalized in normalized_pairs:
        if normalized in PERU_DEPARTMENTS:
            logger.info("department selected: %s", value)
            return value

    for value in values:
        candidate = normalize_text(value)
        normalized = normalize_for_matching(candidate)
        if not candidate or normalized in invalid_values:
            continue
        if is_layout_header_value(candidate):
            continue
        logger.info("department selected: %s", candidate)
        return candidate

    logger.info("department selected: %s", None)
    return None


def is_valid_peru_department(value: str | None) -> bool:
    normalized = normalize_for_matching(value)
    if not normalized or normalized in {"EN", "/", "DE", "LA", "EL", "NULL"}:
        return False
    return normalized in PERU_DEPARTMENTS


def cleanup_manufacturer_fragment(value: str | None) -> str | None:
    candidate = normalize_text(value)
    if not candidate:
        return None

    cleaned = re.sub(
        r"\b(FALSIFICADO|FALSIFICADOS|establecimiento|farmaceutico|farmacéutico|Insumos|Drogas|Diresa|DIRESA|Direccion|Dirección|Ejecutiva)\b",
        "",
        candidate,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;/")
    return cleaned or None


def reconstruct_manufacturer_country(
    manufacturer_fragments: list[str],
    fallback_country_fragments: list[str],
) -> tuple[str | None, str | None]:
    cleaned_fragments = [
        cleanup_manufacturer_fragment(fragment)
        for fragment in manufacturer_fragments
    ]
    cleaned_fragments = [fragment for fragment in cleaned_fragments if fragment]
    combined = join_lines(cleaned_fragments)

    if combined and "/" in combined:
        left, right = combined.split("/", 1)
        manufacturer = cleanup_manufacturer_fragment(left)
        right_clean = cleanup_manufacturer_fragment(right)
        if right_clean:
            country_token = next(
                (token for token in COUNTRY_NAMES if token in normalize_for_matching(right_clean)),
                None,
            )
            if country_token:
                original_country = next(
                    (word for word in right_clean.split() if normalize_for_matching(word) == country_token),
                    country_token.title(),
                )
                return manufacturer, original_country
        return manufacturer, right_clean

    if combined:
        for token in COUNTRY_NAMES:
            if token in normalize_for_matching(combined):
                parts = re.split(token, combined, flags=re.IGNORECASE)
                manufacturer = cleanup_manufacturer_fragment(parts[0])
                return manufacturer, token.title()

    for fragment in fallback_country_fragments:
        cleaned = cleanup_manufacturer_fragment(fragment)
        if cleaned:
            for token in COUNTRY_NAMES:
                if token in normalize_for_matching(cleaned):
                    return combined, token.title()

    return combined, None


def select_lot_and_expiry_from_segment(segment: list[dict], table_profile: str) -> tuple[str | None, str | None, list[str]]:
    lot_candidates: list[str] = []
    expiry_candidates: list[str] = []

    for line in segment:
        col_producto = normalize_text(line.get("col_producto"))
        col_lote = normalize_text(line.get("col_lote"))

        if col_lote:
            lot_candidates.append(col_lote)
            if is_expiry_date_candidate(col_lote):
                expiry_candidates.append(col_lote)

        if table_profile == "with_expiry" and col_producto:
            if is_valid_lot_candidate(col_producto):
                lot_candidates.append(col_producto)
            if is_expiry_date_candidate(col_producto):
                expiry_candidates.append(col_producto)

    selected_lot = None
    selected_expiry = None

    if table_profile == "with_expiry":
        for line in segment:
            col_producto = normalize_text(line.get("col_producto"))
            col_lote = normalize_text(line.get("col_lote"))
            if is_valid_lot_candidate(col_producto) and is_expiry_date_candidate(col_lote):
                selected_lot = col_producto
                selected_expiry = col_lote
                break

    if not selected_lot:
        selected_lot = extract_valid_layout_lot(lot_candidates)

    if not selected_expiry:
        selected_expiry = next(
            (candidate for candidate in expiry_candidates if is_expiry_date_candidate(candidate)),
            None,
        )

    return selected_lot, selected_expiry, lot_candidates


def find_with_expiry_row_match(segment: list[dict]) -> dict | None:
    for line in segment:
        col_producto = normalize_text(line.get("col_producto"))
        col_lote = normalize_text(line.get("col_lote"))
        col_departamento = normalize_text(line.get("col_departamento"))

        if is_expiry_date_candidate(col_lote) and is_valid_lot_candidate(col_producto):
            logger.info(
                "with_expiry row match: y_group=%s, col_producto=%s, col_lote=%s, col_departamento=%s",
                line.get("y_group"),
                col_producto,
                col_lote,
                col_departamento,
            )
            return line

    return None


def extract_lot_from_product_column(segment: list[dict], expiry_date: str | None) -> str | None:
    if not expiry_date:
        return None

    for line in segment:
        col_producto = normalize_text(line.get("col_producto"))
        col_lote = normalize_text(line.get("col_lote"))

        if not col_producto or not col_lote:
            continue

        if not is_expiry_date_candidate(col_lote):
            continue

        tokens = [normalize_text(token) for token in re.split(r"\s+", col_producto) if normalize_text(token)]
        for token in tokens:
            if is_valid_lot_candidate(token):
                return token

    return None


def cleanup_product_name(product_name: str | None, lot_number: str | None, expiry_date: str | None) -> tuple[str | None, str | None]:
    before_cleanup = product_name
    cleaned = normalize_text(product_name)

    if not cleaned:
        return before_cleanup, None

    for token in [lot_number, expiry_date]:
        normalized_token = normalize_text(token)
        if normalized_token:
            cleaned = re.sub(
                rf"(?<!\w){re.escape(normalized_token)}(?!\w)",
                " ",
                cleaned,
                flags=re.IGNORECASE,
            )

    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return before_cleanup, cleaned or None


def extract_lot_from_product_name(product_name: str | None, expiry_date: str | None = None) -> str | None:
    normalized_product_name = normalize_text(product_name)
    if not normalized_product_name:
        return None

    normalized_expiry = normalize_text(expiry_date)
    reject_tokens = {
        "MG",
        "ML",
        "CAJA",
        "VIAL",
        "COMPRIMIDOS",
        "COMPRIMIDO",
        "CAPSULAS",
        "CÁPSULAS",
        "BLISTER",
        "BLÍSTER",
        "SOLUCION",
        "SOLUCIÓN",
        "INYECTABLE",
        "ORAL",
    }

    tokens = [
        normalize_text(token)
        for token in re.split(r"\s+", normalized_product_name.replace("(", " ").replace(")", " "))
        if normalize_text(token)
    ]

    for token in tokens:
        normalized_token = normalize_for_matching(token)
        if normalized_expiry and token == normalized_expiry:
            continue
        if is_expiry_date_candidate(token):
            continue
        if normalized_token in reject_tokens:
            continue
        if "." in token:
            continue
        if token.isdigit() and len(token) < 5:
            continue
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9*\-]{2,}", token):
            continue
        if len(token) >= 5 or re.fullmatch(r"[A-Za-z]\d[A-Za-z0-9]{2,}", token):
            if is_valid_lot_candidate(token):
                return token
        elif re.fullmatch(r"[A-Za-z0-9]{4}", token) and any(char.isdigit() for char in token):
            return token

    return None


def extract_lot_from_product_name_regex(product_name: str | None, expiry_date: str | None = None) -> tuple[str | None, list[str]]:
    normalized_product_name = normalize_text(product_name)
    normalized_expiry = normalize_text(expiry_date)

    if not normalized_product_name:
        return None, []

    reject_tokens = {
        "CAJA",
        "VIAL",
        "COMPRIMIDOS",
        "COMPRIMIDO",
        "MG",
        "ML",
        "SOLUCION",
        "SOLUCIÓN",
        "INYECTABLE",
        "ORAL",
    }

    candidates = re.findall(r"(?<![\d.])\b[A-Z0-9]{4,12}\*?\b(?!-\d)", normalized_product_name, flags=re.IGNORECASE)
    numeric_candidates = re.findall(r"\b\d{5,12}\*?\b", normalized_product_name)
    all_candidates: list[str] = []

    for candidate in candidates + numeric_candidates:
        cleaned = normalize_text(candidate)
        if cleaned and cleaned not in all_candidates:
            all_candidates.append(cleaned)

    prioritized = sorted(
        all_candidates,
        key=lambda value: (
            "*" not in value,
            not any(char.isalpha() for char in value),
            -len(value),
        ),
    )

    for candidate in prioritized:
        normalized_candidate = normalize_for_matching(candidate)
        if normalized_expiry and candidate == normalized_expiry:
            continue
        if is_expiry_date_candidate(candidate):
            continue
        if normalized_candidate in reject_tokens:
            continue
        if "." in candidate:
            continue
        if candidate.isdigit() and candidate in {"50", "100", "120", "1"}:
            continue
        if is_valid_lot_candidate(candidate):
            return candidate, all_candidates

    return None, all_candidates


def extract_falsified_products_from_layout(
    supabase,
    document: dict,
    alert_number: str | None,
    alert_type: str,
) -> tuple[list[dict], dict]:
    layout_pages = fetch_document_layout_pages(supabase, document["id"])
    layout_lines = group_words_by_layout_line(layout_pages)
    zone_lines, start_index, end_index = find_layout_table_zone(layout_lines)

    if not zone_lines:
        return [], {
            "layout_fallback_used": True,
            "layout_lines_count": len(layout_lines),
            "layout_columns_detected": [],
            "reason": "No se detecto zona de tabla en layout",
        }

    logger.info(
        "Documento %s | fallback layout activado | lineas reconstruidas: %s",
        document.get("document_key"),
        len(layout_lines),
    )

    column_names = [
        "col_producto",
        "col_lote",
        "col_fabricante_pais",
        "col_intervencion",
        "col_departamento",
    ]
    detected_columns = [
        column for column in column_names
        if any(normalize_text(line.get(column)) for line in zone_lines)
    ]
    table_profile = detect_layout_table_profile(zone_lines)

    logger.info(
        "Documento %s | columnas detectadas en layout: %s",
        document.get("document_key"),
        ", ".join(detected_columns) if detected_columns else "ninguna",
    )
    logger.info(
        "Documento %s | table_profile detectado: %s",
        document.get("document_key"),
        table_profile,
    )

    lot_rows = [
        index for index, line in enumerate(zone_lines)
        if (
            table_profile == "with_expiry"
            and is_valid_lot_candidate(line.get("col_producto"))
            and is_expiry_date_candidate(line.get("col_lote"))
        )
        or extract_valid_layout_lot([line.get("col_lote")]) is not None
    ]
    if not lot_rows:
        lot_rows = [len(zone_lines) - 1]

    products: list[dict] = []
    start_row = 0

    for lot_row in lot_rows:
        segment = zone_lines[start_row:lot_row + 1]
        start_row = lot_row + 1
        if not segment:
            continue

        product_lines = [
            line["col_producto"]
            for line in segment
            if (
                line.get("col_producto")
                and not is_layout_header_value(line.get("col_producto"))
                and not is_valid_lot_candidate(line.get("col_producto"))
                and not is_expiry_date_candidate(line.get("col_producto"))
            )
        ]
        col_lote_values = [
            line["col_lote"]
            for line in segment
            if line.get("col_lote")
        ]
        manufacturer_country_lines = []
        for line in segment:
            fabricante_pais = line.get("col_fabricante_pais")
            col_lote = line.get("col_lote")

            if fabricante_pais and not is_layout_header_value(fabricante_pais):
                manufacturer_country_lines.append(fabricante_pais)

            if (
                table_profile == "with_expiry"
                and col_lote
                and not is_layout_header_value(col_lote)
                and not is_valid_lot_candidate(col_lote)
                and not is_expiry_date_candidate(col_lote)
            ):
                manufacturer_country_lines.append(col_lote)

        intervention_lines = [
            line["col_intervencion"]
            for line in zone_lines
            if line.get("col_intervencion") and not is_layout_header_value(line.get("col_intervencion"))
        ]
        department_values = [
            line["col_departamento"]
            for line in segment
            if (
                line.get("col_departamento")
                and not is_layout_header_value(line.get("col_departamento"))
                and normalize_text(line.get("col_departamento")) != "/"
            )
        ]

        product_name = join_lines(product_lines)
        lot_number, expiry_date, lot_candidates = select_lot_and_expiry_from_segment(segment, table_profile)
        lot_from_product_column = None
        lot_from_product_name = None
        expiry_from_lote_column = None
        department_from_same_row = None

        if table_profile == "with_expiry":
            matched_row = find_with_expiry_row_match(segment)
            if matched_row:
                lot_from_product_column = normalize_text(matched_row.get("col_producto"))
                expiry_from_lote_column = normalize_text(matched_row.get("col_lote"))
                raw_department = normalize_text(matched_row.get("col_departamento"))
                if is_valid_peru_department(raw_department):
                    department_from_same_row = raw_department

                lot_number = lot_from_product_column or lot_number
                expiry_date = expiry_from_lote_column or expiry_date
        logger.info(
            "Documento %s | lot candidates from layout col_lote: %s",
            document.get("document_key"),
            col_lote_values,
        )
        logger.info(
            "Documento %s | selected lot_number: %s",
            document.get("document_key"),
            lot_number,
        )
        logger.info(
            "Documento %s | selected expiry_date: %s",
            document.get("document_key"),
            expiry_date,
        )
        logger.info(
            "lot_from_product_column=%s",
            lot_from_product_column,
        )
        logger.info("lot_from_product_name=%s", lot_from_product_name)
        logger.info("expiry_from_lote_column=%s", expiry_from_lote_column)
        logger.info("department_from_same_row=%s", department_from_same_row)
        product_name_before_cleanup = product_name
        logger.info("product_name_before_lot_cleanup=%s", product_name_before_cleanup)
        product_name_before_cleanup, product_name = cleanup_product_name(
            product_name,
            lot_number,
            expiry_date,
        )
        logger.info("product_name_after_lot_cleanup=%s", product_name)
        logger.info(
            "Documento %s | product_name_before_cleanup: %s",
            document.get("document_key"),
            product_name_before_cleanup,
        )
        logger.info(
            "Documento %s | product_name_after_cleanup: %s",
            document.get("document_key"),
            product_name,
        )
        manufacturer, manufacturer_country = reconstruct_manufacturer_country(
            manufacturer_country_lines,
            manufacturer_country_lines,
        )
        intervention_address = join_lines(intervention_lines)
        department = department_from_same_row or select_layout_department(department_values)
        raw_block = "\n".join(line["full_text"] for line in segment if line.get("full_text"))

        if table_profile == "with_expiry" and not lot_number:
            logger.info("final pre-insert product_name before lot extraction=%s", product_name)
            lot_from_product_name = extract_lot_from_product_name(product_name, expiry_date)
            logger.info("lot_from_product_name=%s", lot_from_product_name)
            if lot_from_product_name:
                lot_number = lot_from_product_name
                _, product_name = cleanup_product_name(
                    product_name,
                    lot_number,
                    expiry_date,
                )
            if not lot_number:
                regex_lot_selected, regex_lot_candidates = extract_lot_from_product_name_regex(
                    product_name,
                    expiry_date,
                )
                logger.info("regex_lot_candidates_from_product_name=%s", regex_lot_candidates)
                logger.info("regex_lot_selected=%s", regex_lot_selected)
                if regex_lot_selected:
                    lot_number = regex_lot_selected
                    _, product_name = cleanup_product_name(
                        product_name,
                        lot_number,
                        expiry_date,
                    )
                logger.info("product_name_after_regex_lot_cleanup=%s", product_name)
            logger.info("final pre-insert product_name after lot extraction=%s", product_name)
            logger.info("final pre-insert lot_number=%s", lot_number)

        if not (product_name or lot_number or intervention_address):
            continue

        logger.info(
            "Documento %s | manufacturer reconstruido: %s | country: %s",
            document.get("document_key"),
            manufacturer,
            manufacturer_country,
        )
        logger.info(
            "Documento %s | department seleccionado: %s",
            document.get("document_key"),
            department,
        )
        logger.info("final product_name=%s", product_name)

        product = build_partial_product(
            document,
            alert_number,
            alert_type,
            [raw_block],
            {
                "product_name": product_name,
                "lot_number": lot_number,
                "manufacturer": manufacturer,
                "manufacturer_country": manufacturer_country,
                "intervention_address": intervention_address,
                "department": department,
                "expiry_date": expiry_date,
                "raw_block": raw_block,
                "extraction_method": LAYOUT_EXTRACTION_METHOD,
                "metadata": {
                    "source": "layout_words_json",
                    "layout_line_count": len(layout_lines),
                    "zone_start_index": start_index,
                    "zone_end_index": end_index,
                    "detected_columns": detected_columns,
                    "table_profile": table_profile,
                },
            },
            confidence=0.75 if product_name and lot_number else 0.65,
        )
        products.append(product)

    valid_products = [
        product for product in products
        if not is_low_quality_product(product)
    ]

    if not valid_products and products:
        valid_products = [
            product for product in products
            if normalize_text(product.get("product_name")) or normalize_text(product.get("lot_number"))
        ]

    if not valid_products:
        return [], {
            "layout_fallback_used": True,
            "layout_lines_count": len(layout_lines),
            "layout_columns_detected": detected_columns,
            "reason": "Layout detectado pero sin datos suficientes para producto",
        }

    return valid_products, {
        "layout_fallback_used": True,
        "layout_lines_count": len(layout_lines),
        "layout_columns_detected": detected_columns,
        "reason": "Extraccion desde layout completada",
    }


def looks_like_registration_holder_line(line: str) -> bool:
    normalized_line = normalize_header(line)
    if is_country_line(line):
        return False
    if any(token in normalized_line for token in ["S.A.C", "S.A.", "E.I.R.L", "LABORATORIO", "DROGUERIA", "DROGUERÍA"]):
        return True
    return normalized_line == line.upper() and len(normalized_line.split()) <= 8


def build_partial_product(
    document: dict,
    alert_number: str | None,
    alert_type: str,
    raw_block_lines: list[str],
    extra_values: dict | None = None,
    confidence: float = 0.6,
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
        "raw_block": "\n".join(raw_block_lines).strip(),
        "extraction_method": EXTRACTION_METHOD,
        "confidence": confidence,
        "metadata": {
            "source": "page_text_concat",
            "line_count": len(raw_block_lines),
        },
    }

    if extra_values:
        product.update(extra_values)

    return product


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


def extract_control_quality_products(
    document: dict,
    lines: list[str],
    alert_number: str | None,
    alert_type: str,
) -> tuple[list[dict], dict]:
    header_start = find_control_quality_header_start(lines)
    preview_lines = get_preview_lines(lines, header_start)

    if header_start is None:
        return [], {
            "table_detected": False,
            "reason": "No se detecto encabezado de tabla de control de calidad",
            "preview_lines": preview_lines,
        }

    data_start = header_start
    while data_start < len(lines):
        if "RESULTADOS ANALITICOS" in normalize_header(lines[data_start]) or "RESULTADOS ANALÍTICOS" in normalize_header(lines[data_start]):
            data_start += 1
            break
        data_start += 1

    if data_start >= len(lines):
        return [], {
            "table_detected": True,
            "reason": "Se detecto encabezado pero no se encontraron lineas de datos",
            "preview_lines": preview_lines,
        }

    analysis_stop = lambda value: any(pattern in normalize_header(value) for pattern in CONTROL_QUALITY_BREAK_PATTERNS)

    product_name_lines, index = collect_until(lines, data_start, looks_like_lot)
    if index >= len(lines):
        partial_product = build_partial_product(
            document,
            alert_number,
            alert_type,
            lines[data_start:data_start + 12],
            {"product_name": join_lines(product_name_lines)},
            confidence=0.55,
        )
        return ([partial_product] if partial_product["product_name"] else []), {
            "table_detected": True,
            "reason": "No se detecto lote despues del nombre del producto",
            "preview_lines": preview_lines,
        }

    lot_number = lines[index]
    index += 1

    sanitary_registration = None
    if index < len(lines) and looks_like_sanitary_registration(lines[index]):
        sanitary_registration = lines[index]
        index += 1

    manufacturer_lines, index = collect_until(lines, index, is_country_line)
    manufacturer_country = None
    if index < len(lines) and is_country_line(lines[index]):
        manufacturer_country = lines[index]
        index += 1

    registration_holder_lines, index = collect_until(
        lines,
        index,
        lambda value: (not looks_like_registration_holder_line(value)) or analysis_stop(value),
    )
    analytical_result_lines, index = collect_until(
        lines,
        index,
        lambda value: analysis_stop(value),
    )

    product_name = join_lines(product_name_lines)
    manufacturer = join_lines(manufacturer_lines)
    registration_holder = join_lines(registration_holder_lines)
    analytical_result = join_lines(analytical_result_lines)

    if not analytical_result and registration_holder:
        pieces = registration_holder.split()
        if len(pieces) > 8:
            cut = max(3, len(pieces) // 2)
            registration_holder = " ".join(pieces[:cut])
            analytical_result = " ".join(pieces[cut:])

    product = build_partial_product(
        document,
        alert_number,
        alert_type,
        lines[data_start:index],
        {
            "product_name": product_name,
            "lot_number": lot_number,
            "sanitary_registration": sanitary_registration,
            "manufacturer": manufacturer,
            "manufacturer_country": manufacturer_country,
            "registration_holder": registration_holder,
            "analytical_result": analytical_result,
        },
        confidence=0.86 if product_name and lot_number else 0.62,
    )

    if product_name and lot_number:
        return [product], {
            "table_detected": True,
            "reason": "Extraccion secuencial de control de calidad",
            "preview_lines": preview_lines,
        }

    return ([product] if product_name or lot_number else []), {
        "table_detected": True,
        "reason": "Se detecto tabla pero solo se pudo extraer parcialmente",
        "preview_lines": preview_lines,
    }


def match_falsified_label(line: str) -> str | None:
    normalized_line = normalize_header(line)
    for field, aliases in FALSIFIED_HEADER_LABELS.items():
        if any(normalized_line == alias for alias in aliases):
            return field
    return None


def extract_falsified_products(
    document: dict,
    lines: list[str],
    alert_number: str | None,
    alert_type: str,
) -> tuple[list[dict], dict]:
    header_start = find_falsified_header_start(lines)
    preview_lines = get_preview_lines(lines, header_start)

    if header_start is None:
        return [], {
            "table_detected": False,
            "reason": "No se detecto encabezado de bloque de falsificados",
            "preview_lines": preview_lines,
        }

    products: list[dict] = []
    current_values: dict = {}
    raw_block_lines: list[str] = []
    current_field: str | None = None

    for index in range(header_start + 1, len(lines)):
        line = lines[index]
        normalized_line = normalize_header(line)

        if any(pattern in normalized_line for pattern in FALSIFIED_BREAK_PATTERNS):
            break

        matched_field = match_falsified_label(line)
        if matched_field:
            if matched_field == "product_name" and current_values.get("product_name"):
                manufacturer, manufacturer_country = split_manufacturer_country(
                    current_values.get("manufacturer_country")
                )
                current_values["manufacturer"] = manufacturer or current_values.get("manufacturer")
                current_values["manufacturer_country"] = manufacturer_country or current_values.get("manufacturer_country")
                products.append(
                    build_partial_product(
                        document,
                        alert_number,
                        alert_type,
                        raw_block_lines,
                        current_values,
                        confidence=0.84 if current_values.get("product_name") and current_values.get("lot_number") else 0.6,
                    )
                )
                current_values = {}
                raw_block_lines = []

            current_field = matched_field
            raw_block_lines.append(line)
            continue

        if current_field:
            current_values[current_field] = join_lines([
                current_values.get(current_field),
                line,
            ])
            raw_block_lines.append(line)

    if current_values.get("product_name") or current_values.get("lot_number"):
        manufacturer, manufacturer_country = split_manufacturer_country(
            current_values.get("manufacturer_country")
        )
        current_values["manufacturer"] = manufacturer or current_values.get("manufacturer")
        current_values["manufacturer_country"] = manufacturer_country or current_values.get("manufacturer_country")
        products.append(
            build_partial_product(
                document,
                alert_number,
                alert_type,
                raw_block_lines,
                current_values,
                confidence=0.84 if current_values.get("product_name") and current_values.get("lot_number") else 0.6,
            )
        )

    valid_products = [
        product for product in products
        if product.get("product_name") or product.get("lot_number") or product.get("raw_block")
    ]

    return valid_products, {
        "table_detected": True,
        "reason": (
            "Extraccion por etiquetas de falsificados"
            if valid_products else
            "Se detecto bloque de falsificados pero no hubo campos suficientes"
        ),
        "preview_lines": preview_lines,
    }


def extract_products_from_text(document: dict, full_text: str) -> tuple[list[dict], dict]:
    lines = normalize_lines(full_text)
    title = document.get("title")
    alert_number = detect_alert_number(title, full_text, document.get("document_key"))
    alert_type = detect_alert_type(title, full_text)
    extractor_selected = select_extractor(alert_type, lines)

    if extractor_selected == "retiro_mercado":
        products, diagnostics = extract_control_quality_products(
            document,
            lines,
            alert_number,
            alert_type,
        )
    elif extractor_selected == "falsificados":
        products, diagnostics = extract_falsified_products(
            document,
            lines,
            alert_number,
            alert_type,
        )
    else:
        header_map, header_index = detect_header_map(lines, 0)
        preview_lines = get_preview_lines(lines, header_index)

        products = []
        diagnostics = {
            "table_detected": bool(header_map and header_index is not None),
            "reason": "Tipo de alerta sin extractor especializado",
            "preview_lines": preview_lines,
        }

        if header_map and header_index is not None:
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

            if not products and diagnostics["table_detected"] and preview_lines:
                partial_product = build_partial_product(
                    document,
                    alert_number,
                    alert_type,
                    preview_lines,
                    confidence=0.5,
                )
                products = [partial_product]
                diagnostics["reason"] = "Se detecto bloque de tabla, se guarda registro parcial"

    return products, {
        "alert_number": alert_number,
        "alert_type": alert_type,
        "extractor_selected": extractor_selected,
        "normalized_match_text": normalize_for_matching(f"{title or ''} {full_text[:1200]}"),
        **diagnostics,
    }


def should_try_layout_fallback(summary: dict, products: list[dict]) -> bool:
    return (
        summary.get("alert_type") in {
            "producto_falsificado",
            "producto_sanitario_falsificado",
            "producto_cosmetico_falsificado",
        }
        and (
            not products
            or any(is_low_quality_product(product) for product in products)
        )
    )


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
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    load_env()

    supabase = get_supabase()
    documents = get_documents_to_process(supabase, args.limit, args.force)

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
            preview_lines = summary.get("preview_lines") or []

            logger.info(
                "Documento %s | alert_type detectado despues de normalizar: %s",
                document_key,
                summary["alert_type"],
            )
            logger.info(
                "Documento %s | extractor seleccionado: %s",
                document_key,
                summary.get("extractor_selected"),
            )
            if preview_lines:
                logger.info(
                    "Documento %s | primeras lineas utiles: %s",
                    document_key,
                    " | ".join(preview_lines[:20]),
                )

            logger.info(
                "Documento %s | modo paginas: %s | productos extraidos: %s",
                document_key,
                page_storage_mode,
                len(products),
            )
            if products:
                logger.info("Documento %s | productos texto plano detectados: %s", document_key, len(products))

            if should_try_layout_fallback(summary, products):
                discarded_plain_products = [
                    product for product in products
                    if is_low_quality_product(product)
                ]
                if discarded_plain_products:
                    logger.info(
                        "Documento %s | productos texto plano descartados por baja calidad: %s",
                        document_key,
                        len(discarded_plain_products),
                    )
                logger.info("Documento %s | fallback layout activado por baja calidad", document_key)
                layout_products, layout_summary = extract_falsified_products_from_layout(
                    supabase,
                    document,
                    summary.get("alert_number"),
                    summary.get("alert_type"),
                )
                if layout_products:
                    products = layout_products
                    summary = {
                        **summary,
                        **layout_summary,
                        "reason": layout_summary.get("reason", summary.get("reason")),
                    }
                    logger.info(
                        "Documento %s | productos extraidos desde layout: %s",
                        document_key,
                        len(layout_products),
                    )
                else:
                    logger.info(
                        "Documento %s | fallback layout sin productos | razon: %s",
                        document_key,
                        layout_summary.get("reason"),
                    )
                    summary = {
                        **summary,
                        **layout_summary,
                        "reason": layout_summary.get("reason", summary.get("reason")),
                    }

            if args.dry_run:
                processed_count += 1
                total_products += len(products)
                if not products:
                    docs_without_products += 1
                    logger.info(
                        "Documento %s | sin productos | razon: %s",
                        document_key,
                        summary.get("reason"),
                    )
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
                logger.info(
                    "Documento %s | sin productos | razon: %s",
                    document_key,
                    summary.get("reason"),
                )
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
