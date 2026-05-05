import argparse
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

def clean_final_field(value):

    
    """
    Limpia campos finales antes de guardarlos en Supabase.
    Sirve para eliminar asteriscos, espacios dobles y caracteres sucios
    que vienen del PDF DIGEMID.
    """
    if value is None:
        return None

    value = str(value).replace("\u00a0", " ").strip()

    # Elimina asteriscos usados como marca visual en algunos PDFs
    value = re.sub(r"\s*\*\s*", " ", value)

    # Normaliza espacios dobles
    value = re.sub(r"\s+", " ", value).strip()

    return value or None

def normalize_registration_holder_and_result(
    registration_holder,
    analytical_result,
) -> tuple[str | None, str | None]:
    """
    Corrige casos donde parte del resultado analítico se pegó al titular.

    Ejemplos:
    - holder: DROGUERÍA DIPHASAC S.A.C. No conforme para el ensayo de
      result: Contenido de Ácido Clavulánico

    - holder: DROGUERÍA PERÚ S.A.C. No conforme para
      result: el ensayo de Contenido de Azatioprina.

    Resultado:
    - holder limpio
    - analytical_result completo
    """

    holder = clean_final_field(registration_holder)
    result = clean_final_field(analytical_result)

    if not holder and not result:
        return None, None

    def normalize_analytical_result(value: str | None) -> str | None:
        text = clean_final_field(value)

        if not text:
            return None

        # Normaliza duplicaciones raras.
        text = re.sub(
            r"(?i)^No conforme para\s+No conforme para\s+",
            "No conforme para ",
            text,
        )

        text = re.sub(
            r"(?i)^No conforme para el ensayo de\s+de\s+",
            "No conforme para el ensayo de ",
            text,
        )

        text = re.sub(
            r"(?i)^No conforme para el ensayo\s+de\s+",
            "No conforme para el ensayo de ",
            text,
        )

        # Caso: "el ensayo de Contenido de Azatioprina."
        if re.search(r"(?i)^el ensayo de\b", text):
            text = f"No conforme para {text}"

        # Caso: "de Contenido de Azatioprina."
        elif re.search(r"(?i)^de\s+", text):
            text = f"No conforme para el ensayo {text}"

        # Caso: "Contenido de Ácido Clavulánico" o "Partículas visibles."
        elif not re.search(r"(?i)^No conforme", text):
            if re.search(
                r"(?i)^(Contenido de|Part[ií]culas|Impurezas|Disoluci[oó]n|Valoraci[oó]n|Esterilidad)",
                text,
            ):
                text = f"No conforme para el ensayo de {text}"

        # Caso: "No conforme para el ensayo Contenido..."
        text = re.sub(
            r"(?i)^No conforme para el ensayo\s+(?!de\b)",
            "No conforme para el ensayo de ",
            text,
        )

        return clean_final_field(text)

    if holder:
        leak_patterns = [
            r"(?i)\b(No conforme para el ensayo de.*)$",
            r"(?i)\b(No conforme para el ensayo.*)$",
            r"(?i)\b(No conforme para.*)$",
            r"(?i)\b(No conforme.*)$",
        ]

        for pattern in leak_patterns:
            match = re.search(pattern, holder)

            if match:
                leaked_text = clean_final_field(match.group(1))
                holder = clean_final_field(holder[:match.start()])

                if result:
                    result = clean_final_field(f"{leaked_text} {result}")
                else:
                    result = leaked_text

                break

    result = normalize_analytical_result(result)

    return holder, result

def clean_product_record(product: dict) -> dict:
    """
    Limpia los campos principales del producto antes de guardarlo en Supabase.
    Evita guardar asteriscos, espacios dobles o caracteres sucios provenientes del PDF.
    Además, corrige contaminación entre titular del registro sanitario y resultado analítico.
    """
    cleaned_product = dict(product)

    fields_to_clean = [
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

    for field in fields_to_clean:
        cleaned_product[field] = clean_final_field(cleaned_product.get(field))

    # Corrige casos como:
    # registration_holder = "DROGUERÍA DIPHASAC S.A.C. No conforme para el ensayo de"
    # analytical_result = "Contenido de Ácido Clavulánico"
    registration_holder, analytical_result = normalize_registration_holder_and_result(
        cleaned_product.get("registration_holder"),
        cleaned_product.get("analytical_result"),
    )

    cleaned_product["registration_holder"] = registration_holder
    cleaned_product["analytical_result"] = analytical_result

    # Si department no es un departamento válido del Perú, lo dejamos en null.
    department = cleaned_product.get("department")
    department_normalized = normalize_for_matching(department)

    if department and department_normalized not in PERU_DEPARTMENTS:
        cleaned_product["department"] = None
    elif department_normalized in PERU_DEPARTMENTS:
        cleaned_product["department"] = department_normalized

    # Marca productos sospechosos para revisión manual.
    if is_suspicious_product_name(cleaned_product.get("product_name")):
        metadata = cleaned_product.get("metadata") or {}
        metadata["needs_manual_review"] = True
        metadata["review_reason"] = "product_name_suspicious_or_contaminated"
        cleaned_product["metadata"] = metadata

        current_confidence = float(cleaned_product.get("confidence") or 0)
        cleaned_product["confidence"] = min(current_confidence, 0.60)

    return cleaned_product
def is_suspicious_product_name(product_name: str | None) -> bool:
    """
    Detecta nombres de producto probablemente contaminados o mal ordenados.
    No elimina el producto automáticamente, pero ayuda a bajarle confianza
    o marcarlo para revisión.
    """
    name = normalize_text(product_name)
    normalized = normalize_for_matching(name)

    if not name:
        return True

    # Casos donde el nombre empieza con presentación/envase y no con marca.
    suspicious_starts = [
        "FOLIO DE ALUMINIO",
        "BLISTER",
        "BLÍSTER",
        "CAJA X",
        "FRASCO",
        "SOLUCION",
        "SOLUCIÓN",
        "TABLETA",
        "CAPSULA",
        "CÁPSULA",
        "AMPOLLA",
    ]

    if any(normalized.startswith(item) for item in suspicious_starts):
        return True

    # Casos donde se coló texto narrativo de intervención.
    suspicious_phrases = [
        "CENTRO COMERCIAL",
        "DONDE SE ALMACENABA",
        "PRODUCTOS INCAUTADOS",
        "ACCIONES DE CONTROL",
        "DIRECCION EJECUTIVA",
        "DIRECCIÓN EJECUTIVA",
    ]

    if any(item in normalized for item in suspicious_phrases):
        return True

    return False

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
ROWSPAN_EXTRACTION_METHOD = "layout_rowspan_table_v1"
NARRATIVE_EXTRACTION_METHOD = "narrative_illegal_product_v1"
COMPARATIVE_PROFILE = "comparative_characteristics_v1"
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

NARRATIVE_ILLEGAL_PATTERNS = [
    "COMERCIALIZACION ILEGAL",
    "COMERCIALIZACIÓN ILEGAL",
    "NO CUENTA CON REGISTRO SANITARIO",
    "PRODUCTO SIN REGISTRO SANITARIO",
    "PRESUNTA FALSIFICACION",
    "PRESUNTA FALSIFICACIÓN",
    "PRODUCTO SOSPECHOSO",
    "NO CORRESPONDE A UNA PRESENTACION COMERCIALIZADA",
    "NO CORRESPONDE A UNA PRESENTACIÓN COMERCIALIZADA",
]

COMPARATIVE_ROW_KEYS = [
    "FRASCO",
    "CAJA",
    "ETIQUETA",
    "CONCENTRACIONES",
    "FORMA FARMACEUTICA",
    "FORMA FARMACÉUTICA",
    "ALMACENAMIENTO",
    "IDIOMA DEL ROTULADO",
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


def normalize_for_matching(value: str | None) -> str:
    if value is None:
        return ""

    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.upper()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def get_documents_to_process(supabase, limit: int, force: bool, document_key: str | None = None) -> list[dict]:
    statuses = DEFAULT_STATUSES + (FORCE_EXTRA_STATUSES if force else [])
    query = (
        supabase
        .table("digemid_documentos")
        .select("id, document_key, title, process_status")
        .eq("source_type", "alerta")
        .order("published_date", desc=True)
        .limit(limit)
    )
    if document_key:
        query = query.eq("document_key", document_key)
    else:
        query = query.in_("process_status", statuses)
    response = query.execute()
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
    if "COMERCIALIZACION ILEGAL" in combined_text and "NO CUENTA CON REGISTRO SANITARIO" in combined_text:
        return "comercializacion_ilegal_producto_sin_rs"

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


def has_narrative_illegal_markers(lines: list[str]) -> bool:
    normalized_lines = [normalize_for_matching(line) for line in lines]
    return any(
        any(pattern in line for pattern in NARRATIVE_ILLEGAL_PATTERNS)
        for line in normalized_lines
    )


def extract_recommended_actions(lines: list[str], full_text: str | None = None) -> list[str]:
    markers = ["RECOMENDACIONES", "SE RECOMIENDA", "SE EXHORTA"]
    end_markers = ["LIMA,", "FUENTE:", "ACCIONES ADOPTADAS", "NOTA:", "DIRECCION EJECUTIVA", "DIRECCIÓN EJECUTIVA"]
    actions: list[str] = []
    collecting = False
    current_action: str | None = None

    for line in lines:
        normalized = normalize_for_matching(line)
        if not collecting and any(marker in normalized for marker in markers):
            collecting = True
            continue

        if collecting:
            if any(marker in normalized for marker in end_markers):
                break
            text = normalize_text(line)
            if not text:
                continue

            starts_bullet = bool(re.match(r"^\s*([-•*]|\d+[.)])\s+", text))
            if starts_bullet:
                if current_action:
                    actions.append(current_action)
                current_action = re.sub(r"^\s*([-•*]|\d+[.)])\s+", "", text).strip()
            else:
                if current_action:
                    current_action = normalize_text(f"{current_action} {text}")
                elif text and not any(token in normalized for token in ["RECOMENDACIONES", "SE RECOMIENDA"]):
                    current_action = text

            if len(actions) >= 8:
                break

    if current_action:
        actions.append(current_action)

    actions = [normalize_text(item) for item in actions if normalize_text(item)]
    if len(actions) >= 2:
        return actions

    # Fallback narrativo para OCR sin bullets visibles.
    haystack = full_text or "\n".join(lines)
    fallback_patterns = [
        r"(?is)(No adquirir y no utilizar productos farmac[eé]uticos.*?registro sanitario[^\.]*\.)",
        r"(?is)(Tener en cuenta que muchos de estos productos ilegales.*?redes sociales[^\.]*\.)",
    ]
    for pattern in fallback_patterns:
        match = re.search(pattern, haystack)
        if match:
            actions.append(normalize_text(match.group(1)))

    dedup: list[str] = []
    seen = set()
    for item in actions:
        key = normalize_for_matching(item)
        if key and key not in seen:
            dedup.append(item)
            seen.add(key)

    return dedup[:6]


def build_opdivo_comparative_fallback(full_text: str) -> list[dict]:
    patterns = {
        "FRASCO": r"(?is)FRASCO\s*[:\-]?\s*(Pl[aá]stico color blanco)\s*(Vial de vidrio incoloro\s*\(transparente\))",
        "CAJA": r"(?is)CAJA\s*[:\-]?\s*(No presenta)\s*(Presenta)",
        "ETIQUETA": r"(?is)ETIQUETA\s*[:\-]?\s*(No indica N[°º]\s*de lote)\s*(Indica N[°º]\s*de lote)",
        "CONCENTRACIONES": r"(?is)CONCENTRACIONES\s*[:\-]?\s*(150\s*mg\s*/\s*15\s*mL\s*\(15\s*mg\s*/\s*mL\))\s*(100\s*mg\s*/\s*10\s*mL\s*\(10\s*mg\s*/\s*mL\)\s*y\s*40\s*mg\s*/\s*4\s*mL\s*\(10\s*mg\s*/\s*mL\))",
        "FORMA FARMACEUTICA": r"(?is)FORMA FARMACEUTICA\s*[:\-]?\s*(Polvo)\s*(Soluci[oó]n inyectable)",
        "ALMACENAMIENTO": r"(?is)ALMACENAMIENTO\s*[:\-]?\s*(Almacenar a temperatura menor que 30[°º]C)\s*(Almacenar de 2[°º]C a 8[°º]C)",
        "IDIOMA DEL ROTULADO": r"(?is)IDIOMA DEL ROTULADO\s*[:\-]?\s*(Ingl[eé]s)\s*(Espa[nñ]ol)",
    }
    rows: list[dict] = []
    for key, pattern in patterns.items():
        match = re.search(pattern, full_text)
        if not match:
            continue
        rows.append({
            "caracteristica": key,
            "producto_sin_rs": normalize_text(match.group(1)),
            "producto_con_rs": normalize_text(match.group(2)),
            "raw_line": normalize_text(match.group(0)),
        })
    return rows


def extract_comparative_characteristics(lines: list[str], full_text: str | None = None) -> tuple[dict, bool]:
    rows: list[dict] = []
    comparative_header_detected = False
    normalized_lines = [normalize_text(line) for line in lines if normalize_text(line)]
    comparative_start = None
    comparative_end = len(normalized_lines)
    row_keys_normalized = [normalize_for_matching(key) for key in COMPARATIVE_ROW_KEYS]

    for i, line in enumerate(normalized_lines):
        norm = normalize_for_matching(line)
        if "PRODUCTO SIN R.S. EN PERU" in norm and "PRODUCTO CON R.S. EN PERU" in norm:
            comparative_header_detected = True
            comparative_start = i + 1
            break

    if comparative_start is None:
        for i, line in enumerate(normalized_lines):
            norm = normalize_for_matching(line)
            if "PRODUCTO SIN R.S. EN PERU" in norm:
                for j in range(i, min(i + 4, len(normalized_lines))):
                    if "PRODUCTO CON R.S. EN PERU" in normalize_for_matching(normalized_lines[j]):
                        comparative_header_detected = True
                        comparative_start = j + 1
                        break
                if comparative_start is not None:
                    break

    if comparative_start is not None:
        for i in range(comparative_start, len(normalized_lines)):
            norm = normalize_for_matching(normalized_lines[i])
            if any(marker in norm for marker in ["RECOMENDACIONES", "SE RECOMIENDA", "SE EXHORTA", "LIMA,"]):
                comparative_end = i
                break

    candidate_lines = normalized_lines[comparative_start:comparative_end] if comparative_start is not None else []

    for i, line in enumerate(candidate_lines):
        norm = normalize_for_matching(line)
        for key in COMPARATIVE_ROW_KEYS:
            key_norm = normalize_for_matching(key)
            if not norm.startswith(key_norm):
                continue

            candidate = line
            if i + 1 < len(candidate_lines):
                next_line = candidate_lines[i + 1]
                next_norm = normalize_for_matching(next_line)
                if key_norm not in next_norm and not any(
                    row_key in next_norm for row_key in row_keys_normalized
                ):
                    candidate = normalize_text(f"{candidate} {next_line}")

            after = re.split(rf"(?i){re.escape(key)}\s*[:\-]?\s*", candidate, maxsplit=1)
            payload = after[1].strip() if len(after) > 1 else candidate
            cells = split_table_cells(payload)

            producto_sin_rs = None
            producto_con_rs = None
            if len(cells) >= 3:
                producto_sin_rs = cells[1]
                producto_con_rs = " ".join(cells[2:])
            elif len(cells) == 2:
                producto_sin_rs = cells[0]
                producto_con_rs = cells[1]
            else:
                parts = [normalize_text(p) for p in re.split(r"\s{2,}", payload) if normalize_text(p)]
                if len(parts) >= 2:
                    producto_sin_rs = parts[0]
                    producto_con_rs = " ".join(parts[1:])
                elif "|" in payload:
                    pipe_parts = [normalize_text(p) for p in payload.split("|") if normalize_text(p)]
                    if len(pipe_parts) >= 2:
                        producto_sin_rs = pipe_parts[0]
                        producto_con_rs = " ".join(pipe_parts[1:])

            rows.append({
                "caracteristica": key_norm.replace("Á", "A"),
                "producto_sin_rs": producto_sin_rs,
                "producto_con_rs": producto_con_rs,
                "raw_line": candidate,
            })
            break

    if full_text and len([r for r in rows if r.get("producto_sin_rs") and r.get("producto_con_rs")]) < 6:
        fallback_rows = build_opdivo_comparative_fallback(full_text)
        if fallback_rows:
            rows = fallback_rows

    complete_rows = [
        row for row in rows
        if row.get("producto_sin_rs") and row.get("producto_con_rs")
    ]

    return {
        "profile": COMPARATIVE_PROFILE,
        "comparative_header_detected": comparative_header_detected,
        "rows": rows,
        "rows_complete": len(complete_rows),
    }, bool(rows)


def extract_narrative_illegal_product(
    document: dict,
    lines: list[str],
    full_text: str,
    alert_number: str | None,
    alert_type: str,
) -> tuple[list[dict], dict]:
    normalized_text = normalize_for_matching(full_text)
    if not has_narrative_illegal_markers(lines):
        return [], {
            "table_detected": False,
            "comparative_table_detected": False,
            "manual_review_required": False,
            "reason": "Sin patrones narrativos de comercializacion ilegal",
        }

    comparative_table, comparative_detected = extract_comparative_characteristics(lines, full_text)
    recommendations = extract_recommended_actions(lines, full_text)

    product_name = None
    if "OPDIVO" in normalized_text:
        product_name = "OPDIVO"
    else:
        m = re.search(r"(?i)\bproducto\s+([A-Z0-9][A-Z0-9\-\s]{2,40})\b", full_text)
        if m:
            product_name = normalize_text(m.group(1))

    active_ingredient = None
    if "NIVOLUMAB" in normalized_text:
        active_ingredient = "nivolumab"
    else:
        m = re.search(r"(?i)(principio activo|dci)\s*[:\-]?\s*([A-Za-z0-9\-\s]{3,80})", full_text)
        if m:
            active_ingredient = normalize_text(m.group(2))

    concentration = None
    m_conc = re.search(r"(?i)\b\d{1,4}\s*mg\s*/\s*\d{1,4}\s*m[lL]\b", full_text)
    if m_conc:
        concentration = normalize_text(m_conc.group(0))

    dosage_values: list[str] = []
    if re.search(r"(?i)soluci[oó]n\s+inyectable", full_text):
        dosage_values.append("solución inyectable")
    if re.search(r"(?i)\bpolvo\b", full_text):
        dosage_values.append("polvo")
    dosage_form = " / ".join(dosage_values) if dosage_values else None
    dosage_observation = None
    if len(dosage_values) > 1:
        dosage_observation = "El comparativo muestra más de una forma farmacéutica reportada."

    sanitary_registration = None
    m_rs = re.search(r"\b[A-Z]{1,3}\s*-\s*\d{4,6}\b", full_text)
    if m_rs:
        sanitary_registration = normalize_text(m_rs.group(0)).replace(" ", "")

    holder = None
    m_holder_precise = re.search(
        r"(?i)en\s+el\s+per[uú]\s+es\s+la\s+empresa\s+(.+?)\s+el\s+titular",
        full_text,
    )
    if m_holder_precise:
        holder = normalize_text(m_holder_precise.group(1)).strip(" ,.;:")
    else:
        m_holder = re.search(
            r"(?i)(titular del registro sanitario)\s*[:\-]?\s*([^\n\.]{4,180})",
            full_text,
        )
        if m_holder:
            holder = normalize_text(m_holder.group(2))

    manufacturer = None
    m_man_precise = re.search(
        r"(?i)el\s+producto\s+original\s+es\s+manufacturado\s+y\s+acondicionado\s+por\s+(.+?)\s+con\s+ubicaci[oó]n",
        full_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m_man_precise:
        manufacturer = normalize_text(m_man_precise.group(1)).strip(" ,.;:")
    else:
        m_man = re.search(r"(?i)\bfabricante\s*[:\-]?\s*([^\n]{4,220})", full_text)
        if m_man:
            manufacturer = normalize_text(m_man.group(1))

    manufacturer_country = None
    m_country_precise = re.search(
        r"(?i)con\s+ubicaci[oó]n\s+en\s+([^,\n]+),\s*puerto\s+rico\s*[-–]\s*estados\s+unidos\s+de\s+americ[aa]",
        full_text,
    )
    if m_country_precise or (
        re.search(r"(?i)puerto rico", full_text) and re.search(r"(?i)estados unidos", full_text)
    ):
        manufacturer_country = "Puerto Rico / Estados Unidos de América"

    alert_reason = "comercialización ilegal / producto sin registro sanitario en Perú"
    if "PRESUNTA FALSIFIC" in normalized_text:
        alert_reason += " / presunta falsificación"

    risk_summary = None
    m_risk = re.search(
        r"(?i)(no cuenta con registro sanitario|producto sin registro sanitario|producto sospechoso|presunta falsificaci[oó]n)[^\n\.]{0,240}",
        full_text,
    )
    if m_risk:
        risk_summary = normalize_text(m_risk.group(0))

    rows = comparative_table.get("rows") or []
    rows_complete = comparative_table.get("rows_complete") or 0
    comparative_complete = rows_complete >= 6
    critical_fields = [product_name, sanitary_registration, holder, manufacturer, manufacturer_country, active_ingredient, concentration]
    critical_score = sum(1 for item in critical_fields if item)
    if comparative_complete and critical_score >= 6:
        confidence = 0.92
    elif rows and critical_score >= 5:
        confidence = 0.82
    else:
        confidence = 0.76

    metadata = {
        "source": "page_text_concat",
        "alert_profile": NARRATIVE_EXTRACTION_METHOD,
        "active_ingredient": active_ingredient,
        "concentration": concentration,
        "dosage_form": dosage_form,
        "dosage_observation": dosage_observation,
        "alert_reason": alert_reason,
        "risk_summary": risk_summary,
        "recommended_actions": recommendations,
        "comparative_characteristics": comparative_table,
    }

    derived_alert_type = alert_type
    if "COMERCIALIZACION ILEGAL" in normalized_text and "NO CUENTA CON REGISTRO SANITARIO" in normalized_text:
        derived_alert_type = "comercializacion_ilegal_producto_sin_rs"

    raw_block_lines = [
        line for line in lines
        if any(
            marker in normalize_for_matching(line)
            for marker in ["COMERCIALIZACION ILEGAL", "NO CUENTA CON REGISTRO SANITARIO", "OPDIVO", "NIVOLUMAB"]
        )
    ][:40]

    product = build_partial_product(
        document,
        alert_number,
        derived_alert_type,
        raw_block_lines,
        {
            "product_name": product_name,
            "sanitary_registration": sanitary_registration,
            "registration_holder": holder,
            "manufacturer": manufacturer,
            "manufacturer_country": manufacturer_country,
            "analytical_result": risk_summary,
            "extraction_method": NARRATIVE_EXTRACTION_METHOD,
            "confidence": confidence,
            "metadata": metadata,
        },
        confidence=confidence,
    )

    if not product_name:
        return [], {
            "table_detected": False,
            "comparative_table_detected": comparative_detected,
            "manual_review_required": True,
            "reason": "Perfil narrativo detectado, pero sin producto confiable",
        }

    return [product], {
        "table_detected": comparative_detected,
        "comparative_table_detected": comparative_detected,
        "manual_review_required": confidence < 0.85 or not comparative_complete,
        "alert_type_override": derived_alert_type,
        "reason": "Extraccion narrativa completada",
    }


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

    if has_narrative_illegal_markers(lines):
        return "narrativo_ilegal"

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
    """
    Reconstruye fabricante y país desde fragmentos de layout.
    Maneja casos como:
    - Indeurec S.A. /Ecuador
    - Laboratorio Cifarma S.A.C. / Perú
    - Laboratorio Mega Labs S.A. / Uruguay
    - Fabricante en una línea y país en la siguiente
    """

    country_canonical = {
        "PERU": "Perú",
        "ECUADOR": "Ecuador",
        "URUGUAY": "Uruguay",
        "ARGENTINA": "Argentina",
        "INDIA": "India",
        "FRANCIA": "Francia",
        "REINO UNIDO": "Reino Unido",
        "ESTADOS UNIDOS": "Estados Unidos",
        "CHINA": "China",
        "CHILE": "Chile",
        "COLOMBIA": "Colombia",
        "BRASIL": "Brasil",
        "MEXICO": "México",
        "PANAMA": "Panamá",
        "ESPANA": "España",
        "ALEMANIA": "Alemania",
        "ITALIA": "Italia",
        "SUIZA": "Suiza",
        "CANADA": "Canadá",
        "JAPON": "Japón",
        "VENEZUELA": "Venezuela",
    }

    def detect_country(value: str | None) -> str | None:
        normalized_value = normalize_for_matching(value)
        if not normalized_value:
            return None

        for country in sorted(COUNTRY_NAMES, key=len, reverse=True):
            normalized_country = normalize_for_matching(country)
            if re.search(rf"(?<![A-Z]){re.escape(normalized_country)}(?![A-Z])", normalized_value):
                return country_canonical.get(normalized_country, country.title())

        return None

    cleaned_fragments = [
        cleanup_manufacturer_fragment(fragment)
        for fragment in manufacturer_fragments
    ]

    cleaned_fallback = [
        cleanup_manufacturer_fragment(fragment)
        for fragment in fallback_country_fragments
    ]

    # Une fragmentos sin duplicarlos, porque a veces se envía la misma lista dos veces.
    all_fragments = []
    for fragment in cleaned_fragments + cleaned_fallback:
        if fragment and fragment not in all_fragments:
            all_fragments.append(fragment)

    combined = join_lines(all_fragments)

    if not combined:
        return None, None

    combined = normalize_text(combined)

    # Caso con separador "/"
    if "/" in combined:
        parts = [
            normalize_text(part)
            for part in re.split(r"\s*/\s*", combined)
            if normalize_text(part)
        ]

        if parts:
            manufacturer = cleanup_manufacturer_fragment(parts[0])
            right_side = join_lines(parts[1:]) if len(parts) > 1 else None

            country = detect_country(right_side) or detect_country(combined)

            if country:
                return manufacturer, country

            return manufacturer, cleanup_manufacturer_fragment(right_side)

    # Caso sin separador pero con país dentro del texto.
    country = detect_country(combined)
    if country:
        normalized_country = normalize_for_matching(country)
        tokens = combined.split()

        manufacturer_tokens = [
            token
            for token in tokens
            if normalize_for_matching(token) != normalized_country
        ]

        manufacturer = cleanup_manufacturer_fragment(" ".join(manufacturer_tokens))
        return manufacturer, country

    return cleanup_manufacturer_fragment(combined), None

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

def looks_like_rowspan_lot_start(value: str | None) -> bool:
    """
    Detecta lotes en tablas con celdas combinadas.
    Soporta casos:
    - 0380-20
    - 1984385
    - 2020381
    - M07484
    - M0655 (envase mediato)
    - 8902*
    - 4344**
    """
    text = normalize_text(value)
    if not text:
        return False

    normalized = normalize_for_matching(text)

    invalid = {
        "LOTE",
        "NOMBRE",
        "FABRICANTE",
        "PAIS",
        "DEPARTAMENTO",
        "DIRECCION",
        "DIRECCIÓN",
    }

    if normalized in invalid:
        return False

    if not re.search(r"\d", text):
        return False

    # Código de lote con posible paréntesis.
    return bool(
        re.match(
            r"^[A-Z0-9][A-Z0-9\-]{2,}\*{0,2}(?:\s*\(.*)?$",
            text,
            flags=re.IGNORECASE,
        )
    )

def normalize_lot_ocr(value: str | None) -> str | None:
    """
    Corrige errores comunes OCR/layout en números de lote.
    Ejemplo:
    MO655 -> M0655
    """
    text = clean_final_field(value)

    if not text:
        return None

    # Caso típico: letra O confundida con cero después de M
    text = re.sub(r"^M[OÓ](\d)", r"M0\1", text, flags=re.IGNORECASE)

    # Normaliza espacios dentro de paréntesis
    text = re.sub(r"\(\s+", "(", text)
    text = re.sub(r"\s+\)", ")", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def looks_like_lot_continuation(value: str | None) -> bool:
    """
    Detecta continuación de lote cuando el PDF parte el texto en varias líneas.
    Ejemplo:
    M0655 (envase
    mediato)
    """
    text = normalize_text(value)
    normalized = normalize_for_matching(text)

    if not text:
        return False

    if looks_like_rowspan_lot_start(text):
        return False

    continuation_markers = [
        "ENVASE",
        "MEDIATO",
        "INMEDIATO",
        "MEDIATA",
        "INMEDIATA",
    ]

    return ")" in text or any(marker in normalized for marker in continuation_markers)


def merge_rowspan_lot_lines(lot_lines: list[str]) -> list[str]:
    """
    Une lotes partidos en varias líneas.
    Ejemplo:
    ['M0655 (envase', 'mediato)', 'M06551 (envase', 'inmediato)']
    ->
    ['M0655 (envase mediato)', 'M06551 (envase inmediato)']
    """
    lots: list[str] = []
    current: str | None = None

    for raw in lot_lines:
        text = clean_final_field(raw)
        if not text:
            continue

        if looks_like_rowspan_lot_start(text):
            if current:
                lots.append(clean_final_field(current))
            current = text
            continue

        if current and looks_like_lot_continuation(text):
            current = normalize_text(f"{current} {text}")
            continue

        if current:
            lots.append(clean_final_field(current))
            current = None

    if current:
        lots.append(clean_final_field(current))

    # Quitar duplicados conservando orden
    unique_lots = []
    for lot in lots:
        if lot and lot not in unique_lots:
            unique_lots.append(lot)

    return unique_lots


def looks_like_packaging_line(value: str | None) -> bool:
    text = normalize_for_matching(value)

    if not text:
        return False

    packaging_starts = [
        "FOLIO DE ALUMINIO",
        "SOLUCION",
        "SOLUCIÓN",
        "CAJA X",
        "TABLETA",
        "BLISTER",
        "BLÍSTER",
        "FRASCO",
        "NATURAL SPRAY",
        "POLVO PARA",
    ]

    return any(text.startswith(item) for item in packaging_starts)


def looks_like_product_start_line(value: str | None) -> bool:
    """
    Detecta si una línea de la columna producto parece iniciar un nuevo producto.
    No considera como nuevo producto líneas de presentación/envase.
    """
    text = normalize_text(value)
    normalized = normalize_for_matching(text)

    if not text:
        return False

    if is_layout_header_value(text):
        return False

    if looks_like_rowspan_lot_start(text):
        return False

    if looks_like_packaging_line(text):
        return False

    invalid_fragments = [
        "PRODUCTOS INCAUTADOS",
        "ACCIONES DE CONTROL",
        "DIRECCION",
        "DIRECCIÓN",
        "ESTABLECIMIENTO",
        "CENTRO COMERCIAL",
    ]

    if any(fragment in normalized for fragment in invalid_fragments):
        return False

    return True

def canonical_country(value: str | None) -> str | None:
    """
    Devuelve el país normalizado solo si realmente es un país conocido.
    Evita guardar basura como 'Centro', 'Centro en', 'Tienda', etc.
    """
    text = clean_final_field(value)
    normalized = normalize_for_matching(text)

    if not normalized:
        return None

    country_map = {
        "PERU": "Perú",
        "PERÚ": "Perú",
        "ECUADOR": "Ecuador",
        "URUGUAY": "Uruguay",
        "FRANCIA": "Francia",
        "INDIA": "India",
        "ARGENTINA": "Argentina",
        "UK": "UK (Reino Unido)",
        "REINO UNIDO": "Reino Unido",
        "UK REINO UNIDO": "UK (Reino Unido)",
        "UK (REINO UNIDO)": "UK (Reino Unido)",
        "ESTADOS UNIDOS": "Estados Unidos",
        "CHINA": "China",
        "CHILE": "Chile",
        "COLOMBIA": "Colombia",
        "BRASIL": "Brasil",
        "MEXICO": "México",
        "MÉXICO": "México",
        "ESPAÑA": "España",
        "ESPANA": "España",
        "ALEMANIA": "Alemania",
        "ITALIA": "Italia",
        "SUIZA": "Suiza",
        "CANADA": "Canadá",
        "CANADÁ": "Canadá",
        "JAPON": "Japón",
        "JAPÓN": "Japón",
    }

    if normalized in country_map:
        return country_map[normalized]

    return None


def extract_country_prefix(value: str | None) -> tuple[str | None, str | None]:
    """
    Si una celda empieza con un país, separa:
    'Francia Cosméticos incautados...' -> ('Francia', 'Cosméticos incautados...')
    """
    text = clean_final_field(value)

    if not text:
        return None, None

    candidates = [
        "UK (Reino Unido)",
        "Reino Unido",
        "Estados Unidos",
        "Francia",
        "Perú",
        "Ecuador",
        "Uruguay",
        "India",
        "Argentina",
        "China",
        "Chile",
        "Colombia",
        "Brasil",
        "México",
        "España",
        "Alemania",
        "Italia",
        "Suiza",
        "Canadá",
        "Japón",
    ]

    for country in candidates:
        pattern = rf"^\s*{re.escape(country)}\b[\.;,:\s-]*"
        if re.search(pattern, text, flags=re.IGNORECASE):
            remaining = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
            return canonical_country(country), remaining or None

    return None, text

def extract_country_anywhere(value: str | None) -> str | None:
    """
    Detecta país aunque esté dentro de una línea mezclada.
    Ejemplo:
    'P&G Prestige Beaute Geneva / UK (Reino Unido)' -> 'UK (Reino Unido)'
    """
    text = clean_final_field(value)

    if not text:
        return None

    normalized = normalize_for_matching(text)

    country_patterns = [
        ("UK (Reino Unido)", [r"\bUK\s*\(?\s*REINO UNIDO\s*\)?", r"\bUK\b"]),
        ("Reino Unido", [r"\bREINO UNIDO\b"]),
        ("Francia", [r"\bFRANCIA\b"]),
        ("Perú", [r"\bPERU\b", r"\bPERÚ\b"]),
        ("Ecuador", [r"\bECUADOR\b"]),
        ("Uruguay", [r"\bURUGUAY\b"]),
        ("India", [r"\bINDIA\b"]),
        ("Argentina", [r"\bARGENTINA\b"]),
        ("Estados Unidos", [r"\bESTADOS UNIDOS\b"]),
        ("China", [r"\bCHINA\b"]),
        ("Chile", [r"\bCHILE\b"]),
        ("Colombia", [r"\bCOLOMBIA\b"]),
        ("Brasil", [r"\bBRASIL\b"]),
        ("México", [r"\bMEXICO\b", r"\bMÉXICO\b"]),
        ("España", [r"\bESPANA\b", r"\bESPAÑA\b"]),
        ("Alemania", [r"\bALEMANIA\b"]),
        ("Italia", [r"\bITALIA\b"]),
        ("Suiza", [r"\bSUIZA\b"]),
        ("Canadá", [r"\bCANADA\b", r"\bCANADÁ\b"]),
        ("Japón", [r"\bJAPON\b", r"\bJAPÓN\b"]),
    ]

    for canonical, patterns in country_patterns:
        for pattern in patterns:
            if re.search(pattern, normalized, flags=re.IGNORECASE):
                return canonical

    return None


def clean_rowspan_manufacturer(value: str | None) -> str | None:
    """
    Limpia fabricante en tablas con celdas combinadas.
    Evita que se mezclen palabras de la dirección como 'sanitaria'.
    """
    text = cleanup_manufacturer_fragment(value)

    if not text:
        return None

    text = re.sub(r"\bsanitaria\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" ,;/")

    return text or None


def find_country_near_y(
    zone_lines: list[dict],
    target_y: float,
    max_gap: float = 80.0,
) -> str | None:
    """
    Busca país cerca de la coordenada Y del fabricante/lote.
    Sirve cuando el país cae desplazado en la columna de intervención,
    fabricante/país o dentro del full_text.
    """
    candidates: list[tuple[float, int, str]] = []

    for line in zone_lines:
        y = get_line_y(line)
        gap = abs(y - target_y)

        if gap > max_gap:
            continue

        values_to_check = [
            line.get("col_fabricante_pais"),
            line.get("col_intervencion"),
            line.get("full_text"),
        ]

        for value in values_to_check:
            text = normalize_text(value)

            if not text:
                continue

            # Prioridad 1: país exacto o al inicio.
            country_from_prefix, _remaining = extract_country_prefix(text)
            if country_from_prefix:
                candidates.append((gap, 1, country_from_prefix))
                continue

            # Prioridad 2: celda completa exactamente igual a un país.
            country_exact = canonical_country(text)
            if country_exact:
                candidates.append((gap, 2, country_exact))
                continue

            # Prioridad 3: país dentro de una línea mezclada.
            country_inside = extract_country_anywhere(text)
            if country_inside:
                candidates.append((gap, 3, country_inside))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]

def split_rowspan_manufacturer_country(value: str | None) -> tuple[str | None, str | None]:
    """
    Separa Fabricante / País.
    Maneja:
    - Indeurec S.A. / Ecuador
    - Laboratorio Cifarma S.A.C. Perú
    - Laboratorio Mega Labs S.A. Uruguay
    - P&G Prestige Beaute Geneva UK (Reino Unido)
    """
    text = normalize_text(value)

    if not text:
        return None, None

    text = re.sub(r"\s+", " ", text).strip()

    def remove_country_from_text(source: str, country: str | None) -> str:
        cleaned = source

        country_patterns = [
            r"\bUK\s*\(?\s*REINO UNIDO\s*\)?",
            r"\bREINO UNIDO\b",
            r"\bFRANCIA\b",
            r"\bPERU\b",
            r"\bPERÚ\b",
            r"\bECUADOR\b",
            r"\bURUGUAY\b",
            r"\bINDIA\b",
            r"\bARGENTINA\b",
            r"\bESTADOS UNIDOS\b",
            r"\bCHINA\b",
            r"\bCHILE\b",
            r"\bCOLOMBIA\b",
            r"\bBRASIL\b",
            r"\bMEXICO\b",
            r"\bMÉXICO\b",
            r"\bESPAÑA\b",
            r"\bESPANA\b",
            r"\bALEMANIA\b",
            r"\bITALIA\b",
            r"\bSUIZA\b",
            r"\bCANADA\b",
            r"\bCANADÁ\b",
            r"\bJAPON\b",
            r"\bJAPÓN\b",
        ]

        for pattern in country_patterns:
            cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)

        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;/")
        return cleaned

    # Caso con separador "/"
    if "/" in text:
        parts = [
            normalize_text(part)
            for part in re.split(r"\s*/\s*", text)
            if normalize_text(part)
        ]

        manufacturer = cleanup_manufacturer_fragment(parts[0]) if parts else None
        country_text = join_lines(parts[1:]) if len(parts) > 1 else None

        country = canonical_country(country_text) or extract_country_anywhere(country_text)

        return manufacturer, country

    # Caso sin separador, pero con país dentro del mismo texto.
    country = canonical_country(text) or extract_country_anywhere(text)

    if country:
        manufacturer_text = remove_country_from_text(text, country)
        manufacturer = cleanup_manufacturer_fragment(manufacturer_text)
        return manufacturer, country

    return cleanup_manufacturer_fragment(text), None
def get_line_y(line: dict) -> float:
    try:
        return float(line.get("y_group") or 0)
    except (TypeError, ValueError):
        return 0.0


def is_country_only_text(value: str | None) -> bool:
    normalized = normalize_for_matching(value)

    if not normalized:
        return False

    country_aliases = {
        "PERU",
        "PERÚ",
        "ECUADOR",
        "URUGUAY",
        "FRANCIA",
        "INDIA",
        "ARGENTINA",
        "UK",
        "UK (REINO UNIDO)",
        "REINO UNIDO",
    }

    normalized_countries = {normalize_for_matching(country) for country in COUNTRY_NAMES}

    return normalized in normalized_countries or normalized in country_aliases


def looks_like_manufacturer_line(value: str | None) -> bool:
    text = normalize_text(value)
    normalized = normalize_for_matching(text)

    if not text:
        return False

    if is_layout_header_value(text):
        return False

    if is_country_only_text(text):
        return False

    manufacturer_markers = [
        "LABORATORIO",
        "LABS",
        "PHARMA",
        "FARMA",
        "S.A.",
        "S.A.C.",
        "SAC",
        "INC",
        "P&G",
        "PRESTIGE",
        "INDEUREC",
        "CATALENT",
        "GLOBELA",
        "SWISS",
        "ZEE",
    ]

    return "/" in text or any(marker in normalized for marker in manufacturer_markers)


def build_product_blocks_by_y(zone_lines: list[dict]) -> list[dict]:
    blocks: list[dict] = []
    current: dict | None = None

    def flush():
        nonlocal current
        if current and current["lines"]:
            current["text"] = join_lines(current["lines"])
            current["center_y"] = sum(current["ys"]) / len(current["ys"])
            blocks.append(current)
        current = None

    for line in zone_lines:
        text = normalize_text(line.get("col_producto"))
        y = get_line_y(line)

        if not text or is_layout_header_value(text):
            continue

        if looks_like_product_start_line(text):
            flush()
            current = {
                "lines": [text],
                "ys": [y],
                "start_y": y,
                "end_y": y,
            }
            continue

        if current and looks_like_packaging_line(text):
            current["lines"].append(text)
            current["ys"].append(y)
            current["end_y"] = y
            continue

        if current and not looks_like_rowspan_lot_start(text):
            # Permite completar nombres largos partidos en varias líneas.
            normalized = normalize_for_matching(text)
            invalid_fragments = [
                "PRODUCTOS INCAUTADOS",
                "ACCIONES DE CONTROL",
                "DIRECCION",
                "DIRECCIÓN",
                "DEPARTAMENTO",
                "FABRICANTE",
                "LOTE",
            ]

            if not any(fragment in normalized for fragment in invalid_fragments):
                current["lines"].append(text)
                current["ys"].append(y)
                current["end_y"] = y

    flush()
    return blocks


def build_manufacturer_blocks_by_y(zone_lines: list[dict]) -> list[dict]:
    blocks: list[dict] = []
    current: dict | None = None

    def flush():
        nonlocal current
        if current and current["lines"]:
            raw_text = join_lines(current["lines"])
            manufacturer, country = split_rowspan_manufacturer_country(raw_text)

            current["raw_text"] = raw_text
            current["manufacturer"] = manufacturer
            current["manufacturer_country"] = country
            current["center_y"] = sum(current["ys"]) / len(current["ys"])

            if manufacturer or country:
                blocks.append(current)

        current = None

    for line in zone_lines:
        text = normalize_text(line.get("col_fabricante_pais"))
        y = get_line_y(line)

        possible_country, _remaining_intervention = extract_country_prefix(
            line.get("col_intervencion")
        )

        if text and text.endswith("/") and possible_country:
            text = normalize_text(f"{text} {possible_country}")

        if not text or is_layout_header_value(text):
            continue

        starts_manufacturer = looks_like_manufacturer_line(text)

        if starts_manufacturer:
            if current:
                previous_y = current["ys"][-1]
                gap = abs(y - previous_y)

                # Si está muy cerca, probablemente es continuación del mismo fabricante.
                if gap <= 18:
                    current["lines"].append(text)
                    current["ys"].append(y)
                    current["end_y"] = y
                    continue

                flush()

            current = {
                "lines": [text],
                "ys": [y],
                "start_y": y,
                "end_y": y,
            }
            continue

        if current and is_country_only_text(text):
            current["lines"].append(text)
            current["ys"].append(y)
            current["end_y"] = y
            continue

        if current and "/" in text:
            current["lines"].append(text)
            current["ys"].append(y)
            current["end_y"] = y
            continue

    flush()
    return blocks


def extract_lot_items_by_y(zone_lines: list[dict]) -> list[dict]:
    lot_items: list[dict] = []
    current: dict | None = None

    def flush():
        nonlocal current
        if current and current.get("lines"):
            raw_lot = join_lines(current["lines"])
            lot_number = normalize_lot_ocr(raw_lot)

            if lot_number:
                current["lot_number"] = lot_number
                current["center_y"] = sum(current["ys"]) / len(current["ys"])
                lot_items.append(current)

        current = None

    for line in zone_lines:
        text = normalize_text(line.get("col_lote"))
        y = get_line_y(line)

        if not text or is_layout_header_value(text):
            continue

        text = normalize_lot_ocr(text)

        if not text:
            continue

        if looks_like_rowspan_lot_start(text):
            flush()
            current = {
                "lines": [text],
                "ys": [y],
                "start_y": y,
                "end_y": y,
                "raw_lines": [normalize_text(line.get("full_text"))],
            }
            continue

        if current and looks_like_lot_continuation(text):
            current["lines"].append(text)
            current["ys"].append(y)
            current["end_y"] = y
            current["raw_lines"].append(normalize_text(line.get("full_text")))
            continue

    flush()

    # Quitar duplicados conservando orden
    unique_items: list[dict] = []
    seen = set()

    for item in lot_items:
        lot = item.get("lot_number")
        if lot in seen:
            continue
        seen.add(lot)
        unique_items.append(item)

    return unique_items


def nearest_block_by_y(y: float, blocks: list[dict]) -> dict | None:
    if not blocks:
        return None

    return min(
        blocks,
        key=lambda block: abs(float(block.get("center_y") or 0) - y)
    )

def choose_rowspan_block_for_lot(
    lot_y: float,
    blocks: list[dict],
    forward_gap: float = 45.0,
) -> dict | None:
    """
    Selecciona el bloque correcto para un lote en tablas con celdas combinadas.

    Criterio:
    - Si el lote cae dentro del bloque, usa ese bloque.
    - Si el lote está justo antes del siguiente producto/fabricante,
      pertenece al siguiente bloque.
    - Si no se cumple lo anterior, usa cercanía vertical.
    """
    if not blocks:
        return None

    sorted_blocks = sorted(
        blocks,
        key=lambda block: float(block.get("center_y") or 0)
    )

    # 1. Si el lote cae dentro del rango vertical del bloque.
    for block in sorted_blocks:
        center_y = float(block.get("center_y") or 0)
        start_y = float(block.get("start_y") or center_y)
        end_y = float(block.get("end_y") or center_y)

        if start_y - 6 <= lot_y <= end_y + 6:
            return block

    previous_block = None
    next_block = None

    for block in sorted_blocks:
        center_y = float(block.get("center_y") or 0)

        if center_y < lot_y:
            previous_block = block
            continue

        next_block = block
        break

    # 2. Si el lote está justo antes del siguiente bloque,
    # pertenece al siguiente bloque.
    if next_block:
        next_center = float(next_block.get("center_y") or 0)
        next_start = float(next_block.get("start_y") or next_center)

        previous_end = None
        if previous_block:
            previous_center = float(previous_block.get("center_y") or 0)
            previous_end = float(previous_block.get("end_y") or previous_center)

        if lot_y <= next_start and (next_start - lot_y) <= forward_gap:
            return next_block

        if previous_end is not None and previous_end <= lot_y <= next_start:
            return next_block

    # 3. Fallback: cercanía vertical.
    return nearest_block_by_y(lot_y, sorted_blocks)

def extract_rowspan_products_from_layout(
    document: dict,
    alert_number: str | None,
    alert_type: str,
    layout_lines: list[dict],
    zone_lines: list[dict],
    start_index: int | None,
    end_index: int | None,
    detected_columns: list[str],
    table_profile: str,
) -> tuple[list[dict], dict]:
    """
    Extractor especializado para tablas con celdas combinadas.
    En lugar de cortar por segmentos, asigna cada lote al bloque de producto
    y fabricante más cercano verticalmente.
    """

    # No usar este extractor para tablas con fecha de vencimiento.
    # Esas tablas tienen otra estructura.
    if table_profile == "with_expiry":
        return [], {
            "layout_rowspan_used": False,
            "reason": "No aplica rowspan para tabla con fecha de vencimiento",
        }

    product_blocks = build_product_blocks_by_y(zone_lines)
    manufacturer_blocks = build_manufacturer_blocks_by_y(zone_lines)
    lot_items = extract_lot_items_by_y(zone_lines)

    if len(product_blocks) < 1 or len(lot_items) < 1:
        return [], {
            "layout_rowspan_used": False,
            "reason": "No se detectaron bloques suficientes de producto/lote",
        }

    # Dirección global
    intervention_values = [
        normalize_text(line.get("col_intervencion"))
        for line in zone_lines
        if normalize_text(line.get("col_intervencion"))
        and not is_layout_header_value(line.get("col_intervencion"))
    ]

    global_intervention = join_lines(intervention_values)

    # Departamento global
    department_values = [
        normalize_text(line.get("col_departamento"))
        for line in zone_lines
        if normalize_text(line.get("col_departamento"))
        and not is_layout_header_value(line.get("col_departamento"))
    ]

    global_department = select_layout_department(department_values)

    products: list[dict] = []

    for lot_item in lot_items:
        lot_number = lot_item.get("lot_number")
        lot_y = float(lot_item.get("center_y") or 0)

        product_block = choose_rowspan_block_for_lot(
            lot_y,
            product_blocks,
            forward_gap=55.0,
        )

        manufacturer_block = choose_rowspan_block_for_lot(
            lot_y,
            manufacturer_blocks,
            forward_gap=65.0,
        )

        product_name = product_block.get("text") if product_block else None
        manufacturer = manufacturer_block.get("manufacturer") if manufacturer_block else None
        manufacturer_country = manufacturer_block.get("manufacturer_country") if manufacturer_block else None

        manufacturer = clean_rowspan_manufacturer(manufacturer)

        if not manufacturer_country:
            country_reference_y = (
                float(manufacturer_block.get("center_y") or lot_y)
                if manufacturer_block
                else lot_y
            )

            manufacturer_country = find_country_near_y(
                zone_lines,
                country_reference_y,
                max_gap=45.0,
            )

        if not product_name or not lot_number:
            continue

        raw_block_lines = []

        if product_block:
            raw_block_lines.append(product_block.get("text") or "")

        raw_block_lines.append(lot_number)

        if manufacturer_block:
            raw_block_lines.append(manufacturer_block.get("raw_text") or "")

        if global_intervention:
            raw_block_lines.append(global_intervention)

        if global_department:
            raw_block_lines.append(global_department)

        raw_block = "\n".join([line for line in raw_block_lines if line])

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
                "intervention_address": global_intervention,
                "department": global_department,
                "raw_block": raw_block,
                "extraction_method": ROWSPAN_EXTRACTION_METHOD,
                "metadata": {
                    "source": "layout_words_json",
                    "table_type": "rowspan_merged_cells_y_nearest",
                    "lot_y": lot_y,
                    "product_center_y": product_block.get("center_y") if product_block else None,
                    "manufacturer_center_y": manufacturer_block.get("center_y") if manufacturer_block else None,
                    "layout_line_count": len(layout_lines),
                    "zone_start_index": start_index,
                    "zone_end_index": end_index,
                    "detected_columns": detected_columns,
                    "table_profile": table_profile,
                },
            },
            confidence=0.88 if product_name and lot_number and manufacturer and manufacturer_country and global_department else 0.72,
        )

        products.append(product)

    # Evitar duplicados por document_key + lot_number
    unique_products: list[dict] = []
    seen_lots = set()

    for product in products:
        key = (
            normalize_text(product.get("document_key")),
            normalize_text(product.get("lot_number")),
        )

        if key in seen_lots:
            continue

        seen_lots.add(key)
        unique_products.append(product)

    if len(unique_products) < 1:
        return [], {
            "layout_rowspan_used": False,
            "reason": "Tabla combinada no produjo productos",
        }

    return unique_products, {
        "layout_rowspan_used": True,
        "layout_lines_count": len(layout_lines),
        "layout_columns_detected": detected_columns,
        "reason": "Extraccion por tabla con celdas combinadas usando cercania vertical",
    }

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

    
    rowspan_products, rowspan_summary = extract_rowspan_products_from_layout(
        document=document,
        alert_number=alert_number,
        alert_type=alert_type,
        layout_lines=layout_lines,
        zone_lines=zone_lines,
        start_index=start_index,
        end_index=end_index,
        detected_columns=detected_columns,
        table_profile=table_profile,
    )

    if rowspan_products:
        logger.info(
            "Documento %s | productos extraidos con rowspan: %s",
            document.get("document_key"),
            len(rowspan_products),
        )
        return rowspan_products, rowspan_summary
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

    last_product_context = {
        "product_name": None,
        "manufacturer": None,
        "manufacturer_country": None,
        "department": None,
    }

    # Cambio 3:
    # Antes el segmento se cortaba mal y arrastraba líneas del producto anterior.
    # Ahora cada segmento nace desde la fila del lote y llega hasta antes del siguiente lote.
    if len(lot_rows) == 1 and lot_rows[0] == len(zone_lines) - 1:
        segment_ranges = [(0, len(zone_lines))]
    else:
        segment_ranges = []

        for lot_index, lot_row in enumerate(lot_rows):
            next_lot_row = (
                lot_rows[lot_index + 1]
                if lot_index + 1 < len(lot_rows)
                else len(zone_lines)
            )

            segment_start = lot_row

            # Si la fila del lote no trae nombre de producto, miramos una fila arriba.
            # Esto ayuda cuando el PDF parte el nombre en varias líneas.
            if (
                lot_row > 0
                and not normalize_text(zone_lines[lot_row].get("col_producto"))
            ):
                segment_start = lot_row - 1

            if segment_start < next_lot_row:
                segment_ranges.append((segment_start, next_lot_row))

    for segment_start, segment_end in segment_ranges:
        segment = zone_lines[segment_start:segment_end]

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

            # En tablas con vencimiento, a veces la columna lote trae fragmentos de fabricante/país.
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
            if line.get("col_intervencion")
            and not is_layout_header_value(line.get("col_intervencion"))
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

        lot_number, expiry_date, lot_candidates = select_lot_and_expiry_from_segment(
            segment,
            table_profile,
        )

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

        logger.info("lot_from_product_column=%s", lot_from_product_column)
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

        # Cambio 4:
        # Si el PDF usa celdas combinadas y la fila trae solo lote,
        # reutilizamos el último producto/fabricante válido.
        if not product_name and last_product_context.get("product_name"):
            product_name = last_product_context.get("product_name")

        # Si el nombre empieza con una presentación/envase, intentamos unirlo al último producto.
        # Ejemplo: "folio de aluminio x 4 cápsula" + "HEPABIONTA".
        if product_name and last_product_context.get("product_name"):
            normalized_product = normalize_for_matching(product_name)
            last_name = last_product_context.get("product_name")
            normalized_last = normalize_for_matching(last_name)

            packaging_starts = (
                "FOLIO DE ALUMINIO",
                "BLISTER",
                "BLÍSTER",
                "CAJA X",
                "FRASCO",
                "SOLUCION",
                "SOLUCIÓN",
                "TABLETA",
                "CAPSULA",
                "CÁPSULA",
                "AMPOLLA",
            )

            if normalized_product.startswith(packaging_starts):
                if normalized_last not in normalized_product:
                    product_name = normalize_text(f"{last_name} {product_name}")
                elif not normalized_product.startswith(normalized_last):
                    product_without_last = re.sub(
                        re.escape(last_name),
                        "",
                        product_name,
                        flags=re.IGNORECASE,
                    ).strip()
                    product_name = normalize_text(f"{last_name} {product_without_last}")

        if not manufacturer and last_product_context.get("manufacturer"):
            manufacturer = last_product_context.get("manufacturer")

        if not manufacturer_country and last_product_context.get("manufacturer_country"):
            manufacturer_country = last_product_context.get("manufacturer_country")

        intervention_address = join_lines(intervention_lines)
        department = department_from_same_row or select_layout_department(department_values)

        if not department and last_product_context.get("department"):
            department = last_product_context.get("department")

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
                    "segment_start": segment_start,
                    "segment_end": segment_end,
                    "detected_columns": detected_columns,
                    "table_profile": table_profile,
                },
            },
            confidence=0.75 if product_name and lot_number else 0.65,
        )

        # Cambio 5:
        # Actualizamos contexto para reutilizarlo en filas siguientes con celdas combinadas.
        if product_name:
            last_product_context["product_name"] = product_name

        if manufacturer:
            last_product_context["manufacturer"] = manufacturer

        if manufacturer_country:
            last_product_context["manufacturer_country"] = manufacturer_country

        if department:
            last_product_context["department"] = department

        products.append(product)

    valid_products = [
        product for product in products
        if not is_low_quality_product(product)
    ]

    if not valid_products and products:
        valid_products = [
            product for product in products
            if normalize_text(product.get("product_name"))
            or normalize_text(product.get("lot_number"))
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
    elif extractor_selected == "narrativo_ilegal":
        products, diagnostics = extract_narrative_illegal_product(
            document,
            lines,
            full_text,
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
        "alert_profile": NARRATIVE_EXTRACTION_METHOD if extractor_selected == "narrativo_ilegal" else EXTRACTION_METHOD,
        "extraction_method_used": (
            products[0].get("extraction_method")
            if products and products[0].get("extraction_method")
            else EXTRACTION_METHOD
        ),
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
    cleaned_products = [
        clean_product_record(product)
        for product in products
    ] if products else []

    # Filtro final de seguridad:
    # No guardar registros muy débiles, sin lote y con nombre evidentemente contaminado.
    cleaned_products = [
        product for product in cleaned_products
        if not (
            not product.get("lot_number")
            and float(product.get("confidence") or 0) <= 0.65
        )
    ]

    (
        supabase
        .table(PRODUCT_TABLE)
        .delete()
        .eq("document_id", document_id)
        .execute()
    )

    if cleaned_products:
        (
            supabase
            .table(PRODUCT_TABLE)
            .insert(cleaned_products)
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
    parser.add_argument("--document-key", type=str, default=None)
    args = parser.parse_args()

    load_env()

    supabase = get_supabase()
    documents = get_documents_to_process(supabase, args.limit, args.force, args.document_key)

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
            logger.info(
                "Documento %s | alert_profile: %s | products_extracted: %s | comparative_table_detected: %s | manual_review_required: %s",
                document_key,
                summary.get("alert_profile"),
                len(products),
                summary.get("comparative_table_detected", False),
                summary.get("manual_review_required", False),
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
                extraction_method_used = summary.get("extraction_method_used") or EXTRACTION_METHOD
                process_message = (
                    "Extracción narrativa completada con narrative_illegal_product_v1"
                    if extraction_method_used == NARRATIVE_EXTRACTION_METHOD
                    else (
                        f"Extraccion estructurada completada con {extraction_method_used}. "
                        f"Productos: {len(products)}. "
                        f"Tipo: {summary['alert_type']}."
                    )
                )
                update_document_status(
                    supabase,
                    document_id,
                    "structured_extracted",
                    process_message,
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
                        f"Tipo: {summary['alert_type']}. "
                        + (
                            "manual_review_expected_v1"
                            if summary.get("extractor_selected") == "narrativo_ilegal"
                            else ""
                        )
                    ).strip(),
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
