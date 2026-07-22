import re
import unicodedata
from datetime import datetime
from urllib.parse import urlparse


def clean_text(text: str | None) -> str:
    """Limpia espacios, saltos de línea y tabulaciones."""
    if not text:
        return ""
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return re.sub(r"\s+", " ", text).strip()


def slug_from_url(url: str | None) -> str:
    """Obtiene el último segmento útil de una URL."""
    if not url:
        return ""
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1] if path else ""


def normalize_date(date_text: str | None) -> str | None:
    """Convierte fecha dd/mm/yyyy a yyyy-mm-dd."""
    if not date_text:
        return None

    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", date_text)
    if not match:
        return None

    day, month, year = match.groups()
    return f"{year}-{int(month):02d}-{int(day):02d}"


def extract_date_display(text: str | None) -> str | None:
    """Extrae fecha en formato dd/mm/yyyy si aparece en el texto."""
    if not text:
        return None

    match = re.search(r"\b(\d{1,2}/\d{1,2}/20\d{2})\b", text)
    return match.group(1) if match else None


MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


def extract_day_month_es(text: str | None) -> tuple[int, int] | None:
    """Extrae dia y mes de textos tipo '22 Jul' (la tarjeta de fecha que
    DIGEMID muestra junto a cada alerta, sin anio ni separador '/')."""
    if not text:
        return None

    match = re.search(
        r"\b(\d{1,2})\s+(ene|feb|mar|abr|may|jun|jul|ago|set|sep|oct|nov|dic)[a-z]*\.?\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    day = int(match.group(1))
    month = MESES_ES.get(match.group(2).lower())

    if not month or not (1 <= day <= 31):
        return None

    return day, month


def year_from_document_key(document_key: str | None) -> int | None:
    """Extrae el anio de un document_key tipo '81-2026'."""
    if not document_key:
        return None

    match = re.search(r"(20\d{2})\b", document_key)
    return int(match.group(1)) if match else None


def remove_accents(text: str) -> str:
    """Remueve tildes para construir slugs estables."""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def extract_alert_number(text: str | None, url: str | None = None) -> str | None:
    """
    Extrae patrones como:
    - alerta-digemid-no-41-2026
    - Alerta DIGEMID N° 41-2026
    - 41-2026
    """
    combined = f"{text or ''} {url or ''}".lower()

    patterns = [
        r"alerta[-_\s]*digemid[-_\s]*(?:n[o°º.]*)?[-_\s]*(\d{1,4})[-_/](20\d{2})",
        r"\b(\d{1,4})[-/](20\d{2})\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, combined, flags=re.IGNORECASE)
        if match:
            number = int(match.group(1))
            year = match.group(2)
            return f"{number}-{year}"

    return None


def generate_document_key(title: str | None, url: str | None = None) -> str:
    """
    Genera un document_key determinístico.
    Prioridad:
    1. Número de alerta, por ejemplo 41-2026.
    2. Slug de URL.
    3. Slug del título.
    """
    alert_number = extract_alert_number(title, url)
    if alert_number:
        return alert_number

    slug = slug_from_url(url)
    if slug:
        return slug[:120]

    base = remove_accents(clean_text(title or "documento-digemid")).lower()
    base = re.sub(r"[^\w\s-]", "", base)
    base = re.sub(r"[-\s]+", "-", base).strip("-")

    return base[:120] or "documento-digemid"


def utc_now_iso() -> str:
    """Fecha/hora UTC en formato ISO."""
    return datetime.utcnow().isoformat()


def is_valid_document(doc: dict) -> bool:
    """Valida campos mínimos antes de registrar."""
    return bool(doc.get("document_key") and doc.get("detail_url"))