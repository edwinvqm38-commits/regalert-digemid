"""Extracción de texto de PDFs con alta fidelidad y puntaje de calidad.

Estrategia por página (de más rápida/limpia a más costosa):
  1. PyMuPDF (texto embebido) — perfecto para PDFs digitales con capa de texto.
  2. pdfplumber — reconstruye espacios a partir de la posición de los caracteres,
     corrige el defecto de "palabras pegadas" que aparece con ciertas fuentes.
  3. OCR (Tesseract, español) — para páginas escaneadas o solo-imagen.

Cada página devuelve el mejor texto disponible junto a un puntaje de calidad
(0.0 a 1.0) para poder marcar transcripciones de baja confiabilidad.
"""

import logging
import re
from dataclasses import dataclass

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

# Import perezoso: pdfplumber y OCR solo se cargan si se necesitan.
try:
    import pdfplumber  # noqa: F401
    _HAS_PDFPLUMBER = True
except Exception:
    _HAS_PDFPLUMBER = False

try:
    import pytesseract
    from PIL import Image
    import io
    _HAS_OCR = True
except Exception:
    _HAS_OCR = False


@dataclass
class PageExtraction:
    page_number: int
    text: str
    method: str
    quality: float
    ocr_used: bool


_TOKEN_RE = re.compile(r"\S+")


def quality_score(text: str) -> float:
    """Heurística 0..1: penaliza texto pegado, basura no alfabética y vacíos."""
    t = (text or "").strip()
    if len(t) < 15:
        return 0.0

    tokens = _TOKEN_RE.findall(t)
    if not tokens:
        return 0.0

    # Palabras pegadas: tokens exageradamente largos sin espacios.
    glued = sum(1 for w in tokens if len(w) > 25)
    glued_ratio = glued / len(tokens)

    # Proporción de caracteres alfabéticos (poco alfabético = tablas/basura).
    letters = sum(1 for c in t if c.isalpha())
    alpha_ratio = letters / max(1, len(t))

    # Largo promedio de palabra (muy alto sugiere pegado).
    avg_len = sum(len(w) for w in tokens) / len(tokens)

    score = 1.0
    score -= glued_ratio * 1.6
    score -= max(0.0, (avg_len - 12) / 22)
    if alpha_ratio < 0.55:
        score -= (0.55 - alpha_ratio)

    return max(0.0, min(1.0, score))


def _pdfplumber_page_text(pdf_path: str, page_index: int) -> str:
    if not _HAS_PDFPLUMBER:
        return ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_index >= len(pdf.pages):
                return ""
            page = pdf.pages[page_index]
            # x_tolerance bajo => inserta espacios donde hay pequeños huecos
            # entre glifos, corrigiendo el pegado de palabras.
            return page.extract_text(x_tolerance=1.5, y_tolerance=3) or ""
    except Exception as error:
        logger.warning("pdfplumber falló en página %s: %s", page_index + 1, error)
        return ""


def _ocr_page(page: "fitz.Page") -> str:
    if not _HAS_OCR:
        return ""
    try:
        # Render a 300 DPI para que el OCR tenga suficiente resolución.
        pix = page.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        return pytesseract.image_to_string(img, lang="spa") or ""
    except Exception as error:
        logger.warning("OCR falló: %s", error)
        return ""


def extract_page(pdf_path: str, page: "fitz.Page", page_index: int) -> PageExtraction:
    candidates: list[tuple[str, str, float, bool]] = []

    # Capa 1: PyMuPDF texto embebido.
    text_plain = (page.get_text("text") or "").strip()
    q_plain = quality_score(text_plain)
    candidates.append(("pymupdf", text_plain, q_plain, False))

    # Capa 2: pdfplumber si el texto embebido salió pegado / dudoso.
    if q_plain < 0.75:
        text_pp = _pdfplumber_page_text(pdf_path, page_index).strip()
        if text_pp:
            candidates.append(("pdfplumber", text_pp, quality_score(text_pp), False))

    best = max(candidates, key=lambda c: c[2])

    # Capa 3: OCR si sigue pobre o casi vacío (probable escaneo/imagen).
    if best[2] < 0.5 or len(best[1]) < 25:
        text_ocr = _ocr_page(page).strip()
        if text_ocr:
            candidates.append(("ocr_tesseract", text_ocr, quality_score(text_ocr), True))
            best = max(candidates, key=lambda c: c[2])

    return PageExtraction(
        page_number=page_index + 1,
        text=best[1],
        method=best[0],
        quality=round(best[2], 3),
        ocr_used=best[3],
    )


def extract_pdf(pdf_path: str) -> list[PageExtraction]:
    """Extrae todas las páginas de un PDF con la mejor calidad disponible."""
    resultados: list[PageExtraction] = []
    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            resultados.append(extract_page(pdf_path, page, page_index))
    return resultados
