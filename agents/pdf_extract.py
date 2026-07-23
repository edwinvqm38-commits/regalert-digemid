"""Extracción de texto de PDFs con alta fidelidad y puntaje de calidad.

Estrategia por página (de más rápida/limpia a más costosa):
  1. PyMuPDF (texto embebido) — perfecto para PDFs digitales con capa de texto.
  2. pdfplumber — reconstruye espacios a partir de la posición de los caracteres,
     corrige el defecto de "palabras pegadas" que aparece con ciertas fuentes.
  3. OCR (Tesseract, español) — para páginas escaneadas o solo-imagen.

Cada página devuelve el mejor texto disponible junto a un puntaje de calidad
(0.0 a 1.0) para poder marcar transcripciones de baja confiabilidad, más:
  - ocr_confidence: confianza real de Tesseract (promedio por palabra), NO
    una heurística de forma. quality_score por sí solo puede verse "limpio"
    aunque el OCR haya confundido una palabra por otra parecida; combinar
    ambos evita sobreestimar la fidelidad de páginas escaneadas.
  - has_tables / tables: detección de tablas vía pdfplumber, guardadas como
    estructura (filas/columnas) además del texto plano, porque una tabla
    aplanada a texto corrido pierde la correspondencia fila-columna.
  - posible_formula: heurística de densidad de símbolos matemáticos/técnicos.
    Ni el texto plano ni el OCR reconstruyen fórmulas de forma confiable, así
    que se prefiere marcar la página para revisión humana antes que fingir
    una transcripción exacta.
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
    ocr_confidence: float | None = None
    has_tables: bool = False
    tables: list | None = None
    posible_formula: bool = False


_TOKEN_RE = re.compile(r"\S+")

# Símbolos que indican notación matemática/técnica (fórmulas, unidades con
# exponentes, etc.) que el texto plano u OCR no reconstruyen con fidelidad.
_SIMBOLOS_FORMULA = set("=×÷≤≥≠≈∑∏∫√πΩ∆αβγδθλμσφ±")


def quality_score(text: str) -> float:
    """Heurística 0..1: penaliza texto pegado, basura no alfabética y vacíos.

    Es una heurística de FORMA del texto (¿se ve como prosa normal?), no una
    medida de si el contenido es correcto palabra por palabra — por eso para
    OCR se combina con ocr_confidence en extract_page().
    """
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


def posible_formula(text: str) -> bool:
    """Marca la página para revisión humana si tiene una densidad alta de
    símbolos matemáticos/técnicos: ni el texto plano ni el OCR reconstruyen
    fórmulas de forma confiable, así que se prefiere avisar en vez de fingir
    una transcripción exacta."""
    t = (text or "").strip()
    if not t:
        return False

    simbolos = sum(1 for c in t if c in _SIMBOLOS_FORMULA)
    if simbolos < 3:
        return False

    return (simbolos / len(t)) > 0.01


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


def _pdfplumber_tables(pdf_path: str, page_index: int) -> list:
    """Detecta tablas reales (>=2 filas y >=2 columnas) para guardarlas como
    estructura ademas del texto plano: aplanar una tabla a texto corrido
    pierde la correspondencia fila-columna que suele importar en normas
    (ej. escalas de sanciones, cronogramas, cuadros de requisitos)."""
    if not _HAS_PDFPLUMBER:
        return []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_index >= len(pdf.pages):
                return []
            page = pdf.pages[page_index]
            tablas = page.extract_tables() or []
            return [
                tabla for tabla in tablas
                if len(tabla) > 1 and tabla[0] and len(tabla[0]) > 1
            ]
    except Exception as error:
        logger.warning("Detección de tablas falló en página %s: %s", page_index + 1, error)
        return []


def _ocr_page(page: "fitz.Page") -> tuple[str, float | None]:
    """OCR de la página, devolviendo tambien la confianza real de Tesseract
    (promedio por palabra reconocida, 0..1), no una heurística de forma."""
    if not _HAS_OCR:
        return "", None
    try:
        # Render a 300 DPI para que el OCR tenga suficiente resolución.
        pix = page.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
        img = Image.open(io.BytesIO(pix.tobytes("png")))

        texto = pytesseract.image_to_string(img, lang="spa") or ""

        datos = pytesseract.image_to_data(img, lang="spa", output_type=pytesseract.Output.DICT)
        confidencias = []
        for valor in datos.get("conf", []):
            try:
                c = float(valor)
            except (TypeError, ValueError):
                continue
            # Tesseract devuelve -1 para bloques/líneas sin texto reconocible.
            if c >= 0:
                confidencias.append(c)

        confianza = (sum(confidencias) / len(confidencias) / 100.0) if confidencias else None

        return texto, confianza
    except Exception as error:
        logger.warning("OCR falló: %s", error)
        return "", None


def extract_page(pdf_path: str, page: "fitz.Page", page_index: int) -> PageExtraction:
    candidates: list[tuple[str, str, float, bool, float | None]] = []

    # Capa 1: PyMuPDF texto embebido.
    text_plain = (page.get_text("text") or "").strip()
    q_plain = quality_score(text_plain)
    candidates.append(("pymupdf", text_plain, q_plain, False, None))

    # Capa 2: pdfplumber si el texto embebido salió pegado / dudoso.
    if q_plain < 0.75:
        text_pp = _pdfplumber_page_text(pdf_path, page_index).strip()
        if text_pp:
            candidates.append(("pdfplumber", text_pp, quality_score(text_pp), False, None))

    best = max(candidates, key=lambda c: c[2])

    # Capa 3: OCR si sigue pobre o casi vacío (probable escaneo/imagen).
    if best[2] < 0.5 or len(best[1]) < 25:
        text_ocr, ocr_confianza = _ocr_page(page)
        text_ocr = text_ocr.strip()
        if text_ocr:
            forma = quality_score(text_ocr)
            # El texto OCR puede "verse" bien (forma de prosa normal) y aun
            # así tener palabras mal reconocidas; se combina con la
            # confianza real de Tesseract para no sobreestimar la fidelidad.
            # Si Tesseract no devolvió confianzas utilizables, se aplica un
            # descuento fijo conservador en vez de asumir 100% de confianza.
            calidad_final = forma * (ocr_confianza if ocr_confianza is not None else 0.6)
            candidates.append(("ocr_tesseract", text_ocr, calidad_final, True, ocr_confianza))
            best = max(candidates, key=lambda c: c[2])

    tablas = _pdfplumber_tables(pdf_path, page_index)

    return PageExtraction(
        page_number=page_index + 1,
        text=best[1],
        method=best[0],
        quality=round(best[2], 3),
        ocr_used=best[3],
        ocr_confidence=round(best[4], 3) if best[4] is not None else None,
        has_tables=bool(tablas),
        tables=tablas or None,
        posible_formula=posible_formula(best[1]),
    )


def extract_pdf(pdf_path: str) -> list[PageExtraction]:
    """Extrae todas las páginas de un PDF con la mejor calidad disponible."""
    resultados: list[PageExtraction] = []
    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            resultados.append(extract_page(pdf_path, page, page_index))
    return resultados
