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
    posible_grafico: bool = False


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


# Fraccion del area de la pagina que debe cubrir una imagen embebida para
# contar como "posible grafico": por debajo del piso es probablemente un
# logo/sello/firma escaneada; por encima del techo es casi seguro un fondo
# de pagina completa (el escaneo mismo), no un grafico o diagrama insertado.
_GRAFICO_AREA_MIN = 0.04
_GRAFICO_AREA_MAX = 0.75


def posible_grafico(page: "fitz.Page") -> bool:
    """Heuristica de imagenes embebidas: ni el texto plano ni el OCR
    reconstruyen graficos de barras, circulares, diagramas, etc. — ni
    siquiera los detectan como algo distinto de texto perdido. Esto NO es
    deteccion real de graficos (no interpreta el contenido), solo marca la
    pagina para que un humano la revise si le toco una imagen de tamaño
    razonable, ni decorativa ni un escaneo de pagina completa."""
    try:
        area_pagina = page.rect.width * page.rect.height
        if area_pagina <= 0:
            return False
        for imagen in page.get_images(full=True):
            xref = imagen[0]
            for rect in page.get_image_rects(xref):
                proporcion = (rect.width * rect.height) / area_pagina
                if _GRAFICO_AREA_MIN <= proporcion <= _GRAFICO_AREA_MAX:
                    return True
    except Exception as error:
        logger.warning("Deteccion de posible grafico fallo: %s", error)
    return False


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


def _tabla_a_markdown(tabla: list[list]) -> str:
    """Convierte una tabla de pdfplumber (filas de celdas) a una tabla
    Markdown real, para que /consulta reciba la correspondencia fila-columna
    en vez de texto aplanado. Los LLM interpretan tablas Markdown de forma
    mucho mas confiable que texto corrido con espacios.

    Las columnas se acolchan (padding) al ancho real de su celda mas larga,
    SIN tope: un tope hacia que las celdas que lo superan quedaran sin
    acolchar, y como esas celdas varian de largo fila a fila, las columnas
    siguientes de esa fila quedaban en una posicion horizontal distinta en
    cada linea (la tabla se veia descuadrada en vez de como grilla). Sin
    tope las lineas pueden ser largas, pero cada columna alinea siempre en
    la misma posicion en todas las filas."""
    def limpiar_celda(celda) -> str:
        texto = "" if celda is None else str(celda)
        # Un salto de linea dentro de una celda rompería la fila Markdown.
        texto = " ".join(texto.split())
        return texto.replace("|", "\\|")

    filas = [[limpiar_celda(c) for c in fila] for fila in tabla]
    n_columnas = max(len(fila) for fila in filas)
    filas = [fila + [""] * (n_columnas - len(fila)) for fila in filas]

    anchos = [
        max(max((len(fila[col]) for fila in filas), default=3), 3)
        for col in range(n_columnas)
    ]

    def formatear_celda(texto: str, ancho: int) -> str:
        return texto.ljust(ancho)

    def formatear_fila(fila: list[str]) -> str:
        return "| " + " | ".join(formatear_celda(c, anchos[i]) for i, c in enumerate(fila)) + " |"

    encabezado = _limpiar_encabezado_sospechoso(filas[0])
    resto = filas[1:]

    lineas = [
        formatear_fila(encabezado),
        "| " + " | ".join("-" * ancho for ancho in anchos) + " |",
    ]
    lineas.extend(formatear_fila(fila) for fila in resto)

    return "\n".join(lineas)


def tablas_a_markdown(tablas: list[list[list]] | None) -> str:
    """Convierte todas las tablas detectadas en una pagina a bloques Markdown
    numerados (por si hay mas de una tabla en la misma pagina, como en los
    cuadros de anexos de El Peruano)."""
    if not tablas:
        return ""

    bloques = []
    for indice, tabla in enumerate(tablas, start=1):
        try:
            md = _tabla_a_markdown(tabla)
        except Exception as error:
            logger.warning("No se pudo convertir tabla %s a Markdown: %s", indice, error)
            continue
        if md:
            etiqueta = f"Tabla {indice}" if len(tablas) > 1 else "Tabla"
            bloques.append(f"{etiqueta}:\n{md}")

    return "\n\n".join(bloques)


_MIN_CELDAS_NO_VACIAS_TABLA = 3
_MIN_CARACTERES_TABLA = 20

# Patrones de celda que delatan un FRAGMENTO de un valor o encabezado mas
# largo, no un dato real: resultado tipico de una linea de grilla espuria
# que pdfplumber detecto donde el PDF no la tenia (ej. "0.1 UIT" partido en
# tres celdas "0" / ".1" / "UIT", o "BOTIQUÍN" partido en "B" / "OTI" /
# "QUÍN"). Estas celdas NO son basura vacia (pasan _tabla_parece_real) pero
# tampoco son un encabezado o valor legible: dejarlas pasar tal cual produce
# encabezados sin sentido como "NE IO CÉ cul lec" en la tabla de infracciones
# y sanciones de DIGEMID (confirmado en DS-020-2024, pag. 4-5).
_FRAGMENTO_DECIMAL_RE = re.compile(r"^\.\d+$")
_FRAGMENTO_ENTERO_SUELTO_RE = re.compile(r"^\d$")
# Palabra suelta de 1 a 4 letras (cualquier mayus/minus): un pedazo de
# "BOTIQUÍN" partido en "B" / "OTI" / "QUÍN" cae aca sin importar el caso.
_FRAGMENTO_ALFA_SUELTO_RE = re.compile(r"^[A-Za-zÁÉÍÓÚÑáéíóúñ]{1,4}$")
_ABREVIATURAS_TABLA_CONOCIDAS = {"NA", "N/A", "UIT", "IVA", "RUC", "DNI"}


def _celda_parece_fragmento(celda: str) -> bool:
    texto = (celda or "").strip()
    if not texto or texto.upper() in _ABREVIATURAS_TABLA_CONOCIDAS:
        return False
    if _FRAGMENTO_DECIMAL_RE.match(texto):
        return True
    if _FRAGMENTO_ENTERO_SUELTO_RE.match(texto):
        return True
    if _FRAGMENTO_ALFA_SUELTO_RE.match(texto):
        return True
    return False


def _fila_parece_fantasma(fila: list) -> bool:
    """Una fila 'fantasma': casi vacia salvo 1-2 fragmentos sueltos de texto,
    resultado de una banda de la grilla que no corresponde a ninguna fila de
    contenido real (ej. un encabezado que hace wrap a 2 lineas de texto
    dentro de una celda, y pdfplumber corta esa celda en 2 "filas" por una
    linea horizontal que en realidad no separa contenido distinto)."""
    celdas = [(str(c) if c is not None else "").strip() for c in fila]
    no_vacias = [c for c in celdas if c]
    return bool(no_vacias) and len(no_vacias) <= 2 and len(celdas) >= 5


def _limpiar_filas_fantasma(tabla: list[list]) -> list[list]:
    """Descarta filas fantasma solo en la zona de encabezado (primeras 2
    filas): una fila casi vacia en medio de los datos puede ser legitima
    (separador visual entre secciones de la tabla), asi que no se toca."""
    if len(tabla) < 3:
        return tabla
    limite = min(2, len(tabla) - 2)
    return [
        fila for indice, fila in enumerate(tabla)
        if not (indice < limite and _fila_parece_fantasma(fila))
    ]


def _fraccion_celdas_fragmentadas(tablas: list[list[list]]) -> float:
    """Mide que tan fragmentada quedo una extraccion: proporcion de celdas
    no vacias que parecen un pedazo de un valor/encabezado mas largo. Sirve
    para comparar dos estrategias de extraccion de pdfplumber y quedarse con
    la que reconstruyo mejor la grilla real de columnas."""
    celdas = [
        (str(c) if c is not None else "").strip()
        for tabla in tablas
        for fila in tabla
        for c in fila
    ]
    celdas_no_vacias = [c for c in celdas if c]
    if not celdas_no_vacias:
        return 1.0
    fragmentadas = sum(1 for c in celdas_no_vacias if _celda_parece_fragmento(c))
    return fragmentadas / len(celdas_no_vacias)


def _limpiar_encabezado_sospechoso(fila_encabezado: list[str]) -> list[str]:
    """Vacia (en vez de dejar pasar) las celdas de encabezado que parecen
    fragmentos sin sentido. El flujo de revision humana en Telegram
    (etiquetaColumna, en el bot) ya sabe mostrar "Columna N" para un
    encabezado vacio -- un encabezado vacio con esa clave interna estable es
    preferible a uno con texto incorrecto que un revisor podria no notar que
    esta mal, sobre todo en tablas juridicas (escalas de sanciones) donde el
    significado de una columna es la informacion sensible."""
    return [
        "" if _celda_parece_fragmento(celda) else celda
        for celda in fila_encabezado
    ]


def _tabla_parece_real(tabla: list[list]) -> bool:
    """pdfplumber a veces "detecta" como tabla un par de lineas de layout
    (ej. una columna de margen o un salto de seccion) sin ninguna tabla real
    ahi: el resultado es una grilla de 2x2 casi vacia con un fragmento de
    palabra suelto. Agregar eso al texto de busqueda es puro ruido, peor que
    no agregar nada, asi que se descarta antes de convertir a Markdown."""
    celdas_no_vacias = 0
    total_caracteres = 0

    for fila in tabla:
        for celda in fila:
            texto = (str(celda) if celda is not None else "").strip()
            if texto:
                celdas_no_vacias += 1
                total_caracteres += len(texto)

    return (
        celdas_no_vacias >= _MIN_CELDAS_NO_VACIAS_TABLA
        and total_caracteres >= _MIN_CARACTERES_TABLA
    )


# La estrategia por defecto de pdfplumber ("lines": detecta la grilla a
# partir de las lineas de trazado del PDF) es la mas precisa cuando el PDF
# trae una grilla limpia, pero una linea de trazado espuria (o un
# encabezado que hace wrap a 2 lineas dentro de una celda) le hace partir
# celdas de mas -- el caso confirmado en las tablas de "Escala de
# infracciones y sanciones" de DIGEMID (DS-020-2024 y otras normas con el
# mismo Anexo 01). La estrategia "text" (infiere columnas por alineacion y
# huecos de texto, ignorando las lineas de trazado) no depende de esas
# lineas espurias y reconstruye mejor esas tablas puntuales. Se prueban
# ambas y se elige la que quede menos fragmentada, en vez de asumir una sola
# estrategia para todas las normas.
_ESTRATEGIAS_EXTRACCION_TABLA = (
    None,
    {"vertical_strategy": "text", "horizontal_strategy": "text"},
)


def _extraer_tablas_validas(page, settings: dict | None) -> list:
    tablas = page.extract_tables(settings) if settings else page.extract_tables()
    resultado = []
    for tabla in tablas or []:
        tabla_limpia = _limpiar_filas_fantasma(tabla)
        if (
            len(tabla_limpia) > 1
            and tabla_limpia[0]
            and len(tabla_limpia[0]) > 1
            and _tabla_parece_real(tabla_limpia)
        ):
            resultado.append(tabla_limpia)
    return resultado


def _pdfplumber_tables(pdf_path: str, page_index: int) -> list:
    """Detecta tablas reales (>=2 filas y >=2 columnas, con suficiente
    contenido para no ser ruido de layout) para guardarlas como estructura
    ademas del texto plano: aplanar una tabla a texto corrido pierde la
    correspondencia fila-columna que suele importar en normas (ej. escalas
    de sanciones, cronogramas, cuadros de requisitos)."""
    if not _HAS_PDFPLUMBER:
        return []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_index >= len(pdf.pages):
                return []
            page = pdf.pages[page_index]

            candidatos = []
            for settings in _ESTRATEGIAS_EXTRACCION_TABLA:
                try:
                    tablas_validas = _extraer_tablas_validas(page, settings)
                except Exception as error:
                    logger.warning(
                        "Estrategia de tabla %s falló en página %s: %s",
                        settings, page_index + 1, error,
                    )
                    continue
                if tablas_validas:
                    candidatos.append(tablas_validas)

            if not candidatos:
                return []

            # La estrategia por defecto (candidatos[0], si produjo algo) se
            # prefiere salvo que una alternativa reduzca claramente la
            # fragmentacion -- un margen minimo evita cambiar de estrategia
            # por ruido cuando ambas quedan igual de bien.
            mejor = candidatos[0]
            mejor_fragmentacion = _fraccion_celdas_fragmentadas(mejor)
            for alternativa in candidatos[1:]:
                fragmentacion = _fraccion_celdas_fragmentadas(alternativa)
                if fragmentacion < mejor_fragmentacion - 0.05:
                    mejor, mejor_fragmentacion = alternativa, fragmentacion
            return mejor
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


def es_pagina_en_blanco(page: "fitz.Page") -> bool:
    """Detecta una pagina GENUINAMENTE en blanco en el PDF original: sin
    texto embebido y con un render practicamente todo blanco.

    Esto es distinto de que la extraccion haya fallado (una pagina con
    contenido real que no se pudo transcribir bien) — sin este chequeo,
    quality_score le pone 0 a ambos casos por igual (por texto < 15
    caracteres), y una pagina en blanco terminaba en la cola de revision
    como si fuera un error de transcripcion cuando en realidad no hay nada
    que corregir.

    OJO: no se descarta por tener imagenes embebidas. Muchas normas de
    DIGEMID traen un membrete/marca de agua institucional en TODAS las
    paginas del PDF (incluidas las que no tienen contenido), asi que
    get_images() no distingue una pagina en blanco de una con contenido
    real — se confirmo con paginas reales donde el promedio de pixel salio
    ~254.99/255 (blanco casi puro) a pesar de tener 1 imagen incrustada.
    El render (promedio de pixel) es la señal confiable, no la presencia
    de imagenes.
    """
    if (page.get_text("text") or "").strip():
        return False

    pix = page.get_pixmap(matrix=fitz.Matrix(72 / 72, 72 / 72))
    samples = pix.samples
    if not samples:
        return True

    promedio = sum(samples) / len(samples)
    return promedio > 250


def extract_page(pdf_path: str, page: "fitz.Page", page_index: int) -> PageExtraction:
    if es_pagina_en_blanco(page):
        return PageExtraction(
            page_number=page_index + 1,
            text="",
            method="pagina_en_blanco",
            quality=1.0,
            ocr_used=False,
        )

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
    posible_formula_detectada = posible_formula(best[1])
    posible_grafico_detectado = posible_grafico(page)

    # Las tablas se agregan al texto final (no al que se usa para elegir el
    # metodo/quality_score, que evalua solo la prosa) como Markdown real, en
    # vez de dejarlas solo en `tables` (estructura) sin usar: asi /consulta
    # recibe la correspondencia fila-columna en el mismo texto que ya busca y
    # cita, sin tocar el SQL ni el bot.
    texto_final = best[1]
    if tablas:
        markdown_tablas = tablas_a_markdown(tablas)
        if markdown_tablas:
            texto_final = f"{texto_final}\n\n{markdown_tablas}".strip()

    return PageExtraction(
        page_number=page_index + 1,
        text=texto_final,
        method=best[0],
        quality=round(best[2], 3),
        ocr_used=best[3],
        ocr_confidence=round(best[4], 3) if best[4] is not None else None,
        has_tables=bool(tablas),
        tables=tablas or None,
        posible_formula=posible_formula_detectada,
        posible_grafico=posible_grafico_detectado,
    )


def extract_pdf(pdf_path: str) -> list[PageExtraction]:
    """Extrae todas las páginas de un PDF con la mejor calidad disponible."""
    resultados: list[PageExtraction] = []
    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            resultados.append(extract_page(pdf_path, page, page_index))
    return resultados
