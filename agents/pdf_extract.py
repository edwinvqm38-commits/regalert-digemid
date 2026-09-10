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
  - has_tables / possible_table: señal conservadora de que uno de los motores
    encontró una posible tabla. `tables` solo contiene estructuras que superan
    el umbral explícito de confianza; si ninguna lo hace se conserva el texto
    plano y se marca `table_requires_review`, sin inventar asociaciones.
  - text_quality_score / table_quality_score / layout_quality_score: calidades
    separadas. `quality` se mantiene como alias compatible de la calidad del
    texto y nunca debe interpretarse como calidad de una tabla.
  - posible_formula: heurística de densidad de símbolos matemáticos/técnicos.
    Ni el texto plano ni el OCR reconstruyen fórmulas de forma confiable, así
    que se prefiere marcar la página para revisión humana antes que fingir
    una transcripción exacta.
"""

import hashlib
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
    text_quality_score: float | None = None
    table_quality_score: float | None = None
    layout_quality_score: float | None = None
    possible_table: bool = False
    table_requires_review: bool = False
    table_diagnostics: list[dict] | None = None
    table_region_summary: dict | None = None
    source_pdf_sha256: str | None = None


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
        # El formato canónico escapa primero el backslash y luego el pipe.
        # Así el parser puede distinguir `\\`, `\|` y la combinación `\\\|`
        # sin acumular escapes en revisiones sucesivas.
        return texto.replace("\\", "\\\\").replace("|", "\\|")

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

    # No se promueve automáticamente tabla[0] a encabezado. Si la primera fila
    # no tiene forma inequívoca de encabezado (por ejemplo, es un título o una
    # banda previa), se usa una cabecera Markdown vacía y se conservan TODAS
    # las filas como datos, sin inventar nombres ni mover asociaciones.
    if _primera_fila_es_encabezado_confiable(filas):
        encabezado = filas[0]
        resto = filas[1:]
    else:
        encabezado = [""] * n_columnas
        resto = filas

    lineas = [
        formatear_fila(encabezado),
        "| " + " | ".join("-" * ancho for ancho in anchos) + " |",
    ]
    lineas.extend(formatear_fila(fila) for fila in resto)

    return "\n".join(lineas)


def _primera_fila_es_encabezado_confiable(filas: list[list[str]]) -> bool:
    """Decide solo si tabla[0] puede promoverse a encabezado Markdown.

    No busca otro encabezado ni reconstruye uno: ante duda usa una cabecera
    vacía y conserva la matriz completa. Se exige ocupación amplia, texto
    predominantemente alfabético y al menos una fila posterior con contenido.
    """
    if len(filas) < 2 or not filas[0]:
        return False
    columnas = max(len(fila) for fila in filas)
    primera = filas[0] + [""] * (columnas - len(filas[0]))
    no_vacias = [celda for celda in primera if celda]
    if len(no_vacias) < max(2, (columnas + 1) // 2):
        return False

    con_letras = sum(any(caracter.isalpha() for caracter in celda) for celda in no_vacias)
    if con_letras / len(no_vacias) < 0.70:
        return False
    if any(len(celda.split()) > 12 for celda in no_vacias):
        return False

    return any(sum(bool(celda) for celda in fila) >= 2 for fila in filas[1:])


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
_UMBRAL_CONFIANZA_TABLA = 0.68

# Estas razones describen una grilla cuya correspondencia fila-columna no es
# segura aunque su aspecto general produzca una puntuacion alta. La confianza
# y la seguridad estructural son dimensiones separadas: ningun bonus entre
# motores puede levantar un bloqueador.
_BLOQUEADORES_ESTRUCTURALES = frozenset({
    "fragmentation_excessive",
    "column_oversegmentation",
    "too_many_empty_columns",
    "alternating_empty_columns",
    "whole_page_editorial_layout",
    "editorial_prose_layout",
    "sparse_column_pattern",
    "dominant_column_imbalance",
    "suspicious_rows",
})

# Una columna se considera casi vacía si contiene datos en, como máximo, el
# 20 % de las filas. Se usa como señal de sobresegmentación, no para borrar la
# columna: una tabla jurídica puede tener celdas legítimamente vacías.
_MAX_OCUPACION_COLUMNA_CASI_VACIA = 0.20

# Patrones intrínsecos de fragmento. Los casos ambiguos (siglas cortas,
# identificadores de una cifra o unidades) se evalúan con sus celdas vecinas
# en _posiciones_celdas_fragmentadas(); no se mantiene una lista de palabras
# o unidades de un documento concreto.
_FRAGMENTO_DECIMAL_RE = re.compile(r"^\.\d+$")
_FRAGMENTO_ALFA_UN_CARACTER_RE = re.compile(r"^[^\W\d_]$", re.UNICODE)
_CELDA_ALFA_CORTA_RE = re.compile(r"^[^\W\d_]{1,5}$", re.UNICODE)
_CELDA_ENTERA_RE = re.compile(r"^\d+$")


def _celda_parece_fragmento(celda: str) -> bool:
    """Detecta solo fragmentos inequívocos sin contexto de fila.

    Una sigla corta o un número de una cifra pueden ser datos reales. La
    detección contextual de palabras, decimales y unidades partidas vive en
    _posiciones_celdas_fragmentadas(), evitando excepciones hardcodeadas.
    """
    texto = (celda or "").strip()
    if not texto:
        return False
    if _FRAGMENTO_DECIMAL_RE.match(texto):
        return True
    if _FRAGMENTO_ALFA_UN_CARACTER_RE.match(texto):
        return True
    return False


def _normalizar_tabla(tabla: list[list]) -> list[list[str]]:
    """Devuelve una matriz rectangular sin alterar asociaciones de celdas."""
    filas = [
        ["" if celda is None else " ".join(str(celda).split()) for celda in fila]
        for fila in (tabla or [])
    ]
    columnas = max((len(fila) for fila in filas), default=0)
    return [fila + [""] * (columnas - len(fila)) for fila in filas]


def _posiciones_celdas_fragmentadas(tabla: list[list]) -> set[tuple[int, int]]:
    """Localiza fragmentos usando contexto geométrico, sin vocabulario legal.

    Se reconocen dos patologías generales:
    - un decimal dividido entre una celda entera y otra que empieza por punto,
      incluyendo una tercera celda alfabética contigua si existe;
    - una palabra dividida en dos o más celdas alfabéticas cortas contiguas,
      cuando hay al menos tres piezas o alguna pieza de un solo carácter.

    Una sigla corta aislada y un identificador numérico aislado no se marcan.
    """
    filas = _normalizar_tabla(tabla)
    fragmentadas: set[tuple[int, int]] = set()

    for indice_fila, fila in enumerate(filas):
        for indice_columna, celda in enumerate(fila):
            if _FRAGMENTO_DECIMAL_RE.match(celda):
                fragmentadas.add((indice_fila, indice_columna))

            if (
                indice_columna + 1 < len(fila)
                and _CELDA_ENTERA_RE.match(celda)
                and _FRAGMENTO_DECIMAL_RE.match(fila[indice_columna + 1])
            ):
                fragmentadas.update({
                    (indice_fila, indice_columna),
                    (indice_fila, indice_columna + 1),
                })
                if (
                    indice_columna + 2 < len(fila)
                    and _CELDA_ALFA_CORTA_RE.match(fila[indice_columna + 2])
                ):
                    fragmentadas.add((indice_fila, indice_columna + 2))

        inicio = 0
        while inicio < len(fila):
            if not _CELDA_ALFA_CORTA_RE.match(fila[inicio]):
                inicio += 1
                continue
            fin = inicio
            while fin < len(fila) and _CELDA_ALFA_CORTA_RE.match(fila[fin]):
                fin += 1
            piezas = fila[inicio:fin]
            if len(piezas) >= 2 and (
                len(piezas) >= 3 or any(len(pieza) == 1 for pieza in piezas)
            ):
                fragmentadas.update(
                    (indice_fila, columna) for columna in range(inicio, fin)
                )
            inicio = fin

    return fragmentadas


def _fila_parece_fantasma(fila: list) -> bool:
    """Detecta una banda casi vacía compuesta por muchos tokens minúsculos.

    Exigir fragmentación interna evita confundir con una fila legítima de
    sección que solo ocupe una o dos celdas. La posición de la fila no importa:
    una banda espuria también puede aparecer en medio de la extracción.
    """
    celdas = [(str(c) if c is not None else "").strip() for c in fila]
    no_vacias = [c for c in celdas if c]
    if not no_vacias or len(no_vacias) > 2 or len(celdas) < 5:
        return False

    tokens = re.findall(r"[^\W_]+", " ".join(no_vacias), flags=re.UNICODE)
    if len(tokens) < 6:
        return False
    cortos = sum(len(token) <= 2 for token in tokens)
    return (cortos / len(tokens)) >= 0.65


def _limpiar_filas_fantasma(tabla: list[list]) -> list[list]:
    """Quita solo filas que cumplen la definición estricta de fantasma.

    No elimina filas vacías internas ni filas legítimas escasas. Además quita
    filas totalmente vacías únicamente en los bordes, donde no representan
    una asociación semántica.
    """
    filas = [fila for fila in tabla if not _fila_parece_fantasma(fila)]
    while filas and not any(str(c or "").strip() for c in filas[0]):
        filas.pop(0)
    while filas and not any(str(c or "").strip() for c in filas[-1]):
        filas.pop()
    return filas


def _fraccion_celdas_fragmentadas(tablas: list[list[list]]) -> float:
    """Mide que tan fragmentada quedo una extraccion: proporcion de celdas
    no vacias que parecen un pedazo de un valor/encabezado mas largo. Sirve
    para comparar dos estrategias de extraccion de pdfplumber y quedarse con
    la que reconstruyo mejor la grilla real de columnas."""
    total_no_vacias = 0
    total_fragmentadas = 0
    for tabla in tablas:
        filas = _normalizar_tabla(tabla)
        total_no_vacias += sum(bool(celda) for fila in filas for celda in fila)
        total_fragmentadas += len(_posiciones_celdas_fragmentadas(filas))
    if not total_no_vacias:
        return 1.0
    return total_fragmentadas / total_no_vacias


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


def _limitar(valor: float, minimo: float = 0.0, maximo: float = 1.0) -> float:
    return max(minimo, min(maximo, valor))


def _interseccion_sobre_union(
    bbox_a: tuple[float, float, float, float] | None,
    bbox_b: tuple[float, float, float, float] | None,
) -> float:
    if not bbox_a or not bbox_b:
        return 0.0
    ax0, ay0, ax1, ay1 = bbox_a
    bx0, by0, bx1, by1 = bbox_b
    ancho = max(0.0, min(ax1, bx1) - max(ax0, bx0))
    alto = max(0.0, min(ay1, by1) - max(ay0, by0))
    interseccion = ancho * alto
    area_a = max(0.0, ax1 - ax0) * max(0.0, ay1 - ay0)
    area_b = max(0.0, bx1 - bx0) * max(0.0, by1 - by0)
    union = area_a + area_b - interseccion
    return interseccion / union if union else 0.0


def _actualizar_estado_aceptacion(candidato: dict) -> None:
    """Aplica el umbral y los bloqueadores como condiciones independientes."""
    razones = candidato["reasons"]
    bloqueadores = sorted(set(razones) & _BLOQUEADORES_ESTRUCTURALES)
    supera_umbral = candidato["candidate_confidence"] >= _UMBRAL_CONFIANZA_TABLA

    candidato["structural_blockers"] = bloqueadores
    candidato["accepted"] = supera_umbral and not bloqueadores

    if supera_umbral:
        candidato["reasons"] = [
            razon for razon in razones if razon != "below_confidence_threshold"
        ]
    elif "below_confidence_threshold" not in razones:
        razones.append("below_confidence_threshold")


def _evaluar_candidato_tabla(
    tabla: list[list],
    *,
    extractor: str,
    strategy: str,
    bbox: tuple[float, float, float, float] | None = None,
    page_width: float = 1.0,
    page_height: float = 1.0,
    table_index: int = 1,
) -> dict:
    """Calcula evidencia y confianza de un candidato sin reparar su grilla."""
    celdas_originales_no_vacias = [
        str(celda)
        for fila in (tabla or [])
        for celda in fila
        if celda is not None and str(celda).strip()
    ]
    filas = _normalizar_tabla(tabla)
    cantidad_filas = len(filas)
    cantidad_columnas = max((len(fila) for fila in filas), default=0)
    total_celdas = max(1, cantidad_filas * cantidad_columnas)
    celdas_no_vacias = [celda for fila in filas for celda in fila if celda]
    caracteres_totales = sum(len(celda) for celda in celdas_no_vacias)

    ocupacion_columnas = [
        sum(bool(filas[fila][columna]) for fila in range(cantidad_filas))
        for columna in range(cantidad_columnas)
    ]
    caracteres_por_columna = [
        sum(len(filas[fila][columna]) for fila in range(cantidad_filas))
        for columna in range(cantidad_columnas)
    ]
    ocupacion_relativa_columnas = [
        ocupacion / cantidad_filas if cantidad_filas else 0.0
        for ocupacion in ocupacion_columnas
    ]
    dispersion_ocupacion_columnas = (
        max(ocupacion_relativa_columnas) - min(ocupacion_relativa_columnas)
        if ocupacion_relativa_columnas else 0.0
    )
    dominant_column_ratio = (
        max(caracteres_por_columna, default=0) / caracteres_totales
        if caracteres_totales else 0.0
    )
    dominant_column_score = _limitar((dominant_column_ratio - 0.65) / 0.30)
    multiline_cell_ratio = (
        sum("\n" in celda for celda in celdas_originales_no_vacias)
        / len(celdas_originales_no_vacias)
        if celdas_originales_no_vacias else 0.0
    )
    columnas_vacias = [
        indice for indice, ocupacion in enumerate(ocupacion_columnas)
        if ocupacion == 0
    ]
    limite_casi_vacia = max(
        1,
        int(cantidad_filas * _MAX_OCUPACION_COLUMNA_CASI_VACIA),
    )
    columnas_casi_vacias = [
        indice for indice, ocupacion in enumerate(ocupacion_columnas)
        if 0 < ocupacion <= limite_casi_vacia
    ]
    columnas_vacias_o_casi = set(columnas_vacias + columnas_casi_vacias)
    columnas_intercaladas = [
        indice
        for indice in range(1, max(1, cantidad_columnas - 1))
        if (
            indice in columnas_vacias_o_casi
            and indice - 1 not in columnas_vacias_o_casi
            and indice + 1 not in columnas_vacias_o_casi
        )
    ] if cantidad_columnas >= 3 else []

    empty_cell_ratio = (
        (total_celdas - len(celdas_no_vacias)) / total_celdas
        if cantidad_filas and cantidad_columnas else 1.0
    )
    empty_column_ratio = (
        len(columnas_vacias_o_casi) / cantidad_columnas
        if cantidad_columnas else 1.0
    )
    alternating_empty_columns_score = (
        len(columnas_intercaladas) / max(1, cantidad_columnas - 2)
    )
    fragmentation_score = _fraccion_celdas_fragmentadas([filas])

    exceso_absoluto = _limitar((cantidad_columnas - 10) / 10)
    sparsity = _limitar((empty_cell_ratio - 0.35) / 0.65)
    oversegmentation_score = max(
        empty_column_ratio,
        exceso_absoluto * sparsity,
    )

    if bbox and page_width > 0 and page_height > 0:
        x0, y0, x1, y1 = bbox
        width_ratio = _limitar((x1 - x0) / page_width)
        height_ratio = _limitar((y1 - y0) / page_height)
    else:
        width_ratio = 0.0
        height_ratio = 0.0
    ancho_pagina = _limitar((width_ratio - 0.80) / 0.10)
    alto_pagina = _limitar((height_ratio - 0.70) / 0.18)
    whole_page_table_penalty = ancho_pagina * alto_pagina

    filas_con_contenido = [fila for fila in filas if any(fila)]
    filas_editoriales = 0
    filas_con_ancla_estructural = 0
    for fila in filas_con_contenido:
        celdas_prosa = []
        palabras_fila = 0
        celdas_ocupadas = [celda for celda in fila if celda]
        for celda in fila:
            palabras = re.findall(r"[^\W\d_]+", celda, flags=re.UNICODE)
            palabras_fila += len(palabras)
            proporcion_alfabetica = (
                sum(caracter.isalpha() for caracter in celda) / max(1, len(celda))
            )
            if len(palabras) >= 2 and len(celda) >= 8 and proporcion_alfabetica >= 0.65:
                celdas_prosa.append(celda)
        if len(celdas_ocupadas) >= 2 and any(
            (
                any(caracter.isdigit() for caracter in celda)
                or bool(re.fullmatch(r"[^\W\d_]{1,4}", celda, flags=re.UNICODE))
                or any(simbolo in celda for simbolo in ("%", "=", "≤", "≥"))
            )
            and len(celda) <= 24
            for celda in celdas_ocupadas
        ):
            filas_con_ancla_estructural += 1
        minimo_columnas_prosa = max(3, (cantidad_columnas + 1) // 2)
        if len(celdas_prosa) >= minimo_columnas_prosa and palabras_fila >= 8:
            filas_editoriales += 1

    proporcion_filas_editoriales = (
        filas_editoriales / len(filas_con_contenido) if filas_con_contenido else 0.0
    )
    celda_mas_larga = max((len(celda) for celda in celdas_no_vacias), default=0)
    dominancia_celda_prosa = (
        celda_mas_larga / caracteres_totales if caracteres_totales else 0.0
    )
    prose_density_penalty = max(
        proporcion_filas_editoriales,
        dominancia_celda_prosa * whole_page_table_penalty,
    )
    semantic_anchor_ratio = (
        filas_con_ancla_estructural / len(filas_con_contenido)
        if filas_con_contenido else 0.0
    )
    # La prosa multilínea no es por sí sola editorial: códigos, valores u
    # otras celdas breves repetidas por fila son anclas típicas de una tabla
    # jurídica real. La penalización editorial se aplica a la combinación.
    editorial_layout_score = prose_density_penalty * (1.0 - semantic_anchor_ratio)
    whole_page_structural_score = whole_page_table_penalty * max(
        editorial_layout_score,
        oversegmentation_score,
        alternating_empty_columns_score,
    )

    filas_sospechosas = [
        indice for indice, fila in enumerate(filas) if _fila_parece_fantasma(fila)
    ]
    suspicious_row_score = (
        len(filas_sospechosas) / len(filas_con_contenido)
        if filas_con_contenido else 0.0
    )

    confianza = 0.96
    confianza -= 1.15 * fragmentation_score
    confianza -= 0.55 * oversegmentation_score
    confianza -= 0.35 * empty_column_ratio
    confianza -= 0.65 * alternating_empty_columns_score
    confianza -= 0.45 * editorial_layout_score
    confianza -= 0.30 * whole_page_structural_score
    confianza -= 0.35 * dominant_column_score
    confianza -= 0.35 * suspicious_row_score
    if filas_sospechosas:
        confianza -= 0.08
    confianza = _limitar(confianza)

    layout_quality_score = _limitar(
        1.0
        - 0.35 * oversegmentation_score
        - 0.25 * empty_column_ratio
        - 0.30 * alternating_empty_columns_score
        - 0.30 * editorial_layout_score
        - 0.25 * whole_page_structural_score
        - 0.25 * dominant_column_score
    )

    reasons = []
    if fragmentation_score >= 0.15:
        reasons.append("fragmentation_excessive")
    if oversegmentation_score >= 0.30:
        reasons.append("column_oversegmentation")
    if empty_column_ratio >= 0.30:
        reasons.append("too_many_empty_columns")
    if (
        cantidad_columnas >= 5
        and empty_column_ratio >= 0.20
        and dispersion_ocupacion_columnas >= 0.60
    ):
        reasons.append("sparse_column_pattern")
    if alternating_empty_columns_score >= 0.20:
        reasons.append("alternating_empty_columns")
    if whole_page_table_penalty >= 0.55:
        reasons.append("whole_page_table")
    if editorial_layout_score >= 0.45:
        reasons.append("editorial_prose_layout")
    if whole_page_table_penalty >= 0.55 and editorial_layout_score >= 0.30:
        reasons.append("whole_page_editorial_layout")
    if (
        cantidad_columnas >= 4
        and cantidad_filas >= 4
        and dominant_column_ratio >= 0.82
    ):
        reasons.append("dominant_column_imbalance")
    if filas_sospechosas:
        reasons.append("suspicious_rows")

    candidato = {
        "extractor": extractor,
        "strategy": strategy,
        "table_index": table_index,
        "table": filas,
        "bbox": tuple(round(valor, 3) for valor in bbox) if bbox else None,
        "rows": cantidad_filas,
        "columns": cantidad_columnas,
        "empty_cell_ratio": round(empty_cell_ratio, 4),
        "empty_column_ratio": round(empty_column_ratio, 4),
        "near_empty_column_ratio": round(empty_column_ratio, 4),
        "column_occupancy_distribution": [
            round(valor, 4) for valor in ocupacion_relativa_columnas
        ],
        "column_occupancy_dispersion": round(dispersion_ocupacion_columnas, 4),
        "fully_empty_columns": [indice + 1 for indice in columnas_vacias],
        "nearly_empty_columns": [indice + 1 for indice in columnas_casi_vacias],
        "alternating_empty_columns": [indice + 1 for indice in columnas_intercaladas],
        "alternating_empty_columns_score": round(alternating_empty_columns_score, 4),
        "fragmentation_score": round(fragmentation_score, 4),
        "oversegmentation_score": round(oversegmentation_score, 4),
        "whole_page_table_penalty": round(whole_page_table_penalty, 4),
        "prose_density_penalty": round(prose_density_penalty, 4),
        "editorial_layout_score": round(editorial_layout_score, 4),
        "semantic_anchor_ratio": round(semantic_anchor_ratio, 4),
        "multiline_cell_ratio": round(multiline_cell_ratio, 4),
        "dominant_column_ratio": round(dominant_column_ratio, 4),
        "suspicious_row_score": round(suspicious_row_score, 4),
        "suspicious_rows": [indice + 1 for indice in filas_sospechosas],
        "width_ratio": round(width_ratio, 4),
        "height_ratio": round(height_ratio, 4),
        "whole_page_like": width_ratio >= 0.80 and height_ratio >= 0.75,
        "captures_both_editorial_columns": (
            whole_page_table_penalty >= 0.55 and editorial_layout_score >= 0.30
        ),
        "characters_per_column": caracteres_por_columna,
        "nonempty_cells_per_column": ocupacion_columnas,
        "candidate_confidence": round(confianza, 4),
        "layout_quality_score": round(layout_quality_score, 4),
        "agreement_extractors": [extractor],
        "accepted": False,
        "structural_blockers": [],
        "reasons": reasons,
    }
    _actualizar_estado_aceptacion(candidato)
    return candidato


# Se prueban líneas físicas y alineación de texto en dos motores independientes.
# Ninguna estrategia gana por defecto: todos sus candidatos se evalúan con el
# mismo umbral y el acuerdo solo aporta evidencia positiva moderada.
_ESTRATEGIAS_EXTRACCION_TABLA = (
    ("lines", None),
    ("text", {"vertical_strategy": "text", "horizontal_strategy": "text"}),
)


def _extraer_tablas_validas(page, settings: dict | None) -> list:
    """Compatibilidad para pruebas/consumidores: filtro mínimo, no confianza."""
    tablas = page.extract_tables(settings) if settings else page.extract_tables()
    resultado = []
    for tabla in tablas or []:
        tabla_limpia = _normalizar_tabla(tabla)
        if (
            len(tabla_limpia) > 1
            and tabla_limpia[0]
            and len(tabla_limpia[0]) > 1
            and _tabla_parece_real(tabla_limpia)
        ):
            resultado.append(tabla_limpia)
    return resultado


def _agregar_acuerdo_entre_extractores(candidatos: list[dict]) -> None:
    """Aumenta confianza por acuerdo sin rescatar patologias estructurales."""
    for indice, candidato in enumerate(candidatos):
        extractores = {candidato["extractor"]}
        for otro_indice, otro in enumerate(candidatos):
            if indice == otro_indice or candidato["extractor"] == otro["extractor"]:
                continue
            if candidato["strategy"] != otro["strategy"]:
                continue
            filas_compatibles = abs(candidato["rows"] - otro["rows"]) <= max(
                2, int(max(candidato["rows"], otro["rows"]) * 0.25)
            )
            columnas_compatibles = abs(candidato["columns"] - otro["columns"]) <= 1
            if (
                filas_compatibles
                and columnas_compatibles
                and _interseccion_sobre_union(candidato["bbox"], otro["bbox"]) >= 0.60
            ):
                extractores.add(otro["extractor"])
        candidato["agreement_extractors"] = sorted(extractores)
        if len(extractores) > 1:
            candidato["candidate_confidence"] = round(
                _limitar(candidato["candidate_confidence"] + 0.06), 4
            )
        _actualizar_estado_aceptacion(candidato)


def _diagnostico_publico_candidato(candidato: dict, selected: bool) -> dict:
    return {
        clave: valor
        for clave, valor in candidato.items()
        if clave != "table"
    } | {"selected": selected}


def _candidatos_misma_region(candidato: dict, otro: dict) -> bool:
    """Compara regiones sin permitir que el IoU aislado una cajas distintas.

    Dos motores pueden desplazar levemente el bbox de una misma tabla. En
    cambio, una caja puente o una tabla anidada suelen diferir mucho en área,
    ancho/alto o centro. Esas geometrías quedan separadas para revisión.
    """
    if candidato.get("bbox") and otro.get("bbox"):
        bbox_a = candidato["bbox"]
        bbox_b = otro["bbox"]
        if _interseccion_sobre_union(bbox_a, bbox_b) < 0.45:
            return False

        ancho_a = max(0.0, bbox_a[2] - bbox_a[0])
        alto_a = max(0.0, bbox_a[3] - bbox_a[1])
        ancho_b = max(0.0, bbox_b[2] - bbox_b[0])
        alto_b = max(0.0, bbox_b[3] - bbox_b[1])
        area_a = ancho_a * alto_a
        area_b = ancho_b * alto_b
        if not area_a or not area_b:
            return False

        proporcion_area = min(area_a, area_b) / max(area_a, area_b)
        proporcion_ancho = min(ancho_a, ancho_b) / max(ancho_a, ancho_b)
        proporcion_alto = min(alto_a, alto_b) / max(alto_a, alto_b)
        centro_ax = (bbox_a[0] + bbox_a[2]) / 2
        centro_ay = (bbox_a[1] + bbox_a[3]) / 2
        centro_bx = (bbox_b[0] + bbox_b[2]) / 2
        centro_by = (bbox_b[1] + bbox_b[3]) / 2
        distancia_x = abs(centro_ax - centro_bx) / max(ancho_a, ancho_b)
        distancia_y = abs(centro_ay - centro_by) / max(alto_a, alto_b)

        return (
            proporcion_area >= 0.60
            and proporcion_ancho >= 0.70
            and proporcion_alto >= 0.70
            and distancia_x <= 0.25
            and distancia_y <= 0.25
        )

    # Fallback conservador para fixtures/motores sin bbox: solo se consideran
    # alternativas de una misma region si coinciden estrategia e indice y su
    # geometria matricial es compatible.
    return (
        not candidato.get("bbox")
        and not otro.get("bbox")
        and candidato.get("strategy") == otro.get("strategy")
        and candidato.get("table_index") == otro.get("table_index")
        and abs(candidato.get("rows", 0) - otro.get("rows", 0)) <= 2
        and abs(candidato.get("columns", 0) - otro.get("columns", 0)) <= 1
    )


def _agrupar_candidatos_por_region(candidatos: list[dict]) -> list[list[dict]]:
    """Agrupa por compatibilidad completa; nunca por cadena transitiva IoU.

    Para entrar en una región, el candidato debe ser compatible con TODOS sus
    miembros. Si coincide con más de una, se añade solo a la de mayor IoU
    mínimo y jamás fusiona regiones preexistentes.
    """
    regiones: list[list[dict]] = []
    for candidato in candidatos:
        coincidentes = [
            indice
            for indice, region in enumerate(regiones)
            if all(_candidatos_misma_region(candidato, otro) for otro in region)
        ]
        if not coincidentes:
            regiones.append([candidato])
            continue

        principal = max(
            coincidentes,
            key=lambda indice: min(
                _interseccion_sobre_union(candidato.get("bbox"), otro.get("bbox"))
                if candidato.get("bbox") and otro.get("bbox") else 1.0
                for otro in regiones[indice]
            ),
        )
        regiones[principal].append(candidato)
    return regiones


def _seleccionar_candidatos(
    candidatos: list[dict],
) -> tuple[list[list], list[dict], dict]:
    """Elige como maximo un candidato seguro por cada region detectada."""
    regiones = _agrupar_candidatos_por_region(candidatos)
    seleccionados: list[dict] = []
    resumen_regiones: list[dict] = []

    for region_id, region in enumerate(regiones, start=1):
        for candidato in region:
            candidato["region_id"] = region_id

        seguros = sorted(
            (candidato for candidato in region if candidato["accepted"]),
            key=lambda candidato: candidato["candidate_confidence"],
            reverse=True,
        )
        seleccionado = seguros[0] if seguros else None
        if seleccionado is not None:
            seleccionados.append(seleccionado)

        mejor = max(
            region,
            key=lambda candidato: candidato["candidate_confidence"],
        )
        resumen_regiones.append({
            "region_id": region_id,
            "candidate_count": len(region),
            "accepted": seleccionado is not None,
            "selected_confidence": (
                seleccionado["candidate_confidence"] if seleccionado else None
            ),
            "best_candidate_confidence": mejor["candidate_confidence"],
            "bbox": seleccionado["bbox"] if seleccionado else mejor["bbox"],
            "structural_blockers": sorted({
                bloqueador
                for candidato in region
                for bloqueador in candidato.get("structural_blockers", [])
            }),
        })

    tablas = [_limpiar_filas_fantasma(candidato["table"]) for candidato in seleccionados]
    ids_seleccionados = {id(candidato) for candidato in seleccionados}
    diagnosticos = [
        _diagnostico_publico_candidato(candidato, id(candidato) in ids_seleccionados)
        for candidato in candidatos
    ]
    aceptadas = sum(region["accepted"] for region in resumen_regiones)
    resumen = {
        "detected_regions": len(regiones),
        "accepted_regions": aceptadas,
        "unresolved_regions": len(regiones) - aceptadas,
        "regions": resumen_regiones,
    }
    return tablas, diagnosticos, resumen


def _consolidar_deteccion_candidatos(candidatos: list[dict]) -> dict:
    """Deriva el estado de pagina sin confundir una region segura con todas."""
    tablas, diagnosticos, resumen_regiones = _seleccionar_candidatos(candidatos)
    mejores_por_region = [
        max(region, key=lambda candidato: candidato["candidate_confidence"])
        for region in _agrupar_candidatos_por_region(candidatos)
    ]
    return {
        "tables": tablas,
        "possible_table": bool(candidatos),
        "table_requires_review": resumen_regiones["unresolved_regions"] > 0,
        "table_quality_score": min(
            (candidato["candidate_confidence"] for candidato in mejores_por_region),
            default=None,
        ),
        "layout_quality_score": min(
            (candidato["layout_quality_score"] for candidato in mejores_por_region),
            default=None,
        ),
        "candidates": diagnosticos,
        "regions": resumen_regiones,
    }


def _detectar_tablas(
    pdf_path: str,
    page_index: int,
    fitz_page: "fitz.Page | None" = None,
) -> dict:
    """Genera, evalúa y selecciona candidatos de pdfplumber y PyMuPDF."""
    candidatos: list[dict] = []

    if _HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_index < len(pdf.pages):
                    page = pdf.pages[page_index]
                    for strategy, settings in _ESTRATEGIAS_EXTRACCION_TABLA:
                        try:
                            tablas_encontradas = page.find_tables(table_settings=settings or {})
                        except Exception as error:
                            logger.warning(
                                "pdfplumber/%s falló en página %s: %s",
                                strategy, page_index + 1, error,
                            )
                            continue
                        for table_index, tabla_objeto in enumerate(tablas_encontradas or [], start=1):
                            tabla = tabla_objeto.extract()
                            if not (
                                len(tabla or []) > 1
                                and max((len(fila) for fila in tabla or []), default=0) > 1
                                and _tabla_parece_real(tabla)
                            ):
                                continue
                            candidatos.append(_evaluar_candidato_tabla(
                                tabla,
                                extractor="pdfplumber",
                                strategy=strategy,
                                bbox=tuple(tabla_objeto.bbox),
                                page_width=page.width,
                                page_height=page.height,
                                table_index=table_index,
                            ))
        except Exception as error:
            logger.warning("pdfplumber falló en página %s: %s", page_index + 1, error)

    documento_temporal = None
    try:
        page = fitz_page
        if page is None:
            documento_temporal = fitz.open(pdf_path)
            if page_index < len(documento_temporal):
                page = documento_temporal[page_index]
        if page is not None and hasattr(page, "find_tables"):
            for strategy, settings in _ESTRATEGIAS_EXTRACCION_TABLA:
                try:
                    buscador = page.find_tables(**(settings or {}))
                    tablas_encontradas = getattr(buscador, "tables", buscador)
                except Exception as error:
                    logger.warning(
                        "PyMuPDF/%s falló en página %s: %s",
                        strategy, page_index + 1, error,
                    )
                    continue
                for table_index, tabla_objeto in enumerate(tablas_encontradas or [], start=1):
                    tabla = tabla_objeto.extract()
                    if not (
                        len(tabla or []) > 1
                        and max((len(fila) for fila in tabla or []), default=0) > 1
                        and _tabla_parece_real(tabla)
                    ):
                        continue
                    candidatos.append(_evaluar_candidato_tabla(
                        tabla,
                        extractor="pymupdf",
                        strategy=strategy,
                        bbox=tuple(tabla_objeto.bbox),
                        page_width=page.rect.width,
                        page_height=page.rect.height,
                        table_index=table_index,
                    ))
    except Exception as error:
        logger.warning("PyMuPDF find_tables falló en página %s: %s", page_index + 1, error)
    finally:
        if documento_temporal is not None:
            documento_temporal.close()

    _agregar_acuerdo_entre_extractores(candidatos)
    return _consolidar_deteccion_candidatos(candidatos)


def _pdfplumber_tables(pdf_path: str, page_index: int) -> list:
    """API compatible: devuelve solo las estructuras aceptadas por umbral."""
    return _detectar_tablas(pdf_path, page_index)["tables"]


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
            text_quality_score=1.0,
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

    deteccion_tablas = _detectar_tablas(pdf_path, page_index, fitz_page=page)
    tablas = deteccion_tablas["tables"]
    posible_formula_detectada = posible_formula(best[1])
    posible_grafico_detectado = posible_grafico(page)

    return PageExtraction(
        page_number=page_index + 1,
        # `text` conserva exclusivamente la evidencia textual elegida. Las
        # estructuras aceptadas se persisten por separado en metadata.
        text=best[1],
        method=best[0],
        quality=round(best[2], 3),
        ocr_used=best[3],
        ocr_confidence=round(best[4], 3) if best[4] is not None else None,
        # `has_tables` conserva la señal de posible tabla para los consumidores
        # existentes. La estructura aceptada se distingue inequívocamente por
        # `tables`; puede ser None si todos los candidatos fueron inseguros.
        has_tables=deteccion_tablas["possible_table"],
        tables=tablas or None,
        posible_formula=posible_formula_detectada,
        posible_grafico=posible_grafico_detectado,
        text_quality_score=round(best[2], 3),
        table_quality_score=(
            round(deteccion_tablas["table_quality_score"], 3)
            if deteccion_tablas["table_quality_score"] is not None else None
        ),
        layout_quality_score=(
            round(deteccion_tablas["layout_quality_score"], 3)
            if deteccion_tablas["layout_quality_score"] is not None else None
        ),
        possible_table=deteccion_tablas["possible_table"],
        table_requires_review=deteccion_tablas["table_requires_review"],
        table_diagnostics=deteccion_tablas["candidates"] or None,
        table_region_summary=deteccion_tablas["regions"] or None,
    )


def extract_pdf(pdf_path: str) -> list[PageExtraction]:
    """Extrae todas las páginas de un PDF con la mejor calidad disponible."""
    resultados: list[PageExtraction] = []
    digest = hashlib.sha256()
    with open(pdf_path, "rb") as archivo_pdf:
        for bloque in iter(lambda: archivo_pdf.read(1024 * 1024), b""):
            digest.update(bloque)
    source_pdf_sha256 = digest.hexdigest()

    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            resultado = extract_page(pdf_path, page, page_index)
            resultado.source_pdf_sha256 = source_pdf_sha256
            resultados.append(resultado)
    return resultados
