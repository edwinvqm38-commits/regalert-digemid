"""Selección de la muestra piloto F-04-A: fidelidad de transcripción. SOLO LECTURA.

    IDENTIDAD NORMATIVA -> PDF CORRECTO -> DOCUMENTO COMPLETO
      -> TRANSCRIPCION FIEL -> INTERPRETACION JURIDICA

F-04 solo estudia el cuarto escalón (TRANSCRIPCION FIEL), y por eso exige que
los tres anteriores ya estén resueltos para la página que entra al piloto: no
tiene sentido medir la fidelidad de la transcripción de un PDF que ni siquiera
es el de la norma que dice ser (eso es F-03), o de un documento al que le
faltan páginas (eso es F-02).

Este módulo NO descarga PDFs ni toca Supabase: recibe los resultados YA
CALCULADOS por las capas de F-02/F-03 (el mismo `analizar_norma()` de
`scripts/auditar_identidad_documental.py`, y `evaluar_completitud()` de
`agents/custodia_documental.py`) y decide, con lógica pura y testeable:

    1. si una norma puede entrar al piloto F-04 (`apto_para_piloto_f04`);
    2. si una página concreta pertenece al rango probado de esa norma
       (`pagina_pertenece_a_norma`, solo relevante en documentos multinorma);
    3. por qué esa página es de alto riesgo (`razones_de_riesgo`);
    4. cuáles ~50 páginas entran a la muestra final (`seleccionar_muestra`);
    5. si una página puede marcarse verificada, dado que exige comparación
       COMPLETA entre motores -nunca por confianza declarada de uno solo-
       (`estado_verificacion_f04`).

La orquestación real (leer Supabase, descargar PDFs, invocar los 3 motores)
vive en scripts/f04_generar_manifest_piloto.py y
scripts/f04_comparar_motores_piloto.py, que son delgados a propósito: la
lógica que decide algo vive aquí, donde se puede probar sin red ni credenciales
-igual que agents/politica_documental.py, agents/custodia_documental.py y
agents/fidelidad_legal.py, de los que este módulo depende y a los que no
duplica nada.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from custodia_documental import (  # noqa: E402
    COMPLETO,
    INCOMPLETO,
    evaluar_completitud,
)
from fidelidad_legal import (  # noqa: E402
    RIESGO_ALTO,
    SenalesPagina,
    UMBRAL_CALIDAD_ACEPTABLE,
    UMBRAL_OCR_ACEPTABLE,
    discrepancia_entre_motores,
    es_pagina_dispositiva,
    evaluar_pagina,
    marcas_ilegible,
    tokens_sensibles,
    verbos_normativos,
)
from identidad_documental import (  # noqa: E402
    CLASIFICACIONES_UTILIZABLES,
    PDF_CONTIENE_NORMA_EN_MULTINORMA,
    PDF_IDENTIDAD_EXACTA,
)

# ---------------------------------------------------------------------------
# 1) Gate F-03 + F-02: ¿esta norma puede entrar al piloto F-04?
# ---------------------------------------------------------------------------
def apto_para_piloto_f04(fila_identidad: dict, completitud_estado: str) -> tuple[bool, str]:
    """`fila_identidad` es el dict que produce
    `scripts.auditar_identidad_documental.analizar_norma()` (F-03), sin
    modificar. `completitud_estado` es el resultado de
    `completitud_para_f04()` (más abajo), que ya sabe distinguir norma única
    de multinorma.

    Regla: PDF_IDENTIDAD_EXACTA, o PDF_CONTIENE_NORMA_EN_MULTINORMA con rango
    documental determinado -nunca AMBIGUA, CONTRADICTORIA,
    NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA ni AUDITORIA_INCOMPLETA-, más
    SHA256 conocido y documento completo. Cualquier duda excluye: ante duda,
    NO ENTRA a la muestra.
    """
    clasificacion = fila_identidad.get("classification")

    if clasificacion not in CLASIFICACIONES_UTILIZABLES:
        return False, f"clasificacion F-03 no apta para F-04: {clasificacion}"

    if not fila_identidad.get("audit_complete"):
        return False, "auditoria F-03 incompleta: no se puede afirmar la identidad del PDF"

    if clasificacion == PDF_CONTIENE_NORMA_EN_MULTINORMA:
        if not fila_identidad.get("rango_completo"):
            return False, "multinorma sin rango documental determinado (falta evidencia de cierre)"
        if fila_identidad.get("start_page") is None or fila_identidad.get("end_page") is None:
            return False, "multinorma sin paginas de inicio/fin resueltas"

    if not fila_identidad.get("pdf_sha256"):
        return False, "SHA256 no disponible: no se pudo descargar o hashear el PDF"

    if completitud_estado != COMPLETO:
        return False, f"documento no completo (completitud={completitud_estado})"

    return True, "cumple F-03 (identidad probada) + SHA256 conocido + documento completo"


def completitud_para_f04(fila_identidad: dict, numeros_pagina_guardados: list[int]) -> tuple[str, str]:
    """Completitud del documento, en el sentido que le importa a F-04.

    Para DOCUMENTO_NORMA_UNICA es la de F-02: paginas guardadas == paginas
    del PDF. Para PDF_CONTIENE_NORMA_EN_MULTINORMA la pregunta cambia: el PDF
    entero incluye OTRAS normas, así que completo significa que están
    guardadas TODAS las páginas dentro del rango [start_page, end_page] ya
    probado por F-03 -no las del PDF completo, que nunca van a estar enteras
    ahi porque le pertenecen a otra norma-.
    """
    if fila_identidad.get("classification") == PDF_CONTIENE_NORMA_EN_MULTINORMA:
        inicio, fin = fila_identidad.get("start_page"), fila_identidad.get("end_page")
        if inicio is None or fin is None:
            return INCOMPLETO, "rango documental sin determinar"
        esperadas = set(range(inicio, fin + 1))
        guardadas_en_rango = {n for n in numeros_pagina_guardados if inicio <= n <= fin}
        faltantes = sorted(esperadas - guardadas_en_rango)
        if faltantes:
            return INCOMPLETO, f"faltan paginas {faltantes} dentro del rango {inicio}-{fin}"
        return COMPLETO, f"las {len(esperadas)} paginas del rango {inicio}-{fin} estan guardadas"

    completitud = evaluar_completitud(fila_identidad.get("pdf_page_count"), numeros_pagina_guardados)
    return completitud.estado, completitud.motivo


def pagina_pertenece_a_norma(page_number: int, start_page: int | None, end_page: int | None) -> bool:
    """Para multinorma: una página fuera del rango probado por F-03 NO
    pertenece a esta norma, aunque el norma_id de la fila diga que sí -el
    rango es la única evidencia de dónde empieza y termina cada norma dentro
    del PDF compartido-. Para norma única, `start_page`/`end_page` vienen en
    None y CUALQUIER página del documento pertenece."""
    if start_page is None and end_page is None:
        return True
    if start_page is None or end_page is None:
        return False
    return start_page <= page_number <= end_page


# ---------------------------------------------------------------------------
# 2) Razones de riesgo por página
# ---------------------------------------------------------------------------
RIESGO_DISPOSITIVA = "pagina_dispositiva"
RIESGO_PRIMERA_PAGINA = "primera_pagina"
RIESGO_ULTIMA_O_PENULTIMA = "ultima_o_penultima_pagina"
RIESGO_INICIO_NORMA_MULTINORMA = "inicio_de_norma_en_multinorma"
RIESGO_FIN_NORMA_MULTINORMA = "fin_de_norma_en_multinorma"
RIESGO_OCR = "pagina_escaneada_ocr"
RIESGO_OCR_BAJA_CONFIANZA = "ocr_baja_confianza"
RIESGO_TABLA = "contiene_tabla"
RIESGO_FORMULA = "posible_formula"
RIESGO_GRAFICO = "posible_grafico"
RIESGO_ILEGIBLE = "marca_ilegible_en_texto_guardado"
RIESGO_QUALITY_BAJO = "quality_score_bajo"

# Señales que por sí solas ya justifican la inclusión, con doble peso al
# priorizar (F-04: "prioriza... páginas dispositivas, SE RESUELVE, artículos
# que modifican/derogan/incorporan/exoneran... OCR de baja confianza...
# primera/penúltima/última página").
_RAZONES_PRIORITARIAS = frozenset({
    RIESGO_DISPOSITIVA, RIESGO_PRIMERA_PAGINA, RIESGO_ULTIMA_O_PENULTIMA,
    RIESGO_INICIO_NORMA_MULTINORMA, RIESGO_FIN_NORMA_MULTINORMA,
    RIESGO_OCR_BAJA_CONFIANZA, RIESGO_ILEGIBLE,
})


def razones_de_riesgo(
    pagina: dict, page_number: int, total_paginas: int,
    start_page: int | None = None, end_page: int | None = None,
) -> list[str]:
    """Por qué esta página es candidata de alto riesgo. Nunca vacía una lista
    con una sola razón implícita: una página puede calificar por varios
    motivos a la vez, y todos quedan trazados en el manifest.

    NOTA (residuo conocido, F-04-A): "letra pequeña" está en la lista de
    prioridades del piloto pero no se implementa aquí -detectarla exige
    tamaño de fuente por span, que digemid_norma_paginas no guarda y que
    F-04-A no descarga por su cuenta (sería reextracción, fuera de alcance
    de una fase read-only). Queda para una fase posterior con acceso a
    metadata de layout (ver digemid_documento_layout_paginas / F-04-B).
    """
    texto = pagina.get("text_normalized") or pagina.get("text_raw") or ""
    razones: list[str] = []

    if es_pagina_dispositiva(texto):
        razones.append(RIESGO_DISPOSITIVA)
    for clase in verbos_normativos(texto):
        razones.append(f"verbo_normativo:{clase}")

    if page_number == 1:
        razones.append(RIESGO_PRIMERA_PAGINA)
    if total_paginas >= 2 and page_number >= total_paginas - 1:
        razones.append(RIESGO_ULTIMA_O_PENULTIMA)
    if start_page is not None and page_number == start_page:
        razones.append(RIESGO_INICIO_NORMA_MULTINORMA)
    if end_page is not None and page_number == end_page:
        razones.append(RIESGO_FIN_NORMA_MULTINORMA)

    if pagina.get("ocr_used"):
        razones.append(RIESGO_OCR)
        confianza = pagina.get("ocr_confidence")
        if confianza is None or confianza < UMBRAL_OCR_ACEPTABLE:
            razones.append(RIESGO_OCR_BAJA_CONFIANZA)

    if pagina.get("has_tables"):
        razones.append(RIESGO_TABLA)
    if pagina.get("posible_formula"):
        razones.append(RIESGO_FORMULA)
    if pagina.get("posible_grafico"):
        razones.append(RIESGO_GRAFICO)
    if marcas_ilegible(texto):
        razones.append(RIESGO_ILEGIBLE)

    calidad = pagina.get("quality_score")
    if calidad is not None and calidad < UMBRAL_CALIDAD_ACEPTABLE:
        razones.append(RIESGO_QUALITY_BAJO)

    for categoria in tokens_sensibles(texto):
        razones.append(f"token_sensible:{categoria}")

    return razones


# ---------------------------------------------------------------------------
# 3) Manifest y selección de la muestra (~50 páginas)
# ---------------------------------------------------------------------------
def fila_manifest(fila_identidad: dict, pagina: dict, razones: list[str]) -> dict:
    """Una fila de F04_MANIFEST_PILOTO.csv. Todas las claves que pide F-04."""
    rango = None
    if fila_identidad.get("classification") == PDF_CONTIENE_NORMA_EN_MULTINORMA:
        rango = f"{fila_identidad.get('start_page')}-{fila_identidad.get('end_page')}"
    texto = pagina.get("text_normalized") or pagina.get("text_raw") or ""
    return {
        "document_key": fila_identidad.get("document_key"),
        "identidad_normativa": fila_identidad.get("identity_expected"),
        "pdf_url": fila_identidad.get("pdf_url"),
        "storage_path": fila_identidad.get("storage_path"),
        "pdf_sha256": fila_identidad.get("pdf_sha256"),
        "page_number": pagina.get("page_number"),
        "pdf_page_count": fila_identidad.get("pdf_page_count"),
        "rango_documental_multinorma": rango,
        "extraction_method": pagina.get("extraction_method"),
        "quality_score": pagina.get("quality_score"),
        "ocr_used": bool(pagina.get("ocr_used")),
        "razon_de_riesgo": ";".join(razones),
        "texto_almacenado": texto,
        "f03_classification": fila_identidad.get("classification"),
        "f03_document_type": fila_identidad.get("document_type"),
        "es_dispositiva": es_pagina_dispositiva(texto),
    }


def _puntaje_riesgo(razones: list[str]) -> int:
    return sum(2 if r in _RAZONES_PRIORITARIAS else 1 for r in razones)


def seleccionar_muestra(candidatas: list[dict], limite: int = 50) -> list[dict]:
    """`candidatas`: filas de manifest ya construidas por `fila_manifest`
    (deben traer `razon_de_riesgo` y `document_key`), de normas que YA
    pasaron `apto_para_piloto_f04` y páginas que YA pasaron
    `pagina_pertenece_a_norma`.

    Selecciona hasta `limite`, priorizando por número y tipo de señales de
    riesgo, con un tope por documento para que un solo PDF largo no agote el
    cupo -la muestra debe cubrir muchas normas distintas, no una a fondo-.
    Descarta candidatas sin ninguna razón de riesgo: esta función no decide
    qué es alto riesgo, solo ordena y recorta lo que ya se decidió que sí lo
    es.
    """
    con_riesgo = [c for c in candidatas if c.get("razon_de_riesgo")]
    ordenadas = sorted(
        con_riesgo,
        key=lambda c: _puntaje_riesgo(c["razon_de_riesgo"].split(";")),
        reverse=True,
    )

    tope_por_documento = max(3, limite // 8)
    seleccion: list[dict] = []
    cupo_por_doc: dict[str, int] = {}
    for fila in ordenadas:
        if len(seleccion) >= limite:
            break
        doc = fila.get("document_key")
        if cupo_por_doc.get(doc, 0) >= tope_por_documento:
            continue
        seleccion.append(fila)
        cupo_por_doc[doc] = cupo_por_doc.get(doc, 0) + 1
    return seleccion


# ---------------------------------------------------------------------------
# 4) Verificación: sin comparación COMPLETA entre motores, nunca "verificada"
# ---------------------------------------------------------------------------
MOTORES_REQUERIDOS = ("pymupdf", "pdfplumber", "ocr_tesseract")
COMPARACION_INCOMPLETA = "COMPARACION_INCOMPLETA"


def todas_las_comparaciones_completas(textos_por_motor: dict[str, str | None]) -> bool:
    """Un motor que falló (None) no es lo mismo que un motor que devolvió
    texto vacío (""): una página realmente en blanco es texto "" en los
    tres motores y SÍ cuenta como comparación completa. None significa que
    ese motor no llegó a correr -descarga fallida, timeout, excepción-, y
    eso sí bloquea cualquier veredicto de verificación."""
    return all(textos_por_motor.get(motor) is not None for motor in MOTORES_REQUERIDOS)


def estado_verificacion_f04(
    textos_por_motor: dict[str, str | None],
    senales: SenalesPagina,
) -> tuple[str, str, list[str]]:
    """Envuelve `fidelidad_legal.evaluar_pagina()` con la regla adicional de
    F-04: si no corrieron los tres motores requeridos, el resultado es
    `COMPARACION_INCOMPLETA` -nunca uno de los estados VERIFICADA_*, nunca
    por confianza declarada de un solo motor-.

    Con comparación completa, la evidencia cruzada que de verdad cuenta es
    contra el RENDER VISUAL (Tesseract), no entre pymupdf y pdfplumber entre
    sí: ambos leen la MISMA capa de texto embebida, así que si esa capa está
    mal -fuente rota, texto invisible, PDF escaneado con una capa OCR previa
    de mala calidad- los dos "coinciden" en el mismo error, y esa
    coincidencia no demuestra nada (es el mismo principio que ya documenta
    scripts/piloto_verificacion_paginas.py, aplicado aquí a la verificación).
    Dos motores que leen la misma fuente estando de acuerdo no son evidencia
    independiente; el cruce contra una fuente de lectura distinta sí lo es.

    Por eso: primero se exige que pymupdf y pdfplumber concuerden en los
    tokens jurídicos (si NI SIQUIERA ellos concuerdan, ya hay evidencia de
    discrepancia y no hace falta mirar más); pero la comparación que
    realmente puede llevar a un estado VERIFICADA_* es la de la capa
    embebida contra el render independiente.
    """
    if not todas_las_comparaciones_completas(textos_por_motor):
        faltantes = [m for m in MOTORES_REQUERIDOS if textos_por_motor.get(m) is None]
        return (
            COMPARACION_INCOMPLETA,
            RIESGO_ALTO,
            [f"no corrieron todos los motores requeridos: falta(n) {', '.join(faltantes)}"],
        )

    cmp_parsers = discrepancia_entre_motores(textos_por_motor["pymupdf"], textos_por_motor["pdfplumber"])
    if cmp_parsers.hay_error_juridico:
        # Ni las dos lecturas de la MISMA capa embebida concuerdan: no hace
        # falta el render para saber que hay un problema.
        return evaluar_pagina(replace(senales, comparacion_motores=cmp_parsers))

    referencia_embebida = textos_por_motor["pymupdf"] or textos_por_motor["pdfplumber"] or ""
    cmp_visual = discrepancia_entre_motores(referencia_embebida, textos_por_motor["ocr_tesseract"])
    return evaluar_pagina(replace(senales, comparacion_motores=cmp_visual))


__all__ = [
    "apto_para_piloto_f04",
    "completitud_para_f04",
    "pagina_pertenece_a_norma",
    "razones_de_riesgo",
    "fila_manifest",
    "seleccionar_muestra",
    "MOTORES_REQUERIDOS",
    "COMPARACION_INCOMPLETA",
    "todas_las_comparaciones_completas",
    "estado_verificacion_f04",
    "PDF_IDENTIDAD_EXACTA",
    "PDF_CONTIENE_NORMA_EN_MULTINORMA",
]
