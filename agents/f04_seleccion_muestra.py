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
    4. qué existe REALMENTE en el pool completo post-gate, antes de elegir
       nada (`resumen_pool`, `diagnostico_ocr_pool`) -F-04-A.1: auditar antes
       de seleccionar, para no diseñar cuotas sobre un pool que no se conoce-;
    5. cuáles ~50 páginas entran a la muestra final, por puro puntaje de
       riesgo (`seleccionar_muestra`) o por cuotas de diversidad que nunca
       relajan el gate (`seleccionar_muestra_estratificada`);
    6. si una página puede marcarse verificada, dado que exige comparación
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
    RIESGO_BAJO,
    RIESGO_CRITICO,
    REQUIERE_REVISION_HUMANA,
    UMBRAL_CALIDAD_ACEPTABLE,
    UMBRAL_OCR_ACEPTABLE,
    VERIFICADA_HUMANO,
    comparar_fidelidad,
    es_pagina_dispositiva,
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
    """Una fila de F04_MANIFEST_PILOTO.csv (y de F04_POOL_POST_GATE.csv, que
    usa la misma forma sobre el pool completo sin truncar). Todas las claves
    que pide F-04, más columnas explícitas para lo que ya estaba implícito
    en `razon_de_riesgo` -primera/última página, tabla, tokens- para que el
    CSV se pueda auditar por columna sin tener que parsear la razón."""
    rango = None
    if fila_identidad.get("classification") == PDF_CONTIENE_NORMA_EN_MULTINORMA:
        rango = f"{fila_identidad.get('start_page')}-{fila_identidad.get('end_page')}"
    texto = pagina.get("text_normalized") or pagina.get("text_raw") or ""
    tokens = [r.split(":", 1)[1] for r in razones if r.startswith("token_sensible:")]
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
        "ocr_confidence": pagina.get("ocr_confidence"),
        "tiene_tabla": bool(pagina.get("has_tables")),
        "es_primera_pagina": RIESGO_PRIMERA_PAGINA in razones,
        "es_ultima_o_penultima": RIESGO_ULTIMA_O_PENULTIMA in razones,
        "tokens_juridicos_sensibles": ",".join(tokens),
        "razon_de_riesgo": ";".join(razones),
        "texto_almacenado": texto,
        "f03_classification": fila_identidad.get("classification"),
        "f03_document_type": fila_identidad.get("document_type"),
        "es_dispositiva": es_pagina_dispositiva(texto),
    }


def _puntaje_riesgo(razones: list[str]) -> int:
    return sum(2 if r in _RAZONES_PRIORITARIAS else 1 for r in razones)


def _con_riesgo_y_gate_valido(candidatas: list[dict]) -> list[dict]:
    """Filtro defensivo: además de exigir alguna razón de riesgo, vuelve a
    comprobar que la clasificación F-03 sea una de las aptas.

    `candidatas` debería llegar ya gateada por `apto_para_piloto_f04` -este
    módulo no vuelve a resolver identidad ni a tocar la red-, pero ninguna
    cuota de diversidad (ni OCR, ni tablas, ni el balance exacta/multinorma)
    puede ser el motivo por el que una fila con clasificación no apta entre
    a la muestra. Si algo con `f03_classification` inválida llegara hasta
    aquí -un bug en el llamador-, esta función lo descarta igual.
    """
    return [
        c for c in candidatas
        if c.get("razon_de_riesgo") and c.get("f03_classification") in CLASIFICACIONES_UTILIZABLES
    ]


def _orden_riesgo(fila: dict) -> tuple:
    """Clave de orden determinista: mayor puntaje de riesgo primero: empates
    se resuelven por (document_key, page_number), nunca por el orden de
    llegada de la consulta a Supabase -que no está garantizado-."""
    return (-_puntaje_riesgo(fila["razon_de_riesgo"].split(";")), fila["document_key"], fila["page_number"])


def seleccionar_muestra(candidatas: list[dict], limite: int = 50) -> list[dict]:
    """`candidatas`: filas de manifest ya construidas por `fila_manifest`
    (deben traer `razon_de_riesgo`, `document_key` y `f03_classification`),
    de normas que YA pasaron `apto_para_piloto_f04` y páginas que YA
    pasaron `pagina_pertenece_a_norma`.

    Selecciona hasta `limite`, priorizando por número y tipo de señales de
    riesgo, con un tope por documento para que un solo PDF largo no agote el
    cupo -la muestra debe cubrir muchas normas distintas, no una a fondo-.
    Descarta candidatas sin ninguna razón de riesgo: esta función no decide
    qué es alto riesgo, solo ordena y recorta lo que ya se decidió que sí lo
    es. Determinista: mismo `candidatas` (en cualquier orden) siempre
    produce la misma selección.
    """
    ordenadas = sorted(_con_riesgo_y_gate_valido(candidatas), key=_orden_riesgo)

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
# 3.1) Auditoría del pool POST-GATE (F-04-A.1): qué existe antes de elegir 50
# ---------------------------------------------------------------------------
def resumen_pool(filas: list[dict]) -> dict:
    """Composición real de un conjunto de filas de manifest F-04 (misma
    forma que produce `fila_manifest`). Sirve tanto para auditar el POOL
    COMPLETO post-gate F-03 -antes de seleccionar nada- como para auditar
    una selección final: la composición se calcula igual, solo cambia qué
    lista se le pasa. No decide nada, solo cuenta lo que ya está en `filas`.
    """
    total = len(filas)
    if total == 0:
        return {
            "total_paginas": 0, "normas_distintas": 0, "con_sha256_pct": None,
            "paginas_ocr_previo": 0, "normas_distintas_ocr_previo": 0,
            "paginas_texto_digital": 0, "normas_distintas_texto_digital": 0,
            "paginas_ocr_baja_confianza": 0, "paginas_quality_score_bajo": 0,
            "paginas_con_tablas": 0, "paginas_dispositivas": 0,
            "paginas_primera": 0, "paginas_ultima_o_penultima": 0,
            "por_f03_classification": {}, "normas_distintas_por_clasificacion": {},
            "distribucion_por_razon_de_riesgo": {},
        }

    def cuenta_razon(tag: str) -> int:
        return sum(1 for f in filas if tag in (f.get("razon_de_riesgo") or "").split(";"))

    def normas_donde(pred) -> int:
        return len({f["document_key"] for f in filas if pred(f)})

    por_clasificacion: dict[str, int] = {}
    for f in filas:
        c = f.get("f03_classification")
        por_clasificacion[c] = por_clasificacion.get(c, 0) + 1

    por_razon: dict[str, int] = {}
    for f in filas:
        for r in (f.get("razon_de_riesgo") or "").split(";"):
            if r:
                por_razon[r] = por_razon.get(r, 0) + 1

    con_sha256 = sum(1 for f in filas if f.get("pdf_sha256"))

    return {
        "total_paginas": total,
        "normas_distintas": len({f["document_key"] for f in filas}),
        "con_sha256_pct": round(100 * con_sha256 / total, 1),
        "paginas_ocr_previo": sum(1 for f in filas if f.get("ocr_used")),
        "normas_distintas_ocr_previo": normas_donde(lambda f: f.get("ocr_used")),
        "paginas_texto_digital": sum(1 for f in filas if not f.get("ocr_used")),
        "normas_distintas_texto_digital": normas_donde(lambda f: not f.get("ocr_used")),
        "paginas_ocr_baja_confianza": cuenta_razon(RIESGO_OCR_BAJA_CONFIANZA),
        "paginas_quality_score_bajo": cuenta_razon(RIESGO_QUALITY_BAJO),
        "paginas_con_tablas": sum(1 for f in filas if f.get("tiene_tabla")),
        "paginas_dispositivas": sum(1 for f in filas if f.get("es_dispositiva")),
        "paginas_primera": sum(1 for f in filas if f.get("es_primera_pagina")),
        "paginas_ultima_o_penultima": sum(1 for f in filas if f.get("es_ultima_o_penultima")),
        "por_f03_classification": por_clasificacion,
        "normas_distintas_por_clasificacion": {
            c: normas_donde(lambda f, c=c: f.get("f03_classification") == c) for c in por_clasificacion
        },
        "distribucion_por_razon_de_riesgo": dict(sorted(por_razon.items(), key=lambda kv: -kv[1])),
    }


def diagnostico_ocr_pool(resumen: dict) -> str:
    """Mensaje explícito exigido por F-04-A.1: si el pool post-gate no tiene
    NINGUNA página OCR apta, se dice tal cual -no se rellena la cuota con
    documentos ambiguos o no encontrados para simular diversidad-."""
    if resumen.get("paginas_ocr_previo", 0) == 0:
        return "NO EXISTEN PAGINAS OCR DOCUMENTALMENTE APTAS"
    return (f"{resumen['paginas_ocr_previo']} paginas OCR aptas en el pool "
            f"({resumen.get('paginas_ocr_baja_confianza', 0)} de baja confianza)")


# ---------------------------------------------------------------------------
# 3.2) Selección estratificada V2 (F-04-A.1)
# ---------------------------------------------------------------------------
def seleccionar_muestra_estratificada(
    candidatas: list[dict],
    limite: int = 50,
    objetivo_digital: tuple[int, int] = (15, 20),
    objetivo_ocr: tuple[int, int] = (15, 20),
    minimo_tablas: int = 5,
) -> tuple[list[dict], dict]:
    """Muestreo por cuotas que NUNCA relaja el gate F-03: opera
    exclusivamente sobre `candidatas` ya gateadas (mismo filtro defensivo
    que `seleccionar_muestra`), y cada cuota se llena SOLO con lo que el
    pool realmente tiene -si no alcanza, se reporta como cuota no
    disponible en vez de rellenarla con páginas fuera de gate o de menor
    señal de riesgo de la que la cuota pide-.

    El balance EXACTA vs MULTINORMA no usa una proporción fija: para el
    cupo que queda después de las cuotas OCR/digital/tablas, se reparte en
    proporción a la disponibilidad REAL de cada clasificación en lo que
    resta del pool.

    Devuelve (seleccion, diagnostico). `diagnostico` documenta qué cuota se
    cumplió, cuál no y por qué, más `avisos` con el literal
    `CUOTA_NO_DISPONIBLE_EN_POOL_VALIDADO` cuando corresponda.
    """
    con_riesgo = _con_riesgo_y_gate_valido(candidatas)

    def ordenar(lista: list[dict]) -> list[dict]:
        return sorted(lista, key=_orden_riesgo)

    tope_por_documento = max(3, limite // 8)
    seleccion: list[dict] = []
    vistos: set[tuple] = set()
    cupo_por_doc: dict[str, int] = {}

    def clave(f: dict) -> tuple:
        return (f["document_key"], f["page_number"])

    def agregar(pool: list[dict], cuota: int) -> int:
        agregados = 0
        for fila in pool:
            if agregados >= cuota or len(seleccion) >= limite:
                break
            k = clave(fila)
            if k in vistos or cupo_por_doc.get(fila["document_key"], 0) >= tope_por_documento:
                continue
            seleccion.append(fila)
            vistos.add(k)
            cupo_por_doc[fila["document_key"]] = cupo_por_doc.get(fila["document_key"], 0) + 1
            agregados += 1
        return agregados

    def _diagnostico_cuota(pool: list[dict], objetivo_min: int, agregado: int) -> dict:
        """F-04-A.2 · 7: `cuota_no_disponible` (comparar contra el tamaño
        CRUDO del pool) puede dar `false` y ocultar que, bajo el tope de
        diversidad por documento, la cuota jamás fue alcanzable: si un pool
        de 169 páginas viene de solo 2 normas y el tope es 6/norma, la
        capacidad EFECTIVA es 12, no 169. Se reportan ambas lecturas para
        que el resumen no oculte la restricción real."""
        normas_distintas = len({f["document_key"] for f in pool})
        capacidad_efectiva = normas_distintas * tope_por_documento
        return {
            "disponible_en_pool": len(pool),
            "normas_distintas_disponibles": normas_distintas,
            "objetivo_min": objetivo_min,
            "capacidad_efectiva_por_diversidad": capacidad_efectiva,
            "seleccionado": agregado,
            "agregado": agregado,  # alias retrocompatible con F-04-A.1
            "limitada_por_diversidad": capacidad_efectiva < objetivo_min <= len(pool),
            "cuota_no_disponible": len(pool) < objetivo_min,
            "cuota_no_disponible_bajo_restricciones": agregado < objetivo_min,
        }

    diagnostico: dict = {"avisos": []}

    ocr_pool = ordenar([c for c in con_riesgo if c.get("ocr_used")])
    objetivo_ocr_max = min(objetivo_ocr[1], len(ocr_pool))
    agregado_ocr = agregar(ocr_pool, objetivo_ocr_max)
    diagnostico["ocr"] = _diagnostico_cuota(ocr_pool, objetivo_ocr[0], agregado_ocr)
    diagnostico["ocr"]["objetivo_max"] = objetivo_ocr[1]
    diagnostico["ocr"]["baja_confianza_en_pool"] = sum(
        1 for f in ocr_pool if RIESGO_OCR_BAJA_CONFIANZA in f["razon_de_riesgo"].split(";"))
    diagnostico["ocr"]["baja_confianza_seleccionada"] = sum(
        1 for f in seleccion if RIESGO_OCR_BAJA_CONFIANZA in f["razon_de_riesgo"].split(";"))
    if diagnostico["ocr"]["cuota_no_disponible_bajo_restricciones"]:
        motivo_diversidad = (
            f" -limitada por diversidad: solo {diagnostico['ocr']['normas_distintas_disponibles']} "
            f"norma(s) distinta(s) x tope {tope_por_documento}/norma = capacidad efectiva "
            f"{diagnostico['ocr']['capacidad_efectiva_por_diversidad']}"
            if diagnostico["ocr"]["limitada_por_diversidad"] else ""
        )
        diagnostico["avisos"].append(
            f"CUOTA_NO_DISPONIBLE_EN_POOL_VALIDADO: ocr (objetivo minimo "
            f"{objetivo_ocr[0]}, seleccionado {agregado_ocr}, disponible en el pool "
            f"{len(ocr_pool)}){motivo_diversidad}"
        )

    # Tablas ANTES que el llenado flexible de "digital": es una cuota con
    # piso fijo pequeño (5+) que "digital" (hasta 20) podria agotar por
    # completo si corriera primero, dejando la cuota de tablas sin cupo
    # aunque el pool si tuviera suficientes.
    tabla_pool = ordenar([c for c in con_riesgo if c.get("tiene_tabla")])
    agregado_tablas = agregar(tabla_pool, minimo_tablas)
    diagnostico["tablas"] = _diagnostico_cuota(tabla_pool, minimo_tablas, agregado_tablas)
    diagnostico["tablas"]["objetivo_minimo"] = minimo_tablas  # alias retrocompatible
    if diagnostico["tablas"]["cuota_no_disponible_bajo_restricciones"]:
        diagnostico["avisos"].append(
            f"CUOTA_NO_DISPONIBLE_EN_POOL_VALIDADO: tablas (objetivo minimo "
            f"{minimo_tablas}, seleccionado {agregado_tablas}, disponible en el pool {len(tabla_pool)})"
        )

    digital_pool = ordenar([c for c in con_riesgo if not c.get("ocr_used")])
    objetivo_digital_max = min(objetivo_digital[1], len(digital_pool))
    agregado_digital = agregar(digital_pool, objetivo_digital_max)
    diagnostico["digital"] = _diagnostico_cuota(digital_pool, objetivo_digital[0], agregado_digital)
    diagnostico["digital"]["objetivo_max"] = objetivo_digital[1]
    if diagnostico["digital"]["cuota_no_disponible_bajo_restricciones"]:
        diagnostico["avisos"].append(
            f"CUOTA_NO_DISPONIBLE_EN_POOL_VALIDADO: digital (objetivo minimo "
            f"{objetivo_digital[0]}, seleccionado {agregado_digital}, disponible en el pool "
            f"{len(digital_pool)})"
        )

    # Balance EXACTA vs MULTINORMA para TODO el cupo que queda tras las
    # cuotas de OCR/tablas/digital -no solo un piso seguido de un relleno
    # generico-: `_con_riesgo_y_gate_valido` ya filtra a las unicas dos
    # clasificaciones aptas, asi que "restantes" queda completamente
    # particionado entre estas dos. Un relleno generico por puntaje puro
    # aqui reintroduciria el sesgo que motiva esta funcion: si muchas
    # paginas empatan en puntaje (comun cuando comparten la misma razon,
    # p.ej. "ultima_o_penultima_pagina"), el desempate por document_key
    # favorece a quien tenga menos claves "por delante" alfabeticamente, NO
    # a una clasificacion sobre otra -asi es como una muestra puede terminar
    # dominada por multinorma sin que nadie lo haya decidido-.
    restantes = [c for c in con_riesgo if clave(c) not in vistos]
    restantes_exacta = ordenar([c for c in restantes if c.get("f03_classification") == PDF_IDENTIDAD_EXACTA])
    restantes_multi = ordenar([c for c in restantes if c.get("f03_classification") == PDF_CONTIENE_NORMA_EN_MULTINORMA])
    cupo_libre = limite - len(seleccion)
    total_clasificable = len(restantes_exacta) + len(restantes_multi)
    cuota_exacta_objetivo = 0
    if cupo_libre > 0 and total_clasificable and restantes_exacta:
        cuota_exacta_objetivo = min(
            len(restantes_exacta), cupo_libre,
            max(1, round(cupo_libre * len(restantes_exacta) / total_clasificable)),
        )
    agregado_exacta_cuota = agregar(restantes_exacta, cuota_exacta_objetivo)
    # El resto del cupo va primero a multinorma; si su pool se agota antes
    # de llenar el cupo, lo que sobra se rellena con mas exacta -nunca con
    # paginas fuera de gate, y nunca con un desempate alfabetico ciego-.
    agregado_multi = agregar(restantes_multi, limite - len(seleccion))
    agregado_exacta_resto = agregar(restantes_exacta, limite - len(seleccion)) if len(seleccion) < limite else 0

    diagnostico["exacta_vs_multinorma"] = {
        "disponible_exacta_restante": len(restantes_exacta),
        "disponible_multinorma_restante": len(restantes_multi),
        "cuota_exacta_objetivo": cuota_exacta_objetivo,
        "agregado_exacta_total": agregado_exacta_cuota + agregado_exacta_resto,
        "agregado_multinorma_total": agregado_multi,
    }

    diagnostico["seleccionadas_total"] = len(seleccion)
    diagnostico["normas_distintas_seleccionadas"] = len({f["document_key"] for f in seleccion})
    diagnostico["por_f03_classification_seleccion"] = {
        c: sum(1 for f in seleccion if f.get("f03_classification") == c)
        for c in sorted({f.get("f03_classification") for f in seleccion})
    }
    return sorted(seleccion, key=_orden_riesgo), diagnostico


# ---------------------------------------------------------------------------
# 4) Verificación (F-04-A.2): CONCORDANCIA != VERDAD
# ---------------------------------------------------------------------------
# F-04-A (v1) delegaba la decisión en `fidelidad_legal.evaluar_pagina()`, que
# puede devolver VERIFICADA_AUTOMATICAMENTE -un nombre que implica una verdad
# comprobada que ninguna comparación automática, por sí sola, puede probar.
# Esa función también trataba "pymupdf/pdfplumber vacíos" igual que
# "pymupdf/pdfplumber con texto que discrepa": una página ESCANEADA sin capa
# embebida no está proponiendo una transcripción alternativa, así que no
# puede "discrepar" de nada.
#
# F-04-A.2 corrige ambas cosas con su PROPIO vocabulario, sin tocar
# fidelidad_legal.py (F-01, ya en producción, con su propio contrato y
# tests): ninguna comparación automática produce aquí un estado cuyo nombre
# implique verificación comprobada. Eso queda reservado a VERIFICADA_HUMANO
# (reexportado de fidelidad_legal, mismo concepto), y solo cuando se provee
# `texto_golden` explícito -ausente en todo F-04-A: `golden_available`
# siempre False-.
MOTORES_REQUERIDOS = ("pymupdf", "pdfplumber", "ocr_tesseract")
COMPARACION_INCOMPLETA = "COMPARACION_INCOMPLETA"
CONCORDANCIA_AUTOMATICA_ALTA = "CONCORDANCIA_AUTOMATICA_ALTA"
DISCREPANCIA_ENTRE_FUENTES = "DISCREPANCIA_ENTRE_FUENTES"

MOTOR_SIN_TEXTO_UTIL = "MOTOR_SIN_TEXTO_UTIL"
AMBAS_FUENTES_SIN_TEXTO = "AMBAS_FUENTES_SIN_TEXTO"
FUENTES_COMPARABLES = "FUENTES_COMPARABLES"


def todas_las_comparaciones_completas(textos_por_motor: dict[str, str | None]) -> bool:
    """Un motor que falló (None) no es lo mismo que un motor que devolvió
    texto vacío (""): una página realmente en blanco es texto "" en los
    tres motores y SÍ cuenta como comparación completa. None significa que
    ese motor no llegó a correr -descarga fallida, timeout, excepción-, y
    eso sí bloquea cualquier veredicto de verificación."""
    return all(textos_por_motor.get(motor) is not None for motor in MOTORES_REQUERIDOS)


def tiene_texto_util(texto: str | None) -> bool:
    """Un motor que devuelve "" (o solo espacios) no está proponiendo una
    transcripción alternativa: no encontró capa que leer. Confundir eso con
    "el motor propuso un texto distinto y vacío" es lo que convertía una
    página escaneada sin capa embebida en una falsa DISCREPANCIA_CRITICA."""
    return bool((texto or "").strip())


def es_mismo_motor_que_almacenado(
    extraction_method_almacenado: str | None, ocr_used_almacenado: bool = False,
) -> bool:
    """True si el texto YA GUARDADO en Supabase fue producido por Tesseract
    (`agents/pdf_extract.py::_ocr_page`, spa, ~300 DPI -el mismo motor,
    mismo idioma, misma resolución que el render que F-04 corre de nuevo en
    `scripts/piloto_verificacion_paginas.py::texto_render_ocr`).

    Comparar ese texto guardado contra un Tesseract fresco NO es evidencia
    cruzada independiente: es el MISMO motor leyendo la MISMA imagen dos
    veces. Sirve para medir REPRODUCIBILIDAD, nunca para verificar -por eso
    nunca debe ser, por sí sola, la razón para declarar concordancia con
    otra fuente."""
    return bool(ocr_used_almacenado) or extraction_method_almacenado == "ocr_tesseract"


def comparar_fuentes(referencia: str | None, hipotesis: str | None):
    """Compara dos fuentes de texto DISTINGUIENDO "no hay nada que
    comparar" de "hay una discrepancia real" (F-04-A.2 · 3).

    Devuelve (clasificacion, comparacion):
      - AMBAS_FUENTES_SIN_TEXTO: ninguna aporta texto útil -no hay nada que
        comparar, ni concordancia ni discrepancia-.
      - MOTOR_SIN_TEXTO_UTIL: solo una de las dos encontró texto -la otra no
        está proponiendo una transcripción alternativa, así que no puede
        "discrepar" de la que sí tiene contenido-. `comparacion` es None.
      - FUENTES_COMPARABLES: ambas aportan texto útil; `comparacion` es el
        `ResultadoFidelidad` real de `fidelidad_legal.comparar_fidelidad()`.
    """
    ref_util, hip_util = tiene_texto_util(referencia), tiene_texto_util(hipotesis)
    if not ref_util and not hip_util:
        return AMBAS_FUENTES_SIN_TEXTO, None
    if not ref_util or not hip_util:
        return MOTOR_SIN_TEXTO_UTIL, None
    return FUENTES_COMPARABLES, comparar_fidelidad(referencia, hipotesis)


def estado_verificacion_f04(
    textos_por_motor: dict[str, str | None],
    *,
    es_tabla: bool = False,
    texto_golden: str | None = None,
) -> dict:
    """Decide el estado epistemológico de una página F-04. Regla de oro:
    CONCORDANCIA != VERDAD -ninguna rama de esta función produce un nombre
    que implique verificación comprobada salvo que haya golden humano.

    Devuelve un dict (no una tupla) porque F-04-A.2 necesita transportar más
    que (estado, riesgo, motivos): `golden_available` y las métricas
    `*_vs_golden` viajan siempre, aunque sean None/False en esta fase.

    Orden de evaluación:
      1. Sin los 3 motores completos -> COMPARACION_INCOMPLETA.
      2. Si hay `texto_golden`: compara contra el mejor candidato disponible
         y decide con eso -es la única fuente que si puede "verificar"-.
      3. pymupdf vs pdfplumber (misma capa embebida): si YA discrepan en un
         token jurídico, no hace falta mirar el render -DISCREPANCIA_ENTRE_FUENTES-.
      4. Sin ninguna capa embebida útil (página escaneada): Tesseract es la
         ÚNICA fuente -sin cruce independiente posible, techo
         REQUIERE_REVISION_HUMANA, nunca "concordancia" ni "discrepancia"-.
      5. Capa embebida vs render (Tesseract): la comparación que sí cuenta
         como evidencia cruzada. Si el render no aportó texto útil, es una
         limitación del render, no una discrepancia. Si discrepan en un
         token jurídico -> DISCREPANCIA_ENTRE_FUENTES. Si concuerdan pero es
         una página con tabla, el techo es REQUIERE_REVISION_HUMANA (F-04-A.2
         · 6: la estructura fila/columna no la verifica una comparación de
         texto). Si concuerdan y no es tabla -> CONCORDANCIA_AUTOMATICA_ALTA.
    """
    resultado: dict = {
        "golden_available": texto_golden is not None,
        "cer_vs_golden": None, "wer_vs_golden": None, "lter_vs_golden": None,
    }

    if not todas_las_comparaciones_completas(textos_por_motor):
        faltantes = [m for m in MOTORES_REQUERIDOS if textos_por_motor.get(m) is None]
        resultado.update(
            estado=COMPARACION_INCOMPLETA, riesgo=RIESGO_ALTO,
            motivos=[f"no corrieron todos los motores requeridos: falta(n) {', '.join(faltantes)}"],
        )
        return resultado

    pymupdf, pdfplumber, tesseract = (
        textos_por_motor["pymupdf"], textos_por_motor["pdfplumber"], textos_por_motor["ocr_tesseract"],
    )

    if texto_golden is not None:
        candidato = pymupdf if tiene_texto_util(pymupdf) else (
            pdfplumber if tiene_texto_util(pdfplumber) else tesseract)
        cmp_golden = comparar_fidelidad(texto_golden, candidato)
        resultado.update(cer_vs_golden=cmp_golden.cer, wer_vs_golden=cmp_golden.wer,
                         lter_vs_golden=cmp_golden.legal_token_error_rate)
        if not cmp_golden.hay_error_juridico:
            resultado.update(estado=VERIFICADA_HUMANO, riesgo=RIESGO_BAJO,
                             motivos=["coincide con la transcripción humana validada (golden)"])
        else:
            resultado.update(estado=DISCREPANCIA_ENTRE_FUENTES, riesgo=RIESGO_CRITICO,
                             motivos=["discrepa de la transcripción humana validada (golden) "
                                      "en un token jurídico"])
        return resultado

    clasif_parsers, cmp_parsers = comparar_fuentes(pymupdf, pdfplumber)
    if clasif_parsers == FUENTES_COMPARABLES and cmp_parsers.hay_error_juridico:
        resultado.update(
            estado=DISCREPANCIA_ENTRE_FUENTES, riesgo=RIESGO_CRITICO,
            motivos=["pymupdf y pdfplumber (misma capa embebida) discrepan en un token jurídico"],
        )
        return resultado

    referencia_embebida = pymupdf if tiene_texto_util(pymupdf) else (
        pdfplumber if tiene_texto_util(pdfplumber) else None)

    if referencia_embebida is None:
        resultado.update(
            estado=REQUIERE_REVISION_HUMANA, riesgo=RIESGO_ALTO,
            motivos=["sin capa embebida util (pymupdf y pdfplumber sin texto): Tesseract es la "
                     "unica fuente disponible, no hay evidencia cruzada independiente posible"],
        )
        return resultado

    clasif_visual, cmp_visual = comparar_fuentes(referencia_embebida, tesseract)
    if clasif_visual == MOTOR_SIN_TEXTO_UTIL:
        resultado.update(
            estado=REQUIERE_REVISION_HUMANA, riesgo=RIESGO_ALTO,
            motivos=["el render (Tesseract) no produjo texto util: no hay evidencia cruzada "
                     "independiente posible, esto NO es una discrepancia"],
        )
        return resultado

    if cmp_visual.hay_error_juridico:
        resultado.update(
            estado=DISCREPANCIA_ENTRE_FUENTES, riesgo=RIESGO_CRITICO,
            motivos=["la capa embebida y el render (Tesseract) discrepan en un token jurídico"],
        )
        return resultado

    if es_tabla:
        resultado.update(
            estado=REQUIERE_REVISION_HUMANA, riesgo=RIESGO_ALTO,
            motivos=["pagina con tabla: la concordancia de texto no verifica la estructura "
                     "fila/columna (F-04-A.2 · 6)"],
        )
        return resultado

    resultado.update(
        estado=CONCORDANCIA_AUTOMATICA_ALTA, riesgo=RIESGO_BAJO,
        motivos=[f"capa embebida y render (Tesseract) concuerdan en los tokens juridicos "
                 f"(CER {cmp_visual.cer:.3f}); esto es concordancia automatica, NO verificacion"],
    )
    return resultado


__all__ = [
    "apto_para_piloto_f04",
    "completitud_para_f04",
    "pagina_pertenece_a_norma",
    "razones_de_riesgo",
    "fila_manifest",
    "seleccionar_muestra",
    "seleccionar_muestra_estratificada",
    "resumen_pool",
    "diagnostico_ocr_pool",
    "MOTORES_REQUERIDOS",
    "COMPARACION_INCOMPLETA",
    "CONCORDANCIA_AUTOMATICA_ALTA",
    "DISCREPANCIA_ENTRE_FUENTES",
    "MOTOR_SIN_TEXTO_UTIL",
    "AMBAS_FUENTES_SIN_TEXTO",
    "FUENTES_COMPARABLES",
    "todas_las_comparaciones_completas",
    "tiene_texto_util",
    "es_mismo_motor_que_almacenado",
    "comparar_fuentes",
    "estado_verificacion_f04",
    "PDF_IDENTIDAD_EXACTA",
    "PDF_CONTIENE_NORMA_EN_MULTINORMA",
    "RIESGO_OCR_BAJA_CONFIANZA",
    "RIESGO_QUALITY_BAJO",
    "RIESGO_PRIMERA_PAGINA",
    "RIESGO_ULTIMA_O_PENULTIMA",
    "RIESGO_TABLA",
]
