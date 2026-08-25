"""Fidelidad de una transcripción normativa frente al PDF oficial (F-01).

Principio: el PDF oficial es la fuente de verdad. Ningún OCR, LLM,
quality_score ni transcripción previa se considera verdad por sí solo.

Este módulo NO mide si un texto "se ve bien" -eso ya lo hace
agents/pdf_extract.quality_score, que es una heurística de FORMA-. Mide otra
cosa: si el texto REPRESENTA FIELMENTE el documento. Son dimensiones
independientes y confundirlas es el error de fondo del pipeline actual:

    "Artículo 13" en vez de "Artículo 18"  → quality_score 1.0, jurídicamente falso.

Por eso aquí se separan:

    quality_visual      ¿se ve como prosa? (legibilidad, ya existente)
    fidelity_*          ¿coincide con el original? (exige comparación)
    verification_status ¿qué evidencia respalda esa afirmación?

Sin una segunda fuente contra la cual comparar, la fidelidad es DESCONOCIDA.
Nunca "alta por defecto".
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Estados de verificación (F-01 · punto 16)
# ---------------------------------------------------------------------------
NO_EVALUADA = "NO_EVALUADA"
EXTRACCION_DIGITAL_ALTA_CONCORDANCIA = "EXTRACCION_DIGITAL_ALTA_CONCORDANCIA"
OCR_PENDIENTE_VERIFICACION = "OCR_PENDIENTE_VERIFICACION"
DISCREPANCIA_ENTRE_MOTORES = "DISCREPANCIA_ENTRE_MOTORES"
REQUIERE_REVISION_HUMANA = "REQUIERE_REVISION_HUMANA"
VERIFICADA_AUTOMATICAMENTE = "VERIFICADA_AUTOMATICAMENTE"
VERIFICADA_HUMANO = "VERIFICADA_HUMANO"
ILEGIBLE_PARCIAL = "ILEGIBLE_PARCIAL"
DOCUMENTO_INCOMPLETO = "DOCUMENTO_INCOMPLETO"
PDF_NO_DISPONIBLE = "PDF_NO_DISPONIBLE"

# Estados que autorizan a usar la página como CITA LEGAL SEGURA.
ESTADOS_CONFIABLES = frozenset({VERIFICADA_HUMANO, VERIFICADA_AUTOMATICAMENTE})

# Estados que además permiten alimentar el detector de relaciones cuando la
# página es dispositiva. Es deliberadamente el mismo conjunto: una derogación
# leída de una página no verificada no puede proponerse como confiable.
ESTADOS_APTOS_DETECTOR = ESTADOS_CONFIABLES

RIESGO_CRITICO, RIESGO_ALTO, RIESGO_MEDIO, RIESGO_BAJO = "CRITICO", "ALTO", "MEDIO", "BAJO"


# ---------------------------------------------------------------------------
# Parte dispositiva (F-01 · punto 10)
# ---------------------------------------------------------------------------
# Una página de considerandos que se transcribe mal es un problema; una
# disposición derogatoria mal transcrita cambia el efecto jurídico.
PATRON_DISPOSITIVA = re.compile(
    r"(SE\s+RESUELVE|DECRETA|SE\s+DECRETA|DISPOSICI[OÓ]N(?:ES)?\s+"
    r"(?:COMPLEMENTARIA|DEROGATORIA|MODIFICATORIA|TRANSITORIA|FINAL)|"
    r"DER[OÓ]G(?:UESE|ASE|AR|A)|D[EÉ]J(?:ESE|AR)\s+SIN\s+EFECTO|"
    r"MODIF[IÍ]C(?:ASE|AR|ANSE)|MODIF[IÍ]QUESE|SUSTIT[UÚ]Y(?:ASE|ESE)|"
    r"INCORP[OÓ]R(?:ASE|ESE)|EXON[EÉ]R(?:ASE|ESE)|SUSP[EÉ]ND(?:ASE|ESE)|"
    r"PRORR[OÓ]G(?:ASE|UESE))",
    re.IGNORECASE,
)

# Verbos jurídicos: confundir uno por otro cambia el efecto. Se comparan como
# CLASE, no como cadena: "deróguese" y "derogar" son el mismo verbo; "derogar"
# y "modificar" NO.
VERBOS_NORMATIVOS = {
    "DEROGA": r"der[oó]g(?:uese|ase|ar|a|an|ada|adas|ado|atoria|atorias)?",
    "DEJA_SIN_EFECTO": r"d[eé]j(?:ese|ar|a)\s+sin\s+efecto|sin\s+efecto",
    "MODIFICA": r"modif[ií](?:c(?:ase|ar|anse|an|a)|quese|catoria|cacion|caci[oó]n)",
    "SUSTITUYE": r"sustit[uú](?:yase|ir|ye|yen|ci[oó]n)",
    "INCORPORA": r"incorp[oó]r(?:ase|ese|ar|a|an)",
    "EXONERA": r"exon[eé]r(?:ase|ese|ar|a)|except[uú](?:ase|ar)",
    "SUSPENDE": r"susp[eé]nd(?:ase|ese|er|e)",
    "PRORROGA": r"prorr[oó]g(?:ase|uese|ar|a)",
}
_VERBOS_RE = {k: re.compile(v, re.IGNORECASE) for k, v in VERBOS_NORMATIVOS.items()}

# Negación que invierte el efecto de un verbo normativo cuando aparece justo
# antes de él: "NO deróguese" produce el efecto jurídico OPUESTO a
# "deróguese", y una transcripción que pierda o invente ese "no" es un error
# jurídico aunque no toque ningún número (F-04).
_NEGACION_RE = re.compile(r"\b(?:no|nunca|jam[aá]s|tampoco)\b", re.IGNORECASE)
_VENTANA_NEGACION = 25  # caracteres inmediatamente antes del verbo
# La ventana no cruza a la clausula anterior: "No aplica el articulo 5.
# Deroguese el articulo 12" no es una negacion de "Deroguese".
_LIMITE_CLAUSULA_RE = re.compile(r"[.;:\n]")

# ---------------------------------------------------------------------------
# Tokens jurídicos sensibles (F-01 · punto 9)
# ---------------------------------------------------------------------------
# Un dígito de diferencia en cualquiera de estos cambia el efecto jurídico.
PATRONES_TOKEN = {
    # "Decreto Supremo N° 014-2011-SA", "RM 339-2023/MINSA", "Ley N° 29459"
    "referencia_normativa": re.compile(
        r"(?:ley|decreto\s+supremo|decreto\s+legislativo|decreto\s+de\s+urgencia|"
        r"resoluci[oó]n\s+(?:ministerial|directoral|suprema|viceministerial|jefatural)|"
        r"\bd\.?s\.?|\br\.?m\.?|\br\.?d\.?|\br\.?s\.?|\bd\.?l\.?|\bd\.?u\.?)"
        r"[\s ]*(?:n[°ºo.\s]*)?(\d{1,6}(?:[-/]\d{2,4})?(?:[-/][A-Z][\w\-/]*)?)",
        re.IGNORECASE),
    # "artículo 113", "art. 5", "artículos 10 y 11"
    "articulo": re.compile(r"art[ií]culos?[\s ]*(?:n[°ºo.\s]*)?(\d+(?:[°º])?)", re.IGNORECASE),
    "numeral": re.compile(r"(?:numeral|sub\s*numeral|inciso|literal)[\s ]*([\w.]+)", re.IGNORECASE),
    # "10 días hábiles", "100 dias"
    "plazo": re.compile(r"(\d+)\s*d[ií]as?\b", re.IGNORECASE),
    "monto": re.compile(r"S/\.?\s*([\d.,\s]*\d)", re.IGNORECASE),
    "porcentaje": re.compile(r"(\d+(?:[.,]\d+)?)\s*%"),
    "fecha": re.compile(r"\b(\d{1,2}\s+de\s+\w+\s+de\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b", re.IGNORECASE),
    # dosis, concentraciones, temperaturas: "0,5 mg/mL", "2-8 °C", "500 mg"
    "medida": re.compile(
        r"(\d+(?:[.,]\d+)?(?:\s*[-–]\s*\d+(?:[.,]\d+)?)?)\s*"
        r"(mg/mL|mg/ml|mcg|mg|g/L|g|mL|ml|L|UI|%p/v|°\s*C|ºC)\b"),
    "anio": re.compile(r"\b(19\d{2}|20\d{2})\b"),
}

_MARCAS_ILEGIBLE = re.compile(r"\[ilegible\]|\[no\s+legible\]|\[\?\]", re.IGNORECASE)


def _sin_acentos(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn")


def es_pagina_dispositiva(texto: str | None) -> bool:
    """La página contiene parte dispositiva (lo que produce efectos jurídicos)."""
    return bool(texto) and bool(PATRON_DISPOSITIVA.search(texto))


def verbos_normativos(texto: str | None) -> dict[str, int]:
    """Cuántas veces aparece cada CLASE de verbo jurídico.

    Una negación INMEDIATAMENTE ANTES del verbo invierte el efecto ("no
    deróguese" no es lo mismo que "deróguese") y se cuenta como una clase
    aparte ("NO_DEROGA"), no como "DEROGA": dos textos que solo difieren en
    esa negación deben verse como clases distintas para que la comparación
    de motores (F-04 · LTER) los marque en desacuerdo, aunque ningún token
    numérico haya cambiado.
    """
    if not texto:
        return {}
    plano = _sin_acentos(texto)
    resultado: dict[str, int] = {}
    for clase, regex in _VERBOS_RE.items():
        for m in regex.finditer(plano):
            ventana = plano[max(0, m.start() - _VENTANA_NEGACION): m.start()]
            limites = list(_LIMITE_CLAUSULA_RE.finditer(ventana))
            if limites:
                ventana = ventana[limites[-1].end():]
            etiqueta = f"NO_{clase}" if _NEGACION_RE.search(ventana) else clase
            resultado[etiqueta] = resultado.get(etiqueta, 0) + 1
    return resultado


def marcas_ilegible(texto: str | None) -> int:
    return len(_MARCAS_ILEGIBLE.findall(texto or ""))


def tokens_sensibles(texto: str | None) -> dict[str, list[str]]:
    """Extrae los tokens cuyo cambio altera el efecto jurídico.

    Se devuelven NORMALIZADOS mínimamente (espacios internos colapsados) pero
    SIN corregir nada: si el OCR leyó "1?4", eso es lo que se compara.
    """
    if not texto:
        return {}
    salida: dict[str, list[str]] = {}
    for nombre, patron in PATRONES_TOKEN.items():
        encontrados = []
        for m in patron.finditer(texto):
            crudo = m.group(1) if m.groups() else m.group(0)
            valor = re.sub(r"\s+", "", crudo).strip(".,;:")
            if valor:
                encontrados.append(valor.upper())
        if encontrados:
            salida[nombre] = encontrados
    return salida


# ---------------------------------------------------------------------------
# Métricas (F-01 · punto 15)
# ---------------------------------------------------------------------------
def _distancia_edicion(a: list[str] | str, b: list[str] | str) -> int:
    """Levenshtein sobre caracteres o sobre listas de palabras."""
    if len(a) < len(b):
        a, b = b, a
    previa = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        actual = [i]
        for j, cb in enumerate(b, 1):
            actual.append(min(previa[j] + 1, actual[j - 1] + 1, previa[j - 1] + (ca != cb)))
        previa = actual
    return previa[-1]


def cer(referencia: str, hipotesis: str) -> float:
    """Character Error Rate contra la transcripción de referencia."""
    ref = re.sub(r"\s+", " ", (referencia or "").strip())
    if not ref:
        return 0.0 if not (hipotesis or "").strip() else 1.0
    return _distancia_edicion(ref, re.sub(r"\s+", " ", (hipotesis or "").strip())) / len(ref)


def wer(referencia: str, hipotesis: str) -> float:
    ref = (referencia or "").split()
    if not ref:
        return 0.0 if not (hipotesis or "").split() else 1.0
    return _distancia_edicion(ref, (hipotesis or "").split()) / len(ref)


@dataclass
class ErrorToken:
    categoria: str
    esperado: str | None
    obtenido: str | None

    def __str__(self) -> str:  # pragma: no cover - solo para reportes
        return f"{self.categoria}: {self.esperado!r} -> {self.obtenido!r}"


@dataclass
class ResultadoFidelidad:
    cer: float
    wer: float
    legal_token_error_rate: float
    errores_token: list[ErrorToken] = field(default_factory=list)
    verbos_cambiados: list[ErrorToken] = field(default_factory=list)

    @property
    def hay_error_juridico(self) -> bool:
        """Un solo token jurídico distinto ya es inaceptable: no se promedia."""
        return bool(self.errores_token or self.verbos_cambiados)


def comparar_fidelidad(referencia: str, hipotesis: str) -> ResultadoFidelidad:
    """Compara una transcripción contra la referencia.

    LEGAL_TOKEN_ERROR_RATE se calcula aparte de CER/WER a propósito: una
    página puede tener WER 0.5% y ser jurídicamente inaceptable si ese 0.5%
    convirtió DEROGAR en MODIFICAR.
    """
    ref_tokens, hip_tokens = tokens_sensibles(referencia), tokens_sensibles(hipotesis)

    errores: list[ErrorToken] = []
    total = 0
    for categoria in set(ref_tokens) | set(hip_tokens):
        esperados = list(ref_tokens.get(categoria, []))
        obtenidos = list(hip_tokens.get(categoria, []))
        total += len(esperados)
        restantes = list(obtenidos)
        faltantes = []
        for valor in esperados:
            if valor in restantes:
                restantes.remove(valor)
            else:
                faltantes.append(valor)

        # Los no emparejados se cruzan como SUSTITUCIONES (un error, no dos):
        # "339-2023" -> "339-2028" es UN token mal leido, no una perdida mas
        # una invencion. Lo que sobre queda como omision o como agregado.
        for faltante, sobrante in zip(faltantes, restantes):
            errores.append(ErrorToken(categoria, faltante, sobrante))
        for faltante in faltantes[len(restantes):]:
            errores.append(ErrorToken(categoria, faltante, None))
        for sobrante in restantes[len(faltantes):]:
            errores.append(ErrorToken(categoria, None, sobrante))

    ref_verbos, hip_verbos = verbos_normativos(referencia), verbos_normativos(hipotesis)
    verbos_cambiados = [
        ErrorToken("verbo_normativo", clase if clase in ref_verbos else None,
                   clase if clase in hip_verbos else None)
        for clase in set(ref_verbos) | set(hip_verbos)
        if ref_verbos.get(clase, 0) != hip_verbos.get(clase, 0)
    ]

    return ResultadoFidelidad(
        cer=cer(referencia, hipotesis),
        wer=wer(referencia, hipotesis),
        legal_token_error_rate=(len(errores) / total) if total else (1.0 if errores else 0.0),
        errores_token=errores,
        verbos_cambiados=verbos_cambiados,
    )


def discrepancia_entre_motores(texto_a: str, texto_b: str) -> ResultadoFidelidad:
    """Compara dos motores entre sí. Ninguno es la verdad: si difieren en un
    token jurídico, el resultado es DISCREPANCIA_CRITICA, no 'confianza media'
    ni un promedio."""
    return comparar_fidelidad(texto_a, texto_b)


# ---------------------------------------------------------------------------
# Motor de estados (F-01 · puntos 16, 17 y 18)
# ---------------------------------------------------------------------------
@dataclass
class SenalesPagina:
    """Lo que hoy sabemos de una página. `quality_score` y `ocr_confidence`
    son señales de LEGIBILIDAD; ninguna prueba fidelidad por sí sola."""

    extraction_method: str | None = None
    quality_score: float | None = None
    ocr_used: bool = False
    ocr_confidence: float | None = None
    texto: str | None = None
    has_tables: bool = False
    posible_formula: bool = False
    posible_grafico: bool = False
    revisado_manual: bool = False
    # Evidencia cruzada: resultado de comparar dos motores independientes.
    comparacion_motores: ResultadoFidelidad | None = None
    # Integridad del documento al que pertenece.
    documento_completo: bool | None = None
    pdf_disponible: bool = True


# Umbrales. Son deliberadamente altos para páginas dispositivas y NUNCA
# alcanzan por sí solos el estado VERIFICADA: hace falta evidencia cruzada.
UMBRAL_OCR_ACEPTABLE = 0.85
UMBRAL_CALIDAD_ACEPTABLE = 0.75
UMBRAL_LTER_DISCREPANCIA = 0.0  # un solo token jurídico distinto ya es discrepancia


def evaluar_pagina(senales: SenalesPagina) -> tuple[str, str, list[str]]:
    """Devuelve (verification_status, risk_level, motivos).

    Regla de oro: sin evidencia cruzada contra el PDF o contra un segundo
    motor, el estado máximo alcanzable es OCR_PENDIENTE_VERIFICACION o
    EXTRACCION_DIGITAL_ALTA_CONCORDANCIA — nunca VERIFICADA. La confianza de
    Tesseract o de un LLM es señal auxiliar, no prueba.
    """
    motivos: list[str] = []
    dispositiva = es_pagina_dispositiva(senales.texto)

    if not senales.pdf_disponible:
        return PDF_NO_DISPONIBLE, RIESGO_CRITICO, ["no hay PDF con el que verificar la transcripcion"]

    if senales.documento_completo is False:
        return DOCUMENTO_INCOMPLETO, RIESGO_CRITICO, [
            "faltan paginas del PDF: el documento no esta completo aunque las paginas presentes se vean bien"
        ]

    if senales.revisado_manual:
        return VERIFICADA_HUMANO, RIESGO_BAJO, ["revisada contra el PDF por una persona"]

    # Evidencia cruzada entre motores: es lo único que puede verificar sola.
    cmp_ = senales.comparacion_motores
    if cmp_ is not None:
        if cmp_.hay_error_juridico:
            detalle = ", ".join(str(e) for e in (cmp_.errores_token + cmp_.verbos_cambiados)[:5])
            return DISCREPANCIA_ENTRE_MOTORES, RIESGO_CRITICO, [
                f"dos motores independientes leen distinto un token juridico: {detalle}"
            ]
        if cmp_.cer <= 0.02:
            estado = VERIFICADA_AUTOMATICAMENTE
            riesgo = RIESGO_BAJO
            motivos.append(f"dos motores independientes coinciden (CER {cmp_.cer:.3f}, sin errores de token)")
            if senales.has_tables or senales.posible_formula or senales.posible_grafico:
                # La coincidencia de texto no dice nada de la estructura de una
                # tabla ni de una formula: eso sigue exigiendo ojo humano.
                return REQUIERE_REVISION_HUMANA, RIESGO_ALTO, motivos + [
                    "coincidencia textual, pero la pagina trae tabla/formula/grafico cuya estructura no se verifica comparando texto"
                ]
            return estado, riesgo, motivos
        motivos.append(f"los motores coinciden en tokens pero difieren en el texto (CER {cmp_.cer:.3f})")

    if marcas_ilegible(senales.texto):
        return ILEGIBLE_PARCIAL, RIESGO_ALTO, ["la transcripcion declara partes ilegibles"]

    if senales.extraction_method == "pagina_en_blanco":
        # Declarada en blanco por un promedio de pixel: es una HEURISTICA, y si
        # se equivoca el resultado es una pagina con contenido guardada vacia.
        return NO_EVALUADA, RIESGO_MEDIO, [
            "declarada en blanco por promedio de pixel; sin confirmar contra el PDF"
        ]

    if senales.ocr_used:
        conf = senales.ocr_confidence
        motivos.append(f"OCR (confianza {conf if conf is not None else 'desconocida'})")
        if conf is None or conf < UMBRAL_OCR_ACEPTABLE:
            motivos.append("confianza de OCR baja o ausente")
            return REQUIERE_REVISION_HUMANA, (RIESGO_CRITICO if dispositiva else RIESGO_ALTO), motivos
        # Confianza alta NO es fidelidad: queda pendiente de verificacion.
        return OCR_PENDIENTE_VERIFICACION, (RIESGO_ALTO if dispositiva else RIESGO_MEDIO), motivos

    if (senales.quality_score or 0) < UMBRAL_CALIDAD_ACEPTABLE:
        motivos.append(f"texto embebido de forma dudosa (quality_score {senales.quality_score})")
        return REQUIERE_REVISION_HUMANA, (RIESGO_CRITICO if dispositiva else RIESGO_ALTO), motivos

    motivos.append("texto embebido del PDF digital, sin señales de degradacion")
    return (
        EXTRACCION_DIGITAL_ALTA_CONCORDANCIA,
        (RIESGO_MEDIO if dispositiva else RIESGO_BAJO),
        motivos,
    )


def puede_citarse_como_fuente_legal(estado: str) -> bool:
    """Puerta de /consulta (F-01 · 17). Fuera de estos estados, el texto puede
    usarse como contexto pero NUNCA presentarse como cita legal segura."""
    return estado in ESTADOS_CONFIABLES


def puede_alimentar_detector(estado: str, dispositiva: bool) -> bool:
    """Puerta del detector de relaciones (F-01 · 18).

    Una página de considerandos no verificada puede leerse (de ahí no salen
    relaciones). Una página DISPOSITIVA no verificada, no: la relación que
    saldría de ella no puede proponerse como confiable.
    """
    # Un documento incompleto o sin PDF nunca alimenta al detector, aunque la
    # pagina concreta parezca inofensiva: la disposicion derogatoria peruana va
    # AL FINAL, asi que justo la pagina que falta suele ser la decisiva.
    if estado in (DOCUMENTO_INCOMPLETO, PDF_NO_DISPONIBLE):
        return False
    if not dispositiva:
        return True
    return estado in ESTADOS_APTOS_DETECTOR


# ---------------------------------------------------------------------------
# NIVELES DE USO (F-02 · 11 y 12)
# ---------------------------------------------------------------------------
# Un booleano "verificada / no verificada" destruiria la utilidad de la app
# mientras se verifica el corpus. Buscar y citar son cosas distintas: un texto
# imperfecto sirve para ENCONTRAR la pagina candidata, y eso no lo autoriza a
# ser la evidencia final de una afirmacion juridica.
NIVEL_0_SOLO_INDICE = "NIVEL_0_SOLO_INDICE"
NIVEL_1_DIGITAL_CONCORDANTE = "NIVEL_1_DIGITAL_CONCORDANTE"
NIVEL_2_AUTO_VERIFICADA = "NIVEL_2_AUTO_VERIFICADA"
NIVEL_3_VERIFICADA_HUMANO = "NIVEL_3_VERIFICADA_HUMANO"

USO_BUSQUEDA = "USO_BUSQUEDA"
USO_CITA_LEGAL = "USO_CITA_LEGAL"
USO_DETECTOR_RELACIONES = "USO_DETECTOR_RELACIONES"

_NIVEL_POR_ESTADO = {
    VERIFICADA_HUMANO: NIVEL_3_VERIFICADA_HUMANO,
    VERIFICADA_AUTOMATICAMENTE: NIVEL_2_AUTO_VERIFICADA,
    EXTRACCION_DIGITAL_ALTA_CONCORDANCIA: NIVEL_1_DIGITAL_CONCORDANTE,
}


def nivel_de_uso(estado: str, documento_completo: bool | None = None) -> str:
    """Nivel alcanzado por una pagina.

    La completitud del DOCUMENTO limita a la pagina: si faltan paginas del PDF,
    ninguna pagina suelta puede sostener una afirmacion juridica, por buena que
    sea su transcripcion.
    """
    if estado in (DOCUMENTO_INCOMPLETO, PDF_NO_DISPONIBLE):
        return NIVEL_0_SOLO_INDICE
    nivel = _NIVEL_POR_ESTADO.get(estado, NIVEL_0_SOLO_INDICE)
    if documento_completo is False and nivel != NIVEL_3_VERIFICADA_HUMANO:
        # Solo una persona que miro el documento puede afirmar algo sobre un
        # documento que sabemos incompleto.
        return NIVEL_0_SOLO_INDICE
    return nivel


def usos_permitidos(nivel: str, dispositiva: bool = False) -> set[str]:
    """Que se puede hacer con una pagina de este nivel.

    * Buscar se permite SIEMPRE: encontrar la pagina candidata no afirma nada.
    * Citar juridicamente exige nivel 2 o 3.
    * Alimentar al detector exige nivel 2 o 3 SOLO si la pagina es dispositiva;
      una pagina de considerandos no produce relaciones.
    """
    usos = {USO_BUSQUEDA}
    if nivel in (NIVEL_2_AUTO_VERIFICADA, NIVEL_3_VERIFICADA_HUMANO):
        usos.add(USO_CITA_LEGAL)
        usos.add(USO_DETECTOR_RELACIONES)
    elif not dispositiva:
        usos.add(USO_DETECTOR_RELACIONES)
    return usos


def advertencia_para_consulta(nivel: str) -> str | None:
    """Texto que /consulta debe mostrar si usa esta pagina como contexto.

    None significa que no hace falta advertir. Nunca se presenta una cita como
    segura desde un nivel insuficiente: si la pregunta es sobre un articulo,
    un plazo o un monto, es preferible no responder.
    """
    if nivel == NIVEL_3_VERIFICADA_HUMANO:
        return None
    if nivel == NIVEL_2_AUTO_VERIFICADA:
        return None
    if nivel == NIVEL_1_DIGITAL_CONCORDANTE:
        return ("⚠️ Texto extraído del PDF pero NO verificado contra el original. "
                "Sirve para ubicar la norma; confirma los números y plazos en el PDF oficial.")
    return ("⚠️ Transcripción de fidelidad NO verificada (posible OCR con errores). "
            "Úsala solo para localizar el documento, no como cita legal.")
