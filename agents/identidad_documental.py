"""Identidad DOCUMENTAL: ¿que norma contiene realmente este PDF? (F-03)

    IDENTIDAD NORMATIVA CORRECTA
      -> PDF CORRECTO
        -> DOCUMENTO COMPLETO
          -> TRANSCRIPCION FIEL
            -> INTERPRETACION JURIDICA

Este modulo cubre el segundo escalon, que hasta ahora no existia: el crawler
guardaba el primer PDF que encontraba en la pagina fuente, sin comprobar jamas
que ese PDF fuera el de la norma buscada.

Regla de prioridad de evidencia:

    CONTENIDO DEL PDF  >  contexto del enlace  >  nombre del archivo

Un nombre de archivo aparentemente correcto NO supera un encabezado que dice
otra cosa. Y un nombre generico -"documento.pdf"- no descalifica a un PDF cuyo
encabezado si coincide.

No decide identidad por su cuenta: reutiliza la capa canonica
scripts/identidad_normativa.py.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from identidad_normativa import (  # noqa: E402
    NormaIdentity,
    construir_identidad,
    normalizar_tipo_norma,
)

# ---------------------------------------------------------------------------
# Clasificacion del documento (F-03 · 6)
# ---------------------------------------------------------------------------
DOCUMENTO_NORMA_UNICA = "DOCUMENTO_NORMA_UNICA"
DOCUMENTO_MULTINORMA = "DOCUMENTO_MULTINORMA"
DOCUMENTO_ANEXO = "DOCUMENTO_ANEXO"
DOCUMENTO_PROYECTO = "DOCUMENTO_PROYECTO"
DOCUMENTO_INDETERMINADO = "DOCUMENTO_INDETERMINADO"

# ---------------------------------------------------------------------------
# Clasificacion por norma (F-03 · 7)
# ---------------------------------------------------------------------------
PDF_IDENTIDAD_EXACTA = "PDF_IDENTIDAD_EXACTA"
PDF_CONTIENE_NORMA_EN_MULTINORMA = "PDF_CONTIENE_NORMA_EN_MULTINORMA"
PDF_IDENTIDAD_CONTRADICTORIA = "PDF_IDENTIDAD_CONTRADICTORIA"
PDF_IDENTIDAD_AMBIGUA = "PDF_IDENTIDAD_AMBIGUA"
PDF_NO_DISPONIBLE = "PDF_NO_DISPONIBLE"
PDF_CORRUPTO = "PDF_CORRUPTO"
FUENTE_NO_VERIFICADA = "FUENTE_NO_VERIFICADA"

CLASIFICACIONES_UTILIZABLES = frozenset(
    {PDF_IDENTIDAD_EXACTA, PDF_CONTIENE_NORMA_EN_MULTINORMA}
)

# Resultado del resolvedor del crawler (F-03 · 11)
MATCH_EXACTO = "MATCH_EXACTO"
MATCH_MULTINORMA = "MATCH_MULTINORMA"
CONTRADICTORIO = "CONTRADICTORIO"
AMBIGUO = "AMBIGUO"
NO_ENCONTRADO = "NO_ENCONTRADO"


def _sin_acentos(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t or "")
                   if unicodedata.category(c) != "Mn")


# ---------------------------------------------------------------------------
# Encabezado propio vs cita
# ---------------------------------------------------------------------------
# El encabezado oficial de una norma peruana aparece como una linea propia, en
# mayusculas, con el tipo escrito completo:
#
#     RESOLUCION MINISTERIAL
#     N° 373-2024/MINSA
#
# Una CITA, en cambio, va dentro de un parrafo en minusculas ("...aprobado por
# Decreto Supremo N° 014-2011-SA..."). Distinguirlas es lo que separa "este PDF
# ES la norma X" de "este PDF MENCIONA la norma X".
TIPOS_LARGOS = {
    "RESOLUCION MINISTERIAL": "RM",
    "RESOLUCION DIRECTORAL": "RD",
    "RESOLUCION SUPREMA": "RS",
    "RESOLUCION VICEMINISTERIAL": "RVM",
    "RESOLUCION JEFATURAL": "RJ",
    "DECRETO SUPREMO": "DS",
    "DECRETO LEGISLATIVO": "DL",
    "DECRETO DE URGENCIA": "DU",
    "LEY": "LEY",
}

_ALTERNATIVAS = "|".join(sorted(TIPOS_LARGOS, key=len, reverse=True))

# El tipo y el numero pueden ir en la misma linea o en lineas consecutivas.
PATRON_ENCABEZADO = re.compile(
    rf"(?P<tipo>{_ALTERNATIVAS})\s*"
    r"(?:\r?\n\s*)?"
    r"(?:N\s*[°ºo\.]*\s*)?"
    r"(?P<numero>\d{1,6}\s*[-/]\s*\d{2,4}(?:\s*[-/]\s*[A-Z][\w\-/]*)?|\d{4,6})",
    re.IGNORECASE,
)

MARCAS_PROYECTO = re.compile(
    r"PROYECTO\s+(?:PARA\s+PUBLICACION|DE\s+(?:DECRETO|RESOLUCION|LEY))|"
    r"\bPROYECTO\s+NORMATIVO\b|\bANTEPROYECTO\b",
    re.IGNORECASE,
)
MARCAS_ANEXO = re.compile(r"^\s*ANEXO\b", re.IGNORECASE | re.MULTILINE)


def misma_identidad(a: NormaIdentity, b: NormaIdentity) -> bool:
    """Dos identidades designan la misma norma.

    El SECTOR y el AÑO se comparan solo si ambos lados lo traen: que la ficha
    de la base no haya registrado el sufijo "/MINSA" no contradice al PDF que
    si lo trae — es informacion adicional, no informacion distinta. Lo que
    nunca se relaja es el TIPO ni el NUMERO: RM 1000 y RM 1001 son normas
    distintas por mucho que compartan todo lo demas.
    """
    if not a.numero or not b.numero or a.numero != b.numero:
        return False
    if a.tipo and b.tipo and a.tipo != b.tipo:
        return False
    if a.anio and b.anio and a.anio != b.anio:
        return False
    if a.sector and b.sector and a.sector != b.sector:
        return False
    return True


@dataclass
class Aparicion:
    """Una identidad hallada en el documento, con su evidencia."""

    identidad: NormaIdentity
    page_number: int
    es_encabezado: bool
    linea: str = ""

    @property
    def peso(self) -> int:
        """Un encabezado propio pesa mucho mas que una mencion."""
        return 10 if self.es_encabezado else 1


def _es_linea_de_encabezado(contexto: str) -> bool:
    """El fragmento viene de una linea en MAYUSCULAS (encabezado oficial), no
    de la prosa de un considerando."""
    letras = [c for c in contexto if c.isalpha()]
    if not letras:
        return False
    mayusculas = sum(1 for c in letras if c.isupper())
    return mayusculas / len(letras) >= 0.8


def identidades_en_texto(texto: str, page_number: int = 1) -> list[Aparicion]:
    """Todas las identidades normativas que aparecen en el texto de una pagina,
    distinguiendo encabezados propios de simples menciones."""
    if not texto:
        return []

    plano = _sin_acentos(texto)
    apariciones: list[Aparicion] = []

    for m in PATRON_ENCABEZADO.finditer(plano):
        tipo = normalizar_tipo_norma(m.group("tipo"))
        if not tipo:
            continue
        numero = re.sub(r"\s+", "", m.group("numero"))
        identidad = construir_identidad(tipo, numero)
        if not identidad.numero:
            continue

        # Contexto: la linea completa donde aparece el hallazgo.
        inicio = plano.rfind("\n", 0, m.start()) + 1
        fin = plano.find("\n", m.end())
        linea = plano[inicio: fin if fin != -1 else len(plano)].strip()

        apariciones.append(Aparicion(
            identidad=identidad,
            page_number=page_number,
            es_encabezado=_es_linea_de_encabezado(linea),
            linea=linea[:160],
        ))

    return apariciones


def tipo_de_documento(apariciones: list[Aparicion], texto_completo: str) -> str:
    encabezados = {a.identidad.clave() for a in apariciones if a.es_encabezado}

    if MARCAS_PROYECTO.search(_sin_acentos(texto_completo or "")):
        # Un proyecto NO es una norma aprobada. Confundirlos ya causo errores
        # reales (RM-419-2025, RM-727-2025).
        return DOCUMENTO_PROYECTO
    if not encabezados:
        if MARCAS_ANEXO.search(texto_completo or ""):
            return DOCUMENTO_ANEXO
        return DOCUMENTO_INDETERMINADO
    if len(encabezados) == 1:
        return DOCUMENTO_NORMA_UNICA
    return DOCUMENTO_MULTINORMA


def rango_de_paginas(apariciones: list[Aparicion], objetivo: NormaIdentity,
                     total_paginas: int) -> tuple[int | None, int | None]:
    """Paginas que corresponden a la norma objetivo dentro de un multinorma.

    El rango va desde el encabezado de la norma hasta la pagina anterior al
    encabezado de la SIGUIENTE norma distinta. No podemos guardar las 20
    paginas de una edicion de El Peruano como si cada norma fuera todo el PDF.
    """
    encabezados = sorted(
        (a for a in apariciones if a.es_encabezado),
        key=lambda a: a.page_number,
    )
    if not encabezados:
        return None, None

    inicio = next((a.page_number for a in encabezados
                   if misma_identidad(a.identidad, objetivo)), None)
    if inicio is None:
        return None, None

    siguiente = next(
        (a.page_number for a in encabezados
         if a.page_number > inicio and not misma_identidad(a.identidad, objetivo)),
        None,
    )
    return inicio, (siguiente - 1) if siguiente else total_paginas


@dataclass
class EvidenciaDocumental:
    """Todo lo que sabemos sobre si este PDF es el de la norma objetivo."""

    identidad_objetivo: NormaIdentity
    apariciones: list[Aparicion] = field(default_factory=list)
    total_paginas: int = 0
    texto_completo: str = ""
    filename: str | None = None
    anchor_text: str | None = None
    url: str | None = None
    # Identidad leida del render/OCR de la primera pagina (F-03 · 14).
    identidad_visual: NormaIdentity | None = None
    pdf_corrupto: bool = False
    pdf_disponible: bool = True

    def _coincide_texto(self, texto: str | None) -> bool:
        """Señal SECUNDARIA: el nombre del archivo o el texto del enlace
        sugieren la norma objetivo.

        Se parsea distinto que el contenido: un nombre como
        "RM_1000-2016.pdf" no tiene forma de encabezado oficial, asi que aqui
        basta con encontrar el tipo y el numero, sin exigir mayusculas ni
        estructura. Nunca decide por si solo: solo aporta contexto.
        """
        if not texto:
            return False
        plano = _sin_acentos(str(texto))
        objetivo = self.identidad_objetivo

        for aparicion in identidades_en_texto(plano):
            if misma_identidad(aparicion.identidad, objetivo):
                return True

        # Abreviaturas sueltas del nombre de archivo: "RM_1000-2016.pdf",
        # "DS023-2005.pdf", "PERUANO_RM_98-2024-MINSA.pdf".
        for m in re.finditer(
            r"\b(RM|RD|RS|RVM|RJ|DS|DL|DU|LEY)[\s_\-]*"
            r"(\d{1,6}(?:[-/]\d{2,4})?(?:[-/][A-Za-z][\w\-/]*)?)",
            plano, re.IGNORECASE,
        ):
            if misma_identidad(construir_identidad(m.group(1), m.group(2)), objetivo):
                return True
        return False

    @property
    def filename_match(self) -> bool:
        return self._coincide_texto(self.filename)

    @property
    def source_context_match(self) -> bool:
        return self._coincide_texto(self.anchor_text)

    @property
    def content_match(self) -> bool:
        return any(misma_identidad(a.identidad, self.identidad_objetivo) and a.es_encabezado
                   for a in self.apariciones)

    @property
    def visual_match(self) -> bool | None:
        if self.identidad_visual is None:
            return None
        return misma_identidad(self.identidad_visual, self.identidad_objetivo)

    @property
    def encabezados(self) -> list[Aparicion]:
        return [a for a in self.apariciones if a.es_encabezado]


def clasificar_identidad_documental(ev: EvidenciaDocumental) -> tuple[str, str, str]:
    """Devuelve (clasificacion, confianza, motivo). Nunca elige arbitrariamente."""
    if not ev.pdf_disponible:
        return PDF_NO_DISPONIBLE, "nula", "no hay PDF que analizar"
    if ev.pdf_corrupto:
        return PDF_CORRUPTO, "nula", "el PDF no se pudo abrir"

    encabezados = ev.encabezados
    claves = {a.identidad.clave() for a in encabezados}
    objetivo = str(ev.identidad_objetivo)
    contiene_objetivo = ev.content_match

    # El render contradice a la capa de texto: eso es critico y manda.
    if ev.visual_match is False and ev.content_match:
        return (PDF_IDENTIDAD_AMBIGUA, "nula",
                f"DISCREPANCIA_IDENTIDAD_CRITICA: la capa de texto dice {objetivo} "
                f"pero el render dice {ev.identidad_visual}")

    if not encabezados:
        # Sin encabezado propio no se puede afirmar de quien es el documento,
        # aunque el nombre del archivo parezca perfecto.
        if ev.filename_match or ev.source_context_match:
            return (PDF_IDENTIDAD_AMBIGUA, "baja",
                    "el nombre o el enlace sugieren la norma, pero el documento no "
                    "trae un encabezado que lo confirme: el nombre no es evidencia")
        return PDF_IDENTIDAD_AMBIGUA, "nula", "no se hallo ningun encabezado normativo"

    if contiene_objetivo:
        if len(claves) == 1:
            return PDF_IDENTIDAD_EXACTA, "alta", "el unico encabezado del documento es la norma objetivo"
        return (PDF_CONTIENE_NORMA_EN_MULTINORMA, "alta",
                f"el documento contiene {len(claves)} normas y una de ellas es la objetivo")

    # Hay encabezados, pero ninguno es la norma objetivo.
    otras = ", ".join(sorted(claves)[:4])
    if ev.filename_match:
        return (PDF_IDENTIDAD_CONTRADICTORIA, "alta",
                f"el nombre del archivo sugiere {objetivo} pero el documento contiene {otras}: "
                "el contenido manda sobre el nombre")
    return (PDF_IDENTIDAD_CONTRADICTORIA, "alta",
            f"el documento contiene {otras}, no {objetivo}")


# ---------------------------------------------------------------------------
# Resolvedor para el crawler (F-03 · 11 y 12)
# ---------------------------------------------------------------------------
@dataclass
class ResultadoResolucion:
    estado: str
    url: str | None = None
    start_page: int | None = None
    end_page: int | None = None
    motivo: str = ""
    candidatos_evaluados: list[dict] = field(default_factory=list)

    @property
    def puede_escribirse(self) -> bool:
        """Solo un match probado autoriza a escribir pdf_url. Ambiguo,
        contradictorio o no encontrado NO escriben: se registra para revision."""
        return self.estado in (MATCH_EXACTO, MATCH_MULTINORMA) and bool(self.url)


def resolver_pdf_para_norma(
    candidatos: list[EvidenciaDocumental],
    identidad_objetivo: NormaIdentity,
) -> ResultadoResolucion:
    """Elige el PDF de la norma objetivo por EVIDENCIA, no por posicion.

    Sustituye a `return candidatos[0]`: el orden en el HTML no dice nada sobre
    a que norma pertenece un documento.
    """
    evaluados, exactos, multinorma = [], [], []

    for ev in candidatos:
        clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
        evaluados.append({
            "url": ev.url,
            "filename_match": ev.filename_match,
            "source_context_match": ev.source_context_match,
            "content_match": ev.content_match,
            "visual_match": ev.visual_match,
            "clasificacion": clasificacion,
            "confianza": confianza,
            "motivo": motivo,
        })
        if clasificacion == PDF_IDENTIDAD_EXACTA:
            exactos.append(ev)
        elif clasificacion == PDF_CONTIENE_NORMA_EN_MULTINORMA:
            multinorma.append(ev)

    if len(exactos) == 1:
        return ResultadoResolucion(MATCH_EXACTO, exactos[0].url, 1, exactos[0].total_paginas,
                                   "un unico documento contiene exclusivamente la norma objetivo",
                                   evaluados)
    if len(exactos) > 1:
        return ResultadoResolucion(AMBIGUO, None, None, None,
                                   f"{len(exactos)} documentos distintos afirman ser la norma objetivo",
                                   evaluados)

    if len(multinorma) == 1:
        ev = multinorma[0]
        inicio, fin = rango_de_paginas(ev.apariciones, identidad_objetivo, ev.total_paginas)
        return ResultadoResolucion(MATCH_MULTINORMA, ev.url, inicio, fin,
                                   "documento multinorma que contiene la norma objetivo",
                                   evaluados)
    if len(multinorma) > 1:
        return ResultadoResolucion(AMBIGUO, None, None, None,
                                   "varios documentos multinorma contienen la norma objetivo",
                                   evaluados)

    return ResultadoResolucion(NO_ENCONTRADO, None, None, None,
                               "ningun candidato demuestra contener la norma objetivo",
                               evaluados)
