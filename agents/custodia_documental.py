"""Cadena de custodia documental (F-02).

    EL PDF OFICIAL ES LA EVIDENCIA.
    LA TRANSCRIPCION ES UNA REPRESENTACION DE ESA EVIDENCIA.
    EL LLM SOLO INTERPRETA UNA TRANSCRIPCION YA VERIFICADA.

Este modulo responde tres preguntas que hoy la base no puede responder:

    1. ¿Que PDF exacto produjo esta transcripcion?   -> sha256 + version documental
    2. ¿Tenemos el documento completo?               -> page_count del PDF vs paginas guardadas
    3. ¿Ese PDF es realmente el oficial?             -> procedencia, no "esta en Storage"

Nada aqui escribe: son reglas puras sobre metadatos.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Completitud (F-02 · 4)
# ---------------------------------------------------------------------------
COMPLETO = "COMPLETO"
INCOMPLETO = "INCOMPLETO"
DESCONOCIDO = "DESCONOCIDO"
PDF_NO_DISPONIBLE = "PDF_NO_DISPONIBLE"
PDF_CORRUPTO = "PDF_CORRUPTO"

# Un documento en estos estados NO habilita confirmar relaciones juridicas.
COMPLETITUD_INSUFICIENTE = frozenset({INCOMPLETO, DESCONOCIDO, PDF_NO_DISPONIBLE, PDF_CORRUPTO})

# ---------------------------------------------------------------------------
# Procedencia (F-02 · 19)
# ---------------------------------------------------------------------------
# Tener el PDF en Storage no demuestra que sea oficial: demuestra que alguien
# lo subio. La procedencia se afirma solo con evidencia del origen.
FUENTE_OFICIAL_VERIFICADA = "FUENTE_OFICIAL_VERIFICADA"
FUENTE_OFICIAL_DECLARADA_NO_REVALIDADA = "FUENTE_OFICIAL_DECLARADA_NO_REVALIDADA"
COPIA_LOCAL_SIN_PROCEDENCIA = "COPIA_LOCAL_SIN_PROCEDENCIA_VERIFICADA"
FUENTE_NO_OFICIAL = "FUENTE_NO_OFICIAL"

DOMINIOS_OFICIALES = ("digemid.minsa.gob.pe", "www.digemid.minsa.gob.pe",
                      "busquedas.elperuano.pe", "diariooficial.elperuano.pe",
                      "www.gob.pe", "cdn.www.gob.pe")


def dominio_de(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return (urlparse(url).hostname or "").lower() or None
    except ValueError:
        return None


def clasificar_procedencia(source_url: str | None, revalidado_contra_origen: bool = False) -> tuple[str, str]:
    """Devuelve (clasificacion, motivo).

    `revalidado_contra_origen` solo puede ser True si se volvio a descargar el
    PDF desde la URL oficial y el hash coincidio. Sin eso, lo maximo que se
    puede afirmar es que la URL declarada es oficial.
    """
    dominio = dominio_de(source_url)

    if dominio is None:
        return COPIA_LOCAL_SIN_PROCEDENCIA, "no hay URL de origen registrada"

    # Una URL firmada de nuestro propio Storage NO es la fuente: es una copia
    # nuestra, y ademas caduca, asi que despues ni siquiera es re-descargable.
    if "supabase.co" in dominio:
        return COPIA_LOCAL_SIN_PROCEDENCIA, (
            "la URL apunta a nuestra propia copia en Storage, no a la fuente oficial"
            + (" (ademas es una URL firmada que caduca)" if "token=" in (source_url or "") else "")
        )

    if dominio in DOMINIOS_OFICIALES:
        if revalidado_contra_origen:
            return FUENTE_OFICIAL_VERIFICADA, f"hash revalidado contra {dominio}"
        return FUENTE_OFICIAL_DECLARADA_NO_REVALIDADA, (
            f"URL de dominio oficial ({dominio}) pero el hash no se revalido contra el origen"
        )

    return FUENTE_NO_OFICIAL, f"dominio no reconocido como oficial: {dominio}"


# ---------------------------------------------------------------------------
# Version documental inmutable (F-02 · 2)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VersionDocumental:
    """Una descarga concreta de un PDF. Un SHA distinto es una VERSION NUEVA,
    nunca una sobrescritura."""

    norma_id: str
    sha256: str
    byte_size: int
    pdf_page_count: int | None
    source_url: str | None = None
    storage_path: str | None = None
    downloaded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_current: bool = True
    previous_version_id: str | None = None

    @property
    def source_domain(self) -> str | None:
        return dominio_de(self.source_url)


class SobrescrituraProhibida(RuntimeError):
    """Se intento pisar evidencia documental. Nunca es aceptable."""


def registrar_version(
    existentes: list[VersionDocumental], nueva: VersionDocumental
) -> tuple[list[VersionDocumental], str]:
    """Devuelve (versiones_resultantes, accion).

    Reglas:
      * mismo SHA que la vigente  -> no pasa nada, es la misma evidencia;
      * SHA distinto              -> VERSION NUEVA; la anterior se conserva
                                     con is_current=False;
      * jamas se elimina ni se modifica el contenido de una version previa.
    """
    if not nueva.sha256 or len(nueva.sha256) != 64:
        raise ValueError("una version documental exige un sha256 completo")

    vigente = next((v for v in existentes if v.is_current), None)

    if vigente and vigente.sha256 == nueva.sha256:
        return existentes, "sin_cambios"

    if any(v.sha256 == nueva.sha256 for v in existentes):
        # Volvimos a una version anterior: se reactiva, no se duplica.
        resultado = [
            VersionDocumental(**{**v.__dict__, "is_current": v.sha256 == nueva.sha256})
            for v in existentes
        ]
        return resultado, "reactivada_version_previa"

    historicas = [VersionDocumental(**{**v.__dict__, "is_current": False}) for v in existentes]
    return historicas + [nueva], "nueva_version"


def sha256_de(datos: bytes) -> str:
    return hashlib.sha256(datos).hexdigest()


# ---------------------------------------------------------------------------
# Completitud del documento (F-02 · 4)
# ---------------------------------------------------------------------------
@dataclass
class Completitud:
    estado: str
    pdf_page_count: int | None
    stored_page_count: int
    faltantes: list[int] = field(default_factory=list)
    duplicadas: list[int] = field(default_factory=list)
    fuera_de_secuencia: bool = False
    extras: list[int] = field(default_factory=list)
    motivo: str = ""

    @property
    def habilita_confirmar_relaciones(self) -> bool:
        return self.estado not in COMPLETITUD_INSUFICIENTE


def evaluar_completitud(
    pdf_page_count: int | None,
    paginas_guardadas: list[int],
    pdf_disponible: bool = True,
    pdf_corrupto: bool = False,
) -> Completitud:
    numeros = sorted(n for n in paginas_guardadas if n is not None)
    duplicadas = sorted({n for n in numeros if numeros.count(n) > 1})
    distintos = sorted(set(numeros))

    if pdf_corrupto:
        return Completitud(PDF_CORRUPTO, None, len(numeros), motivo="el PDF no se pudo abrir")
    if not pdf_disponible:
        return Completitud(PDF_NO_DISPONIBLE, None, len(numeros),
                           motivo="no hay PDF con el que comparar")
    if not numeros:
        return Completitud(INCOMPLETO, pdf_page_count, 0, motivo="documento sin paginas guardadas")

    fuera_de_secuencia = distintos[0] != 1
    if pdf_page_count is None:
        # DESCONOCIDO no es COMPLETO. Es el estado honesto cuando no sabemos
        # cuantas paginas tiene el original.
        faltantes_internos = sorted(set(range(1, distintos[-1] + 1)) - set(distintos))
        return Completitud(
            DESCONOCIDO, None, len(numeros), faltantes_internos, duplicadas, fuera_de_secuencia,
            motivo="no se conoce el numero de paginas del PDF oficial",
        )

    esperadas = set(range(1, pdf_page_count + 1))
    faltantes = sorted(esperadas - set(distintos))
    extras = sorted(set(distintos) - esperadas)

    if not faltantes and not extras and not duplicadas:
        return Completitud(COMPLETO, pdf_page_count, len(numeros), motivo="todas las paginas presentes")

    partes = []
    if faltantes:
        ultima = pdf_page_count in faltantes
        partes.append(
            f"faltan {len(faltantes)} pagina(s): {faltantes[:10]}"
            + (" — INCLUYE LA ULTIMA, donde suele ir la disposicion derogatoria" if ultima else "")
        )
    if extras:
        partes.append(f"hay {len(extras)} pagina(s) que el PDF no tiene: {extras[:10]}")
    if duplicadas:
        partes.append(f"paginas duplicadas: {duplicadas[:10]}")

    return Completitud(INCOMPLETO, pdf_page_count, len(numeros), faltantes, duplicadas,
                       fuera_de_secuencia, extras, "; ".join(partes))


# ---------------------------------------------------------------------------
# Deteccion de transcripcion proveniente de OTRO documento (F-02, hallazgo)
# ---------------------------------------------------------------------------
def detectar_documentos_compartidos(normas: list[dict]) -> list[dict]:
    """Dos normas distintas que comparten el PDF de origen o el texto exacto.

    Es indetectable por quality_score -el texto se ve perfecto- y significa que
    al menos una de las dos guarda la transcripcion de OTRA norma.

    Cada `norma` necesita: document_key, pdf_url, pdf_sha256 (o etag) y
    hash_texto.
    """
    hallazgos: list[dict] = []
    for campo, etiqueta in (("pdf_url", "mismo PDF de origen"),
                            ("pdf_sha256", "mismo PDF (hash identico)"),
                            ("hash_texto", "transcripcion identica")):
        grupos: dict[str, list[str]] = {}
        for n in normas:
            valor = n.get(campo)
            if valor:
                grupos.setdefault(str(valor), []).append(n["document_key"])
        for valor, claves in grupos.items():
            if len(claves) > 1:
                hallazgos.append({
                    "motivo": etiqueta,
                    "campo": campo,
                    "valor": valor,
                    "normas": sorted(claves),
                    "gravedad": "CRITICO" if campo == "hash_texto" else "ALTO",
                })
    return hallazgos


# ---------------------------------------------------------------------------
# Pagina en blanco robusta (F-02 · 14)
# ---------------------------------------------------------------------------
# El promedio de pixel NO sirve: un poco de texto negro sobre A4 blanco deja el
# promedio en ~254/255. Se exigen varias señales coincidentes.
@dataclass
class SenalesBlanco:
    texto_embebido: str = ""
    ratio_pixeles_no_blancos: float | None = None   # proporcion de pixeles oscuros
    varianza_pixeles: float | None = None
    bloques_texto: int | None = None                 # get_text("blocks")
    objetos_dibujo: int | None = None
    imagenes: int | None = None
    es_ultima_pagina: bool = False

# Una hoja A4 con una sola linea de texto ronda 0.1-0.3% de pixeles oscuros.
UMBRAL_TINTA = 0.0015


def evaluar_pagina_en_blanco(s: SenalesBlanco) -> tuple[bool, str, str]:
    """Devuelve (es_blanco, confianza, motivo). `confianza` in {alta, baja}.

    Ante cualquier duda devuelve False: declarar en blanco una pagina con
    contenido la borra silenciosamente del corpus.
    """
    if (s.texto_embebido or "").strip():
        return False, "alta", "tiene texto embebido"
    if s.bloques_texto:
        return False, "alta", f"el PDF declara {s.bloques_texto} bloque(s) de texto"
    if s.objetos_dibujo:
        return False, "alta", f"tiene {s.objetos_dibujo} objeto(s) de dibujo"

    if s.ratio_pixeles_no_blancos is None:
        return False, "baja", "sin analisis de pixeles: no se puede afirmar que este en blanco"

    if s.ratio_pixeles_no_blancos > UMBRAL_TINTA:
        return False, "alta", (
            f"{s.ratio_pixeles_no_blancos:.4%} de pixeles con tinta: hay contenido"
        )

    if s.es_ultima_pagina:
        # La ultima pagina es donde va la disposicion derogatoria. Nunca se
        # declara en blanco por heuristica.
        return False, "baja", (
            "ultima pagina: no se declara en blanco automaticamente, requiere confirmacion"
        )

    return True, "alta", f"sin texto, sin objetos y solo {s.ratio_pixeles_no_blancos:.4%} de tinta"


# ---------------------------------------------------------------------------
# Alto riesgo por POSICION, no solo por texto (F-02 · 13)
# ---------------------------------------------------------------------------
def es_pagina_alto_riesgo(
    page_number: int, total_paginas: int, texto_dispositivo: bool
) -> tuple[bool, str]:
    """Si el OCR destruyo "SE RESUELVE", la pagina no debe dejar de ser
    riesgosa solo porque el texto defectuoso ya no contiene el marcador."""
    if texto_dispositivo:
        return True, "el texto contiene parte dispositiva"
    if page_number == 1:
        return True, "primera pagina (encabezado e identificacion de la norma)"
    if total_paginas >= 2 and page_number >= total_paginas - 1:
        return True, "ultima o penultima pagina (disposiciones finales y derogatorias)"
    return False, "pagina intermedia sin marcadores dispositivos"


# ---------------------------------------------------------------------------
# Auditabilidad del motor de vision (F-02 · 9)
# ---------------------------------------------------------------------------
MODELOS_NO_AUDITABLES = ("auto", "openrouter/auto", "", None)


@dataclass
class EjecucionMotor:
    provider: str | None = None
    model_solicitado: str | None = None
    model_real: str | None = None
    response_id: str | None = None
    prompt_version: str | None = None
    dpi: int | None = None
    pdf_sha256: str | None = None
    page_number: int | None = None
    timestamp: datetime | None = None


def es_auditable(e: EjecucionMotor) -> tuple[bool, list[str]]:
    """Una transcripcion cuyo autor se desconoce NO puede considerarse
    verificada, por muy alta que sea la confianza declarada."""
    faltan = []
    if not e.provider:
        faltan.append("provider")
    if e.model_solicitado in MODELOS_NO_AUDITABLES:
        faltan.append("model (no puede ser 'auto': no queda registro de quien transcribio)")
    if not (e.model_real or e.model_solicitado):
        faltan.append("model_real")
    if not e.response_id:
        faltan.append("response_id")
    if not e.prompt_version:
        faltan.append("prompt_version")
    if not e.dpi:
        faltan.append("dpi")
    if not e.pdf_sha256:
        faltan.append("pdf_sha256")
    return (not faltan), faltan
