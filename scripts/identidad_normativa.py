"""Capa canonica de IDENTIDAD NORMATIVA (hallazgos H-05, H-06 y H-07).

Modulo unico y sin dependencias de red: aqui vive TODA la logica de decidir
"que norma es esta", para no volver a tenerla duplicada y divergente entre el
detector, el bot y los scripts de auditoria.

Tres problemas que resuelve:

H-05  En produccion conviven "DS" y "Decreto Supremo", "RM" y "Resolucion
      Ministerial", "LEY" y "Ley". Se define una representacion CANONICA unica.

H-06  La identidad se resolvia por numero+año, sin tipo: "RM 150-2025" y
      "RD 150-2025" eran indistinguibles, y con año nulo ("Ley 29459") la
      busqueda fallaba siempre y se creaba un stub. Ahora la resolucion es
      jerarquica y ante varias candidatas devuelve IDENTIDAD_AMBIGUA en vez de
      elegir la primera.

H-07  La deduplicacion dependia de la descripcion libre del LLM. Ahora existe
      una clave estable construida sobre la identidad canonica y los articulos
      afectados normalizados, para que el reanalisis sea idempotente.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# H-05 · Representacion canonica del tipo de norma
# ---------------------------------------------------------------------------
# La forma canonica es la ABREVIATURA EN MAYUSCULAS (RM, DS, RD, LEY, ...).
# Se eligio la abreviatura porque es la que ya usan los document_key y la que
# devuelve el modelo; la forma larga solo aparece en 7 filas historicas.
#
# Ampliar esta tabla es la unica forma correcta de soportar un tipo nuevo.
TIPOS_CANONICOS: dict[str, str] = {
    # --- presentes hoy en produccion ---
    "rm": "RM",
    "resolucion ministerial": "RM",
    "ds": "DS",
    "decreto supremo": "DS",
    "rd": "RD",
    "resolucion directoral": "RD",
    "ley": "LEY",
    "du": "DU",
    "decreto de urgencia": "DU",
    "rs": "RS",
    "resolucion suprema": "RS",
    # --- previstos, aun sin filas: la arquitectura queda extensible ---
    "rvm": "RVM",
    "resolucion viceministerial": "RVM",
    "rj": "RJ",
    "resolucion jefatural": "RJ",
    "dl": "DL",
    "decreto legislativo": "DL",
    "rge": "RGE",
    "resolucion de gerencia general": "RGE",
}

# Tipos juridicamente distintos que NO deben fusionarse jamas, aunque
# compartan numero y año.
TIPOS_CONOCIDOS: frozenset[str] = frozenset(TIPOS_CANONICOS.values())


def _sin_acentos(valor: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", valor) if unicodedata.category(c) != "Mn"
    )


def normalizar_tipo_norma(valor) -> str | None:
    """Devuelve la abreviatura canonica, o None si no se reconoce.

    Tolera mayusculas/minusculas, tildes, puntos y espacios sobrantes. NO
    adivina: un valor desconocido devuelve None (y el llamador degrada a un
    nivel de resolucion inferior) en vez de inventar un tipo.
    """
    if valor is None:
        return None
    base = _sin_acentos(str(valor)).lower()

    # Se prueban dos lecturas, porque el punto significa cosas distintas segun
    # el caso: en "R.M." separa las iniciales de una abreviatura ("rm"), y en
    # "Resolucion Ministerial." es solo puntuacion final.
    candidatos = [
        re.sub(r"\s+", " ", base.replace(".", "")).strip(),   # "r.m." -> "rm"
        re.sub(r"\s+", " ", base.replace(".", " ")).strip(),  # "res. ministerial" -> "res ministerial"
    ]
    for plano in candidatos:
        if plano and (canon := TIPOS_CANONICOS.get(plano)):
            return canon
    return None


# ---------------------------------------------------------------------------
# H-06 · Identidad canonica
# ---------------------------------------------------------------------------
# En produccion el "numero" puede venir con el año y el sector pegados:
#   "014-2011-SA"        -> numero 14, año 2011, sector SA
#   "554-2022-MINSA"     -> numero 554, año 2022, sector MINSA
#   "354-99-DG-DIGEMID"  -> numero 354, año 1999, sector DG-DIGEMID
#   "920"                -> numero 920, sin año ni sector
PATRON_NUMERO = re.compile(
    r"^\s*(?P<numero>\d{1,6})"
    r"(?:\s*[-/]\s*(?P<anio>\d{2,4}))?"
    r"(?:\s*[-/]\s*(?P<sector>[A-Za-zÁÉÍÓÚÑ][\w\-/]*))?",
    re.IGNORECASE,
)


def _anio_completo(fragmento: str | None) -> int | None:
    """Expande años de dos digitos ("99" -> 1999) sin inventar nada fuera de
    rango. Las normas peruanas del corpus van de 1990 en adelante."""
    if not fragmento:
        return None
    n = int(fragmento)
    if len(fragmento) == 4:
        return n if 1900 <= n <= 2100 else None
    if len(fragmento) == 2:
        return 1900 + n if n >= 50 else 2000 + n
    return None


def normalizar_numero(valor) -> str | None:
    """Solo el primer grupo de digitos, sin ceros a la izquierda.
    "014" y "14" son la MISMA norma (caso F)."""
    if valor is None:
        return None
    m = re.search(r"\d+", str(valor))
    return str(int(m.group())) if m else None


def normalizar_sector(valor) -> str | None:
    if not valor:
        return None
    plano = _sin_acentos(str(valor)).upper().strip().strip("-/")
    plano = re.sub(r"[^A-Z0-9\-]", "", plano)
    return plano or None


@dataclass(frozen=True)
class NormaIdentity:
    """Identidad canonica de una norma. `tipo` puede ser None cuando la cita no
    lo expresa: eso NO impide resolver, pero baja el nivel de confianza."""

    tipo: str | None
    numero: str | None
    anio: int | None
    sector: str | None = None

    @property
    def es_utilizable(self) -> bool:
        """Sin numero no hay nada que resolver."""
        return bool(self.numero)

    def clave(self) -> str:
        """Representacion textual estable, apta para deduplicar y comparar."""
        return "|".join(
            [
                self.tipo or "?",
                self.numero or "?",
                str(self.anio) if self.anio else "?",
                self.sector or "",
            ]
        )

    def __str__(self) -> str:  # pragma: no cover - only for logs
        partes = [self.tipo or "?", self.numero or "?"]
        if self.anio:
            partes.append(str(self.anio))
        if self.sector:
            partes.append(self.sector)
        return "-".join(partes)


def construir_identidad(tipo, numero, anio=None, sector=None) -> NormaIdentity:
    """Construye la identidad a partir de campos sueltos, extrayendo el año y
    el sector que vengan embebidos dentro de `numero`."""
    tipo_canon = normalizar_tipo_norma(tipo)

    anio_embebido, sector_embebido = None, None
    if numero is not None:
        m = PATRON_NUMERO.match(str(numero))
        if m:
            anio_embebido = _anio_completo(m.group("anio"))
            sector_embebido = m.group("sector")
            # "014-2011-SA": el 2011 es año, no sector. Pero "354-99-DG-DIGEMID"
            # tiene sector compuesto. El patron ya los separa por posicion.

    return NormaIdentity(
        tipo=tipo_canon,
        numero=normalizar_numero(numero),
        anio=anio if anio is not None else anio_embebido,
        sector=normalizar_sector(sector if sector is not None else sector_embebido),
    )


def identidad_de_norma(fila: dict) -> NormaIdentity:
    """Identidad de una fila de `digemid_normas`.

    No se confia en `document_key` como identidad, porque los historicos no
    estan normalizados ("DS-14-2002" y "DS-008-2025-SA" conviven).
    """
    return construir_identidad(
        fila.get("tipo_norma"), fila.get("numero"), fila.get("anio")
    )


# ---------------------------------------------------------------------------
# Niveles de resolucion
# ---------------------------------------------------------------------------
NIVEL_EXACTA = "RESUELTA_EXACTA"
NIVEL_TIPO_NUMERO_ANIO = "RESUELTA_TIPO_NUMERO_ANIO"
NIVEL_NUMERO_ANIO = "RESUELTA_NUMERO_ANIO"
NIVEL_TIPO_NUMERO = "RESUELTA_TIPO_NUMERO"
AMBIGUA = "IDENTIDAD_AMBIGUA"
NO_ENCONTRADA = "NORMA_NO_ENCONTRADA"
DATOS_INSUFICIENTES = "DATOS_INSUFICIENTES"

NIVELES_RESUELTOS = frozenset(
    {NIVEL_EXACTA, NIVEL_TIPO_NUMERO_ANIO, NIVEL_NUMERO_ANIO, NIVEL_TIPO_NUMERO}
)

CONFIANZA = {
    NIVEL_EXACTA: "alta",
    NIVEL_TIPO_NUMERO_ANIO: "alta",
    NIVEL_TIPO_NUMERO: "media",
    NIVEL_NUMERO_ANIO: "media",
    AMBIGUA: "nula",
    NO_ENCONTRADA: "nula",
    DATOS_INSUFICIENTES: "nula",
}


@dataclass
class ResultadoIdentidad:
    nivel: str
    norma: dict | None = None
    candidatas: list[dict] = field(default_factory=list)

    @property
    def resuelta(self) -> bool:
        return self.nivel in NIVELES_RESUELTOS and self.norma is not None

    @property
    def confianza(self) -> str:
        return CONFIANZA.get(self.nivel, "nula")


def resolver_identidad(citada: NormaIdentity, catalogo: list[dict]) -> ResultadoIdentidad:
    """Resolucion JERARQUICA y conservadora.

    Regla de oro: si en cualquier nivel hay mas de una candidata, se devuelve
    IDENTIDAD_AMBIGUA con la lista completa. Nunca se elige "la primera".

    `catalogo` es una lista de filas de digemid_normas (id, document_key,
    tipo_norma, numero, anio).
    """
    if not citada.es_utilizable:
        return ResultadoIdentidad(DATOS_INSUFICIENTES)

    # Se precalcula la identidad de cada norma del catalogo una sola vez.
    indexado = [(identidad_de_norma(f), f) for f in catalogo]

    def elegir(candidatas: list[dict], nivel: str) -> ResultadoIdentidad | None:
        if len(candidatas) == 1:
            return ResultadoIdentidad(nivel, candidatas[0])
        if len(candidatas) > 1:
            return ResultadoIdentidad(AMBIGUA, None, candidatas)
        return None

    # --- NIVEL 1: tipo + numero + año + sector -----------------------------
    if citada.tipo and citada.anio and citada.sector:
        exactas = [
            f for ident, f in indexado
            if ident.tipo == citada.tipo
            and ident.numero == citada.numero
            and ident.anio == citada.anio
            and ident.sector == citada.sector
        ]
        if (r := elegir(exactas, NIVEL_EXACTA)):
            return r

    # --- NIVEL 2: tipo + numero + año --------------------------------------
    if citada.tipo and citada.anio:
        por_tipo_anio = [
            f for ident, f in indexado
            if ident.tipo == citada.tipo
            and ident.numero == citada.numero
            and ident.anio == citada.anio
        ]
        if (r := elegir(por_tipo_anio, NIVEL_TIPO_NUMERO_ANIO)):
            return r

    # --- NIVEL 4 (antes que el 3): tipo + numero, sin año ------------------
    # Caso "Ley 29459": la cita no trae año. Con el tipo disponible esto es
    # MAS seguro que numero+año, asi que se intenta primero. Nunca se inventa
    # el año.
    if citada.tipo and not citada.anio:
        por_tipo = [
            f for ident, f in indexado
            if ident.tipo == citada.tipo and ident.numero == citada.numero
        ]
        if (r := elegir(por_tipo, NIVEL_TIPO_NUMERO)):
            return r

    # --- NIVEL 3: numero + año, SIN tipo -----------------------------------
    # Solo admisible si hay exactamente una candidata en toda la base y no
    # contradice el tipo citado (si lo hubiera).
    if citada.anio:
        por_numero_anio = [
            f for ident, f in indexado
            if ident.numero == citada.numero and ident.anio == citada.anio
        ]
        compatibles = [
            f for f in por_numero_anio
            if not citada.tipo
            or identidad_de_norma(f).tipo is None
            or identidad_de_norma(f).tipo == citada.tipo
        ]
        if (r := elegir(compatibles, NIVEL_NUMERO_ANIO)):
            return r
        if por_numero_anio:
            return ResultadoIdentidad(AMBIGUA, None, por_numero_anio)

    return ResultadoIdentidad(NO_ENCONTRADA)


# ---------------------------------------------------------------------------
# H-07 · Clave estable de deduplicacion
# ---------------------------------------------------------------------------
def normalizar_articulos(valor) -> str:
    """Extrae el CONJUNTO de unidades afectadas, ignorando la redaccion.

    "articulos 10 y 11", "arts. 10 y 11" y "10, 11" producen la misma clave,
    de modo que un reanalisis no duplica la relacion solo porque el modelo
    redacte distinto. Pero "articulo 10" y "articulo 12" siguen siendo
    distintos: no se fusionan afectaciones a articulos diferentes.
    """
    if not valor:
        return ""
    # Se conservan numeros y numerales compuestos (5.1.4, 23.3).
    unidades = re.findall(r"\d+(?:\.\d+)*", str(valor))
    return ",".join(sorted(set(unidades), key=lambda u: [int(p) for p in u.split(".")]))


def clave_dedupe(
    norma_origen_id: str,
    tipo_relacion: str,
    identidad_afectada: NormaIdentity,
    articulos_afectados=None,
    descripcion_afectada: str | None = None,
) -> str:
    """Clave estable de una relacion juridica.

    El FRAGMENTO no participa: es evidencia, no identidad -si participara, un
    cambio de redaccion del modelo crearia un duplicado, que es justo el bug
    H-07-. La descripcion solo se usa como ultimo recurso cuando la identidad
    de la afectada no pudo construirse.
    """
    if identidad_afectada.es_utilizable:
        parte_afectada = identidad_afectada.clave()
    else:
        texto = _sin_acentos((descripcion_afectada or "").lower())
        parte_afectada = "desc:" + re.sub(r"[^a-z0-9]+", "-", texto).strip("-")[:80]

    return "::".join(
        [
            str(norma_origen_id),
            (tipo_relacion or "").lower(),
            parte_afectada,
            normalizar_articulos(articulos_afectados),
        ]
    )
