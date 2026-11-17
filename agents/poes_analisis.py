"""Analisis ORIENTATIVO de un POES (manual de procedimientos de botica).

Sin IA y sin red: trabaja sobre el texto por pagina (lo entrega
`pdf_extract.extract_pdf`, que ya hace OCR con deskew en escaneos) y sobre un
catalogo de normas `{document_key: estado_vigencia}` leido de digemid_normas.

Revisa cuatro cosas verificables:
  1. Completitud: paginas presentes vs. "PAG: n de m" declarado por POE.
  2. Indice vs. cuerpo: codigos y numero de POE que no coinciden.
  3. Vigencia: fechas "Vigencia Hasta" / "Proxima Rev" vencidas.
  4. Normas citadas: cuales estan en la base, su estado (derogada/modificada)
     y cuales faltan.

Todo es orientativo: lo valida el Director Tecnico.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

_TIPOS = {
    "decreto supremo": "DS", "d.s": "DS", "ds": "DS",
    "resolucion ministerial": "RM", "resolución ministerial": "RM",
    "r.m": "RM", "rm": "RM",
    "resolucion directoral": "RD", "resolución directoral": "RD", "r.d": "RD",
    "decreto de urgencia": "DU", "decreto legislativo": "DL",
    "ley": "LEY",
}

_CITA = re.compile(
    r"(Decreto Supremo|D\.S\.?|Resoluci[oó]n Ministerial|Resoluci[oó]n Directoral|"
    r"Decreto de Urgencia|Decreto Legislativo|R\.M\.?|R\.D\.?|Ley)"
    r"\s*(?:N[°ºo]\.?)?\s*(\d{1,6})\s*(?:[-–]\s*(\d{2,4}))?",
    re.I,
)
# Codigo de procedimiento: POE-BF-001, POES N° 3, PO-12, SOP 004, PE 05, "Procedimiento N° 7".
_CODIGO = (
    r"(?:POES?|PO|SOP|PE|PROC(?:EDIMIENTO)?)\b\s*[-–.]?\s*(?:[A-Z]{1,3}\b\s*[-–.]?\s*)?"
    r"(?:N[°º.]?\s*)?0*(\d{1,3})\b"
)
_ENCABEZADO = re.compile(_CODIGO, re.I)
# "PÁG: 3 de 9", "Página 3 de 9", "Pág. 3/9", "Page 3 of 9"
_PAG = re.compile(r"(?:P[ÁA]G(?:INA)?\.?|PAGE)\s*:?\s*(\d+)\s*(?:de|/|of)\s*(\d+)", re.I)
_FECHA = re.compile(r"(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})")
_MESES = {m: i for i, m in enumerate(
    ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
     "setiembre", "octubre", "noviembre", "diciembre"], 1)}
_MESES["septiembre"] = 9
_ETIQUETA_VIGENCIA = (
    r"(Vigencia\s+Hasta|Vigente\s+hasta|Pr[oó]xima\.?\s*Rev(?:isi[oó]n)?|Fecha\s+de\s+Revisi[oó]n)"
)
_FECHA_TEXTO = r"(\d{1,2}[-/.]\d{1,2}[-/.]\d{4}|\d{1,2}\s+de\s+[a-záéíóú]+\s+(?:de\s+)?\d{4})"
_PALABRAS_INDICE = r"LISTADO|[ÍI]NDICE|CONTENIDO|RELACI[ÓO]N DE PROCEDIMIENTOS"


def _anio(valor: str | None) -> int | None:
    if not valor:
        return None
    n = int(valor)
    if n < 100:
        return 1900 + n if n >= 30 else 2000 + n
    return n


def normalizar_clave(clave: str) -> tuple[str, int, int | None] | None:
    """'DS-014-2011-SA' -> ('DS', 14, 2011); 'LEY-29459' -> ('LEY', 29459, None)."""
    m = re.match(r"^(?:NORM-)?(DS|RM|RD|LEY|DU|DL)-0*(\d+)(?:-(\d{2,4}))?", clave.upper())
    return (m.group(1), int(m.group(2)), _anio(m.group(3))) if m else None


def extraer_citas(texto: str) -> list[tuple[str, int, int | None]]:
    citas: list[tuple[str, int, int | None]] = []
    for m in _CITA.finditer(texto):
        tipo = _TIPOS.get(m.group(1).lower().rstrip("."))
        if not tipo:
            continue
        cita = (tipo, int(m.group(2)), _anio(m.group(3)) if tipo != "LEY" else None)
        if cita not in citas:
            citas.append(cita)
    return citas


@dataclass
class ProcedimientoPoes:
    numero: int
    pagina_inicio: int  # 1-based, pagina fisica del PDF
    paginas_fisicas: int = 0
    paginas_declaradas: int | None = None

    @property
    def completo(self) -> bool:
        return self.paginas_declaradas is None or self.paginas_fisicas >= self.paginas_declaradas


@dataclass
class ReportePoes:
    total_paginas: int
    procedimientos: list[ProcedimientoPoes] = field(default_factory=list)
    incompletos: list[str] = field(default_factory=list)
    indice_inconsistente: list[str] = field(default_factory=list)
    vigencia_vencida: list[str] = field(default_factory=list)
    normas: dict[str, list[str]] = field(default_factory=dict)
    paginas_sin_texto: list[int] = field(default_factory=list)
    avisos: list[str] = field(default_factory=list)

    @property
    def lectura_pct(self) -> float:
        if not self.total_paginas:
            return 0.0
        return round(100 * (self.total_paginas - len(self.paginas_sin_texto)) / self.total_paginas, 1)


def _parsear_fecha(valor: str) -> date | None:
    try:
        m = _FECHA.fullmatch(valor.strip())
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        t = re.match(r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:de\s+)?(\d{4})", valor.strip(), re.I)
        if t and t.group(2).lower() in _MESES:
            return date(int(t.group(3)), _MESES[t.group(2).lower()], int(t.group(1)))
    except ValueError:
        pass
    return None


def _procedimientos(paginas: list[str]) -> list[ProcedimientoPoes]:
    """Un procedimiento empieza cuando su codigo cambia (o en 'pag 1 de m').
    El total declarado es el mayor 'de m' visto para ese codigo."""
    procs: list[ProcedimientoPoes] = []
    for i, texto in enumerate(paginas, 1):
        cabecera = texto[:600]
        m, g = _ENCABEZADO.search(cabecera), _PAG.search(cabecera)
        if not (m and g):
            continue
        numero = int(m.group(1))
        if not procs or procs[-1].numero != numero:
            procs.append(ProcedimientoPoes(numero, i))
        total = int(g.group(2))
        actual = procs[-1]
        actual.paginas_declaradas = max(actual.paginas_declaradas or 0, total)
    for idx, p in enumerate(procs):
        fin = procs[idx + 1].pagina_inicio - 1 if idx + 1 < len(procs) else len(paginas)
        p.paginas_fisicas = fin - p.pagina_inicio + 1
    return procs


def analizar_poes(
    paginas: list[str],
    catalogo: dict[str, str | None],
    hoy: date | None = None,
    min_chars: int = 40,
) -> ReportePoes:
    hoy = hoy or date.today()
    r = ReportePoes(total_paginas=len(paginas))
    r.paginas_sin_texto = [i for i, t in enumerate(paginas, 1) if len(t.strip()) < min_chars]
    r.procedimientos = _procedimientos(paginas)

    for p in r.procedimientos:
        if not p.completo:
            r.incompletos.append(
                f"POE {p.numero:03d}: trae {p.paginas_fisicas} de {p.paginas_declaradas} páginas"
            )

    # Indice: codigos "POE- BF - 0NN" listados vs numeros que aparecen en el cuerpo.
    indice = ""
    for t in paginas[:8]:
        if re.search(_PALABRAS_INDICE, t[:400], re.I) and len(_ENCABEZADO.findall(t)) >= 3:
            indice = t
            break
    en_indice = sorted({int(n) for n in _ENCABEZADO.findall(indice)})
    en_cuerpo = sorted({p.numero for p in r.procedimientos})
    if en_indice and en_cuerpo:
        sobran = [n for n in en_indice if n not in en_cuerpo]
        faltan = [n for n in en_cuerpo if n not in en_indice]
        if sobran:
            r.indice_inconsistente.append(
                "El índice lista códigos que no existen en el cuerpo: " + ", ".join(f"{n:03d}" for n in sobran)
            )
        if faltan:
            r.indice_inconsistente.append(
                "Procedimientos del cuerpo ausentes en el índice: " + ", ".join(f"{n:03d}" for n in faltan)
            )

    # Vigencia
    vistos: set[str] = set()
    for m in re.finditer(_ETIQUETA_VIGENCIA + r"\s*:?\s*" + _FECHA_TEXTO, "\n".join(paginas), re.I):
        fecha = _parsear_fecha(m.group(2))
        etiqueta = f"{re.sub(chr(92) + 's+', ' ', m.group(1)).strip()}: {m.group(2)}"
        if fecha and fecha < hoy and etiqueta not in vistos:
            vistos.add(etiqueta)
            r.vigencia_vencida.append(etiqueta)
    if not r.procedimientos:
        r.avisos.append("No se reconoció la estructura de procedimientos (códigos y 'página n de m'); revisión manual.")

    # Normas citadas
    indexado: dict[tuple[str, int, int | None], tuple[str, str | None]] = {}
    for clave, estado in catalogo.items():
        norm = normalizar_clave(clave)
        if norm:
            indexado.setdefault(norm, (clave, estado))
    resumen: dict[str, list[str]] = {"vigente": [], "modificada": [], "derogada": [], "otro_estado": [], "no_esta_en_base": []}
    for tipo, num, anio in extraer_citas("\n".join(paginas)):
        etiqueta = f"{tipo} {num}" + (f"-{anio}" if anio else "")
        hallado = indexado.get((tipo, num, anio)) or (
            next((v for (t, n, _a), v in indexado.items() if t == tipo and n == num), None) if tipo == "LEY" else None
        )
        if not hallado:
            resumen["no_esta_en_base"].append(etiqueta)
            continue
        estado = (hallado[1] or "").lower()
        resumen[estado if estado in ("vigente", "modificada", "derogada") else "otro_estado"].append(etiqueta)
    r.normas = {k: v for k, v in resumen.items() if v}
    return r


def formatear_reporte(r: ReportePoes) -> str:
    lineas = [f"Páginas: {r.total_paginas} | Lectura de texto: {r.lectura_pct}%", f"Procedimientos detectados: {len(r.procedimientos)}"]
    for titulo, items in (
        ("Incompleto", r.incompletos),
        ("Índice", r.indice_inconsistente),
        ("Vigencia vencida", r.vigencia_vencida),
    ):
        for item in items:
            lineas.append(f"[{titulo}] {item}")
    for aviso in r.avisos:
        lineas.append(f"[Aviso] {aviso}")
    for estado, items in r.normas.items():
        lineas.append(f"[Normas {estado}] " + ", ".join(items))
    if r.paginas_sin_texto:
        lineas.append(f"[Sin texto legible] páginas {r.paginas_sin_texto}")
    lineas.append("Orientativo: lo valida el Director Técnico.")
    return "\n".join(lineas)
