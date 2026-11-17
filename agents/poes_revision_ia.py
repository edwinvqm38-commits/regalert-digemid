"""Revision de un POES CON IA, contrastada contra la base de normas.

Solo orquesta: la llamada al modelo y la busqueda de normas se inyectan
(`llm`, `buscar_normas`), asi no hay claves aqui y las pruebas no cobran.

Anti-invencion: toda observacion debe citar una `norma_ref` que este en el
contexto entregado al modelo; las que no, se descartan. Es orientativo: lo
valida el Director Tecnico.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Callable

from agents.poes_analisis import ProcedimientoPoes

TIPOS = {"falta", "desactualizado", "inconsistente"}
SEVERIDADES = {"alta", "media", "baja"}

SYSTEM = (
    "Eres revisor de POES (procedimientos operativos estándar) de establecimientos farmacéuticos de Perú. "
    "Compara el procedimiento con las NORMAS DEL CONTEXTO y nada más. No inventes normas ni artículos: "
    "cada observación debe citar una norma_ref que aparezca en el contexto. Si no hay base, no la incluyas. "
    'Responde SOLO JSON: {"hallazgos":[{"tipo":"falta|desactualizado|inconsistente",'
    '"severidad":"alta|media|baja","descripcion":"...","norma_ref":"<clave del contexto>"}]}'
)


@dataclass
class Hallazgo:
    procedimiento: str
    tipo: str
    severidad: str
    descripcion: str
    norma_ref: str


@dataclass
class ReporteIA:
    hallazgos: list[Hallazgo] = field(default_factory=list)
    descartados: int = 0
    errores: list[str] = field(default_factory=list)


NormasFn = Callable[[str], list[dict]]  # texto -> [{"document_key","titulo","extracto","estado_vigencia"}]
LlmFn = Callable[[str, str], str]  # (system, user) -> texto JSON


def texto_de_procedimiento(paginas: list[str], proc: ProcedimientoPoes) -> str:
    ini = proc.pagina_inicio - 1
    return "\n".join(paginas[ini: ini + max(proc.paginas_fisicas, 1)])


def titulo_de(texto: str, numero: int) -> str:
    for linea in texto.splitlines()[:12]:
        limpia = linea.strip()
        if len(limpia) > 12 and limpia.upper() == limpia and not re.search(r"P[ÁA]G|N[°º]|POE|BOTICA", limpia):
            return limpia[:120]
    return f"Procedimiento {numero:03d}"


def _contexto(normas: list[dict], limite: int = 8) -> str:
    return "\n".join(
        f"[{n['document_key']}] ({n.get('estado_vigencia') or 'estado desconocido'}) {n.get('titulo', '')}\n"
        f"{(n.get('extracto') or '')[:900]}"
        for n in normas[:limite]
    )


def parsear_hallazgos(respuesta: str, claves_validas: set[str], procedimiento: str) -> tuple[list[Hallazgo], int]:
    try:
        data = json.loads(re.sub(r"^```(?:json)?\s*|\s*```$", "", respuesta.strip()))
        items = data["hallazgos"]
        assert isinstance(items, list)
    except Exception:
        return [], 0
    ok, descartados = [], 0
    for it in items:
        if not isinstance(it, dict):
            descartados += 1
            continue
        tipo, sev = it.get("tipo"), it.get("severidad")
        desc, ref = str(it.get("descripcion", "")).strip(), it.get("norma_ref")
        if tipo in TIPOS and sev in SEVERIDADES and desc and ref in claves_validas:
            ok.append(Hallazgo(procedimiento, tipo, sev, desc[:400], ref))
        else:
            descartados += 1
    return ok, descartados


def revisar_poes(
    paginas: list[str],
    procedimientos: list[ProcedimientoPoes],
    buscar_normas: NormasFn,
    llm: LlmFn,
    normas_recientes: Callable[[], list[dict]] | None = None,
) -> ReporteIA:
    rep = ReporteIA()
    titulos: list[str] = []
    for proc in procedimientos:
        texto = texto_de_procedimiento(paginas, proc)
        titulo = titulo_de(texto, proc.numero)
        titulos.append(titulo)
        normas = buscar_normas(titulo + "\n" + texto[:1500])
        if not normas:
            rep.errores.append(f"{titulo}: sin normas relacionadas en la base; no se revisó")
            continue
        usuario = f"NORMAS DEL CONTEXTO:\n{_contexto(normas)}\n\nPROCEDIMIENTO ({titulo}):\n{texto[:12000]}"
        try:
            respuesta = llm(SYSTEM, usuario)
        except Exception as exc:  # un procedimiento fallido no tumba la revision
            rep.errores.append(f"{titulo}: falló la IA ({type(exc).__name__})")
            continue
        h, d = parsear_hallazgos(respuesta, {n["document_key"] for n in normas[:8]}, titulo)
        rep.hallazgos += h
        rep.descartados += d

    if normas_recientes and titulos:
        recientes = normas_recientes()
        if recientes:
            usuario = (
                f"NORMAS DEL CONTEXTO (las más recientes):\n{_contexto(recientes, 12)}\n\n"
                "PROCEDIMIENTOS EXISTENTES:\n- " + "\n- ".join(titulos) + "\n\n"
                "Indica SOLO procedimientos o temas que faltan en este manual por lo que exigen esas normas "
                "(tipo=falta)."
            )
            try:
                h, d = parsear_hallazgos(llm(SYSTEM, usuario), {n["document_key"] for n in recientes[:12]}, "Manual completo")
                rep.hallazgos += h
                rep.descartados += d
            except Exception as exc:
                rep.errores.append(f"Revisión de faltantes: falló la IA ({type(exc).__name__})")
    return rep


def formatear_reporte_ia(rep: ReporteIA) -> str:
    orden = {"alta": 0, "media": 1, "baja": 2}
    lineas = []
    for h in sorted(rep.hallazgos, key=lambda x: orden[x.severidad]):
        lineas.append(f"[{h.severidad.upper()}] {h.procedimiento} — {h.tipo}: {h.descripcion} (base: {h.norma_ref})")
    lineas += [f"[No revisado] {e}" for e in rep.errores]
    if rep.descartados:
        lineas.append(f"({rep.descartados} observaciones de la IA se descartaron por no citar una norma de la base)")
    lineas.append("Orientativo: lo valida el Director Técnico.")
    return "\n".join(lineas)
