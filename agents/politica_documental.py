"""LA politica documental canonica: cuando se puede escribir un pdf_url (F-03B).

Existe UNA sola regla en todo el sistema, y vive aqui. Antes cada ruta tenia la
suya -implicita, y siempre la misma equivocacion-:

    crawl_normativa_pdf_urls        candidatos[0] tras ordenar por ruta
    agent_normative_pdf_detector    candidate_links[0] tras ordenar por score
    crawl_digemid_normativa_inventory  pdf_urls[0]
    import_normativa_inventory      hereda el pdf_urls[0] de la anterior

Las cuatro eligen por POSICION. Ninguna compara el documento con la norma que
dice representar. Por eso RM-1000-2016 y RM-1001-2016 acabaron intercambiadas.

La regla unica es:

    resolver identidad objetivo
      -> enumerar TODOS los candidatos
        -> inspeccionar el CONTENIDO de cada uno
          -> probar la identidad
            -> solo entonces escribir

Y su corolario, que es lo que de verdad cambia el comportamiento:

    NO se escribe con AMBIGUO, CONTRADICTORIO, NO_ENCONTRADA ni
    AUDITORIA_INCOMPLETA. No hay "mejor candidato". No hay score que decida
    entre dos candidatos incompatibles: dos candidatos plausibles es un caso
    para un humano, no para un desempate automatico.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from identidad_documental import (  # noqa: E402
    AUDITORIA_INCOMPLETA,
    MATCH_EXACTO,
    MATCH_MULTINORMA,
    EvidenciaDocumental,
    ResultadoResolucion,
    resolver_pdf_para_norma,
)
from identidad_normativa import NormaIdentity  # noqa: E402

# Motivos por los que una ruta NO escribe. Se registran; no se resuelven solos.
REQUIERE_HUMANO = "REQUIERE_REVISION_HUMANA"
REQUIERE_REAUDITORIA = "REQUIERE_REAUDITORIA_COMPLETA"


@dataclass
class Decision:
    """Lo que una ruta de escritura tiene permitido hacer."""

    escribir: bool
    url: str | None
    estado: str
    motivo: str
    start_page: int | None = None
    end_page: int | None = None
    evidencia: dict | None = None
    seguimiento: str = ""

    @property
    def bloqueada_por_incompletitud(self) -> bool:
        return self.estado == AUDITORIA_INCOMPLETA


def decidir(
    candidatos: list[EvidenciaDocumental],
    identidad_objetivo: NormaIdentity,
    candidatos_omitidos: int = 0,
    motivo_omision: str = "",
) -> Decision:
    """LA decision. Toda ruta que escriba pdf_url debe pasar por aqui."""
    if not identidad_objetivo or not identidad_objetivo.numero:
        return Decision(
            False, None, REQUIERE_HUMANO,
            "no se pudo construir la identidad de la norma objetivo: sin objetivo "
            "no hay nada que comprobar, y sin comprobacion no se escribe",
            seguimiento=REQUIERE_HUMANO,
        )

    resultado: ResultadoResolucion = resolver_pdf_para_norma(
        candidatos, identidad_objetivo,
        candidatos_omitidos=candidatos_omitidos,
        motivo_omision=motivo_omision,
    )
    return de_resultado(resultado)


def de_resultado(resultado: ResultadoResolucion) -> Decision:
    evidencia = {
        "estado": resultado.estado,
        "motivo": resultado.motivo,
        "auditoria_completa": resultado.auditoria_completa,
        "candidatos": resultado.candidatos_evaluados,
        "segmento": resultado.segmento.como_dict() if resultado.segmento else None,
    }

    if resultado.puede_escribirse:
        return Decision(
            True, resultado.url, resultado.estado, resultado.motivo,
            resultado.start_page, resultado.end_page, evidencia,
        )

    seguimiento = (
        REQUIERE_REAUDITORIA if resultado.estado == AUDITORIA_INCOMPLETA
        else REQUIERE_HUMANO
    )
    detalle = resultado.motivo
    if resultado.estado in (MATCH_EXACTO, MATCH_MULTINORMA):
        # Hubo match pero no basta: rango sin cerrar, o auditoria incompleta.
        detalle = (f"{resultado.motivo}; el match no es escribible todavia "
                   "(rango sin evidencia de fin o auditoria incompleta)")

    return Decision(False, None, resultado.estado, detalle,
                    evidencia=evidencia, seguimiento=seguimiento)
