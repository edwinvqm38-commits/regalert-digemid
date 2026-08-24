"""SHADOW VALIDATION de F-03B contra `digemid_documentos` real. SOLO LECTURA.

Antes de fusionar #77 (la política documental para `NormativePdfDetectorAgent`)
se necesita saber qué habría hecho contra datos reales SIN aplicarlo. Este
script usa el código REAL de `agents/agent_normative_pdf_detector.py` -las
mismas funciones que escribirían en producción-, pero nunca llama
`update_document()`, ni `.insert(`, `.update(`, `.upsert(`, `.delete(`, ni
escribe en Storage. Cada candidato se lee con GET; nada se persiste.

Para cada norma calcula, en paralelo:

  1. Qué haría F-03B: `identidad_objetivo_de_documento()` ->
     `_enlaces_candidatos()` -> abrir cada PDF -> `politica_documental.decidir()`.
  2. Qué habría hecho el algoritmo ANTIGUO: `candidate_links[0]` tras ordenar
     por score, SIN abrir ningún PDF ni comprobar nada -exactamente el
     defecto original que causó LEY-29698/RM-373-2024-. Es una simulación
     pura sobre los mismos candidatos que ya se descargaron para F-03B: no
     hace una segunda pasada de red.

Produce `SHADOW_F03B.csv` / `.json` con una fila por norma y un resumen con
los conteos que pide la auditoría.

Uso:
    python scripts/shadow_f03b_pdf_detector.py --out-dir reportes/
    python scripts/shadow_f03b_pdf_detector.py --limite 10 --document-key X

CRÍTICO -igual que en F-03B-: el score nunca valida identidad, un único
candidato tampoco, el filename tampoco. Solo el contenido autoriza
MATCH_EXACTO/MATCH_MULTINORMA. Sin identidad objetivo parseable:
`pdf_identity_unknown`, y este script NUNCA escribe de todos modos.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from agents.agent_normative_pdf_detector import (  # noqa: E402
    NormativePdfDetectorAgent,
    identidad_objetivo_de_documento,
)
from identidad_documental import (  # noqa: E402
    AMBIGUO,
    AUDITORIA_INCOMPLETA,
    MATCH_EXACTO,
    MATCH_MULTINORMA,
    NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
    PDF_IDENTIDAD_CONTRADICTORIA,
    EvidenciaDocumental,
    clasificar_identidad_documental,
)
from politica_documental import REQUIERE_HUMANO, decidir  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Estados donde se declara "IDENTIDAD_OBJETIVO_NO_PARSEABLE" antes de tocar
# la red: sin identidad objetivo no hay nada que comprobar (F-03B).
IDENTIDAD_NO_PARSEABLE = "IDENTIDAD_OBJETIVO_NO_PARSEABLE"


def get_documentos(agent: NormativePdfDetectorAgent, limite: int | None,
                    document_key: str | None) -> list[dict]:
    """SOLO LECTURA: mismas columnas que `fetch_pending_documents`, pero sin
    filtrar por `process_status` -se pide auditar tambien lo que ya tiene
    `file_url`, para comparacion historica-."""
    query = (
        agent.supabase
        .table(agent.table_name)
        .select("id, document_key, title, detail_url, file_url, raw, process_status")
        .eq("source_type", "normativa")
        .not_.is_("detail_url", "null")
        .order("updated_at", desc=True)
    )
    if document_key:
        query = query.eq("document_key", document_key)
    response = query.execute()
    filas = response.data or []
    if limite:
        filas = filas[:limite]
    return filas


def old_algoritmo_candidate0(candidatos_urls: list[tuple[str, str]]) -> str | None:
    """SOLO SIMULACION. Replica exacta de la version anterior a F-03B:
    `candidate_links.sort(key=score, reverse=True); candidate_links[0]`, sin
    tope de empate ni verificacion posterior -esa era la version original
    que causo LEY-29698/RM-373-2024, antes incluso del bloqueo de empate que
    se agrego despues-. `candidatos_urls` ya viene ordenada por score
    descendente (la produce `_enlaces_candidatos`), asi que esto NO hace
    ninguna llamada de red nueva: es puramente una lectura de la lista que
    F-03B ya obtuvo.
    """
    if not candidatos_urls:
        return None
    return candidatos_urls[0][0]


def evidencia_como_dict(ev: EvidenciaDocumental) -> dict:
    clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
    return {
        "url": ev.url,
        "pdf_sha256": ev.pdf_sha256,
        "pdf_disponible": ev.pdf_disponible,
        "total_paginas": ev.total_paginas,
        "paginas_analizadas": ev.paginas_analizadas,
        "auditoria_completa": ev.auditoria_completa,
        "motivo_incompletitud": ev.motivo_incompletitud,
        # El score es dato AUXILIAR de trazabilidad -para poder comparar con
        # el algoritmo antiguo-. Nunca participa en `clasificar_identidad_documental`
        # ni en `decidir()`: eso es justo lo que este shadow existe para probar.
        "filename_match": ev.filename_match,
        "source_context_match": ev.source_context_match,
        "content_match": ev.content_match,
        "identidades_detectadas": sorted({str(a.identidad) for a in ev.encabezados}),
        "clasificacion_individual": clasificacion,
        "confianza_individual": confianza,
        "motivo_individual": motivo,
    }


def analizar_norma(agent: NormativePdfDetectorAgent, row: dict) -> dict:
    document_key = row.get("document_key")
    title = row.get("title")
    detail_url = row.get("detail_url")
    file_url_actual = row.get("file_url")

    fila = {
        "document_key": document_key,
        "title": title,
        "detail_url": detail_url,
        "file_url_actual": file_url_actual,
        "process_status_actual": row.get("process_status"),
        "identity_expected": None,
        "identity_parse_status": None,
        "num_candidatos": 0,
        "candidatos_omitidos": 0,
        "candidatos": [],
        "pdf_sha256_por_candidato": [],
        "paginas_analizadas_total": 0,
        "paginas_pdf_total": 0,
        "auditoria_completa": None,
        "f03b_estado": None,
        "f03b_habria_escrito": False,
        "f03b_url_elegida": None,
        "f03b_start_page": None,
        "f03b_end_page": None,
        "f03b_motivo": "",
        "old_habria_escrito": False,
        "old_url_elegida": None,
        "old_vs_f03b_coincide": None,
        "f03b_elige_distinto_al_viejo": None,
        "viejo_habria_escrito_pero_f03b_bloquea": None,
        "file_url_actual_evaluado": None,
        "file_url_actual_contradice_identidad": None,
        "bucket_resumen": None,
    }

    identidad_objetivo = identidad_objetivo_de_documento(title, document_key)
    if identidad_objetivo is None:
        fila["identity_parse_status"] = "pdf_identity_unknown"
        fila["f03b_estado"] = IDENTIDAD_NO_PARSEABLE
        fila["f03b_motivo"] = (
            "no se pudo construir una identidad objetivo verificable a partir "
            "del titulo/document_key: sin identidad no hay nada que comprobar"
        )
        fila["bucket_resumen"] = IDENTIDAD_NO_PARSEABLE
        return fila

    fila["identity_expected"] = str(identidad_objetivo)
    fila["identity_parse_status"] = "OK"

    # --- Enumerar candidatos (SOLO LECTURA: GET del detail_url) -----------
    try:
        if agent.is_pdf_response(detail_url):
            candidatos_urls = [(detail_url, "")]
        else:
            response = agent.fetch_detail_response(detail_url)
            content_type = response.headers.get("Content-Type", "").lower()
            if "application/pdf" in content_type:
                candidatos_urls = [(detail_url, "")]
            else:
                candidatos_urls = agent._enlaces_candidatos(detail_url, response.text)
    except Exception as error:
        fila["f03b_estado"] = "ERROR_RED"
        fila["f03b_motivo"] = f"no se pudo leer detail_url: {str(error)[:200]}"
        fila["bucket_resumen"] = "ERROR_RED"
        return fila

    omitidos = max(0, len(candidatos_urls) - detector_max_candidatos())
    candidatos_urls = candidatos_urls[:detector_max_candidatos()]
    fila["num_candidatos"] = len(candidatos_urls)
    fila["candidatos_omitidos"] = omitidos

    # --- SOLO SIMULACION: que habria elegido el algoritmo antiguo ---------
    fila["old_url_elegida"] = old_algoritmo_candidate0(candidatos_urls)
    fila["old_habria_escrito"] = fila["old_url_elegida"] is not None

    if not candidatos_urls:
        fila["f03b_estado"] = NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA
        fila["f03b_motivo"] = "no se detecto ningun candidato a PDF en detail_url"
        fila["bucket_resumen"] = "NO_ENCONTRADO"
        return fila

    # --- Abrir cada candidato (SOLO LECTURA: GET + fitz.open en memoria) --
    evidencias = []
    for url, anchor_text in candidatos_urls:
        ev = agent._evidencia_de_candidato(url, anchor_text, identidad_objetivo)
        evidencias.append(ev)
        fila["pdf_sha256_por_candidato"].append(ev.pdf_sha256)
        fila["paginas_analizadas_total"] += ev.paginas_analizadas or 0
        fila["paginas_pdf_total"] += ev.total_paginas or 0
        fila["candidatos"].append(evidencia_como_dict(ev))
        time.sleep(0.5)  # cortesia con el servidor

    fila["auditoria_completa"] = all(ev.auditoria_completa for ev in evidencias) and omitidos == 0

    # --- La decision REAL de F-03B: la misma funcion que usaria en produccion
    decision = decidir(
        evidencias, identidad_objetivo,
        candidatos_omitidos=omitidos,
        motivo_omision=(f"mas de {detector_max_candidatos()} candidatos" if omitidos else ""),
    )
    fila["f03b_estado"] = decision.estado
    fila["f03b_habria_escrito"] = decision.escribir
    fila["f03b_url_elegida"] = decision.url
    fila["f03b_start_page"] = decision.start_page
    fila["f03b_end_page"] = decision.end_page
    fila["f03b_motivo"] = decision.motivo

    if decision.estado == MATCH_EXACTO:
        fila["bucket_resumen"] = MATCH_EXACTO
    elif decision.estado == MATCH_MULTINORMA:
        fila["bucket_resumen"] = MATCH_MULTINORMA
    elif decision.estado == AMBIGUO:
        fila["bucket_resumen"] = AMBIGUO
    elif decision.estado == AUDITORIA_INCOMPLETA:
        fila["bucket_resumen"] = AUDITORIA_INCOMPLETA
    elif decision.estado == REQUIERE_HUMANO:
        fila["bucket_resumen"] = AMBIGUO
    elif decision.estado == NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA:
        # Distinguir CONTRADICTORIO (algun candidato SI tiene encabezado,
        # solo que de otra norma) de NO_ENCONTRADO (ningun candidato tiene
        # ningun encabezado normativo).
        hubo_contradiccion = any(
            c["clasificacion_individual"] == PDF_IDENTIDAD_CONTRADICTORIA
            for c in fila["candidatos"]
        )
        fila["bucket_resumen"] = PDF_IDENTIDAD_CONTRADICTORIA if hubo_contradiccion else "NO_ENCONTRADO"
    else:
        fila["bucket_resumen"] = f"OTRO:{decision.estado}"

    # --- Comparacion old vs F-03B ------------------------------------------
    if fila["old_habria_escrito"] and fila["f03b_habria_escrito"]:
        fila["old_vs_f03b_coincide"] = fila["old_url_elegida"] == fila["f03b_url_elegida"]
        fila["f03b_elige_distinto_al_viejo"] = not fila["old_vs_f03b_coincide"]
        fila["viejo_habria_escrito_pero_f03b_bloquea"] = False
    elif fila["old_habria_escrito"] and not fila["f03b_habria_escrito"]:
        fila["old_vs_f03b_coincide"] = False
        fila["f03b_elige_distinto_al_viejo"] = False
        fila["viejo_habria_escrito_pero_f03b_bloquea"] = True
    else:
        fila["old_vs_f03b_coincide"] = False
        fila["f03b_elige_distinto_al_viejo"] = False
        fila["viejo_habria_escrito_pero_f03b_bloquea"] = False

    # --- file_url_actual: ¿su contenido contradice la identidad objetivo? -
    if file_url_actual:
        evaluado = next((ev for ev in evidencias if ev.url == file_url_actual), None)
        if evaluado is None:
            # No estaba entre los candidatos recien enumerados (puede que la
            # pagina fuente haya cambiado desde que se guardo). Se evalua
            # aparte, con la MISMA funcion de lectura -sigue siendo GET-.
            try:
                evaluado = agent._evidencia_de_candidato(file_url_actual, "", identidad_objetivo)
                fila["pdf_sha256_por_candidato"].append(evaluado.pdf_sha256)
            except Exception as error:
                logger.warning("No se pudo evaluar file_url_actual de %s: %s", document_key, error)
                evaluado = None

        if evaluado is not None:
            clasificacion, _confianza, _motivo = clasificar_identidad_documental(evaluado)
            fila["file_url_actual_evaluado"] = evidencia_como_dict(evaluado)
            fila["file_url_actual_contradice_identidad"] = (
                clasificacion == PDF_IDENTIDAD_CONTRADICTORIA
            )

    return fila


_MAX_CANDIDATOS_CACHE: list[int] = []


def detector_max_candidatos() -> int:
    if not _MAX_CANDIDATOS_CACHE:
        from agents import agent_normative_pdf_detector as mod
        _MAX_CANDIDATOS_CACHE.append(mod.MAX_CANDIDATOS)
    return _MAX_CANDIDATOS_CACHE[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="reportes")
    parser.add_argument("--limite", type=int, default=None)
    parser.add_argument("--document-key", default=None,
                        help="Auditar solo esta norma (para casos puntuales).")
    args = parser.parse_args()

    agent = NormativePdfDetectorAgent()
    filas_db = get_documentos(agent, args.limite, args.document_key)
    logger.info("Registros a auditar en shadow (SOLO LECTURA): %d", len(filas_db))

    resultados, inicio = [], time.time()
    for row in filas_db:
        logger.info("Analizando (shadow): %s | %s", row.get("document_key"), row.get("title"))
        resultados.append(analizar_norma(agent, row))

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    campos_csv = [
        "document_key", "title", "detail_url", "file_url_actual", "process_status_actual",
        "identity_expected", "identity_parse_status",
        "num_candidatos", "candidatos_omitidos",
        "paginas_analizadas_total", "paginas_pdf_total", "auditoria_completa",
        "f03b_estado", "f03b_habria_escrito", "f03b_url_elegida",
        "f03b_start_page", "f03b_end_page", "f03b_motivo",
        "old_habria_escrito", "old_url_elegida",
        "old_vs_f03b_coincide", "f03b_elige_distinto_al_viejo",
        "viejo_habria_escrito_pero_f03b_bloquea",
        "file_url_actual_contradice_identidad",
        "bucket_resumen",
    ]
    with (out / "SHADOW_F03B.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=campos_csv, extrasaction="ignore")
        w.writeheader()
        w.writerows(resultados)

    conteo_buckets: dict[str, int] = {}
    for f in resultados:
        conteo_buckets[f["bucket_resumen"]] = conteo_buckets.get(f["bucket_resumen"], 0) + 1

    resumen = {
        "total_registros": len(resultados),
        "identidad_objetivo_parseable": sum(
            1 for f in resultados if f["identity_parse_status"] == "OK"
        ),
        "identidad_objetivo_no_parseable": sum(
            1 for f in resultados if f["identity_parse_status"] == "pdf_identity_unknown"
        ),
        "MATCH_EXACTO": conteo_buckets.get(MATCH_EXACTO, 0),
        "MATCH_MULTINORMA": conteo_buckets.get(MATCH_MULTINORMA, 0),
        "AMBIGUO": conteo_buckets.get(AMBIGUO, 0),
        "CONTRADICTORIO": conteo_buckets.get("CONTRADICTORIO", 0),
        "NO_ENCONTRADO": conteo_buckets.get("NO_ENCONTRADO", 0),
        "AUDITORIA_INCOMPLETA": conteo_buckets.get(AUDITORIA_INCOMPLETA, 0),
    }
    # Cualquier bucket que no encaje en los ocho pedidos (ERROR_RED, OTRO:*)
    # se reporta aparte, sin desaparecer del resumen.
    resumen["otros_buckets"] = {
        k: v for k, v in conteo_buckets.items()
        if k not in (MATCH_EXACTO, MATCH_MULTINORMA, AMBIGUO, "CONTRADICTORIO",
                     "NO_ENCONTRADO", AUDITORIA_INCOMPLETA, IDENTIDAD_NO_PARSEABLE)
    }
    resumen["pdfs_que_el_viejo_elegiria_pero_f03b_bloquea"] = sum(
        1 for f in resultados if f["viejo_habria_escrito_pero_f03b_bloquea"]
    )
    resumen["pdfs_donde_viejo_y_f03b_coinciden"] = sum(
        1 for f in resultados if f["old_vs_f03b_coincide"]
    )
    resumen["casos_f03b_elige_distinto_al_viejo"] = sum(
        1 for f in resultados if f["f03b_elige_distinto_al_viejo"]
    )
    resumen["casos_file_url_actual_contradice_identidad"] = sum(
        1 for f in resultados if f["file_url_actual_contradice_identidad"]
    )
    resumen["segundos"] = round(time.time() - inicio, 1)

    (out / "SHADOW_F03B.json").write_text(
        json.dumps({"resumen": resumen, "registros": resultados}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\n" + "=" * 74)
    print("SHADOW VALIDATION F-03B — SOLO LECTURA, NADA MODIFICADO EN LA BASE")
    print("=" * 74)
    for k, v in resumen.items():
        print(f"  {k:48} {v}")

    peligrosos = [f for f in resultados if f["file_url_actual_contradice_identidad"]]
    if peligrosos:
        print(f"\n⚠️  file_url_actual CONTRADICE la identidad objetivo ({len(peligrosos)}):")
        for f in peligrosos:
            print(f"    {f['document_key']:40} esperado {f['identity_expected']}")

    distintos = [f for f in resultados if f["f03b_elige_distinto_al_viejo"]]
    if distintos:
        print(f"\n⚠️  F-03B elige un PDF DISTINTO al que elegia el algoritmo antiguo ({len(distintos)}):")
        for f in distintos:
            print(f"    {f['document_key']:40} viejo={f['old_url_elegida']} "
                  f"f03b={f['f03b_url_elegida']}")

    bloqueados = [f for f in resultados if f["viejo_habria_escrito_pero_f03b_bloquea"]]
    if bloqueados:
        print(f"\n⚠️  El algoritmo antiguo habria escrito, F-03B bloquea ({len(bloqueados)}):")
        for f in bloqueados:
            print(f"    {f['document_key']:40} viejo={f['old_url_elegida']} "
                  f"f03b_estado={f['f03b_estado']}")

    print(f"\nMatriz: {out / 'SHADOW_F03B.csv'}")
    print(f"JSON:   {out / 'SHADOW_F03B.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
