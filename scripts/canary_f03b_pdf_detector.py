"""CANARY CONTROLADO de F-03B. Dry-run por defecto. `--apply` esta PREPARADO
pero exige F03B_PDF_WRITE_MODE=canary -que ningun workflow ni secret
configura todavia-, asi que hoy sigue sin poder escribir.

El canary NO es una excepción a la política documental: es MÁS ESTRICTO que
producción, no menos. Todas las reglas de `agents/politica_documental.py`
siguen aplicando -esto añade condiciones ENCIMA, nunca las relaja-.

Reglas propias del canary, todas obligatorias:

  1. Allowlist EXACTA de `document_key`, máximo 3. No es un LIMIT: cualquier
     fila fuera de la allowlist se descarta aunque la consulta la devuelva.
  2. Solo MATCH_EXACTO. Nunca MATCH_MULTINORMA en esta primera corrida -se
     prueba la ruta más simple antes de introducir segmentación-.
  3. `tipo_de_documento` debe ser DOCUMENTO_NORMA_UNICA. Nunca PROYECTO,
     ANEXO ni MULTINORMA, aunque el resolvedor ya haya dado MATCH_EXACTO.
  4. `file_url` actual debe ser NULL, verificado DOS VECES en el dry-run: una
     al leer la fila y otra, con una consulta fresca, justo antes de
     proponer el payload. Si cambió entre medio, esa fila se ABORTA -no hay
     optimistic assumptions-.
  5. La corrida debe ser determinista: ejecutar el mismo dry-run dos veces
     (A y B) y comparar document_key, identity_expected, pdf_url propuesta,
     SHA256, estado y page_count. Cualquier diferencia es CANARY_NO_APTO.

Sin `--apply`, esto NUNCA llama `.insert(`, `.update(`, `.upsert(`,
`.delete(` ni escribe en Storage. El payload que se mostraria si se
escribiera se CONSTRUYE con
`agents.agent_normative_pdf_detector.construir_payload_actualizacion` -el
mismo codigo que usaria `update_document()` en produccion-, pero nunca se
pasa a `.execute()` a menos que TODAS estas guardas de `--apply` se cumplan:

  1.  F03B_PDF_WRITE_MODE == "canary" (exacto, fail-closed por defecto).
  2.  1 a 3 document_key exactos en la allowlist.
  3.  Todas las filas existen.
  4.  file_url actual es NULL (verificado en el dry-run doble).
  5.  Decision == MATCH_EXACTO.
  6.  tipo_de_documento == DOCUMENTO_NORMA_UNICA.
  7.  Auditoria completa.
  8.  SHA256 estable entre dry-run A y B.
  9.  Identidad estable entre A y B.
  10. URL propuesta estable entre A y B.
  11. Relectura de CADA fila inmediatamente antes de su UPDATE -no se
      reutiliza el resultado del dry-run como si el tiempo no hubiera
      pasado-.
  12. Ninguna precondicion cambio en esa relectura final.

Si UNA fila no supera el preflight final: NINGUNA de las filas se escribe
-ALL OR NOTHING-. Cada escritura exitosa se verifica con un READ BACK
inmediato contra Supabase; si no coincide con lo escrito, se detiene sin
procesar mas filas y se reporta CANARY_FALLO_POST_WRITE. Se generan
CANARY_F03B_BEFORE/AFTER/DIFF/ROLLBACK.json -el rollback queda preparado,
NUNCA se ejecuta automaticamente-.

Uso:
    python scripts/canary_f03b_pdf_detector.py --document-keys A,B,C --out-dir reportes
    # Futuro, con autorizacion humana y F03B_PDF_WRITE_MODE=canary exportado:
    python scripts/canary_f03b_pdf_detector.py --document-keys A,B,C --apply
"""

from __future__ import annotations

import argparse
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
    WRITE_MODE_CANARY,
    NormativePdfDetectorAgent,
    construir_payload_actualizacion,
    f03b_write_mode,
    identidad_objetivo_de_documento,
)
from identidad_documental import (  # noqa: E402
    DOCUMENTO_NORMA_UNICA,
    MATCH_EXACTO,
    tipo_de_documento,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MAX_CANARY = 3

# --- Campos que la escritura puede tocar: los mismos que guarda el rollback
ROLLBACK_CAMPOS = ("has_file", "file_url", "file_name", "file_ext", "mime_type",
                   "process_status", "process_message", "raw", "updated_at")

# --- Motivos de aborto, para que el reporte sea legible por un humano ------
ABORT_FUERA_DE_ALLOWLIST = "FUERA_DE_ALLOWLIST"
ABORT_NO_EXISTE = "DOCUMENT_KEY_NO_EXISTE"
ABORT_FILE_URL_NO_NULL = "FILE_URL_YA_NO_ES_NULL"
ABORT_SIN_IDENTIDAD = "IDENTIDAD_OBJETIVO_NO_PARSEABLE"
ABORT_NO_MATCH_EXACTO = "DECISION_DISTINTA_DE_MATCH_EXACTO"
ABORT_AUDITORIA_INCOMPLETA = "AUDITORIA_INCOMPLETA"
ABORT_TIPO_DOCUMENTO = "TIPO_DE_DOCUMENTO_NO_ES_NORMA_UNICA"
ABORT_PRECONDICION_CAMBIO = "PRECONDICION_CAMBIO_ENTRE_LECTURA_Y_PROPUESTA"


def validar_allowlist(document_keys: list[str]) -> None:
    if not document_keys:
        raise ValueError("La allowlist no puede estar vacia.")
    if len(document_keys) > MAX_CANARY:
        raise ValueError(
            f"La allowlist trae {len(document_keys)} document_key; el maximo "
            f"duro de este canary es {MAX_CANARY}."
        )
    if len(document_keys) != len(set(document_keys)):
        raise ValueError("La allowlist tiene document_key repetidos.")


def obtener_filas_allowlist(agent: NormativePdfDetectorAgent,
                            document_keys: list[str]) -> dict[str, dict | None]:
    """SOLO LECTURA. Devuelve {document_key: fila|None}, uno por cada
    document_key de la allowlist -None si no existe-.

    No basta con un LIMIT: se pide `.in_("document_key", document_keys)` y
    LUEGO se descarta cualquier fila cuyo document_key no este exactamente en
    la allowlist (defensa en profundidad si la consulta trajera de mas).
    """
    respuesta = (
        agent.supabase
        .table(agent.table_name)
        .select("id, document_key, title, detail_url, file_url, has_file, "
                "file_name, file_ext, mime_type, process_status, "
                "process_message, raw, updated_at, source_type")
        .in_("document_key", document_keys)
        .execute()
    )
    por_clave = {f["document_key"]: f for f in (respuesta.data or [])
                if f["document_key"] in document_keys}
    return {k: por_clave.get(k) for k in document_keys}


def refrescar_file_url(agent: NormativePdfDetectorAgent, doc_id: str) -> str | None:
    """SOLO LECTURA. Relee el file_url actual de una fila puntual, para la
    segunda comprobacion de precondicion justo antes de proponer el payload,
    y para la relectura final -una TERCERA vez- inmediatamente antes de
    cualquier UPDATE en `--apply`."""
    respuesta = (
        agent.supabase
        .table(agent.table_name)
        .select("file_url")
        .eq("id", doc_id)
        .single()
        .execute()
    )
    return (respuesta.data or {}).get("file_url")


def leer_fila_actual(agent: NormativePdfDetectorAgent, doc_id: str) -> dict | None:
    """SOLO LECTURA. Relee la fila COMPLETA -no solo file_url-, para el READ
    BACK posterior a cada UPDATE de `--apply`."""
    respuesta = (
        agent.supabase
        .table(agent.table_name)
        .select("id, document_key, title, detail_url, file_url, has_file, "
                "file_name, file_ext, mime_type, process_status, "
                "process_message, raw, updated_at")
        .eq("id", doc_id)
        .single()
        .execute()
    )
    return respuesta.data or None


def construir_rollback(snapshot_before: dict) -> dict:
    """CANARY_F03B_ROLLBACK.json: los valores BEFORE exactos de los campos
    que la escritura puede tocar, para poder revertir SOLO esos campos con
    autorizacion humana. No se ejecuta ningun rollback aqui."""
    return {
        clave: ({campo: fila.get(campo) for campo in ROLLBACK_CAMPOS} if fila else None)
        for clave, fila in snapshot_before.items()
    }


def verificar_read_back(fila_leida: dict | None, payload_propuesto: dict,
                        document_key: str, doc_id: str) -> tuple[bool, list[str]]:
    """Compara lo que quedo REALMENTE en Supabase -leido de nuevo, no lo que
    se penso que se escribio- contra el payload propuesto."""
    if fila_leida is None:
        return False, [f"{document_key}: no se pudo releer la fila tras el UPDATE"]

    problemas = []
    if fila_leida.get("id") != doc_id:
        problemas.append(f"id: esperado {doc_id!r}, leido {fila_leida.get('id')!r}")
    if fila_leida.get("document_key") != document_key:
        problemas.append(
            f"document_key: esperado {document_key!r}, leido {fila_leida.get('document_key')!r}"
        )
    for campo in ("file_url", "file_name", "process_status"):
        if fila_leida.get(campo) != payload_propuesto.get(campo):
            problemas.append(
                f"{campo}: esperado {payload_propuesto.get(campo)!r}, "
                f"leido {fila_leida.get(campo)!r}"
            )
    if not (fila_leida.get("raw") or {}).get("pdf_detection"):
        problemas.append("raw.pdf_detection ausente tras el UPDATE")

    return (len(problemas) == 0), problemas


def snapshot_de(fila: dict) -> dict:
    campos = ("id", "document_key", "has_file", "file_url", "file_name",
             "file_ext", "mime_type", "process_status", "process_message",
             "raw", "updated_at")
    return {c: fila.get(c) for c in campos}


def analizar_candidato_unico(agent: NormativePdfDetectorAgent, fila: dict,
                             identidad_objetivo) -> dict:
    """Reutiliza el flujo REAL del detector -_enlaces_candidatos,
    _evidencia_de_candidato, decidir()- pero devuelve TODA la evidencia
    intermedia, no solo el resultado final, para poder auditar por que la
    decision fue MATCH_EXACTO."""
    detail_url = fila["detail_url"]

    if agent.is_pdf_response(detail_url):
        candidatos_urls = [(detail_url, "")]
    else:
        respuesta = agent.fetch_detail_response(detail_url)
        content_type = respuesta.headers.get("Content-Type", "").lower()
        candidatos_urls = (
            [(detail_url, "")] if "application/pdf" in content_type
            else agent._enlaces_candidatos(detail_url, respuesta.text)
        )

    from agents.agent_normative_pdf_detector import MAX_CANDIDATOS
    omitidos = max(0, len(candidatos_urls) - MAX_CANDIDATOS)
    candidatos_urls = candidatos_urls[:MAX_CANDIDATOS]

    evidencias = [
        agent._evidencia_de_candidato(url, texto, identidad_objetivo)
        for url, texto in candidatos_urls
    ]
    for _ in evidencias:
        time.sleep(0.5)

    from politica_documental import decidir
    decision = decidir(evidencias, identidad_objetivo, candidatos_omitidos=omitidos)

    return {"candidatos_urls": candidatos_urls, "evidencias": evidencias,
           "decision": decision}


def evaluar_fila_para_canary(agent: NormativePdfDetectorAgent, document_key: str,
                             fila: dict | None) -> dict:
    """Aplica TODAS las precondiciones. Devuelve una ficha con `apto: bool`,
    `motivo_aborto` si no lo es, y toda la evidencia pedida si lo es."""
    ficha = {
        "document_key": document_key,
        "apto": False,
        "motivo_aborto": None,
        "id": None,
        "title": None,
        "detail_url": None,
        "file_url_actual_inicial": None,
        "file_url_actual_refrescado": None,
        "identity_expected": None,
        "candidate_pdf_url": None,
        "pdf_sha256": None,
        "pdf_bytes": None,
        "page_count": None,
        "identities_detected": [],
        "primary_identity": None,
        "classification": None,
        "tipo_documento": None,
        "audit_complete": None,
        "por_que_match_exacto": None,
        "payload_propuesto": None,
    }

    if fila is None:
        ficha["motivo_aborto"] = ABORT_NO_EXISTE
        return ficha

    ficha.update(id=fila["id"], title=fila.get("title"), detail_url=fila.get("detail_url"),
                file_url_actual_inicial=fila.get("file_url"))

    if fila.get("file_url") is not None:
        ficha["motivo_aborto"] = ABORT_FILE_URL_NO_NULL
        return ficha

    identidad_objetivo = identidad_objetivo_de_documento(fila.get("title"), document_key)
    if identidad_objetivo is None:
        ficha["motivo_aborto"] = ABORT_SIN_IDENTIDAD
        return ficha
    ficha["identity_expected"] = str(identidad_objetivo)

    resultado = analizar_candidato_unico(agent, fila, identidad_objetivo)
    decision = resultado["decision"]

    if decision.estado != MATCH_EXACTO:
        ficha["motivo_aborto"] = ABORT_NO_MATCH_EXACTO
        ficha["classification"] = decision.estado
        return ficha

    auditoria_completa = bool((decision.evidencia or {}).get("auditoria_completa"))
    if not auditoria_completa:
        ficha["motivo_aborto"] = ABORT_AUDITORIA_INCOMPLETA
        return ficha

    ev_ganadora = next(
        (ev for ev in resultado["evidencias"] if ev.url == decision.url), None
    )
    if ev_ganadora is None:  # pragma: no cover - no deberia ocurrir si decidir() es consistente
        ficha["motivo_aborto"] = "EVIDENCIA_GANADORA_NO_ENCONTRADA"
        return ficha

    tipo_doc = tipo_de_documento(ev_ganadora.apariciones, ev_ganadora.texto_completo)
    ficha["tipo_documento"] = tipo_doc
    if tipo_doc != DOCUMENTO_NORMA_UNICA:
        ficha["motivo_aborto"] = ABORT_TIPO_DOCUMENTO
        return ficha

    # --- Segunda comprobacion de precondicion: file_url fresco -------------
    file_url_fresco = refrescar_file_url(agent, fila["id"])
    ficha["file_url_actual_refrescado"] = file_url_fresco
    if file_url_fresco is not None:
        ficha["motivo_aborto"] = ABORT_PRECONDICION_CAMBIO
        return ficha

    identidades = sorted({str(a.identidad) for a in ev_ganadora.encabezados})
    ficha.update(
        candidate_pdf_url=ev_ganadora.url,
        pdf_sha256=ev_ganadora.pdf_sha256,
        page_count=ev_ganadora.total_paginas,
        identities_detected=identidades,
        primary_identity=identidades[0] if identidades else None,
        classification=decision.estado,
        audit_complete=auditoria_completa,
        por_que_match_exacto=(
            f"{len(resultado['candidatos_urls'])} candidato(s) evaluado(s); "
            f"exactamente 1 ({ev_ganadora.url}) contiene, por CONTENIDO, "
            f"unicamente el encabezado de {ficha['identity_expected']} "
            f"({ev_ganadora.paginas_analizadas}/{ev_ganadora.total_paginas} "
            "paginas leidas, auditoria completa)"
        ),
    )

    resultado_deteccion = {
        "status": "pdf_detected",
        "pdf_url": decision.url,
        "mime_type": "application/pdf",
        "message": decision.motivo,
        "candidatos": (decision.evidencia or {}).get("candidatos", []),
    }
    ficha["payload_propuesto"] = construir_payload_actualizacion(fila, resultado_deteccion)
    ficha["apto"] = True
    return ficha


def ejecutar_dry_run(agent: NormativePdfDetectorAgent,
                     document_keys: list[str]) -> list[dict]:
    filas = obtener_filas_allowlist(agent, document_keys)
    return [evaluar_fila_para_canary(agent, k, filas[k]) for k in document_keys]


def comparar_corridas(a: list[dict], b: list[dict]) -> tuple[bool, list[str]]:
    """Compara A vs B en los campos deterministas exigidos. Determinismo
    esperado incluso cuando ambas filas fueron abortadas por el MISMO motivo."""
    campos = ("document_key", "apto", "motivo_aborto", "identity_expected",
             "candidate_pdf_url", "pdf_sha256", "classification", "page_count")
    diferencias = []
    if len(a) != len(b):
        return False, [f"cantidad de filas distinta: A={len(a)} B={len(b)}"]
    for fa, fb in zip(a, b):
        for campo in campos:
            if fa.get(campo) != fb.get(campo):
                diferencias.append(
                    f"{fa.get('document_key')}.{campo}: A={fa.get(campo)!r} != B={fb.get(campo)!r}"
                )
    return (len(diferencias) == 0), diferencias


# --- Veredictos de --apply --------------------------------------------------
APPLY_BLOQUEADO = "CANARY_APPLY_BLOQUEADO"
APPLY_FALLO_ESCRITURA = "CANARY_FALLO_ESCRITURA"
APPLY_FALLO_POST_WRITE = "CANARY_FALLO_POST_WRITE"
APPLY_APLICADO = "CANARY_APLICADO"


def ejecutar_apply(agent: NormativePdfDetectorAgent, document_keys: list[str],
                   corrida_b: list[dict], deterministico: bool = True,
                   diferencias: list[str] | None = None) -> dict:
    """SOLO se invoca si se paso `--apply`. Repite el preflight -relectura
    de CADA fila, inmediatamente antes de su UPDATE- en vez de confiar en el
    resultado del dry-run, por viejo que sea de unos segundos: el tiempo
    pasado entre el dry-run y este momento es exactamente lo que las
    precondiciones estan hechas para detectar.

    `deterministico`/`diferencias` vienen de `comparar_corridas(A, B)`: si el
    SHA256, la identidad o la URL propuesta cambiaron entre las dos corridas
    del dry-run -documento mutable, condicion de carrera, comportamiento no
    determinista del servidor de origen-, NINGUNA fila se escribe, sin
    importar que tan "aptas" hayan parecido individualmente.

    ALL OR NOTHING: si UNA fila no supera este preflight final, NINGUNA de
    las filas se escribe. Cada escritura se verifica con un READ BACK
    inmediato; si no coincide, se detiene sin procesar las filas restantes.
    """
    resultado = {"veredicto": None, "modo": f03b_write_mode(), "motivo": "",
                "escrituras": []}

    modo = resultado["modo"]
    if modo != WRITE_MODE_CANARY:
        resultado["veredicto"] = APPLY_BLOQUEADO
        resultado["motivo"] = (
            f"F03B_PDF_WRITE_MODE={modo!r}: --apply exige exactamente "
            f"{WRITE_MODE_CANARY!r}. NO WRITE."
        )
        return resultado

    if not deterministico:
        resultado["veredicto"] = APPLY_BLOQUEADO
        resultado["motivo"] = (
            "el dry-run A vs B no fue deterministico -SHA256, identidad o URL "
            "propuesta cambiaron entre corridas-: " + "; ".join(diferencias or [])
        )
        return resultado

    if not (1 <= len(document_keys) <= MAX_CANARY):
        resultado["veredicto"] = APPLY_BLOQUEADO
        resultado["motivo"] = (
            f"allowlist fuera de rango: {len(document_keys)} document_key "
            f"(se requiere 1-{MAX_CANARY}). NO WRITE."
        )
        return resultado

    # --- Preflight FINAL: relectura de CADA fila, aqui y ahora -------------
    preflight_final = []
    for ficha in corrida_b:
        problema = None
        if not ficha["apto"]:
            problema = f"no apto en el dry-run: {ficha['motivo_aborto']}"
        else:
            file_url_final = refrescar_file_url(agent, ficha["id"])
            if file_url_final is not None:
                problema = "file_url dejo de ser NULL justo antes del UPDATE"
        preflight_final.append((ficha, problema))

    problemas = [(f["document_key"], p) for f, p in preflight_final if p]
    if problemas:
        resultado["veredicto"] = APPLY_BLOQUEADO
        resultado["motivo"] = (
            "ALL OR NOTHING: al menos una fila no supero el preflight final, "
            f"ninguna de las {len(document_keys)} se escribe. "
            + "; ".join(f"{k}: {p}" for k, p in problemas)
        )
        return resultado

    # --- Todas superaron el preflight final: se escribe, con read-back -----
    for ficha, _ in preflight_final:
        payload = ficha["payload_propuesto"]
        resultado_deteccion = {
            "status": "pdf_detected",
            "pdf_url": payload["file_url"],
            "mime_type": payload.get("mime_type", "application/pdf"),
            "message": ficha["por_que_match_exacto"],
            "candidatos": payload["raw"]["pdf_detection"].get("candidatos", []),
        }
        fila_para_update = {
            "id": ficha["id"], "document_key": ficha["document_key"],
            "detail_url": ficha["detail_url"], "raw": {},
        }

        try:
            agent.update_document(fila_para_update, resultado_deteccion)
        except Exception as error:
            resultado["escrituras"].append({
                "document_key": ficha["document_key"], "escrito": False,
                "error": str(error),
            })
            resultado["veredicto"] = APPLY_FALLO_ESCRITURA
            resultado["motivo"] = f"fallo el UPDATE de {ficha['document_key']}: {error}"
            break

        fila_leida = leer_fila_actual(agent, ficha["id"])
        ok, problemas_rb = verificar_read_back(
            fila_leida, payload, ficha["document_key"], ficha["id"]
        )
        resultado["escrituras"].append({
            "document_key": ficha["document_key"], "escrito": True,
            "read_back_ok": ok, "problemas_read_back": problemas_rb,
            "fila_leida": fila_leida,
        })
        if not ok:
            resultado["veredicto"] = APPLY_FALLO_POST_WRITE
            resultado["motivo"] = (
                f"read-back de {ficha['document_key']} no coincide con lo "
                "escrito. Detenido: no se procesan mas filas."
            )
            break

    if resultado["veredicto"] is None:
        resultado["veredicto"] = APPLY_APLICADO
        resultado["motivo"] = (
            f"{len(resultado['escrituras'])} fila(s) escrita(s) y verificadas por read-back."
        )

    return resultado


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--document-keys", required=True,
                        help=f"Allowlist EXACTA, separada por comas. Maximo {MAX_CANARY}.")
    parser.add_argument("--out-dir", default="reportes")
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Ejecuta la escritura real, TRAS el preflight final y el read-back. "
            f"Exige F03B_PDF_WRITE_MODE={WRITE_MODE_CANARY!r} exportado en el "
            "entorno. Sin esta variable en ese valor exacto: NO WRITE."
        ),
    )
    args = parser.parse_args()

    document_keys = [k.strip() for k in args.document_keys.split(",") if k.strip()]
    validar_allowlist(document_keys)

    agent = NormativePdfDetectorAgent()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # --- Snapshot BEFORE (solo lectura) ------------------------------------
    filas_iniciales = obtener_filas_allowlist(agent, document_keys)
    snapshot_before = {
        k: (snapshot_de(f) if f else None) for k, f in filas_iniciales.items()
    }
    (out / "CANARY_F03B_BEFORE.json").write_text(
        json.dumps(snapshot_before, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("Snapshot BEFORE guardado: %s", out / "CANARY_F03B_BEFORE.json")

    # --- Dry-run doble ------------------------------------------------------
    logger.info("=== DRY-RUN A ===")
    corrida_a = ejecutar_dry_run(agent, document_keys)
    (out / "CANARY_F03B_DRYRUN_A.json").write_text(
        json.dumps(corrida_a, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )

    logger.info("=== DRY-RUN B ===")
    corrida_b = ejecutar_dry_run(agent, document_keys)
    (out / "CANARY_F03B_DRYRUN_B.json").write_text(
        json.dumps(corrida_b, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )

    deterministico, diferencias = comparar_corridas(corrida_a, corrida_b)

    print("\n" + "=" * 74)
    print("CANARY F-03B — SOLO DRY-RUN, CERO ESCRITURAS")
    print("=" * 74)
    for ficha in corrida_a:
        print(f"\n--- {ficha['document_key']} ---")
        print(json.dumps(ficha, ensure_ascii=False, indent=2, default=str))

    print("\n" + "-" * 74)
    print(f"Determinismo A vs B: {'OK' if deterministico else 'FALLA'}")
    for d in diferencias:
        print(f"  DIFERENCIA: {d}")

    todas_aptas = all(f["apto"] for f in corrida_a)
    veredicto = (
        "CANARY_LISTO_PARA_ESCRITURA"
        if (deterministico and todas_aptas and 1 <= len(corrida_a) <= MAX_CANARY)
        else "CANARY_NO_APTO"
    )
    print(f"\nVEREDICTO DRY-RUN: {veredicto}")
    print(f"\nBEFORE:  {out / 'CANARY_F03B_BEFORE.json'}")
    print(f"DRY-RUN A: {out / 'CANARY_F03B_DRYRUN_A.json'}")
    print(f"DRY-RUN B: {out / 'CANARY_F03B_DRYRUN_B.json'}")

    (out / "CANARY_F03B_VEREDICTO.json").write_text(
        json.dumps({
            "veredicto": veredicto,
            "deterministico": deterministico,
            "diferencias": diferencias,
            "document_keys": document_keys,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if not args.apply:
        print("\nNINGUNA fila fue escrita. NINGUN UPDATE/INSERT/UPSERT/DELETE se ejecuto.")
        print("(Pase --apply, con F03B_PDF_WRITE_MODE=canary, para intentar la escritura real.)")
        return 0

    # --- --apply: preflight final + escritura + read-back -------------------
    logger.warning("=== --apply solicitado: preflight final, escritura y read-back ===")
    resultado_apply = ejecutar_apply(
        agent, document_keys, corrida_b,
        deterministico=deterministico, diferencias=diferencias,
    )

    snapshot_after = {}
    for registro in resultado_apply["escrituras"]:
        clave = registro["document_key"]
        snapshot_after[clave] = (
            snapshot_de(registro["fila_leida"]) if registro.get("fila_leida") else None
        )
    for clave in document_keys:
        # Lo que no llego a escribirse -bloqueo ALL OR NOTHING o corte por
        # fallo- queda identico al BEFORE: no se le toco nada.
        snapshot_after.setdefault(clave, snapshot_before.get(clave))

    (out / "CANARY_F03B_AFTER.json").write_text(
        json.dumps(snapshot_after, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    diff = {
        clave: {"before": snapshot_before.get(clave), "after": snapshot_after.get(clave)}
        for clave in document_keys
    }
    (out / "CANARY_F03B_DIFF.json").write_text(
        json.dumps(diff, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    rollback = construir_rollback(snapshot_before)
    (out / "CANARY_F03B_ROLLBACK.json").write_text(
        json.dumps(rollback, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )

    print("\n" + "=" * 74)
    print(f"CANARY APPLY — VEREDICTO: {resultado_apply['veredicto']}")
    print("=" * 74)
    print(resultado_apply["motivo"])
    print(f"\nAFTER:    {out / 'CANARY_F03B_AFTER.json'}")
    print(f"DIFF:     {out / 'CANARY_F03B_DIFF.json'}")
    print(f"ROLLBACK: {out / 'CANARY_F03B_ROLLBACK.json'} (preparado, NO ejecutado)")

    (out / "CANARY_F03B_APPLY_VEREDICTO.json").write_text(
        json.dumps(resultado_apply, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )

    return 0 if resultado_apply["veredicto"] == APPLY_APLICADO else 1


if __name__ == "__main__":
    raise SystemExit(main())
