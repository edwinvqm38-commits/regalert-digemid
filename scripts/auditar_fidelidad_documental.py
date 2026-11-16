"""Auditoría de FIDELIDAD del corpus normativo (F-01). SOLO LECTURA.

Responde una pregunta distinta de la que responde quality_score: no "¿el texto
se ve bien?" sino "¿este texto representa fielmente el PDF oficial?".

Sin una segunda fuente contra la cual comparar, la respuesta honesta es
DESCONOCIDA, y este script la reporta como tal en vez de inventar una
confianza. Nunca escribe en Supabase.

Uso:
    python scripts/auditar_fidelidad_documental.py --out-dir docs/reportes
    python scripts/auditar_fidelidad_documental.py --verificar-pdf   # descarga y compara nº de páginas
    python scripts/auditar_fidelidad_documental.py --desde-json dir/ # offline
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))

from fidelidad_legal import (  # noqa: E402
    DOCUMENTO_INCOMPLETO,
    PDF_NO_DISPONIBLE,
    SenalesPagina,
    es_pagina_dispositiva,
    evaluar_pagina,
    marcas_ilegible,
    puede_alimentar_detector,
    puede_citarse_como_fuente_legal,
    tokens_sensibles,
    verbos_normativos,
)
from secuencia_articulos import analizar_secuencia  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def cargar_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    cliente = create_client(url, key)

    normas = (
        cliente.table("digemid_normas")
        .select("id, document_key, pdf_url, file_storage_path, process_status, tipo_norma, numero, anio")
        .execute().data or []
    )
    paginas, desde = [], 0
    while True:  # paginado: el corpus supera el limite por defecto de PostgREST
        lote = (
            cliente.table("digemid_norma_paginas")
            .select("norma_id, page_number, text_normalized, text_raw, extraction_method, "
                    "ocr_used, ocr_confidence, quality_score, has_tables, posible_formula, "
                    "posible_grafico, revisado_manual, metadata")
            .range(desde, desde + 999).execute().data or []
        )
        paginas.extend(lote)
        if len(lote) < 1000:
            break
        desde += 1000
    return normas, paginas


def cargar_json(directorio: str):
    base = Path(directorio)
    return (
        json.loads((base / "normas.json").read_text(encoding="utf-8")),
        json.loads((base / "paginas.json").read_text(encoding="utf-8")),
    )


def paginas_reales_del_pdf(pdf_url: str | None) -> int | None:
    """Nº de páginas del PDF OFICIAL. Es el único dato que permite afirmar que
    el documento está completo; sin él, la completitud es DESCONOCIDA."""
    if not pdf_url:
        return None
    try:
        import io

        import fitz
        import requests

        respuesta = requests.get(pdf_url, timeout=60)
        respuesta.raise_for_status()
        with fitz.open(stream=io.BytesIO(respuesta.content), filetype="pdf") as doc:
            return doc.page_count
    except Exception as error:  # red bloqueada, PDF caído, PDF corrupto
        logger.warning("No se pudo leer el PDF %s: %s", pdf_url, error)
        return None


def auditar(normas: list[dict], paginas: list[dict], verificar_pdf: bool) -> tuple[list[dict], dict]:
    por_norma: dict[str, list[dict]] = {}
    for p in paginas:
        por_norma.setdefault(p["norma_id"], []).append(p)

    filas: list[dict] = []
    secuencia = {"evaluables": 0, "con_senales": 0, "normas": []}
    for norma in normas:
        pags = sorted(por_norma.get(norma["id"], []), key=lambda x: x.get("page_number") or 0)
        if not pags:
            continue

        # Secuencia de articulos: senal para revision humana, no veredicto.
        seq = analizar_secuencia([p.get("text_normalized") or p.get("text_raw") or "" for p in pags])
        if seq.evaluable:
            secuencia["evaluables"] += 1
            if seq.requiere_revision:
                secuencia["con_senales"] += 1
                secuencia["normas"].append({
                    "document_key": norma["document_key"],
                    "senales": [f"pág. {sn.pagina}: {sn.tipo} - {sn.detalle}" for sn in seq.senales],
                })

        almacenadas = len(pags)
        reales = paginas_reales_del_pdf(norma.get("pdf_url")) if verificar_pdf else None
        # DESCONOCIDO no es lo mismo que COMPLETO: solo se afirma completitud
        # cuando se pudo contar el PDF oficial.
        completo = None if reales is None else (reales == almacenadas)

        numeros = [p.get("page_number") for p in pags]
        faltantes = sorted(set(range(1, max(numeros) + 1)) - set(numeros)) if numeros else []
        duplicadas = sorted({n for n in numeros if numeros.count(n) > 1})

        for pagina in pags:
            texto = pagina.get("text_normalized") or pagina.get("text_raw") or ""
            senales = SenalesPagina(
                extraction_method=pagina.get("extraction_method"),
                quality_score=pagina.get("quality_score"),
                ocr_used=bool(pagina.get("ocr_used")),
                ocr_confidence=pagina.get("ocr_confidence"),
                texto=texto,
                has_tables=bool(pagina.get("has_tables")),
                posible_formula=bool(pagina.get("posible_formula")),
                posible_grafico=bool(pagina.get("posible_grafico")),
                revisado_manual=bool(pagina.get("revisado_manual")),
                documento_completo=completo,
                pdf_disponible=bool(norma.get("pdf_url") or norma.get("file_storage_path")),
            )
            estado, riesgo, motivos = evaluar_pagina(senales)
            dispositiva = es_pagina_dispositiva(texto)
            tokens = tokens_sensibles(texto)

            filas.append({
                "norma_id": norma["id"],
                "document_key": norma["document_key"],
                "page_number": pagina.get("page_number"),
                "source_url": norma.get("pdf_url"),
                "storage_path": norma.get("file_storage_path"),
                "pdf_sha256": None,          # NO EXISTE en el esquema actual
                "pdf_page_count": reales,    # None = no verificado
                "stored_page_count": almacenadas,
                "paginas_faltantes": ",".join(map(str, faltantes)),
                "paginas_duplicadas": ",".join(map(str, duplicadas)),
                "extraction_method": pagina.get("extraction_method"),
                "ocr_used": bool(pagina.get("ocr_used")),
                "ocr_confidence": pagina.get("ocr_confidence"),
                "quality_score": pagina.get("quality_score"),
                "has_tables": bool(pagina.get("has_tables")),
                "posible_formula": bool(pagina.get("posible_formula")),
                "posible_grafico": bool(pagina.get("posible_grafico")),
                "revisado_manual": bool(pagina.get("revisado_manual")),
                "es_dispositiva": dispositiva,
                "verbos_normativos": ",".join(sorted(verbos_normativos(texto))),
                "sensitive_tokens": sum(len(v) for v in tokens.values()),
                "marcas_ilegible": marcas_ilegible(texto),
                "chars": len(texto),
                "verification_status": estado,
                "risk_level": riesgo,
                "apta_para_consulta": puede_citarse_como_fuente_legal(estado),
                "apta_para_detector": puede_alimentar_detector(estado, dispositiva),
                "review_flags": "; ".join(motivos),
                "recommended_action": recomendar(estado, dispositiva),
            })

    resumen = resumir(filas)
    resumen["secuencia_articulos"] = secuencia
    return filas, resumen


def recomendar(estado: str, dispositiva: bool) -> str:
    if estado == DOCUMENTO_INCOMPLETO:
        return "recuperar el PDF completo y reextraer antes de usar la norma"
    if estado == PDF_NO_DISPONIBLE:
        return "recuperar el PDF oficial: sin el no hay forma de verificar nada"
    if dispositiva and estado not in ("VERIFICADA_HUMANO", "VERIFICADA_AUTOMATICAMENTE"):
        return "REVISION HUMANA PRIORITARIA: pagina dispositiva sin fidelidad verificada"
    if estado == "DISCREPANCIA_ENTRE_MOTORES":
        return "resolver la discrepancia contra el PDF oficial"
    if estado == "OCR_PENDIENTE_VERIFICACION":
        return "verificar con un segundo motor independiente"
    if estado == "NO_EVALUADA":
        return "confirmar contra el PDF si la pagina esta realmente en blanco"
    return "sin accion inmediata"


def resumir(filas: list[dict]) -> dict:
    def cuenta(pred):
        return sum(1 for f in filas if pred(f))

    disp = [f for f in filas if f["es_dispositiva"]]
    estados: dict[str, int] = {}
    for f in filas:
        estados[f["verification_status"]] = estados.get(f["verification_status"], 0) + 1

    return {
        "paginas": len(filas),
        "normas": len({f["document_key"] for f in filas}),
        "por_estado": estados,
        "por_riesgo": {
            r: cuenta(lambda f, r=r: f["risk_level"] == r) for r in ("CRITICO", "ALTO", "MEDIO", "BAJO")
        },
        "dispositivas": len(disp),
        "dispositivas_no_verificadas": sum(1 for f in disp if not f["apta_para_detector"]),
        "aptas_para_consulta": cuenta(lambda f: f["apta_para_consulta"]),
        "no_aptas_para_consulta": cuenta(lambda f: not f["apta_para_consulta"]),
        "no_aptas_para_detector": cuenta(lambda f: not f["apta_para_detector"]),
        "ocr": cuenta(lambda f: f["ocr_used"]),
        "con_tablas": cuenta(lambda f: f["has_tables"]),
        "con_formula": cuenta(lambda f: f["posible_formula"]),
        "revisadas_por_humano": cuenta(lambda f: f["revisado_manual"]),
        "completitud_desconocida": cuenta(lambda f: f["pdf_page_count"] is None),
    }


def texto_resumen_telegram(resumen: dict, max_normas: int = 12) -> str:
    sec = resumen.get("secuencia_articulos", {})
    lineas = [
        "📋 Auditoría de fidelidad (solo lectura)",
        f"Páginas: {resumen['paginas']} | Normas: {resumen['normas']}",
        f"Aptas para consulta: {resumen['aptas_para_consulta']} | No aptas: {resumen['no_aptas_para_consulta']}",
        f"Secuencia de artículos: {sec.get('con_senales', 0)} de {sec.get('evaluables', 0)} normas evaluables con señales",
    ]
    for item in sec.get("normas", [])[:max_normas]:
        lineas.append(f"• {item['document_key']}: {item['senales'][0]}")
    if len(sec.get("normas", [])) > max_normas:
        lineas.append(f"… y {len(sec['normas']) - max_normas} más (ver el reporte completo)")
    lineas.append("Orientativo: cada señal se confirma contra el PDF oficial.")
    return "\n".join(lineas)[:3900]


def enviar_resumen_telegram(resumen: dict) -> bool:
    import requests

    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        logger.warning("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID: no se envia el resumen")
        return False
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": texto_resumen_telegram(resumen)},
        timeout=20,
    )
    if not r.ok:
        logger.warning("Telegram respondio %s", r.status_code)
    return r.ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="docs/reportes")
    parser.add_argument("--desde-json", default=None)
    parser.add_argument("--avisar-telegram", action="store_true",
                        help="Envia un resumen al chat admin (TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID)")
    parser.add_argument("--verificar-pdf", action="store_true",
                        help="Descarga cada PDF oficial para contar sus paginas reales")
    args = parser.parse_args()

    normas, paginas = cargar_json(args.desde_json) if args.desde_json else cargar_supabase()
    logger.info("Normas: %d | Paginas: %d", len(normas), len(paginas))

    filas, resumen = auditar(normas, paginas, args.verificar_pdf)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if filas:
        with (out / "MATRIZ_FIDELIDAD_NORMATIVA.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
            w.writeheader()
            w.writerows(filas)
    (out / "MATRIZ_FIDELIDAD_NORMATIVA.json").write_text(
        json.dumps({"resumen": resumen, "paginas": filas}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\n" + "=" * 70)
    print("AUDITORIA DE FIDELIDAD — SOLO LECTURA, NADA MODIFICADO")
    print("=" * 70)
    for clave, valor in resumen.items():
        if clave == "secuencia_articulos":
            print(f"\nsecuencia de artículos: {valor['con_senales']} de {valor['evaluables']} normas evaluables con señales")
        elif isinstance(valor, dict):
            print(f"\n{clave}:")
            for k, v in sorted(valor.items(), key=lambda kv: -kv[1]):
                print(f"    {v:5d}  {k}")
        else:
            print(f"{clave:32} {valor}")
    if args.avisar_telegram:
        enviar_resumen_telegram(resumen)
    if not args.verificar_pdf:
        print("\nNOTA: sin --verificar-pdf la COMPLETITUD de cada documento queda DESCONOCIDA.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
