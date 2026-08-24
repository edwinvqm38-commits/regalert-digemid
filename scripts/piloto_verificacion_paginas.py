"""Piloto de verificacion documental sobre paginas de alto riesgo (F-02 · 17).

NO ESCRIBE NADA. Descarga los PDF, los compara consigo mismos con motores
independientes y emite un informe. Ninguna fila de Supabase se modifica.

Por que tres motores y no dos:

    PyMuPDF y pdfplumber leen LA MISMA capa de texto embebida. Si esa capa
    esta mal -fuente rota, texto invisible, PDF generado desde un escaneo con
    OCR malo-, los dos coinciden en el mismo error y "coincidencia" no
    significa nada. Por eso, para paginas dispositivas, se agrega un motor que
    NO lee la capa embebida sino el RENDER VISUAL: Tesseract sobre la imagen.

Uso:
    python scripts/piloto_verificacion_paginas.py --limite 50 --out-dir reportes/
    python scripts/piloto_verificacion_paginas.py --document-key RM-894-2024
"""

import argparse
import io
import json
import logging
import os
import statistics
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))

from custodia_documental import (  # noqa: E402
    clasificar_procedencia,
    es_pagina_alto_riesgo,
    evaluar_completitud,
    sha256_de,
)
from fidelidad_legal import (  # noqa: E402
    comparar_fidelidad,
    es_pagina_dispositiva,
    tokens_sensibles,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DPI_RENDER = 300          # base provisional para alto riesgo (F-02 · 8)
DPI_LETRA_PEQUENA = 400   # solo si la pagina lo justifica
BUCKET = "digemid-documentos"


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def seleccionar_paginas(supabase, limite: int, document_key: str | None) -> list[dict]:
    """Muestra ESTRATIFICADA de paginas de alto riesgo: digitales y OCR, con y
    sin tablas, confianza alta y baja, documentos largos y ultimos folios."""
    consulta = (
        supabase.table("digemid_norma_paginas")
        .select("id, norma_id, page_number, text_normalized, text_raw, extraction_method, "
                "ocr_used, ocr_confidence, quality_score, has_tables, posible_formula, revisado_manual")
    )
    if document_key:
        norma = (
            supabase.table("digemid_normas").select("id").eq("document_key", document_key)
            .limit(1).execute().data
        )
        if not norma:
            raise SystemExit(f"No existe la norma {document_key}")
        consulta = consulta.eq("norma_id", norma[0]["id"])

    paginas, desde = [], 0
    while True:
        lote = consulta.range(desde, desde + 999).execute().data or []
        paginas.extend(lote)
        if len(lote) < 1000 or document_key:
            break
        desde += 1000

    normas = {
        n["id"]: n for n in (
            supabase.table("digemid_normas")
            .select("id, document_key, pdf_url, file_storage_path")
            .execute().data or []
        )
    }
    total_por_norma: dict[str, int] = {}
    for p in paginas:
        total_por_norma[p["norma_id"]] = max(total_por_norma.get(p["norma_id"], 0),
                                             p.get("page_number") or 0)

    candidatas = []
    for p in paginas:
        norma = normas.get(p["norma_id"])
        if not norma or not norma.get("file_storage_path"):
            continue  # sin PDF no hay nada que verificar
        texto = p.get("text_normalized") or p.get("text_raw") or ""
        dispositiva = es_pagina_dispositiva(texto)
        alto_riesgo, motivo = es_pagina_alto_riesgo(
            p.get("page_number") or 0, total_por_norma.get(p["norma_id"], 0), dispositiva
        )
        if not alto_riesgo:
            continue
        p["_norma"] = norma
        p["_dispositiva"] = dispositiva
        p["_motivo_riesgo"] = motivo
        p["_total_paginas"] = total_por_norma.get(p["norma_id"], 0)
        candidatas.append(p)

    if document_key:
        return candidatas[:limite]

    # Estratos: que el piloto no quede hecho solo de un tipo de pagina.
    estratos = {
        "ocr_confianza_baja": lambda x: x["ocr_used"] and (x.get("ocr_confidence") or 0) < 0.85,
        "ocr_confianza_alta": lambda x: x["ocr_used"] and (x.get("ocr_confidence") or 0) >= 0.85,
        "digital": lambda x: not x["ocr_used"],
        "con_tablas": lambda x: x.get("has_tables"),
        "ultimo_folio": lambda x: x.get("page_number") == x["_total_paginas"],
        "documento_largo": lambda x: x["_total_paginas"] >= 20,
    }
    seleccion: list[dict] = []
    cupo = max(1, limite // len(estratos))
    vistos: set[str] = set()
    for nombre, pred in estratos.items():
        for p in candidatas:
            if len(seleccion) >= limite:
                break
            if p["id"] in vistos or not pred(p):
                continue
            p["_estrato"] = nombre
            seleccion.append(p)
            vistos.add(p["id"])
            if sum(1 for s in seleccion if s.get("_estrato") == nombre) >= cupo:
                break
    for p in candidatas:  # completar el cupo si algun estrato quedo corto
        if len(seleccion) >= limite:
            break
        if p["id"] not in vistos:
            p["_estrato"] = "relleno"
            seleccion.append(p)
            vistos.add(p["id"])
    return seleccion


def descargar_pdf(supabase, storage_path: str) -> bytes:
    return supabase.storage.from_(BUCKET).download(storage_path)


def texto_pymupdf(doc, indice: int) -> str:
    return (doc[indice].get_text("text") or "").strip()


def texto_pdfplumber(datos: bytes, indice: int) -> str:
    import pdfplumber

    with pdfplumber.open(io.BytesIO(datos)) as pdf:
        if indice >= len(pdf.pages):
            return ""
        return (pdf.pages[indice].extract_text() or "").strip()


def texto_render_ocr(doc, indice: int, dpi: int) -> tuple[str, float | None]:
    """Motor INDEPENDIENTE: no lee la capa embebida, lee la imagen."""
    import fitz
    import pytesseract
    from PIL import Image

    pix = doc[indice].get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72))
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    texto = pytesseract.image_to_string(img, lang="spa") or ""
    datos = pytesseract.image_to_data(img, lang="spa", output_type=pytesseract.Output.DICT)
    confs = []
    for valor in datos.get("conf", []):
        try:
            c = float(valor)
        except (TypeError, ValueError):
            continue
        if c >= 0:
            confs.append(c)
    return texto.strip(), (sum(confs) / len(confs) / 100.0 if confs else None)


def analizar(supabase, seleccion: list[dict]) -> list[dict]:
    import fitz

    cache: dict[str, tuple[bytes, str, int]] = {}
    resultados = []

    for pagina in seleccion:
        norma = pagina["_norma"]
        clave = norma["file_storage_path"]
        inicio = time.time()

        try:
            if clave not in cache:
                datos = descargar_pdf(supabase, clave)
                with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
                    cache[clave] = (datos, sha256_de(datos), doc.page_count)
            datos, sha, page_count = cache[clave]
        except Exception as error:
            resultados.append({
                "document_key": norma["document_key"], "page_number": pagina.get("page_number"),
                "estado": "PDF_NO_DESCARGABLE", "error": str(error)[:200],
            })
            continue

        indice = (pagina.get("page_number") or 1) - 1
        if indice < 0 or indice >= page_count:
            resultados.append({
                "document_key": norma["document_key"], "page_number": pagina.get("page_number"),
                "estado": "PAGINA_FUERA_DEL_PDF",
                "pdf_page_count": page_count, "sha256": sha,
            })
            continue

        with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
            t_mupdf = texto_pymupdf(doc, indice)
            t_ocr, conf_ocr = texto_render_ocr(doc, indice, DPI_RENDER)
        t_plumber = texto_pdfplumber(datos, indice)

        guardado = pagina.get("text_normalized") or pagina.get("text_raw") or ""

        # Comparaciones. Ninguna es "la verdad": son evidencias cruzadas.
        cmp_parsers = comparar_fidelidad(t_mupdf, t_plumber)
        cmp_visual = comparar_fidelidad(t_mupdf or t_ocr, t_ocr)
        cmp_guardado = comparar_fidelidad(t_mupdf or t_ocr, guardado)

        procedencia, motivo_proc = clasificar_procedencia(norma.get("pdf_url"))

        resultados.append({
            "document_key": norma["document_key"],
            "page_number": pagina.get("page_number"),
            "estrato": pagina.get("_estrato"),
            "motivo_riesgo": pagina.get("_motivo_riesgo"),
            "es_dispositiva": pagina["_dispositiva"],
            "sha256": sha,
            "byte_size": len(datos),
            "pdf_page_count": page_count,
            "stored_page_count": pagina["_total_paginas"],
            "completitud": evaluar_completitud(
                page_count, list(range(1, pagina["_total_paginas"] + 1))).estado,
            "procedencia": procedencia,
            "procedencia_motivo": motivo_proc,
            "extraction_method": pagina.get("extraction_method"),
            "quality_score": pagina.get("quality_score"),
            "ocr_confidence_guardada": pagina.get("ocr_confidence"),
            "ocr_confidence_render": conf_ocr,
            "dpi_render": DPI_RENDER,
            "chars": {"pymupdf": len(t_mupdf), "pdfplumber": len(t_plumber),
                      "render_ocr": len(t_ocr), "guardado": len(guardado)},
            "parsers": _resumen(cmp_parsers),
            "visual": _resumen(cmp_visual),
            "guardado_vs_pdf": _resumen(cmp_guardado),
            "tokens_detectados": sum(len(v) for v in tokens_sensibles(t_mupdf or t_ocr).values()),
            "segundos": round(time.time() - inicio, 2),
            "estado": _veredicto(pagina["_dispositiva"], cmp_parsers, cmp_visual, cmp_guardado),
        })
        logger.info("%s p.%s -> %s", norma["document_key"], pagina.get("page_number"),
                    resultados[-1]["estado"])

    return resultados


def _resumen(cmp_) -> dict:
    return {
        "cer": round(cmp_.cer, 4),
        "wer": round(cmp_.wer, 4),
        "legal_token_error_rate": round(cmp_.legal_token_error_rate, 4),
        "errores": [str(e) for e in (cmp_.errores_token + cmp_.verbos_cambiados)[:10]],
    }


def _veredicto(dispositiva, cmp_parsers, cmp_visual, cmp_guardado) -> str:
    """Una discrepancia en un token juridico NO se compensa con un CER bueno."""
    if cmp_guardado.hay_error_juridico:
        return "DISCREPANCIA_CRITICA_TEXTO_GUARDADO_VS_PDF"
    if dispositiva and cmp_visual.hay_error_juridico:
        return "DISCREPANCIA_CRITICA_CAPA_EMBEBIDA_VS_RENDER"
    if cmp_parsers.hay_error_juridico:
        return "DISCREPANCIA_CRITICA_ENTRE_PARSERS"
    if dispositiva and cmp_visual.cer > 0.15:
        return "REQUIERE_REVISION_HUMANA"
    return "CONCORDANTE"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limite", type=int, default=50)
    parser.add_argument("--document-key", default=None)
    parser.add_argument("--out-dir", default="reportes")
    args = parser.parse_args()

    supabase = get_supabase()
    seleccion = seleccionar_paginas(supabase, args.limite, args.document_key)
    logger.info("Paginas de alto riesgo seleccionadas: %d", len(seleccion))

    inicio = time.time()
    resultados = analizar(supabase, seleccion)
    duracion = time.time() - inicio

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    criticas = [r for r in resultados if str(r.get("estado", "")).startswith("DISCREPANCIA_CRITICA")]
    concordantes = [r for r in resultados if r.get("estado") == "CONCORDANTE"]
    cers = [r["guardado_vs_pdf"]["cer"] for r in resultados if "guardado_vs_pdf" in r]

    resumen = {
        "paginas_analizadas": len(resultados),
        "concordantes": len(concordantes),
        "discrepancias_criticas": len(criticas),
        "requieren_humano": sum(1 for r in resultados if r.get("estado") == "REQUIERE_REVISION_HUMANA"),
        "pdf_no_descargable": sum(1 for r in resultados if r.get("estado") == "PDF_NO_DESCARGABLE"),
        "pagina_fuera_del_pdf": sum(1 for r in resultados if r.get("estado") == "PAGINA_FUERA_DEL_PDF"),
        "cer_mediano_guardado_vs_pdf": round(statistics.median(cers), 4) if cers else None,
        "documentos_incompletos": sorted({
            r["document_key"] for r in resultados if r.get("completitud") == "INCOMPLETO"}),
        "segundos_total": round(duracion, 1),
        "segundos_por_pagina": round(duracion / max(1, len(resultados)), 2),
        "dpi_render": DPI_RENDER,
        "motor_visual": "tesseract-ocr spa (local, sin costo de API)",
        "costo_api_usd": 0.0,
        "nota_costo": "este piloto no llama a ningun modelo de pago: el tercer motor es Tesseract local",
    }

    (out / "PILOTO_VERIFICACION.json").write_text(
        json.dumps({"resumen": resumen, "paginas": resultados}, ensure_ascii=False, indent=2),
        encoding="utf-8")

    print("\n" + "=" * 72)
    print("PILOTO DE VERIFICACION — SOLO LECTURA, NADA MODIFICADO")
    print("=" * 72)
    for k, v in resumen.items():
        print(f"  {k:34} {v}")
    if criticas:
        print("\nDISCREPANCIAS CRITICAS:")
        for r in criticas:
            print(f"\n  {r['document_key']} p.{r['page_number']}  [{r['estado']}]")
            for bloque in ("guardado_vs_pdf", "visual", "parsers"):
                for e in r.get(bloque, {}).get("errores", [])[:4]:
                    print(f"      {bloque}: {e}")
    print(f"\nInforme: {out / 'PILOTO_VERIFICACION.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
