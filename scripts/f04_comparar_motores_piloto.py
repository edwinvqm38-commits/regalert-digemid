"""F-04-A: harness de comparación de motores sobre la muestra del manifest.

SOLO LECTURA. Dado `F04_MANIFEST_PILOTO.csv` (ya filtrado por el gate F-03 +
completitud + SHA256 de `scripts/f04_generar_manifest_piloto.py`), descarga
cada PDF una vez, extrae cada página de la muestra con los mismos 3 motores
independientes que `scripts/piloto_verificacion_paginas.py` -PyMuPDF y
pdfplumber (capa de texto embebida, parsers distintos) y Tesseract a 300 DPI
sobre el render (motor visual independiente)- y compara con
`agents/fidelidad_legal.comparar_fidelidad` (CER, WER y
legal_token_error_rate) contra el texto YA GUARDADO en la base.

NO usa ningún modelo externo de pago: nada de `openrouter/auto`, nada de
Vision cloud. El tercer motor es Tesseract local, igual que en el piloto
F-02, con costo $0. Si una fase posterior quisiera sumar un modelo pagado,
ese cálculo de qué se enviaría -y la decisión de detenerse antes de llamarlo-
es explícitamente responsabilidad de esa fase, no de este script.

No escribe nada en Supabase, no reemplaza ningún `text_raw`/`text_normalized`
existente. Una página que no logra completar los 3 motores (descarga
fallida, motor con excepción) nunca se marca verificada: queda en
`COMPARACION_INCOMPLETA` (`agents/f04_seleccion_muestra.estado_verificacion_f04`).

El texto de los 3 motores queda en la salida JSON -no solo el veredicto- para
poder alimentar después el golden dataset humano
(`scripts/generar_revision_visual.py --desde-manifest`).

Uso:
    python scripts/f04_generar_manifest_piloto.py --out-dir reportes
    python scripts/f04_comparar_motores_piloto.py --manifest reportes/F04_MANIFEST_PILOTO.csv --out-dir reportes
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from f04_seleccion_muestra import estado_verificacion_f04  # noqa: E402
from fidelidad_legal import SenalesPagina, comparar_fidelidad  # noqa: E402
from piloto_verificacion_paginas import (  # noqa: E402
    BUCKET,
    DPI_RENDER,
    descargar_pdf,
    texto_pdfplumber,
    texto_pymupdf,
    texto_render_ocr,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

OUT_BASENAME = "F04_COMPARACION_MOTORES"


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def cargar_manifest(ruta: str) -> list[dict]:
    with open(ruta, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _resumen_cmp(cmp_) -> dict:
    return {
        "cer": round(cmp_.cer, 4),
        "wer": round(cmp_.wer, 4),
        "legal_token_error_rate": round(cmp_.legal_token_error_rate, 4),
        "hay_error_juridico": cmp_.hay_error_juridico,
        "errores": [str(e) for e in (cmp_.errores_token + cmp_.verbos_cambiados)[:10]],
    }


def comparar_pagina(supabase, fila: dict, cache: dict) -> dict:
    """Descarga (con caché por PDF) y compara los 3 motores para UNA fila
    del manifest. Nunca escribe nada; devuelve el resultado completo,
    incluido el texto de cada motor, para alimentar el golden dataset."""
    storage_path = fila["storage_path"]
    page_number = int(fila["page_number"])
    resultado: dict = {"document_key": fila["document_key"], "page_number": page_number}

    try:
        if storage_path not in cache:
            import fitz

            datos = descargar_pdf(supabase, storage_path)
            with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
                cache[storage_path] = (datos, doc.page_count)
        datos, page_count = cache[storage_path]
    except Exception as error:
        resultado.update(estado="PDF_NO_DESCARGABLE", error=str(error)[:200])
        return resultado

    indice = page_number - 1
    if indice < 0 or indice >= page_count:
        resultado.update(estado="PAGINA_FUERA_DEL_PDF", pdf_page_count=page_count)
        return resultado

    textos: dict[str, str | None] = {"pymupdf": None, "pdfplumber": None, "ocr_tesseract": None}
    conf_ocr = None
    try:
        import fitz

        with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
            textos["pymupdf"] = texto_pymupdf(doc, indice)
            textos["ocr_tesseract"], conf_ocr = texto_render_ocr(doc, indice, DPI_RENDER)
    except Exception as error:
        logger.warning("%s p.%s: motor embebido/OCR fallo: %s", fila["document_key"], page_number, error)
    try:
        textos["pdfplumber"] = texto_pdfplumber(datos, indice)
    except Exception as error:
        logger.warning("%s p.%s: pdfplumber fallo: %s", fila["document_key"], page_number, error)

    guardado = fila.get("texto_almacenado") or ""
    referencia = textos.get("pymupdf") or textos.get("ocr_tesseract") or ""

    senales = SenalesPagina(
        extraction_method=fila.get("extraction_method"),
        quality_score=float(fila["quality_score"]) if fila.get("quality_score") not in (None, "") else None,
        ocr_used=str(fila.get("ocr_used")).strip().lower() == "true",
        texto=referencia,
        has_tables="contiene_tabla" in (fila.get("razon_de_riesgo") or ""),
        # El manifest solo admite normas que ya pasaron el gate F-03 +
        # completitud (agents/f04_seleccion_muestra.apto_para_piloto_f04):
        # llegar aqui YA prueba que el documento esta completo.
        documento_completo=True,
        pdf_disponible=True,
    )
    estado, riesgo, motivos = estado_verificacion_f04(textos, senales)
    cmp_guardado = comparar_fidelidad(referencia, guardado)

    resultado.update(
        estado=estado,
        riesgo=riesgo,
        motivos=motivos,
        sha256=fila.get("pdf_sha256"),
        textos=textos,
        texto_guardado=guardado,
        ocr_confidence_render=conf_ocr,
        dpi_render=DPI_RENDER,
        guardado_vs_pdf=_resumen_cmp(cmp_guardado),
    )
    if textos["pymupdf"] is not None and textos["pdfplumber"] is not None:
        resultado["parsers_entre_si"] = _resumen_cmp(comparar_fidelidad(textos["pymupdf"], textos["pdfplumber"]))
    if textos["ocr_tesseract"] is not None and referencia:
        resultado["embebido_vs_ocr"] = _resumen_cmp(comparar_fidelidad(referencia, textos["ocr_tesseract"]))
    return resultado


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="reportes/F04_MANIFEST_PILOTO.csv")
    parser.add_argument("--out-dir", default="reportes")
    args = parser.parse_args()

    filas = cargar_manifest(args.manifest)
    logger.info("Paginas en el manifest a comparar: %d", len(filas))

    supabase = get_supabase()
    cache: dict[str, tuple[bytes, int]] = {}
    inicio = time.time()
    resultados = [comparar_pagina(supabase, fila, cache) for fila in filas]
    duracion = time.time() - inicio

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    por_estado: dict[str, int] = {}
    for r in resultados:
        por_estado[r.get("estado", "SIN_ESTADO")] = por_estado.get(r.get("estado", "SIN_ESTADO"), 0) + 1
    lters = [r["guardado_vs_pdf"]["legal_token_error_rate"] for r in resultados if "guardado_vs_pdf" in r]

    resumen = {
        "paginas_comparadas": len(resultados),
        "por_estado": por_estado,
        "comparacion_incompleta": por_estado.get("COMPARACION_INCOMPLETA", 0),
        "con_error_juridico_guardado_vs_pdf": sum(
            1 for r in resultados if r.get("guardado_vs_pdf", {}).get("hay_error_juridico")),
        "lter_mediano_guardado_vs_pdf": (
            round(sorted(lters)[len(lters) // 2], 4) if lters else None),
        "dpi_render": DPI_RENDER,
        "motor_visual": "tesseract-ocr spa (local, sin costo de API)",
        "costo_api_usd": 0.0,
        "modelo_pagado_usado": None,
        "segundos_total": round(duracion, 1),
    }

    (out / f"{OUT_BASENAME}.json").write_text(
        json.dumps({"resumen": resumen, "paginas": resultados}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with (out / f"{OUT_BASENAME}.csv").open("w", newline="", encoding="utf-8") as fh:
        campos = ["document_key", "page_number", "estado", "riesgo", "sha256"]
        w = csv.writer(fh)
        w.writerow(campos + ["cer_guardado_vs_pdf", "wer_guardado_vs_pdf", "lter_guardado_vs_pdf"])
        for r in resultados:
            cmp_ = r.get("guardado_vs_pdf", {})
            w.writerow([r.get(c) for c in campos] + [cmp_.get("cer"), cmp_.get("wer"),
                                                      cmp_.get("legal_token_error_rate")])

    print("\n" + "=" * 72)
    print("F-04-A — COMPARACION DE MOTORES — SOLO LECTURA, $0 EN LLAMADAS PAGADAS")
    print("=" * 72)
    for k, v in resumen.items():
        print(f"  {k:34} {v}")
    print(f"\nDetalle completo: {out / f'{OUT_BASENAME}.json'}")
    print(f"Resumen tabular:  {out / f'{OUT_BASENAME}.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
