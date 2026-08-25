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

from f04_seleccion_muestra import (  # noqa: E402
    es_mismo_motor_que_almacenado,
    estado_verificacion_f04,
    tiene_texto_util,
)
from fidelidad_legal import comparar_fidelidad  # noqa: E402
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


def _comparacion_con_nombre_explicito(referencia: str | None, hipotesis: str | None) -> dict | None:
    """Nunca compara si a alguno de los dos lados le falta texto útil (F-04-A.2
    · 3): eso no es una discrepancia, es ausencia de dato. Devuelve None en
    ese caso -el campo correspondiente simplemente no aparece en el
    resultado, en vez de forzar un CER/LTER que no significa nada-."""
    if not (tiene_texto_util(referencia) and tiene_texto_util(hipotesis)):
        return None
    return _resumen_cmp(comparar_fidelidad(referencia, hipotesis))


def comparar_pagina(supabase, fila: dict, cache: dict) -> dict:
    """Descarga (con caché por PDF) y compara los 3 motores para UNA fila
    del manifest. Nunca escribe nada; devuelve el resultado completo,
    incluido el texto de cada motor, para alimentar el golden dataset.

    F-04-A.2 · CONCORDANCIA != VERDAD: ninguna métrica aquí se llama "vs_pdf"
    -el "guardado" puede venir de Tesseract, no de una lectura del PDF en sí,
    así que llamarlo "pdf" sugeriría una verdad que no existe-. Cada
    comparación se nombra por lo que REALMENTE compara: `stored_vs_embedded`,
    `stored_vs_render_tesseract`, `tesseract_reproducibility` (cuando el
    texto guardado YA vino de Tesseract: comparar contra un Tesseract fresco
    mide reproducibilidad, no evidencia independiente) o
    `embedded_vs_render_tesseract`. `cer_vs_golden`/`wer_vs_golden`/
    `lter_vs_golden` quedan siempre en None: F-04-A no tiene golden humano.
    """
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
    referencia_embebida = textos["pymupdf"] if tiene_texto_util(textos["pymupdf"]) else (
        textos["pdfplumber"] if tiene_texto_util(textos["pdfplumber"]) else None)
    es_tabla = "contiene_tabla" in (fila.get("razon_de_riesgo") or "")
    ocr_used_almacenado = str(fila.get("ocr_used")).strip().lower() == "true"
    same_engine = es_mismo_motor_que_almacenado(fila.get("extraction_method"), ocr_used_almacenado)

    veredicto = estado_verificacion_f04(textos, es_tabla=es_tabla, texto_golden=None)

    resultado.update(veredicto)
    resultado.update(
        sha256=fila.get("pdf_sha256"),
        textos=textos,
        texto_guardado=guardado,
        ocr_confidence_render=conf_ocr,
        dpi_render=DPI_RENDER,
        same_engine_as_stored=same_engine,
    )

    cmp_parsers = _comparacion_con_nombre_explicito(textos["pymupdf"], textos["pdfplumber"])
    if cmp_parsers is not None:
        resultado["embedded_pymupdf_vs_embedded_pdfplumber"] = cmp_parsers

    if referencia_embebida is not None:
        cmp_visual = _comparacion_con_nombre_explicito(referencia_embebida, textos["ocr_tesseract"])
        if cmp_visual is not None:
            resultado["embedded_vs_render_tesseract"] = cmp_visual

    # El "guardado" nunca se llama "pdf": puede venir de Tesseract, y
    # comparar Tesseract-guardado contra Tesseract-fresco es reproducibilidad,
    # no una segunda fuente independiente.
    if same_engine:
        cmp_repro = _comparacion_con_nombre_explicito(guardado, textos["ocr_tesseract"])
        if cmp_repro is not None:
            resultado["tesseract_reproducibility"] = cmp_repro
    elif referencia_embebida is not None:
        cmp_stored = _comparacion_con_nombre_explicito(guardado, referencia_embebida)
        if cmp_stored is not None:
            resultado["stored_vs_embedded"] = cmp_stored
    else:
        cmp_stored = _comparacion_con_nombre_explicito(guardado, textos["ocr_tesseract"])
        if cmp_stored is not None:
            resultado["stored_vs_render_tesseract"] = cmp_stored

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

    CAMPOS_COMPARACION = (
        "embedded_pymupdf_vs_embedded_pdfplumber", "embedded_vs_render_tesseract",
        "stored_vs_embedded", "stored_vs_render_tesseract", "tesseract_reproducibility",
    )

    por_estado: dict[str, int] = {}
    for r in resultados:
        por_estado[r.get("estado", "SIN_ESTADO")] = por_estado.get(r.get("estado", "SIN_ESTADO"), 0) + 1

    def _distribucion_lter(campo: str) -> dict | None:
        valores = [r[campo]["legal_token_error_rate"] for r in resultados if campo in r]
        if not valores:
            return None
        valores.sort()
        return {
            "n": len(valores), "mediana": round(valores[len(valores) // 2], 4),
            "maximo": round(max(valores), 4),
            "con_error_juridico": sum(1 for r in resultados if r.get(campo, {}).get("hay_error_juridico")),
        }

    tres_motores_disponibles = sum(
        1 for r in resultados if all(r.get("textos", {}).get(m) is not None
                                     for m in ("pymupdf", "pdfplumber", "ocr_tesseract")))
    paginas_con_error_juridico = sum(
        1 for r in resultados if any(r.get(c, {}).get("hay_error_juridico") for c in CAMPOS_COMPARACION))

    resumen = {
        "paginas_comparadas": len(resultados),
        "paginas_con_3_motores_disponibles": tres_motores_disponibles,
        "paginas_digitales_almacenadas": sum(1 for r in resultados if not r.get("same_engine_as_stored")),
        "paginas_ocr_almacenadas": sum(1 for r in resultados if r.get("same_engine_as_stored")),
        "same_engine_as_stored_count": sum(1 for r in resultados if r.get("same_engine_as_stored")),
        "por_estado": por_estado,
        "comparacion_incompleta": por_estado.get("COMPARACION_INCOMPLETA", 0),
        "concordancia_automatica_alta": por_estado.get("CONCORDANCIA_AUTOMATICA_ALTA", 0),
        "discrepancia_entre_fuentes": por_estado.get("DISCREPANCIA_ENTRE_FUENTES", 0),
        "requiere_revision_humana": por_estado.get("REQUIERE_REVISION_HUMANA", 0),
        "verificada_humano": por_estado.get("VERIFICADA_HUMANO", 0),
        # Regla de oro (F-04-A.2): esto debe ser SIEMPRE 0. Si alguna vez
        # deja de serlo, es una regresion del contrato epistemologico, no un
        # resultado a celebrar.
        "verificada_automaticamente_count": por_estado.get("VERIFICADA_AUTOMATICAMENTE", 0),
        "paginas_con_error_de_tokens_juridicos": paginas_con_error_juridico,
        "distribucion_por_par_de_fuentes": {
            campo: _distribucion_lter(campo) for campo in CAMPOS_COMPARACION
        },
        "golden_available": False,
        "paginas_con_golden": sum(1 for r in resultados if r.get("golden_available")),
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
        campos = ["document_key", "page_number", "estado", "riesgo", "sha256", "same_engine_as_stored"]
        w = csv.writer(fh)
        w.writerow(campos + [f"lter_{c}" for c in CAMPOS_COMPARACION])
        for r in resultados:
            w.writerow([r.get(c) for c in campos]
                       + [r.get(c, {}).get("legal_token_error_rate") for c in CAMPOS_COMPARACION])

    print("\n" + "=" * 72)
    print("F-04-A.2 — COMPARACION DE MOTORES — CONCORDANCIA != VERDAD — $0 EN LLAMADAS PAGADAS")
    print("=" * 72)
    for k, v in resumen.items():
        print(f"  {k:34} {v}")
    print("\n=== RESUMEN (JSON) ===")
    print(json.dumps(resumen, ensure_ascii=False, indent=2))
    print("=== FIN RESUMEN ===")
    print(f"\nDetalle completo: {out / f'{OUT_BASENAME}.json'}")
    print(f"Resumen tabular:  {out / f'{OUT_BASENAME}.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
