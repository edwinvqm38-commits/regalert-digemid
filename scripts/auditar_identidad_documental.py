"""¿Cada norma está asociada al PDF correcto? (F-03). SOLO LECTURA.

Antes de verificar si una transcripción es fiel hay que verificar que el PDF
del que salió sea el de esa norma. El crawler nunca lo comprobó: guardaba el
primer PDF que encontraba en la página fuente.

Este script abre cada PDF de Storage, lee sus encabezados normativos y los
compara con la identidad que la base dice que tiene. No escribe nada: produce
la matriz y un PLAN de corrección para revisión humana.

Uso:
    python scripts/auditar_identidad_documental.py --out-dir reportes/
    python scripts/auditar_identidad_documental.py --limite 50 --con-ocr
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

from custodia_documental import clasificar_procedencia, sha256_de  # noqa: E402
from identidad_documental import (  # noqa: E402
    CLASIFICACIONES_UTILIZABLES,
    DOCUMENTO_MULTINORMA,
    DOCUMENTO_PROYECTO,
    EvidenciaDocumental,
    PDF_CORRUPTO,
    PDF_IDENTIDAD_AMBIGUA,
    PDF_IDENTIDAD_CONTRADICTORIA,
    PDF_NO_DISPONIBLE,
    clasificar_identidad_documental,
    identidades_en_texto,
    rango_de_paginas,
    tipo_de_documento,
)
from identidad_normativa import construir_identidad  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "digemid-documentos"
DPI_OCR_ENCABEZADO = 300   # solo para leer el encabezado, no el documento entero
MAX_PAGINAS_ANALIZADAS = 40


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def identidad_visual(doc, con_ocr: bool):
    """Lee el encabezado del RENDER de la primera pagina (F-03 · 14).

    No hace OCR de todo el documento: solo lo suficiente para saber que norma
    dice ser. Si la capa de texto dice una identidad y el render dice otra, eso
    es DISCREPANCIA_IDENTIDAD_CRITICA.
    """
    if not con_ocr:
        return None
    try:
        import fitz
        import pytesseract
        from PIL import Image

        pix = doc[0].get_pixmap(matrix=fitz.Matrix(DPI_OCR_ENCABEZADO / 72, DPI_OCR_ENCABEZADO / 72))
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        # Solo el tercio superior: ahi va el encabezado oficial.
        img = img.crop((0, 0, img.width, img.height // 3))
        texto = pytesseract.image_to_string(img, lang="spa") or ""
        encabezados = [a for a in identidades_en_texto(texto, 1) if a.es_encabezado]
        return encabezados[0].identidad if encabezados else None
    except Exception as error:
        logger.warning("OCR de encabezado fallo: %s", error)
        return None


def analizar_norma(supabase, norma: dict, stored_pages: int, con_ocr: bool) -> dict:
    objetivo = construir_identidad(norma.get("tipo_norma"), norma.get("numero"), norma.get("anio"))
    procedencia, motivo_proc = clasificar_procedencia(norma.get("pdf_url"))

    fila = {
        "norma_id": norma["id"],
        "document_key": norma["document_key"],
        "tipo": objetivo.tipo,
        "numero": objetivo.numero,
        "anio": objetivo.anio,
        "source_url": norma.get("source_url"),
        "pdf_url": norma.get("pdf_url"),
        "storage_path": norma.get("file_storage_path"),
        "pdf_sha256": None,
        "pdf_page_count": None,
        "stored_page_count": stored_pages,
        "identity_expected": str(objetivo),
        "identity_detected": None,
        "identities_detected_all": "",
        "document_type": None,
        "start_page": None,
        "end_page": None,
        "filename_match": None,
        "source_context_match": None,
        "content_match": None,
        "visual_match": None,
        "classification": None,
        "confidence": None,
        "reason": "",
        "procedencia": procedencia,
        "recommended_action": "",
    }

    if not norma.get("file_storage_path"):
        fila.update(classification=PDF_NO_DISPONIBLE, confidence="nula",
                    reason="la norma no tiene PDF guardado: no hay documento que identificar",
                    recommended_action="recuperar el PDF oficial antes de cualquier otra cosa")
        return fila

    try:
        datos = supabase.storage.from_(BUCKET).download(norma["file_storage_path"])
        fila["pdf_sha256"] = sha256_de(datos)
    except Exception as error:
        fila.update(classification=PDF_NO_DISPONIBLE, confidence="nula",
                    reason=f"no se pudo descargar: {str(error)[:120]}",
                    recommended_action="revisar storage_path")
        return fila

    try:
        import fitz

        with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
            fila["pdf_page_count"] = doc.page_count
            apariciones, texto_completo = [], []
            for indice in range(min(doc.page_count, MAX_PAGINAS_ANALIZADAS)):
                texto = doc[indice].get_text("text") or ""
                texto_completo.append(texto)
                apariciones.extend(identidades_en_texto(texto, indice + 1))
            visual = identidad_visual(doc, con_ocr)
    except Exception as error:
        fila.update(classification=PDF_CORRUPTO, confidence="nula",
                    reason=f"el PDF no se pudo abrir: {str(error)[:120]}",
                    recommended_action="recuperar el PDF desde la fuente oficial")
        return fila

    ev = EvidenciaDocumental(
        identidad_objetivo=objetivo,
        apariciones=apariciones,
        total_paginas=fila["pdf_page_count"],
        texto_completo="\n".join(texto_completo),
        filename=(norma.get("pdf_url") or "").rsplit("/", 1)[-1],
        url=norma.get("pdf_url"),
        identidad_visual=visual,
    )
    clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
    tipo_doc = tipo_de_documento(apariciones, ev.texto_completo)
    inicio, fin = rango_de_paginas(apariciones, objetivo, fila["pdf_page_count"])

    detectadas = sorted({str(a.identidad) for a in ev.encabezados})
    fila.update(
        identity_detected=detectadas[0] if detectadas else None,
        identities_detected_all=" | ".join(detectadas),
        document_type=tipo_doc,
        start_page=inicio, end_page=fin,
        filename_match=ev.filename_match,
        source_context_match=ev.source_context_match,
        content_match=ev.content_match,
        visual_match=ev.visual_match,
        classification=clasificacion, confidence=confianza, reason=motivo,
        recommended_action=recomendar(clasificacion, tipo_doc, fila),
    )
    return fila


def recomendar(clasificacion: str, tipo_doc: str, fila: dict) -> str:
    if clasificacion == PDF_IDENTIDAD_CONTRADICTORIA:
        return "NO usar este texto: el PDF es de otra norma. Buscar el PDF correcto."
    if clasificacion == PDF_IDENTIDAD_AMBIGUA:
        return "revision humana: no se puede probar que documento es"
    if tipo_doc == DOCUMENTO_PROYECTO:
        return "es un PROYECTO, no la norma aprobada: no puede sostener una relacion juridica"
    if tipo_doc == DOCUMENTO_MULTINORMA:
        return (f"multinorma: solo las paginas {fila['start_page']}-{fila['end_page']} "
                "pertenecen a esta norma")
    if fila["pdf_page_count"] and fila["stored_page_count"] != fila["pdf_page_count"]:
        return (f"identidad correcta pero se guardaron {fila['stored_page_count']} de "
                f"{fila['pdf_page_count']} paginas")
    return "sin accion inmediata"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="reportes")
    parser.add_argument("--limite", type=int, default=None)
    parser.add_argument("--con-ocr", action="store_true",
                        help="Ademas del texto embebido, lee el encabezado del render (300 DPI)")
    args = parser.parse_args()

    supabase = get_supabase()
    normas = (
        supabase.table("digemid_normas")
        .select("id, document_key, tipo_norma, numero, anio, source_url, pdf_url, file_storage_path")
        .execute().data or []
    )
    paginas, desde = {}, 0
    while True:
        lote = (supabase.table("digemid_norma_paginas").select("norma_id")
                .range(desde, desde + 999).execute().data or [])
        for p in lote:
            paginas[p["norma_id"]] = paginas.get(p["norma_id"], 0) + 1
        if len(lote) < 1000:
            break
        desde += 1000

    con_texto = [n for n in normas if paginas.get(n["id"])]
    if args.limite:
        con_texto = con_texto[:args.limite]
    logger.info("Normas con texto a auditar: %d (OCR de encabezado: %s)", len(con_texto), args.con_ocr)

    filas, inicio = [], time.time()
    for norma in con_texto:
        fila = analizar_norma(supabase, norma, paginas.get(norma["id"], 0), args.con_ocr)
        filas.append(fila)
        logger.info("%s -> %s (%s)", norma["document_key"], fila["classification"],
                    fila["identities_detected_all"] or "sin encabezados")

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "MATRIZ_IDENTIDAD_DOCUMENTAL.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)

    # PLAN de correccion: propuesta, NUNCA ejecucion (F-03 · 21).
    plan = [
        {
            "norma": f["document_key"],
            "valor_actual": f["pdf_url"],
            "problema": f["classification"],
            "identidad_esperada": f["identity_expected"],
            "identidad_hallada": f["identities_detected_all"],
            "pdf_correcto_propuesto": "",   # se llena tras buscar el PDF real
            "evidencia": f["reason"],
            "accion_futura": f["recommended_action"],
            "riesgo": "ALTO" if f["classification"] == PDF_IDENTIDAD_CONTRADICTORIA else "MEDIO",
            "requiere_humano": "si",
        }
        for f in filas if f["classification"] not in CLASIFICACIONES_UTILIZABLES
    ]
    with (out / "PLAN_CORRECCION_DOCUMENTAL.csv").open("w", newline="", encoding="utf-8") as fh:
        if plan:
            w = csv.DictWriter(fh, fieldnames=list(plan[0].keys()))
            w.writeheader()
            w.writerows(plan)

    resumen: dict = {"normas_auditadas": len(filas), "por_clasificacion": {},
                     "por_tipo_documento": {}, "segundos": round(time.time() - inicio, 1)}
    for f in filas:
        resumen["por_clasificacion"][f["classification"]] = \
            resumen["por_clasificacion"].get(f["classification"], 0) + 1
        if f["document_type"]:
            resumen["por_tipo_documento"][f["document_type"]] = \
                resumen["por_tipo_documento"].get(f["document_type"], 0) + 1

    contradictorias = [f for f in filas if f["classification"] == PDF_IDENTIDAD_CONTRADICTORIA]
    (out / "MATRIZ_IDENTIDAD_DOCUMENTAL.json").write_text(
        json.dumps({"resumen": resumen, "normas": filas, "plan_correccion": plan},
                   ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 74)
    print("IDENTIDAD DOCUMENTAL — SOLO LECTURA, NADA MODIFICADO")
    print("=" * 74)
    for k, v in resumen.items():
        print(f"  {k:26} {v}")
    if contradictorias:
        print(f"\n⚠️  EL PDF ES DE OTRA NORMA ({len(contradictorias)}):")
        for f in contradictorias:
            print(f"    {f['document_key']:22} esperado {f['identity_expected']:20} "
                  f"-> hallado {f['identities_detected_all']}")
    print(f"\nMatriz: {out / 'MATRIZ_IDENTIDAD_DOCUMENTAL.csv'}")
    print(f"Plan:   {out / 'PLAN_CORRECCION_DOCUMENTAL.csv'} ({len(plan)} casos, ninguno aplicado)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
