"""Inventario de cadena de custodia sobre los PDF ya guardados (F-02 · 3).

SOLO LECTURA. Baja cada PDF de Storage, calcula su SHA-256, su tamaño y su
numero real de paginas, y lo compara con lo que la base afirma. No escribe una
sola fila: produce el inventario con el que despues se poblara
digemid_norma_documentos.

Detecta ademas el error que ninguna heuristica de calidad puede ver: dos normas
distintas transcritas del MISMO documento.

Uso:
    python scripts/auditar_custodia_documental.py --out-dir reportes/
    python scripts/auditar_custodia_documental.py --limite 50    # muestra
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

from custodia_documental import (  # noqa: E402
    clasificar_procedencia,
    detectar_documentos_compartidos,
    evaluar_completitud,
    sha256_de,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "digemid-documentos"


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def cargar(supabase, limite: int | None):
    normas = (
        supabase.table("digemid_normas")
        .select("id, document_key, pdf_url, file_storage_path, process_status")
        .execute().data or []
    )
    paginas, desde = [], 0
    while True:
        lote = (
            supabase.table("digemid_norma_paginas")
            .select("norma_id, page_number, text_normalized")
            .range(desde, desde + 999).execute().data or []
        )
        paginas.extend(lote)
        if len(lote) < 1000:
            break
        desde += 1000

    por_norma: dict[str, list[dict]] = {}
    for p in paginas:
        por_norma.setdefault(p["norma_id"], []).append(p)

    con_texto = [n for n in normas if por_norma.get(n["id"])]
    return (con_texto[:limite] if limite else con_texto), por_norma


def main() -> int:
    import hashlib

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="reportes")
    parser.add_argument("--limite", type=int, default=None)
    args = parser.parse_args()

    supabase = get_supabase()
    normas, por_norma = cargar(supabase, args.limite)
    logger.info("Normas con texto a auditar: %d", len(normas))

    filas, inicio = [], time.time()
    for norma in normas:
        pags = por_norma.get(norma["id"], [])
        numeros = [p.get("page_number") for p in pags]
        # Huella del texto guardado: permite detectar dos normas con la MISMA
        # transcripcion sin volver a leerlo entero despues.
        hash_texto = hashlib.md5(
            "|".join((p.get("text_normalized") or "") for p in sorted(
                pags, key=lambda x: x.get("page_number") or 0)).encode("utf-8")
        ).hexdigest()

        procedencia, motivo_proc = clasificar_procedencia(norma.get("pdf_url"))
        fila = {
            "document_key": norma["document_key"],
            "norma_id": norma["id"],
            "source_url": norma.get("pdf_url"),
            "storage_path": norma.get("file_storage_path"),
            "procedencia": procedencia,
            "procedencia_motivo": motivo_proc,
            "stored_page_count": len(pags),
            "hash_texto": hash_texto,
            "pdf_sha256": None,
            "byte_size": None,
            "pdf_page_count": None,
            "completitud": None,
            "detalle_completitud": None,
            "error": None,
        }

        if not norma.get("file_storage_path"):
            fila["completitud"] = "PDF_NO_DISPONIBLE"
            fila["detalle_completitud"] = "la norma tiene texto pero no hay PDF guardado"
            filas.append(fila)
            continue

        try:
            datos = supabase.storage.from_(BUCKET).download(norma["file_storage_path"])
            fila["pdf_sha256"] = sha256_de(datos)
            fila["byte_size"] = len(datos)
            import fitz

            with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
                fila["pdf_page_count"] = doc.page_count
            c = evaluar_completitud(fila["pdf_page_count"], numeros)
        except Exception as error:
            fila["error"] = str(error)[:200]
            corrupto = "cannot open" in str(error).lower() or "damaged" in str(error).lower()
            c = evaluar_completitud(None, numeros, pdf_disponible=not corrupto, pdf_corrupto=corrupto)

        fila["completitud"] = c.estado
        fila["detalle_completitud"] = c.motivo
        filas.append(fila)
        logger.info("%s -> %s (%s pag. PDF / %s guardadas)", norma["document_key"],
                    c.estado, fila["pdf_page_count"], len(pags))

    compartidos = detectar_documentos_compartidos(filas)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "INVENTARIO_CUSTODIA.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)

    resumen = {
        "normas_auditadas": len(filas),
        "por_completitud": {},
        "por_procedencia": {},
        "con_sha256": sum(1 for f in filas if f["pdf_sha256"]),
        "con_page_count": sum(1 for f in filas if f["pdf_page_count"]),
        "documentos_compartidos": len(compartidos),
        "criticos": [c for c in compartidos if c["gravedad"] == "CRITICO"],
        "segundos": round(time.time() - inicio, 1),
    }
    for f in filas:
        resumen["por_completitud"][f["completitud"]] = resumen["por_completitud"].get(f["completitud"], 0) + 1
        resumen["por_procedencia"][f["procedencia"]] = resumen["por_procedencia"].get(f["procedencia"], 0) + 1

    (out / "INVENTARIO_CUSTODIA.json").write_text(
        json.dumps({"resumen": resumen, "normas": filas, "documentos_compartidos": compartidos},
                   ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 72)
    print("INVENTARIO DE CADENA DE CUSTODIA — SOLO LECTURA")
    print("=" * 72)
    for k, v in resumen.items():
        if k == "criticos":
            continue
        print(f"  {k:28} {v}")
    if compartidos:
        print("\n⚠️  DOCUMENTOS COMPARTIDOS ENTRE NORMAS DISTINTAS:")
        for c in compartidos:
            print(f"    [{c['gravedad']}] {c['motivo']}: {', '.join(c['normas'])}")
    print(f"\nInventario: {out / 'INVENTARIO_CUSTODIA.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
