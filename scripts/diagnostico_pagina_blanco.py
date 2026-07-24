"""Script temporal de diagnostico: inspecciona en detalle por que
es_pagina_en_blanco() no detecto como blancas las paginas 18 y 38 de
RM-403-2025, para calibrar el umbral o el criterio."""

import os
import sys
import tempfile
from pathlib import Path

import fitz
from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

norma = supabase.table("digemid_normas").select("id, document_key, file_storage_path, pdf_url").eq("document_key", "RM-403-2025").maybe_single().execute().data
print("norma:", norma)

with tempfile.TemporaryDirectory() as tmp:
    destino = Path(tmp) / "norma.pdf"
    contenido = supabase.storage.from_("digemid-documentos").download(norma["file_storage_path"])
    destino.write_bytes(contenido)

    doc = fitz.open(str(destino))
    print("total paginas:", len(doc))

    for page_number in (18, 38):
        page = doc[page_number - 1]
        texto = (page.get_text("text") or "")
        imagenes = page.get_images()
        pix = page.get_pixmap(matrix=fitz.Matrix(1, 1))
        samples = pix.samples
        promedio = sum(samples) / len(samples) if samples else None

        print(f"--- pagina {page_number} ---")
        print("texto repr (primeros 200):", repr(texto[:200]))
        print("longitud texto strip:", len(texto.strip()))
        print("cantidad imagenes:", len(imagenes))
        print("promedio pixel:", promedio)
