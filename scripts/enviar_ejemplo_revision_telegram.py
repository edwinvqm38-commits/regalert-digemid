"""Prueba de concepto: manda a Telegram, para UNA pagina puntual de una
norma, la imagen real de esa pagina (renderizada del PDF) junto a un reporte
HTML que explica en lenguaje llano por que quedo marcada, en vez del reporte
generico actual (que solo da un quality_score y pide "compara con el PDF").

Reutiliza la descarga de PDF (Storage con fallback a pdf_url) y el render de
pagina a imagen de scripts/export_normativa_review_html.py, pero:
  - opera sobre una sola pagina, no la norma completa (archivo chico, apto
    para Telegram),
  - construye un "motivo" especifico a partir de las senales ya guardadas
    (quality_score, extraction_method, ocr_confidence, posible_formula,
    largo del texto) en vez de solo mostrar los numeros crudos,
  - envia la imagen y el HTML directo al chat de admin en Telegram.

Uso:
    python scripts/enviar_ejemplo_revision_telegram.py --document-key RM-607-2024 --page 94
"""

import argparse
import base64
import html
import logging
import os
from pathlib import Path

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client


NORMA_TABLE_NAME = "digemid_normas"
PAGE_TABLE_NAME = "digemid_norma_paginas"
STORAGE_BUCKET = "digemid-documentos"
UMBRAL_BAJA_CALIDAD = 0.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--document-key", required=True)
    parser.add_argument("--page", type=int, required=True)
    return parser.parse_args()


def get_norma(supabase, document_key: str) -> dict:
    response = (
        supabase.table(NORMA_TABLE_NAME)
        .select("id, document_key, titulo, pdf_url, file_storage_path")
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_pagina(supabase, norma_id: str, page_number: int) -> dict:
    response = (
        supabase.table(PAGE_TABLE_NAME)
        .select(
            "page_number, text_raw, text_normalized, extraction_method, ocr_used, "
            "ocr_confidence, quality_score, has_tables, posible_formula"
        )
        .eq("norma_id", norma_id)
        .eq("page_number", page_number)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"norma_id={norma_id} no tiene la pagina {page_number}")
    return rows[0]


def download_pdf_bytes(supabase, norma: dict) -> bytes:
    storage_path = (norma.get("file_storage_path") or "").strip()
    if storage_path:
        data = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        if data and data.startswith(b"%PDF"):
            return data
        logger.warning("Storage no devolvio PDF valido en %s, intento con pdf_url.", storage_path)

    pdf_url = (norma.get("pdf_url") or "").strip()
    if not pdf_url:
        raise ValueError(f"{norma.get('document_key')} no tiene file_storage_path ni pdf_url.")
    response = requests.get(
        pdf_url, timeout=120, headers={"User-Agent": "RegAlert-DIGEMID-EjemploRevision/1.0"}
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL no devolvio PDF valido: {pdf_url}")
    return response.content


def render_page_png(pdf_bytes: bytes, page_number: int, dpi: int = 150) -> bytes:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        if page_number < 1 or page_number > len(doc):
            raise ValueError(f"El PDF tiene {len(doc)} paginas, no existe la pagina {page_number}")
        page = doc[page_number - 1]
        pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72), alpha=False)
        return pix.tobytes("png")


def construir_motivo(pagina: dict) -> str:
    """Traduce las senales crudas (quality_score, extraction_method, etc.) a
    una frase legible sobre QUE especificamente esta mal, en vez de solo
    mostrar los numeros -- que es la queja concreta que motivo este ejemplo:
    la plantilla actual es "muy general" y no dice que corregir."""
    quality = pagina.get("quality_score")
    quality = 0.0 if quality is None else float(quality)
    texto = (pagina.get("text_normalized") or pagina.get("text_raw") or "").strip()
    metodo = pagina.get("extraction_method") or "?"
    ocr_conf = pagina.get("ocr_confidence")
    posible_formula = pagina.get("posible_formula")

    if quality == 0 and len(texto) < 15:
        return (
            f'El texto extraido tiene solo {len(texto)} caracter(es) ("{texto}"). '
            "Esta pagina casi con certeza es una imagen/escaneo sin capa de texto real "
            "(sello, firma, diagrama o foto), asi que el extractor no pudo leer nada util. "
            "Corrigela transcribiendo a mano lo que se ve en la imagen adjunta."
        )
    if metodo.startswith("ocr") or pagina.get("ocr_used"):
        conf_txt = f" con confianza OCR {ocr_conf}" if ocr_conf is not None else ""
        return (
            f"El texto se obtuvo por OCR{conf_txt} porque no habia texto seleccionable "
            "en el PDF (probablemente escaneado). Compara linea por linea contra la imagen "
            "adjunta: el OCR suele confundir numeros, tildes y saltos de columna."
        )
    if posible_formula:
        return (
            "Se detecto notacion tecnica o matematica (simbolos como =, %, formulas). "
            "Revisa que esos simbolos y subindices se transcribieron bien, comparando "
            "contra la imagen adjunta."
        )
    if quality < UMBRAL_BAJA_CALIDAD:
        return (
            f"El texto extraido (metodo: {metodo}) tiene calidad baja ({quality:.2f}): "
            "palabras pegadas, cortadas o con caracteres sueltos. Compara contra la imagen "
            "adjunta y corrige donde el texto no coincida con lo que se ve."
        )
    return f"Calidad {quality:.2f} (metodo: {metodo}). Revisar igual comparando contra la imagen adjunta."


def construir_html_ejemplo(norma: dict, pagina: dict, img_png_bytes: bytes) -> str:
    img_data_uri = f"data:image/png;base64,{base64.b64encode(img_png_bytes).decode('ascii')}"
    texto = (pagina.get("text_normalized") or pagina.get("text_raw") or "").strip()
    texto_html = html.escape(texto) if texto else "[SIN TEXTO REGISTRADO]"
    motivo = html.escape(construir_motivo(pagina))
    document_key = html.escape(norma.get("document_key") or "")
    titulo = html.escape(norma.get("titulo") or "")
    page_number = pagina["page_number"]

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ejemplo de observacion especifica - {document_key} pag. {page_number}</title>
<style>
  :root {{ --bg:#f4f7fb; --card:#fff; --ink:#1f2a37; --muted:#5a6a7f; --line:#d8e1ec; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; padding:20px; background:var(--bg); color:var(--ink); font-family:"Segoe UI",Tahoma,sans-serif; max-width:1000px; margin-inline:auto; }}
  h1 {{ font-size:1.25rem; }}
  .nota {{ background:#fff3e0; border:1px solid #ffb74d; padding:0.9rem; border-radius:8px; margin-bottom:1rem; }}
  .nota b {{ display:block; margin-bottom:4px; }}
  .page-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; }}
  .img-wrap {{ border:1px solid var(--line); border-radius:8px; padding:8px; background:#fff; }}
  .img-wrap img {{ width:100%; height:auto; display:block; border-radius:6px; }}
  pre {{ white-space:pre-wrap; word-break:break-word; margin:0; padding:10px; border:1px solid var(--line); border-radius:8px; background:#f9fbfd; font-size:13px; line-height:1.4; }}
  @media (max-width:820px) {{ .page-grid {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
  <h1>Ejemplo: observacion especifica -- {document_key}, pagina {page_number}</h1>
  <p>{titulo}</p>
  <div class="nota">
    <b>Que esta mal en esta pagina (en vez de solo "calidad baja"):</b>
    {motivo}
  </div>
  <div class="page-grid">
    <div class="img-wrap"><img alt="Pagina {page_number} del PDF" src="{img_data_uri}"></div>
    <div><pre>{texto_html}</pre></div>
  </div>
</body>
</html>"""


def enviar_telegram(token: str, chat_id: str, img_png_bytes: bytes, motivo: str, html_content: str, document_key: str, page_number: int):
    base = f"https://api.telegram.org/bot{token}"

    intro = (
        f"🧪 Ejemplo de prueba: observacion especifica para {document_key}, pagina {page_number}.\n\n"
        "Asi se veria si el reporte muestra la imagen real de la pagina + el motivo exacto, "
        "en vez de solo un numero de calidad."
    )
    r = requests.post(f"{base}/sendMessage", data={"chat_id": chat_id, "text": intro}, timeout=30)
    r.raise_for_status()

    r = requests.post(
        f"{base}/sendPhoto",
        data={"chat_id": chat_id, "caption": f"📄 Pagina {page_number} tal cual esta en el PDF original."},
        files={"photo": (f"pagina_{page_number}.png", img_png_bytes, "image/png")},
        timeout=60,
    )
    r.raise_for_status()

    r = requests.post(
        f"{base}/sendMessage",
        data={"chat_id": chat_id, "text": f"🔎 Motivo especifico:\n{motivo}"},
        timeout=30,
    )
    r.raise_for_status()

    r = requests.post(
        f"{base}/sendDocument",
        data={"chat_id": chat_id, "caption": "📋 Reporte HTML de ejemplo (imagen + texto lado a lado)."},
        files={
            "document": (
                f"ejemplo_revision_{document_key}_pag{page_number}.html",
                html_content.encode("utf-8"),
                "text/html",
            )
        },
        timeout=30,
    )
    r.raise_for_status()


def main():
    args = parse_args()
    load_env()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise SystemExit("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_ADMIN_CHAT_ID/TELEGRAM_CHAT_ID")

    supabase = get_supabase()
    norma = get_norma(supabase, args.document_key)
    pagina = get_pagina(supabase, norma["id"], args.page)

    pdf_bytes = download_pdf_bytes(supabase, norma)
    img_png_bytes = render_page_png(pdf_bytes, args.page)

    motivo = construir_motivo(pagina)
    html_content = construir_html_ejemplo(norma, pagina, img_png_bytes)

    enviar_telegram(token, chat_id, img_png_bytes, motivo, html_content, args.document_key, args.page)
    logger.info("Ejemplo enviado a chat_id %s para %s pagina %s", chat_id, args.document_key, args.page)


if __name__ == "__main__":
    main()
