"""Genera un HTML de revision para UNA norma: cada pagina del PDF renderizada
como imagen al lado del texto extraido (raw y normalizado), con las senales
de calidad ya calculadas por agents/pdf_extract.py (quality_score,
ocr_confidence, has_tables, posible_formula) para poder confirmar rapido la
transcripcion, las tablas y detectar paginas con firma/sello que ameriten
revision manual antes de usarse en una consulta legal.

No depende de Google Drive: el PDF de respaldo se descarga de Supabase
Storage (bucket digemid-documentos, columna digemid_normas.file_storage_path)
o, si esa norma todavia no tiene respaldo, directo de digemid_normas.pdf_url.
"""

import argparse
import base64
import html
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client


NORMA_TABLE_NAME = "digemid_normas"
PAGE_TABLE_NAME = "digemid_norma_paginas"
STORAGE_BUCKET = "digemid-documentos"
DEFAULT_OUTPUT_DIR = "exports"
UMBRAL_BAJA_CALIDAD = 0.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
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
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--source", default="normalized", choices=["raw", "normalized"])
    args = parser.parse_args()
    if args.max_pages is not None and args.max_pages <= 0:
        raise ValueError("--max-pages debe ser mayor que cero")
    return args


def sanitize_file_component(value: str) -> str:
    cleaned = re.sub(r"[<>:\"/\\|?*]+", "-", value).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "norma"


def get_norma_by_document_key(supabase, document_key: str) -> dict:
    response = (
        supabase.table(NORMA_TABLE_NAME)
        .select(
            "id, document_key, titulo, process_status, ocr_required, has_tables, "
            "pdf_url, file_storage_path, updated_at"
        )
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_pages_for_norma(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(PAGE_TABLE_NAME)
        .select(
            "page_number, text_raw, text_normalized, extraction_method, ocr_used, "
            "ocr_confidence, quality_score, has_tables, posible_formula, "
            "revisado_manual, metadata"
        )
        .eq("norma_id", norma_id)
        .order("page_number", desc=False)
        .execute()
    )
    return response.data or []


def download_pdf_bytes(supabase, norma: dict) -> bytes:
    storage_path = (norma.get("file_storage_path") or "").strip()
    if storage_path:
        data = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        if data and data.startswith(b"%PDF"):
            return data
        logger.warning(
            "Storage no devolvio PDF valido en %s, intento con pdf_url.",
            storage_path,
        )

    pdf_url = (norma.get("pdf_url") or "").strip()
    if not pdf_url:
        raise ValueError(
            f"{norma.get('document_key')} no tiene file_storage_path ni pdf_url para respaldo."
        )
    response = requests.get(
        pdf_url,
        timeout=120,
        headers={"User-Agent": "RegAlert-DIGEMID-NormativaReview/1.0"},
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL no devolvio PDF valido: {pdf_url}")
    return response.content


def render_pdf_pages_to_data_uri(pdf_bytes: bytes, max_pages: int | None = None) -> dict[int, str]:
    output: dict[int, str] = {}
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        limit = len(doc) if max_pages is None else min(max_pages, len(doc))
        for idx in range(limit):
            page = doc[idx]
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
            png_bytes = pix.tobytes("png")
            output[idx + 1] = f"data:image/png;base64,{base64.b64encode(png_bytes).decode('ascii')}"
    return output


def esc(text) -> str:
    return html.escape(str(text if text is not None else ""), quote=True)


def build_tabla_html(tabla: list) -> str:
    filas_html = "".join(
        "<tr>" + "".join(f"<td>{esc(celda)}</td>" for celda in fila) + "</tr>"
        for fila in tabla
    )
    return f'<table class="tabla-detectada">{filas_html}</table>'


def build_page_section(page: dict, img_data_uri: str | None, source: str) -> str:
    quality = page.get("quality_score")
    quality = 0.0 if quality is None else float(quality)

    if quality >= 0.85:
        color = "#43a047"
    elif quality >= UMBRAL_BAJA_CALIDAD:
        color = "#fbc02d"
    else:
        color = "#e53935"

    señales = []
    if page.get("ocr_used"):
        señales.append(f"OCR (confianza {page.get('ocr_confidence')})")
    if page.get("posible_formula"):
        señales.append("posible fórmula/notación técnica — revisar manualmente")
    if page.get("has_tables"):
        señales.append("tabla detectada")
    if page.get("revisado_manual"):
        señales.append("ya revisada manualmente")
    señales_html = " · ".join(esc(s) for s in señales) if señales else "—"

    metadata = page.get("metadata") if isinstance(page.get("metadata"), dict) else {}
    tablas = metadata.get("tables") if isinstance(metadata.get("tables"), list) else []
    tablas_html = "".join(build_tabla_html(tabla) for tabla in tablas)

    text_raw = (page.get("text_raw") or "").strip()
    text_normalized = (page.get("text_normalized") or "").strip()
    text_selected = text_raw if source == "raw" else text_normalized
    texto_html = esc(text_selected) if text_selected else "[SIN TEXTO REGISTRADO]"

    img_html = (
        f"<img alt='PDF pagina {page['page_number']}' src='{img_data_uri}' />"
        if img_data_uri
        else "<p class='bad'>Imagen no disponible para esta pagina.</p>"
    )

    return f"""
    <article class="page" style="border-left: 6px solid {color};">
      <h3>Página {page['page_number']} — calidad {quality:.2f} ({esc(page.get('extraction_method') or '?')})</h3>
      <p class="meta"><b>Señales:</b> {señales_html}</p>
      <div class="page-grid">
        <div class="img-wrap">{img_html}</div>
        <div><pre>{texto_html}</pre>{tablas_html}</div>
      </div>
    </article>"""


def build_html(norma: dict, pages: list[dict], images: dict[int, str], source: str) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    pages_with_text = sum(1 for p in pages if (p.get("text_normalized") or p.get("text_raw")))
    pages_with_tables = sum(1 for p in pages if p.get("has_tables"))
    pages_needing_review = sum(
        1 for p in pages
        if not p.get("revisado_manual") and (
            (p.get("quality_score") or 0) < UMBRAL_BAJA_CALIDAD or p.get("posible_formula")
        )
    )

    secciones = "".join(
        build_page_section(page, images.get(page["page_number"]), source) for page in pages
    )

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Revisión — {esc(norma.get('document_key'))}</title>
<style>
  :root {{ --bg:#f4f7fb; --card:#fff; --ink:#1f2a37; --muted:#5a6a7f; --line:#d8e1ec; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; padding:20px; background:var(--bg); color:var(--ink); font-family:"Segoe UI",Tahoma,sans-serif; max-width:1100px; margin-inline:auto; }}
  h1,h2,h3 {{ margin:0 0 10px; }}
  .header,.summary,.page {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; margin-bottom:14px; }}
  .meta {{ color:var(--muted); font-size:14px; }}
  .summary-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:8px; }}
  .pill {{ display:inline-block; padding:2px 8px; border-radius:999px; background:#eaf2ff; color:#0b4f8a; font-size:12px; margin-right:6px; }}
  .page-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:10px; }}
  .img-wrap {{ border:1px solid var(--line); border-radius:8px; padding:8px; background:#fff; min-height:120px; }}
  .img-wrap img {{ width:100%; height:auto; display:block; border-radius:6px; }}
  pre {{ white-space:pre-wrap; word-break:break-word; margin:0; padding:10px; border:1px solid var(--line); border-radius:8px; background:#f9fbfd; font-size:13px; line-height:1.4; }}
  table.tabla-detectada {{ border-collapse:collapse; margin:10px 0; width:100%; }}
  table.tabla-detectada td {{ border:1px solid #999; padding:0.3rem 0.5rem; font-size:0.85rem; }}
  .bad {{ color:#b42318; font-weight:600; }}
  @media (max-width:980px) {{ .page-grid {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<section class="header">
  <h1>Revisión Normativa DIGEMID</h1>
  <p class="meta">
    <b>Título:</b> {esc(norma.get('titulo'))}<br>
    <b>document_key:</b> {esc(norma.get('document_key'))}<br>
    <b>process_status:</b> {esc(norma.get('process_status'))} | <b>ocr_required:</b> {esc(norma.get('ocr_required'))}<br>
    <b>fuente de texto:</b> {esc(source)} | <b>generado:</b> {esc(generated_at)}
  </p>
</section>
<section class="summary">
  <h2>Resumen</h2>
  <div class="summary-grid">
    <div><span class="pill">Páginas totales</span> <b>{len(pages)}</b></div>
    <div><span class="pill">Con texto</span> <b>{pages_with_text}</b></div>
    <div><span class="pill">Con tablas</span> <b>{pages_with_tables}</b></div>
    <div><span class="pill">Pendientes de revisión</span> <b>{pages_needing_review}</b></div>
  </div>
</section>
{secciones}
</body>
</html>"""


def build_output_path(output_dir: str, document_key: str) -> Path:
    dir_path = Path(output_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_name = f"{sanitize_file_component(document_key)}__revision.html"
    return dir_path / file_name


def run_export(document_key: str, output_dir: str, source: str, max_pages: int | None) -> dict:
    load_env()
    supabase = get_supabase()

    norma = get_norma_by_document_key(supabase, document_key)
    pages = get_pages_for_norma(supabase, norma["id"])
    if max_pages is not None:
        pages = [p for p in pages if p["page_number"] <= max_pages]

    pdf_bytes = download_pdf_bytes(supabase, norma)
    images = render_pdf_pages_to_data_uri(pdf_bytes, max_pages=max_pages)

    html_content = build_html(norma, pages, images, source=source)
    output_path = build_output_path(output_dir, document_key)
    output_path.write_text(html_content, encoding="utf-8")

    return {
        "output_path": output_path,
        "document_key": document_key,
        "pages_total": len(pages),
        "source": source,
    }


def main():
    args = parse_args()
    result = run_export(
        document_key=args.document_key,
        output_dir=args.output_dir,
        source=args.source,
        max_pages=args.max_pages,
    )
    logger.info(
        "HTML generado: %s | paginas: %s",
        result["output_path"],
        result["pages_total"],
    )


if __name__ == "__main__":
    main()
