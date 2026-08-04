"""Reprocesa paginas normativas de baja calidad con vision OCR via OpenAI.

Uso recomendado:
  1. Correr primero en dry-run para ver candidatas.
  2. Correr con --apply para guardar la transcripcion IA en metadata.
  3. Agregar --replace-text solo cuando se quiere que /consulta use ese texto.

La salida del modelo no se trata como verdad absoluta: siempre queda guardada
la extraccion previa en metadata cuando se reemplaza text_raw/text_normalized.
"""

import argparse
import base64
import io
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PAGE_TABLE = "digemid_norma_paginas"
NORMAS_TABLE = "digemid_normas"
STORAGE_BUCKET = "digemid-documentos"
DEFAULT_MODEL = "gpt-5.6"
DEFAULT_DETAIL = "original"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


def load_env() -> None:
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
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--document-key")
    parser.add_argument("--quality-below", type=float, default=0.85)
    parser.add_argument("--all-pages", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--replace-text", action="store_true")
    parser.add_argument("--model", default=os.getenv("OPENAI_OCR_MODEL", DEFAULT_MODEL))
    parser.add_argument("--detail", default=os.getenv("OPENAI_OCR_DETAIL", DEFAULT_DETAIL))
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()
    if args.limit <= 0:
        raise ValueError("--limit debe ser mayor que cero")
    if args.dpi <= 0:
        raise ValueError("--dpi debe ser mayor que cero")
    if args.replace_text and not args.apply:
        raise ValueError("--replace-text requiere --apply")
    return args


def norm_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def get_candidate_pages(supabase, args) -> list[dict]:
    if args.document_key:
        normas = (
            supabase.table(NORMAS_TABLE)
            .select("id, document_key, titulo, pdf_url, file_storage_path")
            .eq("document_key", args.document_key)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not normas:
            raise ValueError(f"No existe document_key={args.document_key}")
        norma = normas[0]
        query = (
            supabase.table(PAGE_TABLE)
            .select("id, norma_id, page_number, text_raw, text_normalized, extraction_method, quality_score, metadata")
            .eq("norma_id", norma["id"])
            .order("page_number")
        )
        if not args.all_pages:
            query = query.lt("quality_score", args.quality_below)
        pages = query.limit(args.limit).execute().data or []
        for page in pages:
            page["norma"] = norma
        return pages

    query = (
        supabase.table(PAGE_TABLE)
        .select("id, norma_id, page_number, text_raw, text_normalized, extraction_method, quality_score, metadata")
        .order("quality_score")
    )
    if not args.all_pages:
        query = query.lt("quality_score", args.quality_below)
    pages = query.limit(args.limit).execute().data or []
    norma_ids = sorted({page["norma_id"] for page in pages if page.get("norma_id")})
    if not norma_ids:
        return []

    normas = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, titulo, pdf_url, file_storage_path")
        .in_("id", norma_ids)
        .execute()
        .data
        or []
    )
    normas_by_id = {norma["id"]: norma for norma in normas}
    output = []
    for page in pages:
        norma = normas_by_id.get(page.get("norma_id"))
        if not norma:
            continue
        page["norma"] = norma
        output.append(page)
    return output


def download_pdf_bytes(supabase, norma: dict) -> bytes:
    storage_path = norm_text(norma.get("file_storage_path"))
    if storage_path:
        data = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        if data and data.startswith(b"%PDF"):
            return data
        raise ValueError(f"Storage no devolvio PDF valido: {storage_path}")

    pdf_url = norm_text(norma.get("pdf_url"))
    if not pdf_url:
        raise ValueError(f"{norma.get('document_key')} no tiene file_storage_path ni pdf_url")
    response = requests.get(
        pdf_url,
        timeout=120,
        headers={"User-Agent": "RegAlert-DIGEMID-OpenAI-OCR/1.0"},
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL no devolvio PDF valido: {pdf_url}")
    return response.content


def render_page_png_base64(pdf_path: Path, page_number: int, dpi: int) -> str:
    with fitz.open(pdf_path) as doc:
        page_index = page_number - 1
        if page_index < 0 or page_index >= len(doc):
            raise ValueError(f"Pagina fuera de rango: {page_number}")
        page = doc[page_index]
        pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72), alpha=False)
        png_bytes = pix.tobytes("png")
    return base64.b64encode(png_bytes).decode("ascii")


def extract_json(text: str) -> dict:
    clean = (text or "").strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```(?:json)?", "", clean).strip()
        clean = re.sub(r"```$", "", clean).strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def transcribe_page_openai(
    api_key: str,
    model: str,
    detail: str,
    document_key: str,
    title: str | None,
    page_number: int,
    image_base64: str,
) -> dict:
    prompt = (
        "Transcribe literalmente esta pagina de una norma legal peruana. "
        "No resumas, no corrijas el contenido legal y no inventes texto. "
        "Conserva saltos de linea importantes, numerales, articulos, fechas, "
        "unidades, tablas y encabezados. Si una palabra no es legible escribe "
        "[ilegible]. Devuelve solo JSON valido con estas claves: "
        "transcripcion, tablas_markdown, advertencias, confianza_estimada."
    )
    payload = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            f"Documento: {document_key}\n"
                            f"Titulo: {title or ''}\n"
                            f"Pagina: {page_number}\n\n{prompt}"
                        ),
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:image/png;base64,{image_base64}",
                        "detail": detail,
                    },
                ],
            }
        ],
    }
    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    output_text = data.get("output_text")
    if not output_text:
        chunks: list[str] = []
        for item in data.get("output", []) or []:
            for content in item.get("content", []) or []:
                if content.get("type") in {"output_text", "text"} and content.get("text"):
                    chunks.append(content["text"])
        output_text = "\n".join(chunks).strip()
    if not output_text:
        raise ValueError("OpenAI no devolvio output_text")

    parsed = extract_json(output_text)
    transcript = norm_text(parsed.get("transcripcion"))
    if not transcript:
        raise ValueError("La transcripcion IA vino vacia")
    return {
        "raw_response_id": data.get("id"),
        "transcripcion": parsed.get("transcripcion") or "",
        "tablas_markdown": parsed.get("tablas_markdown") or "",
        "advertencias": parsed.get("advertencias") or [],
        "confianza_estimada": parsed.get("confianza_estimada"),
    }


def build_replacement_text(ai_result: dict) -> str:
    text = (ai_result.get("transcripcion") or "").strip()
    tables = (ai_result.get("tablas_markdown") or "").strip()
    if tables:
        return f"{text}\n\n{tables}".strip()
    return text


def update_page(supabase, page: dict, ai_result: dict, args) -> None:
    now = datetime.now(timezone.utc).isoformat()
    metadata = page.get("metadata") if isinstance(page.get("metadata"), dict) else {}
    metadata["openai_vision_ocr"] = {
        "model": args.model,
        "detail": args.detail,
        "dpi": args.dpi,
        "generated_at": now,
        **ai_result,
    }
    payload: dict[str, Any] = {
        "metadata": metadata,
        "updated_at": now,
    }

    if args.replace_text:
        metadata["previous_extraction_before_openai_vision"] = {
            "text_raw": page.get("text_raw"),
            "text_normalized": page.get("text_normalized"),
            "extraction_method": page.get("extraction_method"),
            "quality_score": page.get("quality_score"),
        }
        payload.update(
            {
                "text_raw": ai_result.get("transcripcion") or "",
                "text_normalized": build_replacement_text(ai_result),
                "extraction_method": "openai_vision_ocr",
                "quality_score": 0.9,
            }
        )

    supabase.table(PAGE_TABLE).update(payload).eq("id", page["id"]).execute()


def main() -> None:
    args = parse_args()
    load_env()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key and args.apply:
        raise ValueError("Falta OPENAI_API_KEY")

    supabase = get_supabase()
    pages = get_candidate_pages(supabase, args)
    logger.info("Paginas candidatas: %s", len(pages))
    if not pages:
        return

    for page in pages:
        norma = page["norma"]
        document_key = norma.get("document_key")
        page_number = int(page["page_number"])
        logger.info(
            "%s pagina %s | quality=%s | method=%s",
            document_key,
            page_number,
            page.get("quality_score"),
            page.get("extraction_method"),
        )

        if not args.apply:
            continue

        pdf_bytes = download_pdf_bytes(supabase, norma)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            tmp_file.write(pdf_bytes)
            tmp_path = Path(tmp_file.name)
        try:
            image_base64 = render_page_png_base64(tmp_path, page_number, args.dpi)
        finally:
            tmp_path.unlink(missing_ok=True)

        ai_result = transcribe_page_openai(
            api_key=api_key,
            model=args.model,
            detail=args.detail,
            document_key=document_key,
            title=norma.get("titulo"),
            page_number=page_number,
            image_base64=image_base64,
        )
        update_page(supabase, page, ai_result, args)
        logger.info("%s pagina %s actualizada con OpenAI OCR.", document_key, page_number)


if __name__ == "__main__":
    main()
