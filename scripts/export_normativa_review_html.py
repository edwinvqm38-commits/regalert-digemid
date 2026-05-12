import argparse
import html
import io
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import fitz
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from supabase import create_client


SCOPES = ["https://www.googleapis.com/auth/drive"]
NORMA_TABLE_NAME = "digemid_normas"
ASSET_TABLE_NAME = "digemid_norma_assets"
PAGE_TABLE_NAME = "digemid_norma_paginas"
DEFAULT_OUTPUT_DIR = "exports"
MIN_TEXT_SHORT = 80
RARE_CHAR_PATTERN = re.compile(r"[^\w\s.,;:!?¿¡()\-\/\"'%\n]")
SYMBOL_HEAVY_PATTERN = re.compile(r"[^A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ\s]")
TEMPERATURE_RE = re.compile(r"(?<!\d)-?\d{1,3}\s*[°º]?\s*[Cc]\b")
ARTICLE_RE = re.compile(r"\bart[íi]culo\s+\d+\b", re.IGNORECASE)
NUMERAL_RE = re.compile(r"\b\d+(?:\.\d+){1,4}\b")
LIST_INDEX_RE = re.compile(r"(?m)^\s*(?:[-*•]|[a-zA-Z]\)|\d+\)|\d+\.)\s+")
NUMBERED_LINE_RE = re.compile(r"(?m)^\s*(?:\d+(?:\.\d+){0,4}\.?|[a-zA-Z]\))\s+")
DATE_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b")

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


def get_drive_service():
    client_path_raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON_PATH")
    token_path_raw = os.getenv("GOOGLE_OAUTH_TOKEN_PATH")
    if not client_path_raw:
        raise ValueError("Falta GOOGLE_OAUTH_CLIENT_JSON_PATH")
    if not token_path_raw:
        raise ValueError("Falta GOOGLE_OAUTH_TOKEN_PATH")

    client_path = Path(client_path_raw)
    token_path = Path(token_path_raw)
    if not client_path.exists():
        raise FileNotFoundError(f"No existe GOOGLE_OAUTH_CLIENT_JSON_PATH: {client_path}")
    if not token_path.exists():
        raise FileNotFoundError(f"No existe GOOGLE_OAUTH_TOKEN_PATH: {token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise ValueError("El token OAuth no es valido o requiere reautorizacion")

    return build("drive", "v3", credentials=creds)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--document-key", required=True)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--source", required=True, choices=["raw", "normalized"])
    args = parser.parse_args()
    if args.max_pages is not None and args.max_pages <= 0:
        raise ValueError("--max-pages debe ser mayor que cero")
    return args


def normalize_text(value):
    if value is None:
        return ""
    return str(value).replace("\r\n", "\n").replace("\r", "\n")


def sanitize_file_component(value: str) -> str:
    cleaned = re.sub(r"[<>:\"/\\|?*]+", "-", value).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "norma"


def get_norma_by_document_key(supabase, document_key: str) -> dict:
    response = (
        supabase.table(NORMA_TABLE_NAME)
        .select("id, document_key, titulo, process_status, ocr_required, updated_at")
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_assets_for_norma(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(ASSET_TABLE_NAME)
        .select("id, asset_subtipo, file_name, drive_file_id")
        .eq("norma_id", norma_id)
        .order("id", desc=False)
        .execute()
    )
    return response.data or []


def get_pages_for_norma(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(PAGE_TABLE_NAME)
        .select(
            "id, source_asset_id, page_number, text_raw, text_normalized, "
            "ocr_used, extraction_method, metadata"
        )
        .eq("norma_id", norma_id)
        .order("source_asset_id", desc=False)
        .order("page_number", desc=False)
        .execute()
    )
    return response.data or []


def download_drive_file_bytes(service, drive_file_id: str) -> bytes:
    request = service.files().get_media(fileId=drive_file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def render_pdf_pages_to_base64_png(pdf_bytes: bytes, max_pages: int | None = None) -> dict[int, str]:
    output: dict[int, str] = {}
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        limit = len(doc) if max_pages is None else min(max_pages, len(doc))
        for idx in range(limit):
            page = doc[idx]
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
            output[idx + 1] = pix.tobytes("png").hex()
    return output


def bytes_hex_to_data_uri(png_hex: str) -> str:
    raw = bytes.fromhex(png_hex)
    b64 = io.BytesIO()
    import base64

    b64.write(base64.b64encode(raw))
    return f"data:image/png;base64,{b64.getvalue().decode('ascii')}"


def detect_issues(text_value: str) -> list[str]:
    issues: list[str] = []
    text = normalize_text(text_value).strip()
    if not text:
        return ["pagina sin texto"]

    if len(text) < MIN_TEXT_SHORT:
        issues.append("texto muy corto")

    rare_chars = len(RARE_CHAR_PATTERN.findall(text))
    if rare_chars >= 8:
        issues.append("caracteres raros")

    symbols = len(SYMBOL_HEAVY_PATTERN.findall(text))
    ratio = symbols / max(1, len(text))
    if ratio > 0.18:
        issues.append("muchos simbolos")

    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if len(lines) >= 4:
        first = lines[0][:120].lower()
        last = lines[-1][:120].lower()
        if first and last and first == last:
            issues.append("posible encabezado/pie mezclado")
        elif re.search(r"^\d+$", lines[-1]) and len(lines[-1]) <= 3:
            issues.append("posible encabezado/pie mezclado")

    return issues


def extract_review_data(page: dict) -> dict:
    metadata = page.get("metadata") if isinstance(page.get("metadata"), dict) else {}
    review = metadata.get("review") if isinstance(metadata.get("review"), dict) else {}
    structure = review.get("structure") if isinstance(review.get("structure"), dict) else {}
    detected_headings = (
        structure.get("detected_headings")
        if isinstance(structure.get("detected_headings"), list)
        else []
    )
    review_flags = (
        review.get("review_flags")
        if isinstance(review.get("review_flags"), dict)
        else {}
    )
    sensitive_matches = (
        review.get("sensitive_matches")
        if isinstance(review.get("sensitive_matches"), dict)
        else {}
    )
    sensitive_values_detected = (
        review.get("sensitive_values_detected")
        if isinstance(review.get("sensitive_values_detected"), list)
        else []
    )
    return {
        "review_flags": {
            "possible_handwritten_interference": bool(review_flags.get("possible_handwritten_interference")),
            "possible_heading_error": bool(review_flags.get("possible_heading_error")),
            "possible_numeric_error": bool(review_flags.get("possible_numeric_error")),
            "possible_temperature_value": bool(review_flags.get("possible_temperature_value")),
            "possible_roman_numeral_error": bool(review_flags.get("possible_roman_numeral_error")),
            "possible_numbered_list_split": bool(review_flags.get("possible_numbered_list_split")),
            "possible_index_alignment_issue": bool(review_flags.get("possible_index_alignment_issue")),
            "possible_bad_layout_order": bool(review_flags.get("possible_bad_layout_order")),
        },
        "detected_headings": detected_headings,
        "sensitive_matches": sensitive_matches,
        "sensitive_values_detected": sensitive_values_detected,
    }


def detect_sensitive_tokens(text: str) -> dict:
    return {
        "temperatures": sorted(set(TEMPERATURE_RE.findall(text))),
        "articles": sorted(set(ARTICLE_RE.findall(text))),
        "numerals": sorted(set(NUMERAL_RE.findall(text))),
        "dates": sorted(set(DATE_RE.findall(text))),
        "lists": ["list_item"] if LIST_INDEX_RE.search(text) else [],
    }


def highlight_sensitive_html(text: str) -> str:
    escaped = esc(text)
    patterns = [
        (r"(?<!\d)-?\d{1,3}\s*[°º]?\s*[Cc]\b", "sens"),
        (r"\bart[íi]culo\s+\d+\b", "sens"),
        (r"\b\d+(?:\.\d+){1,4}\b", "sens"),
        (r"(?m)^\s*(?:[-*•]|[a-zA-Z]\)|\d+\)|\d+\.)\s+", "sens"),
        (r"(?m)^\s*(?:\d+(?:\.\d+){0,4}\.?|[a-zA-Z]\))\s+", "sens"),
    ]
    for pattern, klass in patterns:
        escaped = re.sub(
            pattern,
            lambda m: f"<mark class='{klass}'>{m.group(0)}</mark>",
            escaped,
            flags=re.IGNORECASE | re.MULTILINE,
        )
    return escaped


def build_asset_index(assets: list[dict]) -> OrderedDict[int, dict]:
    grouped: OrderedDict[int, dict] = OrderedDict()
    for asset in assets:
        asset_id = asset["id"]
        grouped[asset_id] = {
            "asset_id": asset_id,
            "asset_subtipo": asset.get("asset_subtipo"),
            "file_name": asset.get("file_name"),
            "drive_file_id": asset.get("drive_file_id"),
            "pages": [],
            "warnings": [],
        }
    return grouped


def build_page_groups(
    assets: list[dict], pages: list[dict], source: str, max_pages: int | None
) -> OrderedDict[int, dict]:
    grouped = build_asset_index(assets)

    for page in pages:
        asset_id = page.get("source_asset_id")
        if asset_id not in grouped:
            grouped[asset_id] = {
                "asset_id": asset_id,
                "asset_subtipo": None,
                "file_name": None,
                "drive_file_id": None,
                "pages": [],
                "warnings": ["asset no encontrado en digemid_norma_assets"],
            }

        page_number = page.get("page_number")
        if max_pages is not None and (page_number is None or int(page_number) > max_pages):
            continue

        text_raw = normalize_text(page.get("text_raw")).strip()
        text_normalized = normalize_text(page.get("text_normalized")).strip()
        text_selected = text_raw if source == "raw" else text_normalized
        issues = detect_issues(text_selected)
        review_data = extract_review_data(page)
        sensitive_matches = detect_sensitive_tokens(text_selected)
        for key, values in review_data["sensitive_matches"].items():
            if isinstance(values, list):
                current = sensitive_matches.get(key, [])
                sensitive_matches[key] = sorted(set(current + values))
        sensitive_values_detected = []
        if isinstance(review_data["sensitive_values_detected"], list):
            sensitive_values_detected.extend(review_data["sensitive_values_detected"])
        for temp_value in sensitive_matches.get("temperatures", []):
            sensitive_values_detected.append({"type": "temperature", "value": temp_value})
        for date_value in sensitive_matches.get("dates", []):
            sensitive_values_detected.append({"type": "date", "value": date_value})
        for art_value in sensitive_matches.get("articles", []):
            sensitive_values_detected.append({"type": "article", "value": art_value})
        for numeral_value in sensitive_matches.get("numerals", []):
            sensitive_values_detected.append({"type": "numeral", "value": numeral_value})
        dedup_sens = []
        seen_sens = set()
        for item in sensitive_values_detected:
            if not isinstance(item, dict):
                continue
            key = (str(item.get("type")), str(item.get("value")))
            if key in seen_sens:
                continue
            seen_sens.add(key)
            dedup_sens.append({"type": key[0], "value": key[1]})
        sensitive_values_detected = dedup_sens

        flag_issues = [
            key for key, value in review_data["review_flags"].items() if value
        ]
        if flag_issues:
            issues.extend(flag_issues)

        grouped[asset_id]["pages"].append(
            {
                "page_number": page_number,
                "ocr_used": bool(page.get("ocr_used")),
                "extraction_method": page.get("extraction_method") or "",
                "text_raw": text_raw,
                "text_normalized": text_normalized,
                "text_selected": text_selected,
                "text_chars": len(text_selected),
                "issues": issues,
                "review_flags": review_data["review_flags"],
                "detected_headings": review_data["detected_headings"],
                "sensitive_matches": sensitive_matches,
                "sensitive_values_detected": sensitive_values_detected,
                "img_data_uri": None,
                "warnings": [],
            }
        )

    for asset in grouped.values():
        asset["pages"].sort(key=lambda p: (p["page_number"] or 0))
    return grouped


def attach_page_images(drive_service, grouped_assets: OrderedDict[int, dict], max_pages: int | None):
    for asset in grouped_assets.values():
        drive_file_id = asset.get("drive_file_id")
        if not drive_file_id:
            asset["warnings"].append("asset sin drive_file_id")
            continue

        if not asset["pages"]:
            continue

        try:
            pdf_bytes = download_drive_file_bytes(drive_service, drive_file_id)
            images_hex = render_pdf_pages_to_base64_png(pdf_bytes, max_pages=max_pages)
            for page in asset["pages"]:
                page_number = page["page_number"]
                if page_number not in images_hex:
                    page["warnings"].append("pagina PDF no disponible para render")
                    continue
                page["img_data_uri"] = bytes_hex_to_data_uri(images_hex[page_number])
        except Exception as exc:
            msg = f"no se pudo descargar/renderizar PDF: {exc}"
            asset["warnings"].append(msg)
            for page in asset["pages"]:
                page["warnings"].append(msg)


def summarize(grouped_assets: OrderedDict[int, dict]) -> dict:
    assets_with_pages = 0
    pages_total = 0
    pages_with_text = 0
    pages_without_text = 0
    pages_with_issues = 0
    pages_with_image = 0

    for asset in grouped_assets.values():
        if asset["pages"]:
            assets_with_pages += 1
        for page in asset["pages"]:
            pages_total += 1
            if page["text_selected"]:
                pages_with_text += 1
            else:
                pages_without_text += 1
            if page["issues"] or page["warnings"]:
                pages_with_issues += 1
            if page["img_data_uri"]:
                pages_with_image += 1

    return {
        "assets_with_pages": assets_with_pages,
        "pages_total": pages_total,
        "pages_with_text": pages_with_text,
        "pages_without_text": pages_without_text,
        "pages_with_issues": pages_with_issues,
        "pages_with_image": pages_with_image,
    }


def esc(text: str) -> str:
    return html.escape(text, quote=True)


def build_html(norma: dict, grouped_assets: OrderedDict[int, dict], summary: dict, source: str) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    parts: list[str] = []
    parts.append(
        """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Revision OCR DIGEMID</title>
  <style>
    :root {
      --bg: #f4f7fb;
      --card: #ffffff;
      --ink: #1f2a37;
      --muted: #5a6a7f;
      --line: #d8e1ec;
      --warn: #c77900;
      --bad: #b42318;
      --accent: #0f6cbd;
    }
    * { box-sizing: border-box; }
    body { margin: 0; padding: 20px; background: var(--bg); color: var(--ink); font-family: "Segoe UI", Tahoma, sans-serif; }
    h1, h2, h3 { margin: 0 0 10px; }
    .header, .summary, .asset, .page { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px; margin-bottom: 14px; }
    .meta { color: var(--muted); font-size: 14px; }
    .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #eaf2ff; color: #0b4f8a; font-size: 12px; margin-right: 6px; }
    .warn { color: var(--warn); font-weight: 600; }
    .bad { color: var(--bad); font-weight: 700; }
    .critical { color: #7a0916; background: #fde8e8; border: 1px solid #f5b7b1; padding: 8px 10px; border-radius: 8px; font-weight: 700; }
    .page-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .text-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
    .img-wrap { border: 1px solid var(--line); border-radius: 8px; padding: 8px; background: #fff; min-height: 120px; }
    .img-wrap img { width: 100%; height: auto; display: block; border-radius: 6px; }
    pre { white-space: pre-wrap; word-break: break-word; margin: 0; padding: 10px; border: 1px solid var(--line); border-radius: 8px; background: #f9fbfd; font-size: 13px; line-height: 1.4; }
    .suspect pre { border-color: #f0b429; background: #fff7e2; }
    mark.sens { background: #ffe08a; color: #263238; padding: 0 2px; border-radius: 3px; }
    ul { margin: 6px 0 0; }
    @media (max-width: 980px) { .page-grid, .text-grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
"""
    )

    parts.append("<section class='header'>")
    parts.append("<h1>Revision OCR Normativa DIGEMID</h1>")
    parts.append(
        f"<p class='meta'><strong>Titulo:</strong> {esc(norma.get('titulo') or '')}<br>"
        f"<strong>document_key:</strong> {esc(norma.get('document_key') or '')}<br>"
        f"<strong>process_status:</strong> {esc(str(norma.get('process_status')))} | "
        f"<strong>ocr_required:</strong> {esc(str(norma.get('ocr_required')))}<br>"
        f"<strong>source:</strong> {esc(source)} | <strong>generated_at:</strong> {esc(generated_at)}</p>"
    )
    parts.append("</section>")

    parts.append("<section class='summary'><h2>Resumen de Paginas</h2><div class='summary-grid'>")
    for label, value in (
        ("Assets con paginas", summary["assets_with_pages"]),
        ("Paginas comparadas", summary["pages_total"]),
        ("Paginas con texto", summary["pages_with_text"]),
        ("Paginas sin texto", summary["pages_without_text"]),
        ("Paginas con alertas", summary["pages_with_issues"]),
        ("Paginas con imagen", summary["pages_with_image"]),
    ):
        parts.append(f"<div><span class='pill'>{esc(label)}</span> <strong>{value}</strong></div>")
    parts.append("</div></section>")

    for asset in grouped_assets.values():
        if not asset["pages"]:
            continue
        parts.append("<section class='asset'>")
        parts.append(
            f"<h2>PDF / Asset {asset['asset_id']}</h2>"
            f"<p class='meta'><strong>asset_subtipo:</strong> {esc(asset.get('asset_subtipo') or '')}<br>"
            f"<strong>file_name:</strong> {esc(asset.get('file_name') or '')}</p>"
        )
        if asset["warnings"]:
            parts.append("<p class='bad'>Advertencias del asset:</p><ul>")
            for warning in asset["warnings"]:
                parts.append(f"<li>{esc(warning)}</li>")
            parts.append("</ul>")

        for page in asset["pages"]:
            suspect_page = any(page["review_flags"].values())
            page_class = "page suspect" if suspect_page else "page"
            parts.append(f"<article class='{page_class}'>")
            parts.append(
                f"<h3>Pagina {page['page_number']}</h3>"
                f"<p class='meta'><strong>ocr_used:</strong> {esc(str(page['ocr_used']))} | "
                f"<strong>extraction_method:</strong> {esc(page['extraction_method'])} | "
                f"<strong>caracteres:</strong> {page['text_chars']}</p>"
            )
            parts.append(
                "<p class='meta'>"
                f"<strong>flags:</strong> "
                f"handwritten={page['review_flags']['possible_handwritten_interference']} | "
                f"heading={page['review_flags']['possible_heading_error']} | "
                f"numeric={page['review_flags']['possible_numeric_error']} | "
                f"temperature={page['review_flags']['possible_temperature_value']} | "
                f"roman={page['review_flags']['possible_roman_numeral_error']} | "
                f"numbered_list_split={page['review_flags']['possible_numbered_list_split']} | "
                f"index_alignment={page['review_flags']['possible_index_alignment_issue']} | "
                f"bad_layout_order={page['review_flags']['possible_bad_layout_order']}"
                "</p>"
            )
            if (
                page["review_flags"]["possible_numbered_list_split"]
                or page["review_flags"]["possible_bad_layout_order"]
                or page["review_flags"]["possible_index_alignment_issue"]
            ):
                parts.append(
                    "<p class='critical'>ALERTA FUERTE: posible problema de layout OCR. Revisión manual recomendada.</p>"
                )
            if page["issues"] or page["warnings"]:
                parts.append("<p class='warn'>Posibles problemas detectados:</p><ul>")
                for issue in page["issues"]:
                    parts.append(f"<li>{esc(issue)}</li>")
                for warning in page["warnings"]:
                    parts.append(f"<li class='bad'>{esc(warning)}</li>")
                parts.append("</ul>")

            if page["detected_headings"]:
                parts.append("<p class='meta'><strong>Encabezados detectados:</strong></p><ul>")
                for heading in page["detected_headings"][:20]:
                    parts.append(
                        f"<li>linea {heading.get('line_number')}: "
                        f"[{esc(str(heading.get('kind') or 'heading'))}] "
                        f"{esc(heading.get('text') or '')}</li>"
                    )
                parts.append("</ul>")

            sensitive = page["sensitive_matches"]
            parts.append(
                "<p class='meta'><strong>Valores sensibles:</strong> "
                f"temperaturas={len(sensitive.get('temperatures', []))}, "
                f"articulos={len(sensitive.get('articles', []))}, "
                f"numerales={len(sensitive.get('numerals', []))}, "
                f"listas={len(sensitive.get('lists', []))}</p>"
            )
            if page["sensitive_values_detected"]:
                parts.append("<p class='meta'><strong>sensitive_values_detected:</strong></p><ul>")
                for item in page["sensitive_values_detected"][:60]:
                    parts.append(
                        f"<li>{esc(str(item.get('type', 'unknown')))}: {esc(str(item.get('value', '')))}</li>"
                    )
                parts.append("</ul>")

            parts.append("<div class='page-grid'>")
            parts.append("<div class='img-wrap'>")
            if page["img_data_uri"]:
                parts.append(f"<img alt='PDF pagina {page['page_number']}' src='{page['img_data_uri']}' />")
            else:
                parts.append("<p class='bad'>Imagen no disponible para esta pagina.</p>")
            parts.append("</div>")
            raw_block = page["text_raw"] if page["text_raw"] else "[SIN TEXTO REGISTRADO]"
            norm_block = (
                page["text_normalized"]
                if page["text_normalized"]
                else "[SIN TEXTO NORMALIZADO]"
            )
            parts.append("<div>")
            parts.append("<div class='text-grid'>")
            parts.append("<div><p class='meta'><strong>text_raw</strong></p>")
            parts.append(f"<pre>{highlight_sensitive_html(raw_block)}</pre></div>")
            parts.append("<div><p class='meta'><strong>text_normalized</strong></p>")
            parts.append(f"<pre>{highlight_sensitive_html(norm_block)}</pre></div>")
            parts.append("</div>")
            parts.append("</div>")
            parts.append("</div>")
            parts.append("</article>")
        parts.append("</section>")

    parts.append("</body></html>")
    return "".join(parts)


def build_output_path(output_dir: str, document_key: str, source: str) -> Path:
    dir_path = Path(output_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_name = f"{sanitize_file_component(document_key)}__ocr_review__{source}.html"
    return dir_path / file_name


def run_export(document_key: str, output_dir: str, source: str, max_pages: int | None) -> dict:
    load_env()
    supabase = get_supabase()
    drive_service = get_drive_service()

    norma = get_norma_by_document_key(supabase, document_key)
    assets = get_assets_for_norma(supabase, norma["id"])
    pages = get_pages_for_norma(supabase, norma["id"])
    grouped_assets = build_page_groups(assets, pages, source=source, max_pages=max_pages)
    attach_page_images(drive_service, grouped_assets, max_pages=max_pages)
    summary = summarize(grouped_assets)
    html_content = build_html(norma, grouped_assets, summary, source=source)
    output_path = build_output_path(output_dir, document_key, source=source)
    output_path.write_text(html_content, encoding="utf-8")

    return {
        "output_path": output_path,
        "summary": summary,
        "document_key": document_key,
        "source": source,
        "max_pages": max_pages,
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
        "HTML generado: %s | paginas comparadas: %s | paginas con alertas: %s",
        result["output_path"],
        result["summary"]["pages_total"],
        result["summary"]["pages_with_issues"],
    )


if __name__ == "__main__":
    main()
