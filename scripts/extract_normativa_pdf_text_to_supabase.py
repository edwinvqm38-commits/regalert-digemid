import argparse
import io
import importlib
import json
import logging
import os
import re
import shutil
from copy import deepcopy
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
EXTRACTION_VERSION = "normativa_text_extraction_v2"
REPORTS_DIR = Path("reports")
DRY_RUN_REPORT_PATH = REPORTS_DIR / "normativa_text_extraction_dry_run.md"
RESULT_REPORT_PATH = REPORTS_DIR / "normativa_text_extraction_result.md"
RESULT_JSON_PATH = REPORTS_DIR / "normativa_text_extraction_result.json"
MIN_PDF_BYTES = 10 * 1024
PAGE_TABLE_NAME = "digemid_norma_paginas"
ASSET_TABLE_NAME = "digemid_norma_assets"
NORMA_TABLE_NAME = "digemid_normas"
NORMA_SELECT_FIELDS = (
    "id, document_key, titulo, raw, drive_structure, process_status, updated_at"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

OCR_ENGINE_NAME = "tesseract_pymupdf_render"
ROMAN_HEADING_RE = re.compile(r"^\s*([IVXLCDM]{1,8})[\.\)]\s+")
NUMERIC_HEADING_RE = re.compile(r"^\s*(\d+(?:\.\d+){1,4})\.?\s+")
ARTICLE_HEADING_RE = re.compile(r"^\s*art[íi]culo\s+\d+", re.IGNORECASE)
LIST_ITEM_RE = re.compile(r"^\s*(?:[-*•]|[a-zA-Z]\)|\d+\)|\d+\.)\s+")
TEMPERATURE_RE = re.compile(r"(?<!\d)-?\d{1,3}\s*[°º]?\s*[Cc]\b")
ARTICLE_REF_RE = re.compile(r"\bart[íi]culo\s+\d+\b", re.IGNORECASE)
NUMERAL_REF_RE = re.compile(r"\b\d+(?:\.\d+){1,4}\b")
NUMERAL_LINE_RE = re.compile(r"^(?:\d+(?:\.\d+){0,4}\.?|[a-zA-Z]\))$")
ROMAN_NUMERAL_RE = re.compile(r"\b[IVXLCDM]{1,8}\b")
ROMAN_SUSPECT_RE = re.compile(r"\b[IVXLCDMil1]{1,8}\b")
RARITY_RE = re.compile(r"[^\w\s.,;:!?¿¡()\-\/\"'%°º\n]")
SYMBOL_HEAVY_RE = re.compile(r"[^A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ\s]")
DATE_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b")
INDEX_PAGE_RE = re.compile(r"^\d{1,4}$")
TEMPERATURE_COMBINED_RE = re.compile(
    r"(?<!\d)-?\d{1,3}\s*[°º]?\s*[Cc](?:\s*y\s*-?\d{1,3}\s*[°º]?\s*[Cc])?"
)


class OCRDependencyError(Exception):
    """Error controlado para dependencias OCR faltantes."""


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
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--ocr", action="store_true")
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--ocr-lang", default="spa")
    parser.add_argument("--ocr-dpi", type=int, default=200)
    parser.add_argument("--update-existing-normalized", action="store_true")
    args = parser.parse_args()

    if args.dry_run and args.apply:
        raise ValueError("No puedes usar --dry-run y --apply al mismo tiempo")
    if args.max_pages is not None and args.max_pages < 0:
        raise ValueError("--max-pages no puede ser negativo")
    if args.ocr_dpi <= 0:
        raise ValueError("--ocr-dpi debe ser mayor que cero")
    if args.force and args.update_existing_normalized:
        raise ValueError("No puedes combinar --force con --update-existing-normalized")

    if args.apply:
        args.mode = "apply"
    else:
        args.mode = "dry-run"
        args.dry_run = True

    return args


def normalize_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def normalize_page_text(raw_text: str) -> str:
    text = (raw_text or "").replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.strip() for line in text.split("\n"))
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def simplify_page_text(raw_text: str) -> str:
    return normalize_text(normalize_page_text(raw_text))


def cautious_normalize_page_text(raw_text: str) -> str:
    text = normalize_page_text(raw_text)
    normalized_lines: list[str] = []
    for line in text.split("\n"):
        line_clean = re.sub(r"[ \t]+", " ", line).strip()
        line_clean = re.sub(r"\s+([,.;:!?])", r"\1", line_clean)
        normalized_lines.append(line_clean)
    normalized_text = "\n".join(normalized_lines)
    normalized_text = re.sub(r"\n{3,}", "\n\n", normalized_text)
    return normalized_text.strip()


def has_useful_text(text: str) -> bool:
    return bool(normalize_text(text))


def classify_line_kind(line: str) -> tuple[str, str | None]:
    if not line.strip():
        return "blank", None
    if ARTICLE_HEADING_RE.match(line):
        return "heading", "article_heading"
    if ROMAN_HEADING_RE.match(line):
        return "heading", "roman_heading"
    if NUMERIC_HEADING_RE.match(line):
        return "heading", "numeric_heading"
    if LIST_ITEM_RE.match(line):
        return "list_item", None
    return "paragraph", None


def detect_probable_structure(text: str) -> dict:
    lines = text.split("\n")
    detected_headings: list[dict] = []
    line_kinds: list[dict] = []
    counts = {"heading": 0, "paragraph": 0, "list_item": 0, "blank": 0}

    for idx, line in enumerate(lines, start=1):
        kind, heading_kind = classify_line_kind(line)
        counts[kind] += 1
        line_kinds.append({"line_number": idx, "kind": kind})
        if kind == "heading":
            detected_headings.append(
                {
                    "line_number": idx,
                    "kind": heading_kind,
                    "text": line.strip()[:180],
                }
            )
    return {
        "detected_headings": detected_headings,
        "line_kinds": line_kinds,
        "line_kind_counts": counts,
    }


def detect_review_flags(
    text_raw: str,
    text_normalized: str,
    ocr_used: bool,
    layout_signals: dict | None = None,
) -> dict:
    text = text_raw or ""
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    rare_ratio = len(RARITY_RE.findall(text)) / max(1, len(text))
    symbol_ratio = len(SYMBOL_HEAVY_RE.findall(text)) / max(1, len(text))
    short_fragments = sum(1 for ln in lines if len(ln) <= 3)

    roman_suspect = [
        tok for tok in ROMAN_SUSPECT_RE.findall(text)
        if tok != tok.upper() or "1" in tok or "l" in tok
    ]
    malformed_heading_lines = [
        ln for ln in lines[:40]
        if re.match(r"^\s*[IVXLCDMil1]{1,8}[\.\)]\s+\S+", ln)
        and not ROMAN_HEADING_RE.match(ln)
    ]

    numeric_confusion_tokens = re.findall(r"\b(?:\d+[A-Za-z]|[A-Za-z]\d+)\b", text)
    possible_numeric_error = bool(numeric_confusion_tokens)
    possible_roman_numeral_error = bool(roman_suspect)
    possible_heading_error = bool(malformed_heading_lines)
    possible_numbered_list_split = bool((layout_signals or {}).get("possible_numbered_list_split"))
    possible_index_alignment_issue = bool((layout_signals or {}).get("possible_index_alignment_issue"))
    possible_bad_layout_order = bool((layout_signals or {}).get("possible_bad_layout_order"))
    possible_handwritten_interference = bool(
        ocr_used and (
            rare_ratio > 0.02
            or symbol_ratio > 0.14
            or short_fragments >= 5
            or re.search(r"[\\/_|]{3,}", text)
        )
    )

    sensitive_matches = {
        "temperatures": sorted(set(TEMPERATURE_RE.findall(text_normalized))),
        "articles": sorted(set(ARTICLE_REF_RE.findall(text_normalized))),
        "numerals": sorted(set(NUMERAL_REF_RE.findall(text_normalized))),
        "roman_numerals": sorted(set(ROMAN_NUMERAL_RE.findall(text_normalized))),
        "dates": sorted(set(DATE_RE.findall(text_normalized))),
    }
    sensitive_values_detected = build_sensitive_values_detected(text_normalized)
    if any(item["type"] == "temperature" for item in sensitive_values_detected):
        possible_temperature_value = True
    else:
        possible_temperature_value = bool(sensitive_matches["temperatures"])

    return {
        "possible_handwritten_interference": possible_handwritten_interference,
        "possible_heading_error": possible_heading_error,
        "possible_numeric_error": possible_numeric_error,
        "possible_temperature_value": possible_temperature_value,
        "possible_roman_numeral_error": possible_roman_numeral_error,
        "possible_numbered_list_split": possible_numbered_list_split,
        "possible_index_alignment_issue": possible_index_alignment_issue,
        "possible_bad_layout_order": possible_bad_layout_order,
        "sensitive_matches": sensitive_matches,
        "sensitive_values_detected": sensitive_values_detected,
        "signal_metrics": {
            "rare_ratio": round(rare_ratio, 4),
            "symbol_ratio": round(symbol_ratio, 4),
            "short_fragments": short_fragments,
            "numeric_confusion_tokens": numeric_confusion_tokens[:20],
            "roman_suspect_tokens": roman_suspect[:20],
            "malformed_heading_lines": malformed_heading_lines[:12],
            "layout_signals": layout_signals or {},
        },
    }


def build_page_review_metadata(
    text_raw: str,
    text_normalized: str,
    ocr_used: bool,
    layout_signals: dict | None = None,
) -> dict:
    flags = detect_review_flags(
        text_raw=text_raw,
        text_normalized=text_normalized,
        ocr_used=ocr_used,
        layout_signals=layout_signals,
    )
    structure = detect_probable_structure(text_normalized or text_raw or "")
    return {
        "review_flags": {
            "possible_handwritten_interference": flags["possible_handwritten_interference"],
            "possible_heading_error": flags["possible_heading_error"],
            "possible_numeric_error": flags["possible_numeric_error"],
            "possible_temperature_value": flags["possible_temperature_value"],
            "possible_roman_numeral_error": flags["possible_roman_numeral_error"],
            "possible_numbered_list_split": flags["possible_numbered_list_split"],
            "possible_index_alignment_issue": flags["possible_index_alignment_issue"],
            "possible_bad_layout_order": flags["possible_bad_layout_order"],
        },
        "sensitive_matches": flags["sensitive_matches"],
        "sensitive_values_detected": flags["sensitive_values_detected"],
        "signal_metrics": flags["signal_metrics"],
        "structure": structure,
    }


def build_ocr_dependency_error(detail: str) -> str:
    return (
        f"OCR no disponible: {detail}. "
        "En Windows debes instalar Tesseract OCR y agregarlo al PATH. "
        "En GitHub Actions Ubuntu se debera instalar `tesseract-ocr` y `tesseract-ocr-spa`."
    )


def build_ocr_config(args) -> dict | None:
    if not args.ocr:
        return None

    try:
        pytesseract = importlib.import_module("pytesseract")
    except ImportError as exc:
        raise OCRDependencyError(
            build_ocr_dependency_error(
                "falta la dependencia Python `pytesseract`"
            )
        ) from exc

    try:
        pil_image = importlib.import_module("PIL.Image")
    except ImportError as exc:
        raise OCRDependencyError(
            build_ocr_dependency_error(
                "falta la dependencia Python `Pillow`"
            )
        ) from exc

    tesseract_cmd = shutil.which("tesseract")
    if not tesseract_cmd:
        raise OCRDependencyError(
            build_ocr_dependency_error(
                "no se encontro el binario `tesseract` en el PATH"
            )
        )

    pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
    try:
        tesseract_version = str(pytesseract.get_tesseract_version())
    except Exception:
        tesseract_version = "unknown"

    return {
        "enabled": True,
        "engine": OCR_ENGINE_NAME,
        "lang": normalize_text(args.ocr_lang) or "spa",
        "dpi": args.ocr_dpi,
        "max_pages": args.max_pages,
        "dependencies_ok": True,
        "tesseract_version": tesseract_version,
        "pytesseract": pytesseract,
        "pil_image_module": pil_image,
        "runtime": {
            "pages_attempted": 0,
            "pages_success": 0,
            "pages_failed": 0,
            "ocr_text_chars": 0,
        },
    }


def deep_merge_dicts(base: dict, patch: dict) -> dict:
    result = deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def detect_table_like_text(text: str) -> bool:
    if not text:
        return False
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return False

    separated_lines = 0
    numeric_column_pattern = re.compile(r"\b\d[\d.,]*\s{2,}\d[\d.,]*\b")
    for line in lines:
        if "\t" in line or "|" in line or re.search(r"\s{2,}\S+\s{2,}", line):
            separated_lines += 1
            continue
        if numeric_column_pattern.search(line):
            separated_lines += 1

    return separated_lines >= 2


def ensure_reports_dir():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def get_norma_by_document_key(supabase, document_key: str) -> dict:
    response = (
        supabase.table(NORMA_TABLE_NAME)
        .select(NORMA_SELECT_FIELDS)
        .eq("document_key", document_key)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        raise ValueError(f"No existe digemid_normas.document_key={document_key}")
    return rows[0]


def get_pdf_assets(supabase, norma_id: str) -> list[dict]:
    response = (
        supabase.table(ASSET_TABLE_NAME)
        .select("id, norma_id, asset_tipo, asset_subtipo, drive_file_id, file_name, mime_type")
        .eq("norma_id", norma_id)
        .eq("asset_tipo", "pdf_original")
        .not_.is_("drive_file_id", "null")
        .order("id", desc=False)
        .execute()
    )
    return response.data or []


def get_existing_pages_by_asset(supabase, norma_id: str, source_asset_id: int) -> dict[int, dict]:
    response = (
        supabase.table(PAGE_TABLE_NAME)
        .select("id, page_number, source_asset_id")
        .eq("norma_id", norma_id)
        .eq("source_asset_id", source_asset_id)
        .execute()
    )
    page_rows = response.data or []
    return {
        int(row["page_number"]): row
        for row in page_rows
        if row.get("id") and row.get("page_number") is not None
    }


def download_drive_file_bytes(service, drive_file_id: str) -> bytes:
    request = service.files().get_media(fileId=drive_file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def validate_pdf_bytes(content: bytes):
    if len(content) <= MIN_PDF_BYTES:
        raise ValueError(f"PDF demasiado pequeno: {len(content)} bytes")
    if not content.startswith(b"%PDF"):
        raise ValueError("Magic bytes invalidos: no inicia con %PDF")


def can_attempt_ocr_page(ocr_config: dict | None) -> bool:
    if not ocr_config or not ocr_config.get("enabled"):
        return False
    max_pages = ocr_config.get("max_pages")
    attempted = ocr_config.get("runtime", {}).get("pages_attempted", 0)
    return max_pages is None or attempted < max_pages


def render_page_for_ocr(page, dpi: int, pil_image_module):
    zoom = dpi / 72
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    image = pil_image_module.open(io.BytesIO(pix.tobytes("png")))
    image.load()
    return image


def build_sensitive_values_detected(text_value: str) -> list[dict]:
    candidates: list[tuple[str, str]] = []
    for match in TEMPERATURE_COMBINED_RE.finditer(text_value or ""):
        candidates.append(("temperature", normalize_text(match.group(0))))
    for match in DATE_RE.finditer(text_value or ""):
        candidates.append(("date", normalize_text(match.group(0))))
    for match in ARTICLE_REF_RE.finditer(text_value or ""):
        candidates.append(("article", normalize_text(match.group(0))))
    for match in NUMERAL_REF_RE.finditer(text_value or ""):
        candidates.append(("numeral", normalize_text(match.group(0))))

    seen: set[tuple[str, str]] = set()
    output: list[dict] = []
    for kind, value in candidates:
        key = (kind, value)
        if key in seen:
            continue
        seen.add(key)
        output.append({"type": kind, "value": value})
    return output


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _line_to_text(words: list[dict]) -> str:
    if not words:
        return ""
    ordered = sorted(words, key=lambda item: item["left"])
    chunks: list[str] = []
    last_right = None
    for word in ordered:
        token = word["text"]
        if not token:
            continue
        if chunks and last_right is not None:
            gap = word["left"] - last_right
            if gap > max(8, word["height"] * 0.45):
                chunks.append(" ")
        chunks.append(token)
        last_right = word["right"]
    return normalize_text("".join(chunks))


def _looks_like_index_line(words: list[dict]) -> bool:
    if len(words) < 3:
        return False
    ordered = sorted(words, key=lambda item: item["left"])
    last_word = ordered[-1]["text"]
    if not INDEX_PAGE_RE.match(last_word):
        return False
    gap = ordered[-1]["left"] - ordered[-2]["right"]
    return gap >= max(45, int(ordered[-1]["height"] * 2.8))


def rebuild_layout_aware_text(ocr_data: dict) -> dict:
    words: list[dict] = []
    text_values = ocr_data.get("text", []) if isinstance(ocr_data, dict) else []
    total = len(text_values)
    for index in range(total):
        token = normalize_text(text_values[index])
        if not token:
            continue
        conf_value = _safe_float((ocr_data.get("conf") or [0])[index], 0.0)
        left = _safe_int((ocr_data.get("left") or [0])[index], 0)
        top = _safe_int((ocr_data.get("top") or [0])[index], 0)
        width = _safe_int((ocr_data.get("width") or [0])[index], 0)
        height = _safe_int((ocr_data.get("height") or [0])[index], 0)
        if width <= 0 or height <= 0:
            continue
        words.append(
            {
                "text": token,
                "conf": conf_value,
                "left": left,
                "top": top,
                "width": width,
                "height": height,
                "right": left + width,
                "center_y": top + (height / 2.0),
            }
        )

    if not words:
        return {
            "text_normalized": "",
            "line_count": 0,
            "possible_numbered_list_split": False,
            "possible_index_alignment_issue": False,
            "possible_bad_layout_order": False,
            "numbered_list_merge_count": 0,
            "index_line_count": 0,
            "index_alignment_fail_count": 0,
        }

    words.sort(key=lambda item: (item["center_y"], item["left"]))
    avg_height = sum(item["height"] for item in words) / max(1, len(words))
    y_tolerance = max(4.0, min(18.0, avg_height * 0.65))

    visual_lines: list[dict] = []
    for word in words:
        if not visual_lines:
            visual_lines.append(
                {
                    "center_y": word["center_y"],
                    "top": word["top"],
                    "bottom": word["top"] + word["height"],
                    "words": [word],
                }
            )
            continue

        line = visual_lines[-1]
        if abs(word["center_y"] - line["center_y"]) <= y_tolerance:
            line["words"].append(word)
            line["center_y"] = (line["center_y"] + word["center_y"]) / 2.0
            line["top"] = min(line["top"], word["top"])
            line["bottom"] = max(line["bottom"], word["top"] + word["height"])
        else:
            visual_lines.append(
                {
                    "center_y": word["center_y"],
                    "top": word["top"],
                    "bottom": word["top"] + word["height"],
                    "words": [word],
                }
            )

    numbered_list_merge_count = 0
    unmerged_numeral_lines = 0
    merged_lines: list[dict] = []
    index = 0
    while index < len(visual_lines):
        current = visual_lines[index]
        current_words = sorted(current["words"], key=lambda item: item["left"])
        current_text = _line_to_text(current_words)
        token_only = len(current_words) == 1 and NUMERAL_LINE_RE.match(current_text or "")

        if token_only and index + 1 < len(visual_lines):
            next_line = visual_lines[index + 1]
            next_words = sorted(next_line["words"], key=lambda item: item["left"])
            next_text = _line_to_text(next_words)
            gap_vertical = next_line["top"] - current["bottom"]
            first_next_left = next_words[0]["left"] if next_words else 0
            first_curr_right = current_words[0]["right"]
            aligns_horizontally = first_next_left >= first_curr_right - 6
            if next_text and gap_vertical <= max(20, avg_height * 1.8) and aligns_horizontally:
                merged_words = current_words + next_words
                merged_lines.append(
                    {
                        "words": merged_words,
                        "top": min(current["top"], next_line["top"]),
                    }
                )
                numbered_list_merge_count += 1
                index += 2
                continue

            unmerged_numeral_lines += 1

        merged_lines.append({"words": current_words, "top": current["top"]})
        index += 1

    merged_lines.sort(key=lambda item: item["top"])
    text_lines: list[str] = []
    index_line_count = 0
    index_alignment_fail_count = 0
    for line in merged_lines:
        words_in_line = sorted(line["words"], key=lambda item: item["left"])
        line_text = _line_to_text(words_in_line)
        if not line_text:
            continue
        if _looks_like_index_line(words_in_line):
            left_text = _line_to_text(words_in_line[:-1])
            right_page = words_in_line[-1]["text"]
            if left_text and right_page:
                line_text = f"{left_text} .......... {right_page}"
                index_line_count += 1
            else:
                index_alignment_fail_count += 1
        text_lines.append(line_text)

    dense_fragmentation = len([line for line in text_lines if len(line) <= 3]) >= 6
    return {
        "text_normalized": cautious_normalize_page_text("\n".join(text_lines)),
        "line_count": len(text_lines),
        "possible_numbered_list_split": numbered_list_merge_count > 0 or unmerged_numeral_lines > 0,
        "possible_index_alignment_issue": index_alignment_fail_count > 0,
        "possible_bad_layout_order": dense_fragmentation or unmerged_numeral_lines >= 3,
        "numbered_list_merge_count": numbered_list_merge_count,
        "index_line_count": index_line_count,
        "index_alignment_fail_count": index_alignment_fail_count,
    }


def run_ocr_on_page(page, ocr_config: dict, source_asset_id: int, page_number: int) -> dict:
    runtime = ocr_config["runtime"]
    runtime["pages_attempted"] += 1

    try:
        image = render_page_for_ocr(
            page=page,
            dpi=ocr_config["dpi"],
            pil_image_module=ocr_config["pil_image_module"],
        )
        ocr_output = ocr_config["pytesseract"].Output
        ocr_data = ocr_config["pytesseract"].image_to_data(
            image,
            lang=ocr_config["lang"],
            output_type=ocr_output.DICT,
        )
        ocr_raw = ocr_config["pytesseract"].image_to_string(
            image,
            lang=ocr_config["lang"],
        )
        text_raw = normalize_page_text(ocr_raw or "")
        layout_result = rebuild_layout_aware_text(ocr_data)
        text_normalized = layout_result["text_normalized"] or cautious_normalize_page_text(text_raw)
        text_chars = len(text_normalized)

        if has_useful_text(text_normalized):
            runtime["pages_success"] += 1
            runtime["ocr_text_chars"] += text_chars
            return {
                "ok": True,
                "text_raw": text_raw,
                "text_normalized": text_normalized,
                "text_chars": text_chars,
                "layout_result": layout_result,
                "error": None,
            }

        runtime["pages_failed"] += 1
        return {
            "ok": False,
            "text_raw": "",
            "text_normalized": "",
            "text_chars": 0,
            "layout_result": None,
            "error": f"pagina {page_number}: OCR sin texto util",
        }
    except Exception as exc:
        runtime["pages_failed"] += 1
        return {
            "ok": False,
            "text_raw": "",
            "text_normalized": "",
            "text_chars": 0,
            "layout_result": None,
            "error": f"pagina {page_number}: OCR error: {exc}",
        }


def extract_pages_from_pdf_bytes(
    pdf_bytes: bytes,
    source_asset_id: int,
    source_asset_file_name: str,
    source_asset_subtipo: str | None,
    ocr_config: dict | None = None,
) -> list[dict]:
    extracted_at = datetime.now(timezone.utc).isoformat()
    pages: list[dict] = []

    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        for page_number, page in enumerate(pdf, start=1):
            embedded_text_raw = normalize_page_text(page.get_text("text") or "")
            embedded_text_normalized = cautious_normalize_page_text(embedded_text_raw)
            text_raw = embedded_text_raw
            text_normalized = embedded_text_normalized
            extraction_method = "pymupdf"
            ocr_used = False
            ocr_error = None
            ocr_attempted = False
            layout_signals = None
            was_empty_embedded_text = not has_useful_text(embedded_text_normalized)

            if was_empty_embedded_text and can_attempt_ocr_page(ocr_config):
                ocr_attempted = True
                ocr_result = run_ocr_on_page(
                    page=page,
                    ocr_config=ocr_config,
                    source_asset_id=source_asset_id,
                    page_number=page_number,
                )
                ocr_error = ocr_result.get("error")
                if ocr_result.get("ok"):
                    text_raw = ocr_result["text_raw"]
                    text_normalized = ocr_result["text_normalized"]
                    layout_signals = ocr_result.get("layout_result")
                    extraction_method = OCR_ENGINE_NAME
                    ocr_used = True

            page_review = build_page_review_metadata(
                text_raw=text_raw,
                text_normalized=text_normalized,
                ocr_used=ocr_used,
                layout_signals=layout_signals,
            )
            page_text_length = len(text_normalized)
            page_word_count = len(text_normalized.split()) if text_normalized else 0
            metadata = {
                "source_asset_id": source_asset_id,
                "source_asset_file_name": source_asset_file_name,
                "source_asset_subtipo": source_asset_subtipo,
                "page_text_length": page_text_length,
                "page_word_count": page_word_count,
                "extracted_at": extracted_at,
                "extraction_version": EXTRACTION_VERSION,
                "review": page_review,
            }
            if ocr_attempted:
                metadata["ocr"] = {
                    "ocr_engine": OCR_ENGINE_NAME,
                    "ocr_lang": ocr_config["lang"] if ocr_config else None,
                    "ocr_dpi": ocr_config["dpi"] if ocr_config else None,
                    "text_chars": page_text_length,
                    "was_empty_embedded_text": True,
                }
                if layout_signals:
                    metadata["ocr"]["layout"] = layout_signals
                if ocr_error:
                    metadata["ocr"]["error"] = ocr_error

            pages.append(
                {
                    "page_number": page_number,
                    "text_raw": text_raw,
                    "text_normalized": text_normalized,
                    "has_tables": detect_table_like_text(text_raw),
                    "metadata": metadata,
                    "extraction_method": extraction_method,
                    "ocr_used": ocr_used,
                    "ocr_attempted": ocr_attempted,
                    "ocr_error": ocr_error,
                    "had_embedded_text": not was_empty_embedded_text,
                }
            )

    return pages


def build_page_payload(norma_id: str, asset: dict, page: dict) -> dict:
    asset_subtipo = normalize_text(asset.get("asset_subtipo")) or None
    file_name = normalize_text(asset.get("file_name")) or f"asset_{asset['id']}.pdf"
    document_part = asset_subtipo or file_name
    metadata_patch = {
        "source_asset_id": asset["id"],
        "asset_subtipo": asset_subtipo,
        "document_part": document_part,
        "text_chars": len(page["text_normalized"]),
    }
    if page.get("ocr_used"):
        metadata_patch["ocr_engine"] = OCR_ENGINE_NAME
        metadata_patch["ocr_lang"] = page.get("ocr_lang")
        metadata_patch["ocr_dpi"] = page.get("ocr_dpi")
        metadata_patch["was_empty_embedded_text"] = True
    metadata = deep_merge_dicts(page["metadata"], metadata_patch)
    return {
        "norma_id": norma_id,
        "source_asset_id": asset["id"],
        "asset_subtipo": asset_subtipo,
        "document_part": document_part,
        "page_number": page["page_number"],
        "text_raw": page["text_raw"],
        "text_normalized": page["text_normalized"],
        "extraction_method": page.get("extraction_method") or "pymupdf",
        "ocr_used": bool(page.get("ocr_used")),
        "has_tables": page["has_tables"],
        "metadata": metadata,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def write_page_record(
    supabase,
    payload: dict,
    existing: dict | None,
    apply_changes: bool,
    force: bool,
    update_existing_normalized: bool,
    operations: list[dict],
) -> str:
    page_number = payload["page_number"]
    source_asset_id = payload["source_asset_id"]

    if existing and update_existing_normalized:
        operations.append(
            {
                "action": "update_existing_normalized",
                "page_id": existing.get("id"),
                "source_asset_id": source_asset_id,
                "page_number": page_number,
            }
        )
        if not apply_changes:
            return "planned_update_normalized"
        supabase.table(PAGE_TABLE_NAME).update(
            {
                "text_normalized": payload["text_normalized"],
                "metadata": payload["metadata"],
            }
        ).eq("id", existing["id"]).execute()
        return "updated_normalized"

    if existing and not force:
        operations.append(
            {
                "action": "reuse_page",
                "page_id": existing.get("id"),
                "source_asset_id": source_asset_id,
                "page_number": page_number,
            }
        )
        return "reused"

    if existing and force:
        operations.append(
            {
                "action": "update_page",
                "page_id": existing.get("id"),
                "source_asset_id": source_asset_id,
                "page_number": page_number,
            }
        )
        if not apply_changes:
            return "planned_update"

        supabase.table(PAGE_TABLE_NAME).update(payload).eq("id", existing["id"]).execute()
        return "updated"

    if update_existing_normalized and not existing:
        operations.append(
            {
                "action": "skip_missing_page_for_update_normalized",
                "source_asset_id": source_asset_id,
                "page_number": page_number,
            }
        )
        return "skipped_update_normalized_missing"

    operations.append(
        {
            "action": "insert_page",
            "source_asset_id": source_asset_id,
            "page_number": page_number,
        }
    )
    if not apply_changes:
        return "planned_insert"

    supabase.table(PAGE_TABLE_NAME).insert(payload).execute()
    return "inserted"


def process_asset(
    service,
    supabase,
    norma_id: str,
    asset: dict,
    apply_changes: bool,
    force: bool,
    update_existing_normalized: bool,
    operations: list[dict],
    ocr_config: dict | None = None,
) -> dict:
    asset_result = {
        "asset_id": asset.get("id"),
        "asset_subtipo": asset.get("asset_subtipo"),
        "file_name": asset.get("file_name"),
        "drive_file_id": asset.get("drive_file_id"),
        "status": "pending",
        "requires_ocr": False,
        "ocr_reason": None,
        "ocr_enabled": bool(ocr_config and ocr_config.get("enabled")),
        "ocr_dependencies_ok": (
            ocr_config.get("dependencies_ok") if ocr_config else None
        ),
        "ocr_engine": ocr_config.get("engine") if ocr_config else None,
        "ocr_lang": ocr_config.get("lang") if ocr_config else None,
        "ocr_dpi": ocr_config.get("dpi") if ocr_config else None,
        "pdf_size_bytes": None,
        "pages_total": 0,
        "pages_with_text": 0,
        "pages_without_text": 0,
        "skipped_empty_pages": 0,
        "pages_inserted": 0,
        "pages_updated": 0,
        "pages_updated_normalized": 0,
        "pages_reused": 0,
        "pages_planned_insert": 0,
        "pages_planned_update": 0,
        "pages_planned_update_normalized": 0,
        "pages_ocr_attempted": 0,
        "pages_ocr_success": 0,
        "pages_ocr_failed": 0,
        "ocr_text_chars": 0,
        "total_text_chars": 0,
        "errors": [],
    }

    drive_file_id = normalize_text(asset.get("drive_file_id"))
    if not drive_file_id:
        asset_result["errors"].append("Asset sin drive_file_id")
        asset_result["status"] = "error"
        return asset_result

    pdf_bytes = download_drive_file_bytes(service, drive_file_id)
    validate_pdf_bytes(pdf_bytes)
    asset_result["pdf_size_bytes"] = len(pdf_bytes)

    source_asset_file_name = normalize_text(asset.get("file_name")) or f"asset_{asset['id']}.pdf"
    source_asset_subtipo = normalize_text(asset.get("asset_subtipo")) or None
    ocr_runtime_before = deepcopy(ocr_config.get("runtime", {})) if ocr_config else None
    pages = extract_pages_from_pdf_bytes(
        pdf_bytes=pdf_bytes,
        source_asset_id=asset["id"],
        source_asset_file_name=source_asset_file_name,
        source_asset_subtipo=source_asset_subtipo,
        ocr_config=ocr_config,
    )
    if ocr_config and ocr_runtime_before is not None:
        runtime_after = ocr_config["runtime"]
        asset_result["pages_ocr_attempted"] = (
            runtime_after["pages_attempted"] - ocr_runtime_before.get("pages_attempted", 0)
        )
        asset_result["pages_ocr_success"] = (
            runtime_after["pages_success"] - ocr_runtime_before.get("pages_success", 0)
        )
        asset_result["pages_ocr_failed"] = (
            runtime_after["pages_failed"] - ocr_runtime_before.get("pages_failed", 0)
        )
        asset_result["ocr_text_chars"] = (
            runtime_after["ocr_text_chars"] - ocr_runtime_before.get("ocr_text_chars", 0)
        )

    asset_result["pages_total"] = len(pages)
    asset_result["pages_with_text"] = sum(
        1 for page in pages if has_useful_text(page["text_normalized"])
    )
    asset_result["pages_without_text"] = (
        asset_result["pages_total"] - asset_result["pages_with_text"]
    )
    asset_result["total_text_chars"] = sum(
        len(page["text_normalized"])
        for page in pages
        if has_useful_text(page["text_normalized"])
    )

    if asset_result["pages_total"] > 0 and asset_result["total_text_chars"] == 0:
        asset_result["requires_ocr"] = True
        asset_result["ocr_reason"] = (
            "PDF sin texto util despues de extraccion"
            if ocr_config
            else "PDF sin capa de texto extraible"
        )
        asset_result["status"] = "ocr_required"
        asset_result["skipped_empty_pages"] = asset_result["pages_total"]
        return asset_result

    existing_pages = get_existing_pages_by_asset(
        supabase=supabase,
        norma_id=norma_id,
        source_asset_id=asset["id"],
    )

    for page in pages:
        if not has_useful_text(page["text_normalized"]):
            asset_result["skipped_empty_pages"] += 1
            operations.append(
                {
                    "action": "skip_empty_page",
                    "source_asset_id": asset["id"],
                    "page_number": page["page_number"],
                    "reason": "empty_text_after_ocr" if page.get("ocr_attempted") else "empty_text",
                }
            )
            if page.get("ocr_error"):
                asset_result["errors"].append(page["ocr_error"])
            continue

        page_number = page["page_number"]
        if page.get("ocr_used") and isinstance(page.get("metadata"), dict):
            ocr_meta = page["metadata"].get("ocr")
            if isinstance(ocr_meta, dict):
                ocr_meta["source_asset_id"] = asset["id"]
                ocr_meta["asset_subtipo"] = source_asset_subtipo
                ocr_meta["document_part"] = (
                    source_asset_subtipo or source_asset_file_name
                )
            page["ocr_lang"] = ocr_config.get("lang") if ocr_config else None
            page["ocr_dpi"] = ocr_config.get("dpi") if ocr_config else None
        payload = build_page_payload(norma_id=norma_id, asset=asset, page=page)
        existing = existing_pages.get(page_number)
        try:
            action = write_page_record(
                supabase=supabase,
                payload=payload,
                existing=existing,
                apply_changes=apply_changes,
                force=force,
                update_existing_normalized=update_existing_normalized,
                operations=operations,
            )
            if action == "inserted":
                asset_result["pages_inserted"] += 1
            elif action == "updated":
                asset_result["pages_updated"] += 1
            elif action == "updated_normalized":
                asset_result["pages_updated_normalized"] += 1
            elif action == "reused":
                asset_result["pages_reused"] += 1
            elif action == "planned_insert":
                asset_result["pages_planned_insert"] += 1
            elif action == "planned_update":
                asset_result["pages_planned_update"] += 1
            elif action == "planned_update_normalized":
                asset_result["pages_planned_update_normalized"] += 1
        except Exception as exc:
            asset_result["errors"].append(
                f"pagina {page_number}: {exc}"
            )

    if asset_result["errors"]:
        asset_result["status"] = "error"
    elif asset_result["pages_without_text"] == 0 and asset_result["pages_ocr_success"] > 0:
        asset_result["status"] = "text_extracted_ocr"
    elif asset_result["pages_with_text"] > 0 and asset_result["pages_without_text"] > 0:
        asset_result["requires_ocr"] = True
        asset_result["ocr_reason"] = (
            "OCR parcial: algunas paginas quedaron sin texto util"
            if asset_result["pages_ocr_success"] > 0
            else "PDF con paginas mixtas: algunas sin texto extraible"
        )
        asset_result["status"] = "partial_text"
    else:
        asset_result["status"] = "text_extracted"

    return asset_result


def summarize_results(asset_results: list[dict]) -> dict:
    summary = {
        "pdf_assets_total": len(asset_results),
        "pdf_assets_processed": 0,
        "total_pages_extracted": 0,
        "total_text_chars": 0,
        "pages_with_text": 0,
        "pages_without_text": 0,
        "skipped_empty_pages": 0,
        "pages_inserted": 0,
        "pages_updated": 0,
        "pages_updated_normalized": 0,
        "pages_reused": 0,
        "pages_planned_insert": 0,
        "pages_planned_update": 0,
        "pages_planned_update_normalized": 0,
        "pages_ocr_attempted": 0,
        "pages_ocr_success": 0,
        "pages_ocr_failed": 0,
        "ocr_text_chars": 0,
        "ocr_enabled": False,
        "ocr_dependencies_ok": None,
        "ocr_engine": None,
        "ocr_lang": None,
        "ocr_dpi": None,
        "requires_ocr": False,
        "errors_count": 0,
    }
    for item in asset_results:
        has_errors = bool(item.get("errors"))
        if not has_errors and item.get("status") != "ocr_required":
            summary["pdf_assets_processed"] += 1
        summary["total_pages_extracted"] += item.get("pages_total", 0)
        summary["total_text_chars"] += item.get("total_text_chars", 0)
        summary["pages_with_text"] += item.get("pages_with_text", 0)
        summary["pages_without_text"] += item.get("pages_without_text", 0)
        summary["skipped_empty_pages"] += item.get("skipped_empty_pages", 0)
        summary["pages_inserted"] += item.get("pages_inserted", 0)
        summary["pages_updated"] += item.get("pages_updated", 0)
        summary["pages_updated_normalized"] += item.get("pages_updated_normalized", 0)
        summary["pages_reused"] += item.get("pages_reused", 0)
        summary["pages_planned_insert"] += item.get("pages_planned_insert", 0)
        summary["pages_planned_update"] += item.get("pages_planned_update", 0)
        summary["pages_planned_update_normalized"] += item.get("pages_planned_update_normalized", 0)
        summary["pages_ocr_attempted"] += item.get("pages_ocr_attempted", 0)
        summary["pages_ocr_success"] += item.get("pages_ocr_success", 0)
        summary["pages_ocr_failed"] += item.get("pages_ocr_failed", 0)
        summary["ocr_text_chars"] += item.get("ocr_text_chars", 0)
        summary["ocr_enabled"] = summary["ocr_enabled"] or bool(item.get("ocr_enabled"))
        if summary["ocr_dependencies_ok"] is None:
            summary["ocr_dependencies_ok"] = item.get("ocr_dependencies_ok")
        if summary["ocr_engine"] is None:
            summary["ocr_engine"] = item.get("ocr_engine")
        if summary["ocr_lang"] is None:
            summary["ocr_lang"] = item.get("ocr_lang")
        if summary["ocr_dpi"] is None:
            summary["ocr_dpi"] = item.get("ocr_dpi")
        summary["requires_ocr"] = summary["requires_ocr"] or bool(item.get("requires_ocr"))
        summary["errors_count"] += len(item.get("errors", []))
    return summary


def build_norma_update_payload(norma_row: dict, summary: dict, text_extracted_at: str, status: str) -> dict:
    existing_raw = norma_row.get("raw") if isinstance(norma_row.get("raw"), dict) else {}
    raw_patch = {
        "text_extraction_version": EXTRACTION_VERSION,
        "text_extracted_at": text_extracted_at,
        "pdf_assets_processed": summary["pdf_assets_processed"],
        "total_pages_extracted": summary["total_pages_extracted"],
        "total_text_chars": summary["total_text_chars"],
        "pages_with_text": summary["pages_with_text"],
        "pages_without_text": summary["pages_without_text"],
        "ocr_required": summary["requires_ocr"],
        "pages_ocr_attempted": summary.get("pages_ocr_attempted", 0),
        "pages_ocr_success": summary.get("pages_ocr_success", 0),
        "pages_ocr_failed": summary.get("pages_ocr_failed", 0),
        "ocr_text_chars": summary.get("ocr_text_chars", 0),
        "ocr_lang": summary.get("ocr_lang"),
        "ocr_dpi": summary.get("ocr_dpi"),
        "ocr_engine": summary.get("ocr_engine"),
        "errors_count": summary["errors_count"],
    }
    merged_raw = deep_merge_dicts(existing_raw, raw_patch)
    return {
        "raw": merged_raw,
        "ocr_required": summary["requires_ocr"],
        "process_status": status,
        "updated_at": text_extracted_at,
    }


def build_dependency_failure_report(args, error_message: str) -> dict:
    summary = {
        "pdf_assets_total": 0,
        "pdf_assets_processed": 0,
        "total_pages_extracted": 0,
        "total_text_chars": 0,
        "pages_with_text": 0,
        "pages_without_text": 0,
        "skipped_empty_pages": 0,
        "pages_inserted": 0,
        "pages_updated": 0,
        "pages_updated_normalized": 0,
        "pages_reused": 0,
        "pages_planned_insert": 0,
        "pages_planned_update": 0,
        "pages_planned_update_normalized": 0,
        "pages_ocr_attempted": 0,
        "pages_ocr_success": 0,
        "pages_ocr_failed": 0,
        "ocr_text_chars": 0,
        "ocr_enabled": True,
        "ocr_dependencies_ok": False,
        "ocr_engine": OCR_ENGINE_NAME,
        "ocr_lang": normalize_text(args.ocr_lang) or "spa",
        "ocr_dpi": args.ocr_dpi,
        "requires_ocr": True,
        "errors_count": 1,
    }
    return {
        "mode": args.mode,
        "document_key": args.document_key,
        "force": args.force,
        "update_existing_normalized": args.update_existing_normalized,
        "ocr": {
            "enabled": True,
            "ocr_engine": OCR_ENGINE_NAME,
            "ocr_lang": normalize_text(args.ocr_lang) or "spa",
            "ocr_dpi": args.ocr_dpi,
            "max_pages": args.max_pages,
            "ocr_dependencies_ok": False,
        },
        "ocr_enabled": True,
        "planned_process_status": "ocr_dependency_error",
        "summary": summary,
        "assets": [],
        "operations": [],
        "dependency_error": error_message,
        "extraction_version": EXTRACTION_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def write_report_files(mode: str, report_payload: dict):
    ensure_reports_dir()
    RESULT_JSON_PATH.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = report_payload.get("summary", {})
    lines = [
        f"# DIGEMID Normativa Text Extraction - {mode}",
        "",
        f"- Mode: `{mode}`",
        f"- Document key: `{report_payload.get('document_key')}`",
        f"- Force: `{report_payload.get('force')}`",
        f"- Update existing normalized only: `{report_payload.get('update_existing_normalized')}`",
        f"- PDF assets total: **{summary.get('pdf_assets_total', 0)}**",
        f"- PDF assets processed ok: **{summary.get('pdf_assets_processed', 0)}**",
        f"- Total pages extracted: **{summary.get('total_pages_extracted', 0)}**",
        f"- Total text chars: **{summary.get('total_text_chars', 0)}**",
        f"- Pages with text / without text: **{summary.get('pages_with_text', 0)} / {summary.get('pages_without_text', 0)}**",
        f"- Skipped empty pages: **{summary.get('skipped_empty_pages', 0)}**",
        f"- Requires OCR: `{summary.get('requires_ocr', False)}`",
        f"- OCR enabled: `{summary.get('ocr_enabled', False)}`",
        f"- OCR dependencies ok: `{summary.get('ocr_dependencies_ok')}`",
        f"- OCR engine: `{summary.get('ocr_engine') or ''}`",
        f"- OCR lang: `{summary.get('ocr_lang') or ''}`",
        f"- OCR dpi: `{summary.get('ocr_dpi')}`",
        f"- OCR attempted/success/failed: **{summary.get('pages_ocr_attempted', 0)} / {summary.get('pages_ocr_success', 0)} / {summary.get('pages_ocr_failed', 0)}**",
        f"- OCR text chars: **{summary.get('ocr_text_chars', 0)}**",
        f"- Planned process_status: `{report_payload.get('planned_process_status')}`",
        f"- Pages inserted/updated/reused: **{summary.get('pages_inserted', 0)} / {summary.get('pages_updated', 0)} / {summary.get('pages_reused', 0)}**",
        f"- Pages updated normalized only: **{summary.get('pages_updated_normalized', 0)}**",
        f"- Planned inserts/updates: **{summary.get('pages_planned_insert', 0)} / {summary.get('pages_planned_update', 0)}**",
        f"- Planned update normalized only: **{summary.get('pages_planned_update_normalized', 0)}**",
        f"- Errors: **{summary.get('errors_count', 0)}**",
        "",
    ]

    dependency_error = report_payload.get("dependency_error")
    if dependency_error:
        lines.extend([
            "## OCR Dependency Error",
            "",
            f"- Message: `{dependency_error}`",
            "",
        ])

    lines.extend([
        "## PDFs",
        "",
    ])

    for item in report_payload.get("assets", []):
        lines.append(
            f"### asset_id={item.get('asset_id')} | subtipo={item.get('asset_subtipo') or ''}"
        )
        lines.append(f"- file_name: `{item.get('file_name') or ''}`")
        lines.append(f"- drive_file_id: `{item.get('drive_file_id') or ''}`")
        lines.append(f"- status: `{item.get('status')}`")
        lines.append(f"- requires_ocr: `{item.get('requires_ocr')}`")
        lines.append(f"- ocr_reason: `{item.get('ocr_reason') or ''}`")
        lines.append(f"- ocr_dependencies_ok: `{item.get('ocr_dependencies_ok')}`")
        lines.append(f"- ocr_engine: `{item.get('ocr_engine') or ''}`")
        lines.append(f"- ocr_lang: `{item.get('ocr_lang') or ''}`")
        lines.append(f"- ocr_dpi: `{item.get('ocr_dpi')}`")
        lines.append(f"- pdf_size_bytes: `{item.get('pdf_size_bytes')}`")
        lines.append(f"- pages_total: `{item.get('pages_total')}`")
        lines.append(
            f"- pages_with_text / pages_without_text: `{item.get('pages_with_text')}` / `{item.get('pages_without_text')}`"
        )
        lines.append(f"- skipped_empty_pages: `{item.get('skipped_empty_pages')}`")
        lines.append(
            f"- OCR attempted/success/failed: `{item.get('pages_ocr_attempted')}` / `{item.get('pages_ocr_success')}` / `{item.get('pages_ocr_failed')}`"
        )
        lines.append(f"- OCR text chars: `{item.get('ocr_text_chars')}`")
        lines.append(
            f"- pages inserted/updated/reused: `{item.get('pages_inserted')}` / `{item.get('pages_updated')}` / `{item.get('pages_reused')}`"
        )
        lines.append(f"- pages updated normalized only: `{item.get('pages_updated_normalized')}`")
        lines.append(
            f"- pages planned insert/update: `{item.get('pages_planned_insert')}` / `{item.get('pages_planned_update')}`"
        )
        lines.append(f"- pages planned update normalized only: `{item.get('pages_planned_update_normalized')}`")
        lines.append(f"- total_text_chars: `{item.get('total_text_chars')}`")
        if item.get("errors"):
            lines.append("- errors:")
            for err in item["errors"]:
                lines.append(f"  - {err}")
        lines.append("")

    target_path = DRY_RUN_REPORT_PATH if mode == "dry-run" else RESULT_REPORT_PATH
    target_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    other_path = RESULT_REPORT_PATH if mode == "dry-run" else DRY_RUN_REPORT_PATH
    other_path.write_text(
        f"# DIGEMID Normativa Text Extraction - {mode}\n\nEste reporte no fue generado en esta ejecucion.\n",
        encoding="utf-8",
    )


def process_norma(
    service,
    supabase,
    norma_row: dict,
    apply_changes: bool,
    force: bool,
    update_existing_normalized: bool,
    ocr_config: dict | None = None,
) -> dict:
    operations: list[dict] = []
    assets = get_pdf_assets(supabase, norma_row["id"])
    if not assets:
        raise ValueError(
            f"No hay assets pdf_original con drive_file_id para norma {norma_row.get('document_key')}"
        )

    asset_results: list[dict] = []
    for asset in assets:
        try:
            logger.info(
                "Procesando asset PDF %s (%s)",
                asset.get("id"),
                asset.get("asset_subtipo") or asset.get("file_name"),
            )
            result = process_asset(
                service=service,
                supabase=supabase,
                norma_id=norma_row["id"],
                asset=asset,
                apply_changes=apply_changes,
                force=force,
                update_existing_normalized=update_existing_normalized,
                operations=operations,
                ocr_config=ocr_config,
            )
        except Exception as exc:
            result = {
                "asset_id": asset.get("id"),
                "asset_subtipo": asset.get("asset_subtipo"),
                "file_name": asset.get("file_name"),
                "drive_file_id": asset.get("drive_file_id"),
                "pdf_size_bytes": None,
                "pages_total": 0,
                "pages_inserted": 0,
                "pages_updated": 0,
                "pages_reused": 0,
                "pages_planned_insert": 0,
                "pages_planned_update": 0,
                "pages_ocr_attempted": 0,
                "pages_ocr_success": 0,
                "pages_ocr_failed": 0,
                "ocr_text_chars": 0,
                "total_text_chars": 0,
                "errors": [str(exc)],
            }
        asset_results.append(result)

    summary = summarize_results(asset_results)
    text_extracted_at = datetime.now(timezone.utc).isoformat()
    assets_with_text = sum(1 for item in asset_results if item.get("pages_with_text", 0) > 0)
    assets_without_text = sum(
        1
        for item in asset_results
        if item.get("pages_total", 0) > 0 and item.get("total_text_chars", 0) == 0
    )
    all_assets_ok = summary["errors_count"] == 0 and assets_with_text == summary["pdf_assets_total"]

    if ocr_config and summary["ocr_enabled"]:
        if summary["pages_without_text"] == 0 and summary["pages_ocr_success"] > 0:
            process_status = "text_extracted_ocr"
            summary["requires_ocr"] = False
        elif summary["pages_ocr_success"] > 0 and summary["pages_without_text"] > 0:
            process_status = "text_extraction_partial"
            summary["requires_ocr"] = True
        elif summary["pages_ocr_attempted"] > 0 and summary["pages_ocr_success"] == 0:
            process_status = "ocr_required"
            summary["requires_ocr"] = True
        elif summary["total_text_chars"] == 0 and summary["total_pages_extracted"] > 0:
            process_status = "ocr_required"
            summary["requires_ocr"] = True
        elif assets_with_text > 0 and (assets_without_text > 0 or summary["errors_count"] > 0):
            process_status = "text_extraction_partial"
            summary["requires_ocr"] = True
        elif all_assets_ok:
            process_status = "text_extracted"
            summary["requires_ocr"] = False
        else:
            process_status = "text_extraction_partial"
            summary["requires_ocr"] = summary["pages_without_text"] > 0
    else:
        if summary["total_text_chars"] == 0 and summary["total_pages_extracted"] > 0:
            process_status = "ocr_required"
        elif assets_with_text > 0 and (assets_without_text > 0 or summary["errors_count"] > 0):
            process_status = "text_extraction_partial"
        elif all_assets_ok:
            process_status = "text_extracted"
        else:
            process_status = "text_extraction_partial"

    operations.append(
        {
            "action": "update_norma_status",
            "norma_id": norma_row["id"],
            "document_key": norma_row.get("document_key"),
            "process_status": process_status,
        }
    )
    if apply_changes:
        payload = build_norma_update_payload(
            norma_row=norma_row,
            summary=summary,
            text_extracted_at=text_extracted_at,
            status=process_status,
        )
        supabase.table(NORMA_TABLE_NAME).update(payload).eq("id", norma_row["id"]).execute()

    return {
        "norma_id": norma_row["id"],
        "document_key": norma_row.get("document_key"),
        "title": norma_row.get("titulo"),
        "force": force,
        "update_existing_normalized": update_existing_normalized,
        "mode": "apply" if apply_changes else "dry-run",
        "ocr_enabled": bool(ocr_config and ocr_config.get("enabled")),
        "planned_process_status": process_status,
        "summary": summary,
        "assets": asset_results,
        "operations": operations,
    }


def main():
    args = parse_args()
    load_env()
    try:
        ocr_config = build_ocr_config(args)
    except OCRDependencyError as exc:
        report_payload = build_dependency_failure_report(args, str(exc))
        write_report_files(args.mode, report_payload)
        logger.error(str(exc))
        raise SystemExit(str(exc)) from exc

    supabase = get_supabase()
    drive_service = get_drive_service()
    norma_row = get_norma_by_document_key(supabase, args.document_key)
    result = process_norma(
        service=drive_service,
        supabase=supabase,
        norma_row=norma_row,
        apply_changes=args.mode == "apply",
        force=args.force,
        update_existing_normalized=args.update_existing_normalized,
        ocr_config=ocr_config,
    )

    report_payload = {
        "mode": args.mode,
        "document_key": args.document_key,
        "force": args.force,
        "update_existing_normalized": args.update_existing_normalized,
        "ocr": {
            "enabled": bool(ocr_config and ocr_config.get("enabled")),
            "ocr_engine": ocr_config.get("engine") if ocr_config else None,
            "ocr_lang": ocr_config.get("lang") if ocr_config else None,
            "ocr_dpi": ocr_config.get("dpi") if ocr_config else None,
            "max_pages": ocr_config.get("max_pages") if ocr_config else None,
            "ocr_dependencies_ok": (
                ocr_config.get("dependencies_ok") if ocr_config else None
            ),
        },
        "extraction_version": EXTRACTION_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    write_report_files(args.mode, report_payload)
    logger.info("Reportes generados en %s", REPORTS_DIR)


if __name__ == "__main__":
    main()
