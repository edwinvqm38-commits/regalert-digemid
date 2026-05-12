import argparse
import csv
import hashlib
import json
import logging
import re
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag


DEFAULT_BASE_URL = "https://www.digemid.minsa.gob.pe/webDigemid/publicaciones/normas-legales/"
DEFAULT_OUTPUT_DIR = "reports"
JSON_REPORT_NAME = "digemid_normativa_inventory.json"
CSV_REPORT_NAME = "digemid_normativa_inventory.csv"
MD_REPORT_NAME = "digemid_normativa_inventory.md"

SKIP_HREF_PREFIXES = ("#", "javascript:", "mailto:", "tel:")
CONTAINER_TAGS = ("article", "li", "tr", "div", "p")
BLOCK_TERMS = {
    "leer mas",
    "leer más",
    "ver mas",
    "ver más",
    "descargar",
    "pdf",
}

CATEGORY_SLUGS = {
    "resolucion-directoral",
    "decreto-urgencia",
    "decreto-legislativo",
    "decreto-ley",
    "resolucion",
    "resolucion-suprema",
    "ley",
    "resolucion-ministerial",
    "decreto-supremo",
}

IGNORED_LINK_KEYS = ("author", "category", "listing_page", "navigation", "other")

TYPE_PATTERNS = [
    ("RM", re.compile(r"(?:R\.?\s*M\.?|RESOLUCI[ÓO]N\s+MINISTERIAL)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})", re.IGNORECASE)),
    ("DS", re.compile(r"(?:D\.?\s*S\.?|DECRETO\s+SUPREMO)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})", re.IGNORECASE)),
    ("RD", re.compile(r"(?:R\.?\s*D\.?|RESOLUCI[ÓO]N\s+DIRECTORAL)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})", re.IGNORECASE)),
    ("RS", re.compile(r"(?:R\.?\s*S\.?|RESOLUCI[ÓO]N\s+SUPREMA)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})", re.IGNORECASE)),
    ("LEY", re.compile(r"(?:\bLEY)\s*(?:N[°Oº.]*)?\s*(\d{1,6})(?:[-/](20\d{2}))?", re.IGNORECASE)),
    ("DL", re.compile(r"(?:D\.?\s*L\.?|DECRETO\s+LEGISLATIVO)\s*(?:N[°Oº.]*)?\s*(\d{1,4})(?:[-/](20\d{2}))?", re.IGNORECASE)),
    ("DU", re.compile(r"(?:D\.?\s*U\.?|DECRETO\s+DE\s+URGENCIA)\s*(?:N[°Oº.]*)?\s*(\d{1,4})(?:[-/](20\d{2}))?", re.IGNORECASE)),
]
NORMATIVE_HINT_RE = re.compile(
    r"\b(resoluci[óo]n|decreto|ley|normas?\s+legales?|r\.m\.|r\.d\.|d\.s\.|r\.s\.)\b",
    re.IGNORECASE,
)
DATE_SLASH_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")
YEAR_RE = re.compile(r"\b(20\d{2})\b")
PAGE_PATH_RE = re.compile(r"/page/(\d+)/?$", re.IGNORECASE)

MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "setiembre": 9,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
DATE_TEXTUAL_RE = re.compile(
    r"\b(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(20\d{2})\b",
    re.IGNORECASE,
)

CSV_FIELDS = [
    "document_key",
    "title",
    "tipo_norma_probable",
    "numero",
    "anio",
    "fecha_publicacion",
    "source_url",
    "categoria",
    "descripcion_corta",
    "read_more_url",
    "pdf_url",
    "pdf_urls_count",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = parser.parse_args()
    if args.max_pages <= 0:
        raise ValueError("--max-pages debe ser mayor que cero")
    return args


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    normalized_path = re.sub(r"/{2,}", "/", parsed.path or "/")
    return urlunparse(parsed._replace(path=normalized_path, fragment=""))


def build_page_candidates(base_url: str, page: int) -> list[str]:
    base_url = base_url.strip()
    if page <= 1:
        return [base_url]

    parsed = urlparse(base_url)
    base_path = parsed.path.rstrip("/")

    path_candidate = urlunparse(
        parsed._replace(path=f"{base_path}/page/{page}/", query="", fragment="")
    )
    query_map = parse_qs(parsed.query, keep_blank_values=True)
    query_map["paged"] = [str(page)]
    query_candidate = urlunparse(
        parsed._replace(query=urlencode(query_map, doseq=True), fragment="")
    )

    candidates = []
    for item in [path_candidate, query_candidate]:
        if item not in candidates:
            candidates.append(item)
    return candidates


def fetch_html(session: requests.Session, url: str) -> tuple[int, str, str]:
    response = session.get(url, timeout=30, allow_redirects=True)
    return response.status_code, response.text or "", response.url


def is_blocked_response(status_code: int, html_text: str) -> bool:
    sample = (html_text or "").lower()
    if status_code in {401, 403, 429, 503}:
        return True
    if "attention required! | cloudflare" in sample:
        return True
    if "cf-browser-verification" in sample or "__cf_chl_" in sample:
        return True
    return False


def detect_date_info(text: str) -> tuple[str | None, str | None]:
    value = clean_text(text)
    if not value:
        return None, None

    match = DATE_SLASH_RE.search(value)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        if year < 100:
            year += 2000
        try:
            normalized = date(year, month, day).isoformat()
            return normalized, match.group(0)
        except ValueError:
            return None, match.group(0)

    match = DATE_TEXTUAL_RE.search(value)
    if match:
        day = int(match.group(1))
        month_name = match.group(2).lower()
        year = int(match.group(3))
        month = MONTHS_ES.get(month_name)
        if month:
            try:
                normalized = date(year, month, day).isoformat()
                return normalized, match.group(0)
            except ValueError:
                return None, match.group(0)
        return None, match.group(0)

    return None, None


def infer_tipo_numero_anio(*parts: str) -> tuple[str | None, str | None, str | None]:
    combined = " ".join(clean_text(part) for part in parts if part).upper()
    for tipo, pattern in TYPE_PATTERNS:
        match = pattern.search(combined)
        if match:
            number = match.group(1).lstrip("0") or match.group(1)
            year = match.group(2) if match.lastindex and match.lastindex >= 2 else None
            return tipo, number, year
    return None, None, None


def infer_year_fallback(*parts: str) -> str | None:
    combined = " ".join(clean_text(part) for part in parts if part)
    match = YEAR_RE.search(combined)
    return match.group(1) if match else None


def build_document_key(
    tipo_norma: str | None,
    numero: str | None,
    anio: str | None,
    title: str,
    canonical_url: str,
) -> str:
    if tipo_norma and numero and anio:
        return f"{tipo_norma}-{numero}-{anio}"
    if tipo_norma and numero:
        return f"{tipo_norma}-{numero}"
    digest = hashlib.sha1(f"{title}|{canonical_url}".encode("utf-8")).hexdigest()[:10].upper()
    return f"NORM-{digest}"


def is_pdf_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.path.lower().endswith(".pdf"):
        return True
    return ".pdf" in url.lower()


def new_ignored_counter() -> dict[str, int]:
    return {key: 0 for key in IGNORED_LINK_KEYS}


def classify_ignored_url(url: str) -> str | None:
    parsed = urlparse(url)
    path = re.sub(r"/{2,}", "/", (parsed.path or "/").lower())
    path_no_slash = path.rstrip("/")
    parts = [segment for segment in path_no_slash.split("/") if segment]

    if "author" in parts:
        return "author"

    if PAGE_PATH_RE.search(path_no_slash):
        return "listing_page"

    if path_no_slash.endswith("/publicaciones/normas-legales") or path_no_slash.endswith("/normas-legales"):
        return "navigation"

    for marker in ("/publicaciones/normas-legales/", "/normas-legales/"):
        if marker in path_no_slash:
            tail = path_no_slash.split(marker, 1)[1].strip("/")
            if not tail:
                return "navigation"
            tail_parts = [segment for segment in tail.split("/") if segment]
            if tail_parts[:1] == ["page"] and len(tail_parts) >= 2 and tail_parts[1].isdigit():
                return "listing_page"
            if len(tail_parts) == 1 and tail_parts[0] in CATEGORY_SLUGS:
                return "category"
            break

    return None


def is_listing_like_url(url: str) -> bool:
    ignored_kind = classify_ignored_url(url)
    return ignored_kind in {"listing_page", "navigation", "category"}


def is_detail_page_url(url: str) -> bool:
    path = re.sub(r"/{2,}", "/", (urlparse(url).path or "").lower())
    if classify_ignored_url(url):
        return False
    return bool(re.search(r"/normas-legales/20\d{2}/[^/]+/?$", path))


def choose_canonical_detail_url(detail_urls: set[str]) -> str | None:
    if not detail_urls:
        return None
    ordered = sorted(
        detail_urls,
        key=lambda item: (
            is_listing_like_url(item),
            not is_detail_page_url(item),
            len(item),
        ),
    )
    return ordered[0]


def infer_categoria(page_url: str, detail_url: str | None, tipo_norma: str | None) -> str | None:
    if tipo_norma:
        mapping = {
            "RM": "resolucion_ministerial",
            "DS": "decreto_supremo",
            "RD": "resolucion_directoral",
            "RS": "resolucion_suprema",
            "LEY": "ley",
            "DL": "decreto_legislativo",
            "DU": "decreto_urgencia",
        }
        return mapping.get(tipo_norma, tipo_norma.lower())

    candidate = detail_url or page_url
    path = urlparse(candidate).path.strip("/")
    for segment in [seg for seg in path.split("/") if seg]:
        if segment.lower() in {"webdigemid", "publicaciones", "normas-legales"}:
            continue
        if re.fullmatch(r"20\d{2}", segment):
            continue
        if len(segment) >= 4:
            return segment.lower()
    return None


def choose_container(anchor: Tag) -> Tag:
    for parent in anchor.parents:
        if not isinstance(parent, Tag):
            continue
        if parent.name in CONTAINER_TAGS:
            return parent
    return anchor


def choose_title(anchor: Tag, container: Tag) -> str:
    anchor_text = clean_text(anchor.get_text(" "))
    if len(anchor_text) >= 8 and anchor_text.lower() not in BLOCK_TERMS:
        return anchor_text

    heading = container.find(["h1", "h2", "h3", "h4"])
    if heading:
        heading_text = clean_text(heading.get_text(" "))
        if len(heading_text) >= 8:
            return heading_text

    return clean_text(container.get_text(" "))[:320]


def choose_description(container: Tag, title: str) -> str | None:
    paragraph = container.find("p")
    if not paragraph:
        return None
    text = clean_text(paragraph.get_text(" "))
    if text and text != title and len(text) > 16:
        return text[:400]
    return None


def parse_inventory_items(html_text: str, page_url: str) -> tuple[list[dict], dict[str, int]]:
    soup = BeautifulSoup(html_text, "html.parser")
    anchors = soup.select(
        "article a[href], li a[href], h1 a[href], h2 a[href], h3 a[href], .post a[href], .entry a[href]"
    )

    ignored_counts = new_ignored_counter()
    seen_urls: set[str] = set()
    container_map: dict[int, dict] = {}

    for anchor in anchors:
        href = clean_text(anchor.get("href"))
        if not href or href.lower().startswith(SKIP_HREF_PREFIXES):
            continue

        url = normalize_url(urljoin(page_url, href))
        if url in seen_urls:
            continue
        seen_urls.add(url)

        ignored_type = classify_ignored_url(url)
        if ignored_type:
            ignored_counts[ignored_type] += 1
            continue

        container = choose_container(anchor)
        container_id = id(container)
        entry = container_map.get(container_id)
        if entry is None:
            title = choose_title(anchor, container)
            if len(title) < 8 or title.lower() in BLOCK_TERMS:
                continue

            context_text = clean_text(container.get_text(" "))
            fecha_iso, fecha_display = detect_date_info(f"{context_text} {title}")
            tipo_norma, numero, anio = infer_tipo_numero_anio(title, context_text)
            if not anio:
                anio = fecha_iso[:4] if fecha_iso else infer_year_fallback(context_text, title)
            entry = {
                "title": title,
                "context_text": context_text,
                "tipo_norma_probable": tipo_norma,
                "numero": numero,
                "anio": anio,
                "fecha_publicacion": fecha_iso or fecha_display,
                "source_url": page_url,
                "descripcion_corta": choose_description(container, title),
                "detail_urls": set(),
                "pdf_urls": set(),
                "_fecha_sort": fecha_iso or "",
            }
            container_map[container_id] = entry

        if is_pdf_url(url):
            entry["pdf_urls"].add(url)
        else:
            entry["detail_urls"].add(url)

    items: list[dict] = []
    for entry in container_map.values():
        canonical_detail = choose_canonical_detail_url(
            {url for url in entry["detail_urls"] if is_detail_page_url(url)}
        )
        canonical_url = canonical_detail or (sorted(entry["pdf_urls"])[0] if entry["pdf_urls"] else None)
        if not canonical_url:
            continue

        if not NORMATIVE_HINT_RE.search(f"{entry['title']} {canonical_url} {entry['context_text']}"):
            continue

        document_key = build_document_key(
            entry.get("tipo_norma_probable"),
            entry.get("numero"),
            entry.get("anio"),
            entry.get("title"),
            canonical_url,
        )
        pdf_urls = sorted(entry["pdf_urls"])
        items.append(
            {
                "document_key": document_key,
                "title": entry.get("title"),
                "tipo_norma_probable": entry.get("tipo_norma_probable"),
                "numero": entry.get("numero"),
                "anio": entry.get("anio"),
                "fecha_publicacion": entry.get("fecha_publicacion"),
                "source_url": canonical_detail or (pdf_urls[0] if pdf_urls else entry.get("source_url")),
                "categoria": infer_categoria(page_url, canonical_detail, entry.get("tipo_norma_probable")),
                "descripcion_corta": entry.get("descripcion_corta"),
                "read_more_url": canonical_detail,
                "pdf_url": pdf_urls[0] if pdf_urls else None,
                "pdf_urls": pdf_urls,
                "pdf_urls_count": len(pdf_urls),
                "_fecha_sort": entry.get("_fecha_sort") or "",
            }
        )

    return items, ignored_counts


def merge_canonical_entries(raw_items: list[dict]) -> tuple[list[dict], dict[str, list[dict]]]:
    by_doc_key: dict[str, list[dict]] = {}
    for item in raw_items:
        by_doc_key.setdefault(item["document_key"], []).append(item)

    merged_items: list[dict] = []
    duplicates_real: dict[str, list[dict]] = {}

    for doc_key, entries in by_doc_key.items():
        entries_sorted = sorted(
            entries,
            key=lambda row: (
                row.get("read_more_url") is None,
                len(row.get("pdf_urls", [])) * -1,
            ),
        )
        base = dict(entries_sorted[0])
        all_pdf_urls = set(base.get("pdf_urls", []))
        all_detail_urls = set(
            url
            for url in [base.get("read_more_url"), base.get("source_url")]
            if url and is_detail_page_url(url)
        )

        for other in entries_sorted[1:]:
            all_pdf_urls.update(other.get("pdf_urls", []))
            if other.get("read_more_url") and is_detail_page_url(other["read_more_url"]):
                all_detail_urls.add(other["read_more_url"])
            if other.get("source_url") and is_detail_page_url(other["source_url"]):
                all_detail_urls.add(other["source_url"])
            if not base.get("fecha_publicacion") and other.get("fecha_publicacion"):
                base["fecha_publicacion"] = other["fecha_publicacion"]
                base["_fecha_sort"] = other.get("_fecha_sort") or base.get("_fecha_sort", "")
            if not base.get("tipo_norma_probable") and other.get("tipo_norma_probable"):
                base["tipo_norma_probable"] = other["tipo_norma_probable"]
            if not base.get("numero") and other.get("numero"):
                base["numero"] = other["numero"]
            if not base.get("anio") and other.get("anio"):
                base["anio"] = other["anio"]
            if not base.get("descripcion_corta") and other.get("descripcion_corta"):
                base["descripcion_corta"] = other["descripcion_corta"]

        canonical_detail = choose_canonical_detail_url(
            {url for url in all_detail_urls if url and not is_pdf_url(url)}
        )
        pdf_urls_sorted = sorted(all_pdf_urls)
        base["read_more_url"] = canonical_detail
        base["source_url"] = canonical_detail or base.get("source_url")
        base["pdf_urls"] = pdf_urls_sorted
        base["pdf_url"] = pdf_urls_sorted[0] if pdf_urls_sorted else None
        base["pdf_urls_count"] = len(pdf_urls_sorted)
        base["categoria"] = infer_categoria(base.get("source_url") or "", canonical_detail, base.get("tipo_norma_probable"))

        merged_items.append(base)
        if len(all_detail_urls) > 1:
            duplicates_real[doc_key] = entries

    merged_items.sort(
        key=lambda row: (
            row.get("_fecha_sort") or "",
            row.get("anio") or "",
            row.get("title") or "",
        ),
        reverse=True,
    )
    return merged_items, duplicates_real


def crawl_inventory(base_url: str, max_pages: int) -> dict:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )

    visited_urls: set[str] = set()
    html_fingerprints: set[str] = set()
    raw_items: list[dict] = []
    blocked_pages: list[dict] = []
    fetched_pages: list[str] = []
    ignored_links = new_ignored_counter()

    for page in range(1, max_pages + 1):
        candidates = build_page_candidates(base_url, page)
        page_loaded = False
        for candidate in candidates:
            if candidate in visited_urls:
                continue
            visited_urls.add(candidate)
            logger.info("Crawling page %s: %s", page, candidate)
            try:
                status_code, html_text, final_url = fetch_html(session, candidate)
            except Exception as exc:
                logger.warning("No se pudo consultar %s: %s", candidate, exc)
                continue

            if is_blocked_response(status_code, html_text):
                blocked_pages.append(
                    {
                        "requested_url": candidate,
                        "final_url": final_url,
                        "status_code": status_code,
                    }
                )
                logger.warning("Respuesta bloqueada (%s): %s", status_code, candidate)
                continue

            fingerprint = hashlib.sha1(clean_text(html_text).encode("utf-8")).hexdigest()
            if fingerprint in html_fingerprints:
                logger.info("Contenido repetido detectado en %s, se omite.", final_url)
                continue
            html_fingerprints.add(fingerprint)
            fetched_pages.append(final_url)

            page_items, page_ignored = parse_inventory_items(html_text, final_url)
            for key in IGNORED_LINK_KEYS:
                ignored_links[key] += page_ignored.get(key, 0)
            logger.info("Items detectados en %s: %s", final_url, len(page_items))
            raw_items.extend(page_items)
            page_loaded = True
            break

        if not page_loaded and page > 1:
            logger.info("Sin contenido util en la pagina %s; se detiene el crawler.", page)
            break

    canonical_items, duplicates_real = merge_canonical_entries(raw_items)
    associated_pdfs_total = sum(len(item.get("pdf_urls", [])) for item in canonical_items)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "max_pages": max_pages,
        "fetched_pages": fetched_pages,
        "blocked_pages": blocked_pages,
        "ignored_links": ignored_links,
        "raw_detected_total": len(raw_items),
        "unique_detected_total": len(canonical_items),
        "canonical_norms_total": len(canonical_items),
        "associated_pdfs_total": associated_pdfs_total,
        "items": canonical_items,
        "duplicates": {
            "possible_real_duplicates_by_document_key": duplicates_real,
        },
    }


def count_by_type(items: list[dict]) -> dict[str, int]:
    counter = Counter((item.get("tipo_norma_probable") or "SIN_TIPO") for item in items)
    return dict(sorted(counter.items(), key=lambda pair: (-pair[1], pair[0])))


def count_by_year(items: list[dict]) -> dict[str, int]:
    counter = Counter((item.get("anio") or "SIN_ANIO") for item in items)
    return dict(sorted(counter.items(), key=lambda pair: (pair[0] == "SIN_ANIO", pair[0]), reverse=True))


def write_json_report(output_dir: Path, payload: dict):
    path = output_dir / JSON_REPORT_NAME
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("JSON generado: %s", path)


def write_csv_report(output_dir: Path, items: list[dict]):
    path = output_dir / CSV_REPORT_NAME
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for item in items:
            row = {field: item.get(field) for field in CSV_FIELDS}
            writer.writerow(row)
    logger.info("CSV generado: %s", path)


def write_markdown_report(output_dir: Path, payload: dict):
    items = payload["items"]
    by_type = count_by_type(items)
    by_year = count_by_year(items)
    without_number = [item for item in items if not item.get("numero")]
    recent_20 = items[:20]
    duplicates_key = payload.get("duplicates", {}).get("possible_real_duplicates_by_document_key", {})
    ignored_links = payload.get("ignored_links", {}) or {}
    ignored_total = sum(int(value) for value in ignored_links.values())

    lines: list[str] = [
        "# DIGEMID Normativa Inventory",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- base_url: `{payload.get('base_url')}`",
        f"- max_pages: `{payload.get('max_pages')}`",
        f"- páginas consultadas: **{len(payload.get('fetched_pages', []))}**",
        f"- total documentos detectados (raw): **{payload.get('raw_detected_total', 0)}**",
        f"- total normas canónicas: **{payload.get('canonical_norms_total', payload.get('unique_detected_total', 0))}**",
        f"- total PDFs asociados: **{payload.get('associated_pdfs_total', 0)}**",
        f"- total enlaces ignorados: **{ignored_total}**",
        f"- páginas bloqueadas: **{len(payload.get('blocked_pages', []))}**",
        "",
        "## Total por tipo de norma",
        "",
    ]

    for tipo, count in by_type.items():
        lines.append(f"- {tipo}: **{count}**")

    lines.extend(
        [
            "",
            "## Enlaces ignorados por tipo",
            "",
        ]
    )
    for key in IGNORED_LINK_KEYS:
        lines.append(f"- {key}: **{ignored_links.get(key, 0)}**")

    lines.extend(
        [
            "",
            "## Total por año",
            "",
        ]
    )
    for year, count in by_year.items():
        lines.append(f"- {year}: **{count}**")

    lines.extend(
        [
            "",
            "## Primeras 20 normas más recientes",
            "",
            "| document_key | fecha_publicacion | tipo | número | título |",
            "|---|---|---|---|---|",
        ]
    )
    for item in recent_20:
        lines.append(
            "| {document_key} | {fecha_publicacion} | {tipo} | {numero} | {title} |".format(
                document_key=item.get("document_key") or "",
                fecha_publicacion=item.get("fecha_publicacion") or "",
                tipo=item.get("tipo_norma_probable") or "",
                numero=item.get("numero") or "",
                title=(item.get("title") or "").replace("|", " "),
            )
        )

    lines.extend(["", "## Posibles duplicados reales", ""])
    if not duplicates_key:
        lines.append("- Sin duplicados detectados.")
    else:
        for key, entries in list(duplicates_key.items())[:120]:
            urls = sorted(
                set(
                    entry.get("read_more_url") or entry.get("source_url")
                    for entry in entries
                    if (entry.get("read_more_url") or entry.get("source_url"))
                    and is_detail_page_url(entry.get("read_more_url") or entry.get("source_url"))
                )
            )
            if len(urls) > 1:
                lines.append(f"- `{key}` => {len(entries)} registros | urls={', '.join(urls)}")
        if lines[-1] == "":
            lines.append("- Sin duplicados detectados.")

    lines.extend(["", "## Documentos sin número detectado", ""])
    if not without_number:
        lines.append("- Sin casos detectados.")
    else:
        for item in without_number[:200]:
            lines.append(
                f"- `{item.get('document_key')}` | {item.get('fecha_publicacion') or 'sin_fecha'} | {item.get('title')}"
            )

    if payload.get("blocked_pages"):
        lines.extend(["", "## Páginas bloqueadas", ""])
        for blocked in payload["blocked_pages"][:80]:
            lines.append(
                f"- status={blocked.get('status_code')} | requested=`{blocked.get('requested_url')}` | final=`{blocked.get('final_url')}`"
            )

    path = output_dir / MD_REPORT_NAME
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    logger.info("Markdown generado: %s", path)


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = crawl_inventory(base_url=args.base_url, max_pages=args.max_pages)
    write_json_report(output_dir, payload)
    write_csv_report(output_dir, payload["items"])
    write_markdown_report(output_dir, payload)

    by_type = count_by_type(payload["items"])
    logger.info("Total normas canónicas: %s", payload["canonical_norms_total"])
    logger.info("Total PDFs asociados: %s", payload["associated_pdfs_total"])
    logger.info(
        "Total enlaces ignorados: %s",
        sum((payload.get("ignored_links") or {}).values()),
    )
    logger.info("Enlaces ignorados por tipo: %s", payload.get("ignored_links") or {})
    logger.info("Conteo por tipo: %s", by_type)


if __name__ == "__main__":
    main()
