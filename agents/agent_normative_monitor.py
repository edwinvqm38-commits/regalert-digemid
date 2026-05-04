import hashlib
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from agents.agent_utils import (
    clean_text,
    extract_date_display,
    normalize_date,
    remove_accents,
    slug_from_url,
    utc_now_iso,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "digemid_normative_sources.json"
CONTAINER_TAGS = ("li", "article", "tr", "p", "div")
SKIP_HREF_PREFIXES = ("#", "javascript:", "mailto:", "tel:")
SKIP_TEXTS = {
    "",
    "pdf",
    "leer mas",
    "leer más",
    "ver mas",
    "ver más",
    "descargar",
    "download",
    "click aqui",
    "click aquí",
}
TYPE_PATTERNS = [
    ("RM", re.compile(r"\b(?:R\.?\s*M\.?|RESOLUCION MINISTERIAL)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
    ("DS", re.compile(r"\b(?:D\.?\s*S\.?|DECRETO SUPREMO)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
    ("RD", re.compile(r"\b(?:R\.?\s*D\.?|RESOLUCION DIRECTORAL)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
    ("RS", re.compile(r"\b(?:R\.?\s*S\.?|RESOLUCION SUPREMA)\s*(?:N[°Oº.]*)?\s*(\d{1,4})[-/](20\d{2})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
    ("DL", re.compile(r"\b(?:D\.?\s*L\.?|DECRETO LEGISLATIVO)\s*(?:N[°Oº.]*)?\s*(\d{1,4})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
    ("DU", re.compile(r"\b(?:D\.?\s*U\.?|DECRETO DE URGENCIA)\s*(?:N[°Oº.]*)?\s*(\d{1,4})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
    ("LEY", re.compile(r"\b(?:LEY)\s*(?:N[°Oº.]*)?\s*(\d{1,6})([/\-][A-Z0-9\-]+)?", re.IGNORECASE)),
]


def is_pdf_url(url: str | None) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return ".pdf" in lowered


def normalize_suffix(value: str | None) -> str:
    if not value:
        return ""
    return value.replace("/", "-").replace("--", "-").strip("-").upper()


def short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:8].upper()


def guess_year(published_date: str | None, text: str) -> str:
    if published_date and len(published_date) >= 4:
        return published_date[:4]

    match = re.search(r"\b(20\d{2})\b", text)
    if match:
        return match.group(1)

    return "0000"


def extract_file_name(file_url: str | None) -> str | None:
    if not file_url:
        return None
    path = urlparse(file_url).path
    file_name = path.rsplit("/", 1)[-1]
    return file_name or None


def normalize_spaces_upper(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().upper()


def generate_normative_document_key(
    title: str,
    detail_url: str,
    source_section: str,
    published_date: str | None,
) -> str:
    combined = normalize_spaces_upper(f"{title} {detail_url}")

    for prefix, pattern in TYPE_PATTERNS:
        match = pattern.search(combined)
        if not match:
            continue

        number = str(int(match.group(1))) if match.group(1).isdigit() else match.group(1)

        if prefix == "LEY":
            return f"LEY-{number}"

        year = match.group(2)
        suffix = normalize_suffix(match.group(3))
        return f"{prefix}-{number}-{year}" + (f"-{suffix}" if suffix else "")

    year = guess_year(published_date, combined)
    return f"NORM-{source_section.upper()}-{year}-{short_hash(combined)}"


def is_meaningful_text(text: str) -> bool:
    normalized = remove_accents(clean_text(text)).lower()
    if normalized in SKIP_TEXTS:
        return False
    if len(normalized) < 8:
        return False
    return True


class NormativeMonitorAgent:
    def __init__(self, config_path: str | None = None):
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self.headers = {
            "User-Agent": "RegAlert-DIGEMID-NormativeMonitor/1.0",
        }
        self.sources = self._load_sources()

    def _load_sources(self) -> list[dict]:
        with self.config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        if not isinstance(data, list) or not data:
            raise ValueError("El archivo de fuentes normativas no contiene una lista valida.")

        return data

    def fetch_html(self, source: dict) -> str:
        source_url = source["source_url"]
        logger.info("Fuente procesada: %s | url=%s", source["label"], source_url)
        response = requests.get(source_url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.text

    def find_candidate_container(self, anchor: Tag) -> Tag:
        for parent in anchor.parents:
            if not isinstance(parent, Tag):
                continue
            if parent.name not in CONTAINER_TAGS:
                continue

            links = parent.find_all("a", href=True)
            text = clean_text(parent.get_text(" "))

            if not text:
                continue
            if len(links) > 8:
                continue
            if len(text) > 1500:
                continue

            return parent

        return anchor

    def choose_title(self, container: Tag) -> str:
        anchors = container.find_all("a", href=True)

        non_pdf_texts = [
            clean_text(anchor.get_text(" "))
            for anchor in anchors
            if not is_pdf_url(anchor.get("href", "")) and is_meaningful_text(anchor.get_text(" "))
        ]

        if non_pdf_texts:
            return max(non_pdf_texts, key=len)

        container_text = clean_text(container.get_text(" "))
        if is_meaningful_text(container_text):
            return container_text[:500]

        for anchor in anchors:
            text = clean_text(anchor.get_text(" "))
            if is_meaningful_text(text):
                return text

        return ""

    def build_document(self, container: Tag, source: dict) -> dict | None:
        anchors = container.find_all("a", href=True)
        if not anchors:
            return None

        normalized_links: list[tuple[str, str]] = []
        for anchor in anchors:
            href = clean_text(anchor.get("href", ""))
            if not href or href.lower().startswith(SKIP_HREF_PREFIXES):
                continue
            absolute_url = urljoin(source["source_url"], href)
            anchor_text = clean_text(anchor.get_text(" "))
            normalized_links.append((absolute_url, anchor_text))

        if not normalized_links:
            return None

        detail_url = None
        file_url = None

        for absolute_url, _anchor_text in normalized_links:
            if is_pdf_url(absolute_url) and not file_url:
                file_url = absolute_url
            elif not detail_url:
                detail_url = absolute_url

        if not detail_url:
            detail_url = file_url

        if not detail_url:
            return None

        title = self.choose_title(container)
        if not title:
            return None

        context_text = clean_text(container.get_text(" "))
        date_display = extract_date_display(context_text) or extract_date_display(title)
        published_date = normalize_date(date_display)
        document_key = generate_normative_document_key(
            title=title,
            detail_url=detail_url,
            source_section=source["source_section"],
            published_date=published_date,
        )

        if not document_key:
            return None

        file_name = extract_file_name(file_url)
        file_ext = "pdf" if file_url else None
        has_file = bool(file_url)
        scraped_at = utc_now_iso()
        document_slug = slug_from_url(detail_url) or slug_from_url(file_url) or short_hash(title)

        raw = {
            "source": "github_actions_python_normative",
            "source_listing": source,
            "source_url": source["source_url"],
            "scraped_at": scraped_at,
            "content_status": "pendiente_contenido",
            "documento_tipo": source.get("documento_tipo"),
            "documento_subtipo": source.get("documento_subtipo"),
            "url_canonica": detail_url,
            "traza_origen": {
                "fuente": source["label"],
                "scraped_at": scraped_at,
                "url_consultada": source["source_url"],
            },
            "detected_links": [
                {"url": absolute_url, "text": anchor_text}
                for absolute_url, anchor_text in normalized_links[:10]
            ],
            "context_excerpt": context_text[:1000],
        }

        return {
            "source_type": "normativa",
            "source_section": source["source_section"],
            "source_page": source["source_url"],
            "source_site": "DIGEMID",
            "document_key": document_key,
            "title": title,
            "document_slug": document_slug,
            "published_date": published_date,
            "published_date_display": date_display,
            "detail_url": detail_url,
            "has_file": has_file,
            "file_url": file_url,
            "file_name": file_name,
            "file_ext": file_ext,
            "discovery_mode": "normative_listing_v1",
            "process_status": "registered",
            "process_message": "Metadata normativa registrada desde listado DIGEMID",
            "raw": raw,
        }

    def merge_documents(self, current: dict, candidate: dict) -> dict:
        merged = dict(current)

        if not merged.get("file_url") and candidate.get("file_url"):
            merged["file_url"] = candidate["file_url"]
            merged["file_name"] = candidate.get("file_name")
            merged["file_ext"] = candidate.get("file_ext")
            merged["has_file"] = candidate.get("has_file", False)

        if not merged.get("published_date") and candidate.get("published_date"):
            merged["published_date"] = candidate["published_date"]
            merged["published_date_display"] = candidate.get("published_date_display")

        merged_raw = dict(merged.get("raw") or {})
        merged_raw.update(candidate.get("raw") or {})

        detected_links = []
        for item in (current.get("raw") or {}).get("detected_links", []):
            if item not in detected_links:
                detected_links.append(item)
        for item in (candidate.get("raw") or {}).get("detected_links", []):
            if item not in detected_links:
                detected_links.append(item)

        merged_raw["detected_links"] = detected_links[:15]
        merged["raw"] = merged_raw

        return merged

    def collect_documents(self) -> list[dict]:
        documents_by_key: dict[str, dict] = {}

        for source in self.sources:
            found_for_source = 0

            try:
                html = self.fetch_html(source)
                soup = BeautifulSoup(html, "html.parser")
                seen_container_ids: set[int] = set()

                for anchor in soup.find_all("a", href=True):
                    container = self.find_candidate_container(anchor)
                    container_id = id(container)

                    if container_id in seen_container_ids:
                        continue

                    seen_container_ids.add(container_id)
                    document = self.build_document(container, source)

                    if not document:
                        continue

                    found_for_source += 1
                    current = documents_by_key.get(document["document_key"])
                    if current:
                        documents_by_key[document["document_key"]] = self.merge_documents(current, document)
                    else:
                        documents_by_key[document["document_key"]] = document

                logger.info(
                    "Fuente procesada: %s | registros encontrados: %s",
                    source["label"],
                    found_for_source,
                )
            except Exception as error:
                logger.exception("Error procesando fuente normativa %s: %s", source["label"], error)

        result = list(documents_by_key.values())
        logger.info("Total de registros normativos detectados: %s", len(result))
        return result
