import logging
import os
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from supabase import Client, create_client

from agents.agent_utils import clean_text, utc_now_iso

logger = logging.getLogger(__name__)

PDF_ANCHOR_HINTS = (
    "descargar",
    "pdf",
    "ver documento",
    "archivo",
)


def is_pdf_url(url: str | None) -> bool:
    if not url:
        return False
    return ".pdf" in url.lower()


def extract_file_name(file_url: str | None) -> str | None:
    if not file_url:
        return None
    path = urlparse(file_url).path
    file_name = path.rsplit("/", 1)[-1]
    return file_name or None


class NormativePdfDetectorAgent:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError(
                "Faltan variables de entorno SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY"
            )

        self.supabase: Client = create_client(url, key)
        self.table_name = "digemid_documentos"
        self.headers = {
            "User-Agent": "RegAlert-DIGEMID-NormativePdfDetector/1.0",
        }

    def fetch_pending_documents(self) -> list[dict]:
        response = (
            self.supabase
            .table(self.table_name)
            .select("id, document_key, title, detail_url, file_url, raw, process_status")
            .eq("source_type", "normativa")
            .in_("process_status", ["registered", "pdf_detection_error"])
            .not_.is_("detail_url", "null")
            .order("updated_at", desc=True)
            .execute()
        )
        return response.data or []

    def head_or_get_content_type(self, url: str) -> str:
        try:
            response = requests.head(
                url,
                headers=self.headers,
                timeout=20,
                allow_redirects=True,
            )
            content_type = response.headers.get("Content-Type", "")
            if content_type:
                return content_type.lower()
        except Exception:
            pass

        response = requests.get(
            url,
            headers=self.headers,
            timeout=25,
            allow_redirects=True,
            stream=True,
        )
        try:
            return response.headers.get("Content-Type", "").lower()
        finally:
            response.close()

    def is_pdf_response(self, url: str) -> bool:
        if is_pdf_url(url):
            return True
        return "application/pdf" in self.head_or_get_content_type(url)

    def fetch_detail_response(self, url: str) -> requests.Response:
        response = requests.get(
            url,
            headers=self.headers,
            timeout=25,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response

    def score_pdf_link(self, absolute_url: str, anchor_text: str) -> int:
        score = 0
        lowered_text = clean_text(anchor_text).lower()

        if is_pdf_url(absolute_url):
            score += 10

        if "application/pdf" in self.head_or_get_content_type(absolute_url):
            score += 8

        for hint in PDF_ANCHOR_HINTS:
            if hint in lowered_text:
                score += 4

        if lowered_text == "pdf":
            score += 2

        return score

    def detect_pdf_url(self, detail_url: str) -> dict:
        if self.is_pdf_response(detail_url):
            return {
                "status": "pdf_detected",
                "pdf_url": detail_url,
                "mime_type": "application/pdf",
                "message": "PDF normativo detectado desde detail_url",
            }

        response = self.fetch_detail_response(detail_url)
        content_type = response.headers.get("Content-Type", "").lower()

        if "application/pdf" in content_type:
            return {
                "status": "pdf_detected",
                "pdf_url": detail_url,
                "mime_type": "application/pdf",
                "message": "PDF normativo detectado desde detail_url",
            }

        soup = BeautifulSoup(response.text, "html.parser")
        candidate_links: list[tuple[int, str]] = []

        for anchor in soup.find_all("a", href=True):
            href = clean_text(anchor.get("href", ""))
            if not href or href.lower().startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            absolute_url = urljoin(detail_url, href)
            anchor_text = clean_text(anchor.get_text(" "))
            score = self.score_pdf_link(absolute_url, anchor_text)
            if score > 0:
                candidate_links.append((score, absolute_url))

        if not candidate_links:
            return {
                "status": "pdf_not_found",
                "pdf_url": None,
                "mime_type": None,
                "message": "No se detecto enlace PDF en detail_url",
            }

        candidate_links.sort(key=lambda item: item[0], reverse=True)
        best_url = candidate_links[0][1]

        if not self.is_pdf_response(best_url):
            return {
                "status": "pdf_not_found",
                "pdf_url": None,
                "mime_type": None,
                "message": "No se confirmo PDF oficial desde enlaces detectados",
            }

        return {
            "status": "pdf_detected",
            "pdf_url": best_url,
            "mime_type": "application/pdf",
            "message": "PDF normativo detectado desde detail_url",
        }

    def update_document(self, row: dict, result: dict) -> None:
        now = utc_now_iso()
        raw = dict(row.get("raw") or {})
        raw["pdf_detection"] = {
            "status": result["status"],
            "detail_url": row.get("detail_url"),
            "pdf_url": result.get("pdf_url"),
            "detected_at": now,
            "message": result.get("message"),
            "mime_type_detectado": result.get("mime_type"),
        }

        payload = {
            "has_file": result["status"] == "pdf_detected",
            "process_status": result["status"],
            "process_message": result["message"],
            "updated_at": now,
            "raw": raw,
        }

        if result["status"] == "pdf_detected":
            payload.update(
                {
                    "file_url": result["pdf_url"],
                    "file_name": extract_file_name(result["pdf_url"]),
                    "file_ext": "pdf",
                    "mime_type": result.get("mime_type") or "application/pdf",
                }
            )

        (
            self.supabase
            .table(self.table_name)
            .update(payload)
            .eq("id", row["id"])
            .execute()
        )

    def process(self) -> dict:
        rows = self.fetch_pending_documents()
        summary = {
            "total_pending": len(rows),
            "pdf_detected": 0,
            "pdf_not_found": 0,
            "pdf_detection_error": 0,
        }

        logger.info("total_pending=%s", summary["total_pending"])

        for row in rows:
            document_key = row.get("document_key")
            detail_url = row.get("detail_url")

            try:
                logger.info("Detectando PDF normativo: %s | %s", document_key, detail_url)
                result = self.detect_pdf_url(detail_url)
                self.update_document(row, result)
                summary[result["status"]] += 1
            except Exception as error:
                now = utc_now_iso()
                raw = dict(row.get("raw") or {})
                raw["pdf_detection"] = {
                    "status": "pdf_detection_error",
                    "detail_url": detail_url,
                    "detected_at": now,
                    "message": str(error)[:300],
                }

                (
                    self.supabase
                    .table(self.table_name)
                    .update(
                        {
                            "has_file": False,
                            "process_status": "pdf_detection_error",
                            "process_message": str(error)[:300],
                            "updated_at": now,
                            "raw": raw,
                        }
                    )
                    .eq("id", row["id"])
                    .execute()
                )

                summary["pdf_detection_error"] += 1
                logger.exception(
                    "Error detectando PDF normativo %s: %s",
                    document_key,
                    error,
                )

        logger.info(
            "Resumen deteccion PDF | total_pending=%s | pdf_detected=%s | pdf_not_found=%s | pdf_detection_error=%s",
            summary["total_pending"],
            summary["pdf_detected"],
            summary["pdf_not_found"],
            summary["pdf_detection_error"],
        )
        return summary
