import logging
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from agents.agent_utils import clean_text, normalize_date

logger = logging.getLogger(__name__)


class DetailAgent:
    """Extrae metadata real desde la página detalle de una alerta DIGEMID."""

    def __init__(self):
        self.headers = {
            "User-Agent": "RegAlert-DIGEMID-DetailAgent/1.0"
        }

    def fetch_html(self, detail_url: str) -> str:
        response = requests.get(detail_url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.text

    def extract_title(self, soup: BeautifulSoup) -> str | None:
        selectors = [
            "h1",
            "h2",
            ".entry-title",
            ".post-title",
            ".page-title",
            "title",
        ]

        for selector in selectors:
            node = soup.select_one(selector)

            if not node:
                continue

            title = clean_text(node.get_text(" "))

            if not title:
                continue

            title = title.replace("DIGEMID", "").replace("|", "").strip()
            title = re.sub(r"\s+", " ", title).strip(" -")

            if title and title.lower() not in ["read more...", "leer más", "leer mas", "alertas"]:
                return title

        return None

    def extract_date_display(self, soup: BeautifulSoup) -> str | None:
        text = clean_text(soup.get_text(" "))
        match = re.search(r"\b(\d{1,2}/\d{1,2}/20\d{2})\b", text)
        return match.group(1) if match else None

    def extract_pdf(self, soup: BeautifulSoup, detail_url: str) -> dict:
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            full_url = urljoin(detail_url, href)

            if ".pdf" not in full_url.lower():
                continue

            file_name = full_url.rstrip("/").split("/")[-1]

            return {
                "file_url": full_url,
                "file_name": file_name,
                "file_ext": "pdf",
                "has_file": True,
                "mime_type": "application/pdf",
            }

        return {
            "file_url": None,
            "file_name": None,
            "file_ext": None,
            "has_file": False,
            "mime_type": None,
        }

    def extract(self, detail_url: str) -> dict:
        logger.info("Leyendo detalle: %s", detail_url)

        html = self.fetch_html(detail_url)
        soup = BeautifulSoup(html, "html.parser")

        title = self.extract_title(soup)
        date_display = self.extract_date_display(soup)
        published_date = normalize_date(date_display)

        pdf_data = self.extract_pdf(soup, detail_url)

        return {
            "title": title,
            "published_date": published_date,
            "published_date_display": date_display,
            **pdf_data,
            "raw": {
                "detail_url": detail_url,
                "title_detected": title,
                "published_date_display_detected": date_display,
                "pdf_detected": pdf_data,
            },
        }