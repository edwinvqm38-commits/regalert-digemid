import html
import logging
import os

import requests

logger = logging.getLogger(__name__)


class NotifyAgent:
    """Agente responsable de notificar novedades por Telegram."""

    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not self.token or not self.chat_id:
            raise ValueError(
                "Faltan variables de entorno TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID"
            )

    def build_message(self, new_docs: list[dict]) -> str:
        """Construye el mensaje HTML para Telegram."""
        lines = [
            "🚨 <b>Nuevas alertas DIGEMID detectadas</b>",
            "",
        ]

        for doc in new_docs[:10]:
            key = html.escape(str(doc.get("document_key", "")))
            title = html.escape(str(doc.get("title", "")))[:250]
            detail_url = html.escape(str(doc.get("detail_url", "")))

            lines.append(f"• <b>{key}</b> — {title}")
            lines.append(f"🔗 {detail_url}")
            lines.append("")

        if len(new_docs) > 10:
            lines.append(f"Y {len(new_docs) - 10} documentos más.")

        return "\n".join(lines)

    def send_summary(self, new_docs: list[dict]) -> None:
        """Envía resumen solo si existen documentos nuevos."""
        if not new_docs:
            logger.info("No hay documentos nuevos para notificar.")
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": self.build_message(new_docs),
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }

        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()

        logger.info("Notificación enviada a Telegram.")