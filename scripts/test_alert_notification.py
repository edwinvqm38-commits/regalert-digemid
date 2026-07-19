import logging
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.agent_notify import NotifyAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def log_bot_identity():
    """Identifica que bot esta usando este token (no expone el token, solo su username)."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        return

    response = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=20)

    if response.ok:
        bot_info = response.json().get("result", {})
        logger.info(
            "Este script usa el bot: @%s (id %s)",
            bot_info.get("username"),
            bot_info.get("id"),
        )
    else:
        logger.error("No se pudo identificar el bot: %s", response.text)


def main():
    load_dotenv()
    log_bot_identity()

    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    logger.info("TELEGRAM_CHAT_ID recibido: %r (longitud %d)", chat_id, len(chat_id))

    fake_doc = {
        "document_key": "PRUEBA-000-2026",
        "title": "[PRUEBA] Este es un mensaje de prueba - no es una alerta oficial de DIGEMID",
        "detail_url": "https://www.digemid.minsa.gob.pe/webDigemid/alertas-modificaciones/",
    }

    NotifyAgent().send_summary([fake_doc])
    logger.info("Notificacion de prueba enviada.")


if __name__ == "__main__":
    main()
