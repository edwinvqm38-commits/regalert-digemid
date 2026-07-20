"""Envia un archivo (ej. una guia/prospecto) al chat de Telegram del admin.

Uso:
    python scripts/send_telegram_document.py docs/GUIA_BOT_REGALERT.md \
        --caption "Guia rapida del bot"
"""

import argparse
import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", type=Path)
    parser.add_argument("--caption", default="")
    args = parser.parse_args()

    load_env()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise SystemExit("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_ADMIN_CHAT_ID/TELEGRAM_CHAT_ID")

    with args.file_path.open("rb") as file_obj:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id, "caption": args.caption},
            files={"document": (args.file_path.name, file_obj, "text/markdown")},
            timeout=30,
        )
    response.raise_for_status()
    logger.info("Documento enviado a chat_id %s: %s", chat_id, response.json().get("ok"))


if __name__ == "__main__":
    main()
