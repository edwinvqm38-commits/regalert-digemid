"""Envia una alerta de EJEMPLO (no una alerta real de DIGEMID) por Telegram a
todos los usuarios que estan actualmente en prueba gratuita activa. Sirve para
que un usuario en prueba vea como luce el mensaje de una alerta real, sin
gastar su cupo de PRUEBA_LIMITE_ALERTAS (no se toca prueba_alertas_enviadas).
Se ejecuta manualmente via GitHub Actions (workflow_dispatch).
"""

import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

USUARIOS_TABLE = "digemid_bot_usuarios"

MENSAJE_EJEMPLO = (
    "⚠️ <b>Mensaje de ejemplo — no es una alerta real de DIGEMID</b>\n"
    "Así se ve una alerta cuando llega de verdad:\n\n"
    "🚨 <b>Nuevas alertas DIGEMID detectadas</b>\n\n"
    "• <b>ALERTA Nº 12-2026</b> — Retiro voluntario de un lote por hallazgos en control de calidad.\n"
    "🔗 https://digemid.minsa.gob.pe/alertas/ejemplo\n\n"
    "Cuando te llegue una alerta real, podrás usar <code>/consulta</code> para preguntar "
    "sobre ella y el bot te responderá citando esta misma fuente."
)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def enviar_mensaje(token: str, chat_id: str, texto: str) -> None:
    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": texto,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    if not response.ok:
        logger.warning("No se pudo enviar a %s: %s", chat_id, response.text)


def main():
    load_env()
    supabase = get_supabase()
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        raise SystemExit("Falta TELEGRAM_BOT_TOKEN")

    en_prueba = (
        supabase.table(USUARIOS_TABLE)
        .select("telegram_chat_id")
        .eq("prueba_estado", "activa")
        .execute()
    )

    filas = en_prueba.data or []
    logger.info("Usuarios en prueba activa: %s", len(filas))

    for fila in filas:
        chat_id = fila["telegram_chat_id"]
        enviar_mensaje(token, chat_id, MENSAJE_EJEMPLO)
        logger.info("Alerta de ejemplo enviada a: %s", chat_id)


if __name__ == "__main__":
    main()
