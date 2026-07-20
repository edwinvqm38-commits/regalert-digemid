"""Cierra las pruebas gratuitas que llegaron a su limite de tiempo (14 dias)
sin haber recibido las 3 alertas que tambien terminan la prueba (eso lo
maneja agents.agent_notify.NotifyAgent.send_individual_alerts en cada corrida
del monitor). Corre una vez al dia via GitHub Actions.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
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
PRUEBA_LIMITE_DIAS = 14

NIVEL_PRECIOS = {"basico": 29, "consultoria": 79, "empresarial": 199}


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def mensaje_prueba_terminada() -> tuple[str, dict]:
    texto = (
        "⏰ <b>Tu prueba gratuita de RegAlert DIGEMID terminó.</b>\n\n"
        "¿Quieres seguir recibiendo alertas automáticas y consultas con IA? Elige un plan:"
    )
    botones = {
        "inline_keyboard": [
            [{"text": f"Solicitar Básico — S/{NIVEL_PRECIOS['basico']}/mes", "callback_data": "plan:basico"}],
            [{"text": f"Solicitar Consultoría — S/{NIVEL_PRECIOS['consultoria']}/mes", "callback_data": "plan:consultoria"}],
            [{"text": f"Solicitar Empresarial — S/{NIVEL_PRECIOS['empresarial']}/mes", "callback_data": "plan:empresarial"}],
        ]
    }
    return texto, botones


def enviar_mensaje(token: str, chat_id: str, texto: str, botones: dict) -> None:
    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": texto,
            "parse_mode": "HTML",
            "reply_markup": botones,
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

    limite = (datetime.now(timezone.utc) - timedelta(days=PRUEBA_LIMITE_DIAS)).isoformat()

    vencidas = (
        supabase.table(USUARIOS_TABLE)
        .select("telegram_chat_id, prueba_inicio")
        .eq("prueba_estado", "activa")
        .lte("prueba_inicio", limite)
        .execute()
    )

    filas = vencidas.data or []
    logger.info("Pruebas vencidas por tiempo (%s dias): %s", PRUEBA_LIMITE_DIAS, len(filas))

    texto, botones = mensaje_prueba_terminada()

    for fila in filas:
        chat_id = fila["telegram_chat_id"]
        supabase.table(USUARIOS_TABLE).update({"prueba_estado": "finalizada"}).eq(
            "telegram_chat_id", chat_id
        ).execute()
        enviar_mensaje(token, chat_id, texto, botones)
        logger.info("Prueba finalizada por tiempo: %s", chat_id)


if __name__ == "__main__":
    main()
