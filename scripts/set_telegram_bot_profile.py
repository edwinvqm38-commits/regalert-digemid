"""Configura el perfil publico del bot en Telegram: descripcion corta,
descripcion larga, y el menu nativo de comandos ("/" en el chat).

Usa la Bot API directamente (setMyShortDescription, setMyDescription,
setMyCommands), reutilizando el TELEGRAM_BOT_TOKEN que ya existe como
secret. No hay endpoint de Bot API para la foto de perfil del bot: esa
se sube a mano una vez desde BotFather (/setuserpic).
"""

import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SHORT_DESCRIPTION = (
    "Alertas oficiales de DIGEMID + consultas con IA citando la norma exacta. "
    "Para químicos farmacéuticos en Perú."
)

DESCRIPTION = (
    "🔔 RegAlert DIGEMID vigila las alertas sanitarias y la normativa de DIGEMID "
    "(Perú) y te avisa apenas sale algo nuevo.\n\n"
    "✅ Alertas automáticas de medicamentos falsificados, retiros y modificaciones\n"
    "✅ Consultas en lenguaje natural con IA, citando el documento y la página exacta\n"
    "✅ Busca por producto, laboratorio, lote o número de alerta\n\n"
    "Toca Iniciar para probarlo gratis."
)

COMANDOS_USUARIO = [
    {"command": "start", "description": "Inicia el bot y muestra el menú"},
    {"command": "menu", "description": "Menú principal con botones"},
    {"command": "ayuda", "description": "Lista de comandos y opciones"},
    {"command": "ultimas", "description": "Últimas 5 alertas de DIGEMID"},
    {"command": "hoy", "description": "Alertas publicadas hoy"},
    {"command": "semana", "description": "Alertas publicadas esta semana"},
    {"command": "mes", "description": "Alertas publicadas este mes"},
    {"command": "recientes", "description": "Alertas registradas recién en el sistema"},
    {"command": "buscar", "description": "Busca alertas por palabra clave"},
    {"command": "detalle", "description": "Detalle de una alerta por número o código"},
    {"command": "consulta", "description": "Pregunta en lenguaje natural, cito la fuente"},
    {"command": "suscribirme", "description": "Pide un plan pagado (básico, consultoría, empresarial)"},
    {"command": "pague", "description": "Reporta tu código de operación de Yape"},
    {"command": "chatid", "description": "Muestra tu chat_id"},
]

COMANDOS_ADMIN_EXTRA = [
    {"command": "activar", "description": "[Admin] Activa un plan a un usuario"},
    {"command": "desactivar", "description": "[Admin] Cancela la suscripción de un usuario"},
    {"command": "usuarios", "description": "[Admin] Resumen de usuarios y suscripciones"},
    {"command": "membresias", "description": "[Admin] Lista de suscripciones con fechas"},
    {"command": "ingresos", "description": "[Admin] Ingresos del mes actual"},
    {"command": "invitar", "description": "[Admin] Genera un enlace de invitación"},
    {"command": "renombrar", "description": "[Admin] Cambia el nombre de un usuario"},
]


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def call(token: str, method: str, payload: dict) -> None:
    response = requests.post(f"https://api.telegram.org/bot{token}/{method}", json=payload, timeout=20)
    ok = response.ok and response.json().get("ok")
    logger.info("%s -> %s", method, "ok" if ok else response.text)


def main():
    load_env()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit("Falta TELEGRAM_BOT_TOKEN")

    call(token, "setMyShortDescription", {"short_description": SHORT_DESCRIPTION})
    call(token, "setMyDescription", {"description": DESCRIPTION})
    call(token, "setMyCommands", {"commands": COMANDOS_USUARIO})

    admin_chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID")
    if admin_chat_id:
        call(
            token,
            "setMyCommands",
            {
                "commands": COMANDOS_USUARIO + COMANDOS_ADMIN_EXTRA,
                "scope": {"type": "chat", "chat_id": admin_chat_id},
            },
        )


if __name__ == "__main__":
    main()
