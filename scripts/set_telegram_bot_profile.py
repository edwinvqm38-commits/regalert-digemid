"""Configura el perfil publico del bot en Telegram: descripcion corta,
descripcion larga, y el menu nativo de comandos ("/" en el chat).

Usa la Bot API directamente (setMyShortDescription, setMyDescription,
setMyCommands), reutilizando el TELEGRAM_BOT_TOKEN que ya existe como
secret. No hay endpoint de Bot API para la foto de perfil del bot: esa
se sube a mano una vez desde BotFather (/setuserpic).

El menu "/" muestra COMANDOS_USUARIO a todos por defecto, y
COMANDOS_USUARIO + COMANDOS_ADMIN_EXTRA solo a los chats listados en
ADMIN_CHAT_IDS (scope por chat), para que los usuarios normales no vean
comandos de administrador que no pueden usar.
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

# Estos son los unicos comandos que ve un usuario normal al presionar "/".
COMANDOS_USUARIO = [
    {"command": "start", "description": "Inicia el bot y muestra el menú"},
    {"command": "menu", "description": "Menú principal con botones"},
    {"command": "ayuda", "description": "Lista de comandos y opciones"},
    {"command": "ultimas", "description": "Últimas alertas registradas"},
    {"command": "hoy", "description": "Alertas publicadas hoy"},
    {"command": "semana", "description": "Alertas publicadas esta semana"},
    {"command": "mes", "description": "Alertas publicadas este mes"},
    {"command": "recientes", "description": "Alertas registradas recién en el sistema"},
    {"command": "buscar", "description": "Busca alertas por palabra clave"},
    {"command": "detalle", "description": "Detalle de una alerta por número o código"},
    {"command": "consulta", "description": "Pregunta en lenguaje natural, cito la fuente"},
    {"command": "suscribirme", "description": "Pide un plan pagado (básico, consultoría, empresarial)"},
    {"command": "pague", "description": "Reporta tu código de operación de Yape"},
    {"command": "registrarme", "description": "Registra tu nombre para identificarte"},
    {"command": "miperfil", "description": "Tu nombre y el estado de tu prueba o plan"},
    {"command": "chatid", "description": "Muestra tu chat_id"},
]

# Estos se suman a COMANDOS_USUARIO solo en el menu de los chats admin.
COMANDOS_ADMIN_EXTRA = [
    {"command": "activar", "description": "Activa un plan a un usuario"},
    {"command": "desactivar", "description": "Cancela la suscripción de un usuario"},
    {"command": "usuarios", "description": "Resumen de usuarios y suscripciones"},
    {"command": "membresias", "description": "Lista de suscripciones con fechas"},
    {"command": "directorio", "description": "Usuarios por estado, con botón de recordatorio"},
    {"command": "ingresos", "description": "Ingresos del mes según planes activados"},
    {"command": "pagosyape", "description": "Montos reales reportados por Yape este mes"},
    {"command": "invitar", "description": "Genera un enlace de invitación"},
    {"command": "renombrar", "description": "Cambia el nombre mostrado de un usuario"},
    {"command": "gratis", "description": "Da acceso gratis permanente a un usuario"},
    {"command": "saldodeepseek", "description": "Saldo de la API de DeepSeek al instante"},
]


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def call(token: str, method: str, payload: dict) -> None:
    response = requests.post(f"https://api.telegram.org/bot{token}/{method}", json=payload, timeout=20)
    ok = response.ok and response.json().get("ok")
    logger.info("%s -> %s", method, "ok" if ok else response.text)


def admin_chat_ids() -> list[str]:
    """Lee ADMIN_CHAT_IDS (coma-separado, el mismo que usa el bot en vivo
    para autorizar comandos). Si no esta configurado, cae a
    TELEGRAM_ADMIN_CHAT_ID (un solo id) por compatibilidad con el setup
    anterior."""
    lista = os.getenv("ADMIN_CHAT_IDS", "")
    ids = [item.strip() for item in lista.split(",") if item.strip()]

    if ids:
        return ids

    single = os.getenv("TELEGRAM_ADMIN_CHAT_ID")
    return [single] if single else []


def main():
    load_env()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit("Falta TELEGRAM_BOT_TOKEN")

    call(token, "setMyShortDescription", {"short_description": SHORT_DESCRIPTION})
    call(token, "setMyDescription", {"description": DESCRIPTION})
    call(token, "setMyCommands", {"commands": COMANDOS_USUARIO})

    admins = admin_chat_ids()
    if not admins:
        logger.warning("Sin ADMIN_CHAT_IDS ni TELEGRAM_ADMIN_CHAT_ID: ningún chat tendrá el menú de administrador.")

    for chat_id in admins:
        call(
            token,
            "setMyCommands",
            {
                "commands": COMANDOS_USUARIO + COMANDOS_ADMIN_EXTRA,
                "scope": {"type": "chat", "chat_id": chat_id},
            },
        )


if __name__ == "__main__":
    main()
