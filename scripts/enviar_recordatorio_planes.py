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

MAX_RECORDATORIOS = 5
DIAS_ENTRE_RECORDATORIOS = 2

PLANES_TEXTO = (
    "👋 Recordatorio: sigues en el plan gratis de RegAlert DIGEMID "
    "(5 consultas de IA al día).\n\n"
    "Si necesitas más, estos son los planes disponibles:\n\n"
    "• <b>Básico</b> — S/29/mes (30 consultas/día)\n"
    "• <b>Consultoría</b> — S/79/mes (100 consultas/día)\n"
    "• <b>Empresarial</b> — S/199/mes (sin límite)\n\n"
    "Escribe <code>/suscribirme basico</code> (o el plan que prefieras) "
    "y te contactamos para activarlo."
)

PLANES_TEXTO_PRUEBA = (
    "👋 ¿Te acuerdas de RegAlert DIGEMID? Tu prueba gratuita ya terminó, así que "
    "las alertas automáticas y las consultas con IA están pausadas hasta que "
    "actives un plan.\n\n"
    "• <b>Básico</b> — S/29/mes (30 consultas/día)\n"
    "• <b>Consultoría</b> — S/79/mes (100 consultas/día)\n"
    "• <b>Empresarial</b> — S/199/mes (sin límite)\n\n"
    "Escribe <code>/suscribirme basico</code> (o el plan que prefieras) "
    "cuando quieras retomarlo."
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


def get_invitaciones_elegibles(supabase) -> list[dict]:
    response = (
        supabase.table("digemid_bot_invitaciones")
        .select("id, telegram_chat_id, used_at, recordatorios_enviados, ultimo_recordatorio_at")
        .eq("estado", "usado")
        .not_.is_("telegram_chat_id", "null")
        .lt("recordatorios_enviados", MAX_RECORDATORIOS)
        .execute()
    )

    return response.data or []


def get_pruebas_finalizadas_elegibles(supabase) -> list[dict]:
    response = (
        supabase.table("digemid_bot_usuarios")
        .select(
            "telegram_chat_id, nombre, prueba_inicio, recordatorio_prueba_enviados, "
            "ultimo_recordatorio_prueba_at, plan_gratis_legado"
        )
        .eq("prueba_estado", "finalizada")
        .eq("plan_gratis_legado", False)
        .lt("recordatorio_prueba_enviados", MAX_RECORDATORIOS)
        .execute()
    )

    return response.data or []


def tiene_plan_pagado(supabase, chat_id: str) -> bool:
    today = datetime.now(timezone.utc).date().isoformat()

    response = (
        supabase.table("digemid_suscripciones")
        .select("nivel, estado, fecha_fin")
        .eq("telegram_chat_id", chat_id)
        .neq("nivel", "gratis")
        .eq("estado", "activo")
        .execute()
    )

    for row in response.data or []:
        fecha_fin = row.get("fecha_fin")
        if not fecha_fin or fecha_fin >= today:
            return True

    return False


def debe_enviar_recordatorio_desde(referencia: str | None) -> bool:
    if not referencia:
        return False

    referencia_dt = datetime.fromisoformat(referencia.replace("Z", "+00:00"))
    return datetime.now(timezone.utc) - referencia_dt >= timedelta(days=DIAS_ENTRE_RECORDATORIOS)


def enviar_telegram(token: str, chat_id: str, texto: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    response = requests.post(url, json=payload, timeout=20)

    if not response.ok:
        logger.error("Error enviando recordatorio a %s: %s", chat_id, response.text)

    return response.ok


def main():
    load_env()
    supabase = get_supabase()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Falta TELEGRAM_BOT_TOKEN")

    invitaciones = get_invitaciones_elegibles(supabase)
    logger.info("Invitaciones candidatas: %s", len(invitaciones))

    enviados = 0
    omitidos_ya_pagan = 0
    omitidos_muy_pronto = 0

    for inv in invitaciones:
        chat_id = inv["telegram_chat_id"]

        if tiene_plan_pagado(supabase, chat_id):
            omitidos_ya_pagan += 1
            continue

        referencia = inv.get("ultimo_recordatorio_at") or inv.get("used_at")
        if not debe_enviar_recordatorio_desde(referencia):
            omitidos_muy_pronto += 1
            continue

        if enviar_telegram(token, chat_id, PLANES_TEXTO):
            supabase.table("digemid_bot_invitaciones").update(
                {
                    "recordatorios_enviados": inv["recordatorios_enviados"] + 1,
                    "ultimo_recordatorio_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("id", inv["id"]).execute()
            enviados += 1

    logger.info(
        "Invitaciones -> Enviados: %s | Ya tienen plan: %s | Aun no toca: %s",
        enviados,
        omitidos_ya_pagan,
        omitidos_muy_pronto,
    )

    pruebas = get_pruebas_finalizadas_elegibles(supabase)
    logger.info("Pruebas finalizadas candidatas: %s", len(pruebas))

    enviados_prueba = 0
    omitidos_prueba_ya_pagan = 0
    omitidos_prueba_muy_pronto = 0

    for usuario in pruebas:
        chat_id = usuario["telegram_chat_id"]

        if tiene_plan_pagado(supabase, chat_id):
            omitidos_prueba_ya_pagan += 1
            continue

        referencia = usuario.get("ultimo_recordatorio_prueba_at") or usuario.get("prueba_inicio")
        if not debe_enviar_recordatorio_desde(referencia):
            omitidos_prueba_muy_pronto += 1
            continue

        if enviar_telegram(token, chat_id, PLANES_TEXTO_PRUEBA):
            supabase.table("digemid_bot_usuarios").update(
                {
                    "recordatorio_prueba_enviados": usuario["recordatorio_prueba_enviados"] + 1,
                    "ultimo_recordatorio_prueba_at": datetime.now(timezone.utc).isoformat(),
                }
            ).eq("telegram_chat_id", chat_id).execute()
            enviados_prueba += 1

    logger.info(
        "Pruebas finalizadas -> Enviados: %s | Ya tienen plan: %s | Aun no toca: %s",
        enviados_prueba,
        omitidos_prueba_ya_pagan,
        omitidos_prueba_muy_pronto,
    )


if __name__ == "__main__":
    main()
