"""Vigía de saldo de DeepSeek: guarda un historial de saldo y proyecta
cuántos días de crédito quedan al ritmo de consumo actual, para avisar por
Telegram CON ANTICIPACIÓN — no cuando el saldo ya llegó a cero y los
usuarios se quedaron sin poder hacer consultas de IA.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

HISTORIAL_TABLE = "deepseek_balance_historial"

# Umbrales de aviso: se dispara si se cruza CUALQUIERA de los dos.
UMBRAL_SALDO_CRITICO_USD = 2.0
UMBRAL_DIAS_RESTANTES = 5
DIAS_VENTANA_PROMEDIO = 14


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def consultar_saldo_deepseek(api_key: str) -> dict:
    response = requests.get(
        "https://api.deepseek.com/user/balance",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def extraer_balance_usd(data: dict) -> float | None:
    """El endpoint puede devolver varias monedas; se prioriza USD y si no
    esta disponible se toma la primera que venga."""
    infos = data.get("balance_infos") or []
    if not infos:
        return None

    for info in infos:
        if str(info.get("currency", "")).upper() == "USD":
            try:
                return float(info["total_balance"])
            except (KeyError, TypeError, ValueError):
                return None

    try:
        return float(infos[0]["total_balance"])
    except (KeyError, TypeError, ValueError):
        return None


def guardar_snapshot(supabase, balance_usd: float | None, is_available: bool, raw: dict) -> None:
    supabase.table(HISTORIAL_TABLE).insert({
        "balance_usd": balance_usd,
        "is_available": is_available,
        "raw": raw,
    }).execute()


def proyectar_dias_restantes(supabase, balance_actual: float) -> float | None:
    """Calcula el consumo diario promedio de la ventana reciente, ignorando
    el tramo previo a la ultima recarga (un salto hacia arriba de saldo),
    para no mezclar "antes de recargar" con "despues de recargar"."""
    desde = (datetime.now(timezone.utc) - timedelta(days=DIAS_VENTANA_PROMEDIO)).isoformat()

    try:
        response = (
            supabase.table(HISTORIAL_TABLE)
            .select("checked_at, balance_usd")
            .gte("checked_at", desde)
            .order("checked_at", desc=False)
            .execute()
        )
    except Exception:
        logger.warning("No se pudo leer el historial de saldo (tabla no disponible aún).")
        return None

    historial = [
        row for row in (response.data or [])
        if row.get("balance_usd") is not None
    ]

    if len(historial) < 2:
        return None

    # Si hubo una recarga (el saldo subio) en la ventana, solo se usa el
    # tramo posterior a la recarga mas reciente para el promedio.
    ultimo_indice_recarga = None
    for i in range(1, len(historial)):
        if historial[i]["balance_usd"] > historial[i - 1]["balance_usd"]:
            ultimo_indice_recarga = i

    tramo = historial[ultimo_indice_recarga:] if ultimo_indice_recarga is not None else historial
    if len(tramo) < 2:
        return None

    primero = tramo[0]
    ultimo = tramo[-1]

    dias_transcurridos = (
        datetime.fromisoformat(ultimo["checked_at"].replace("Z", "+00:00"))
        - datetime.fromisoformat(primero["checked_at"].replace("Z", "+00:00"))
    ).total_seconds() / 86400

    consumo_total = primero["balance_usd"] - ultimo["balance_usd"]

    if dias_transcurridos <= 0 or consumo_total <= 0:
        return None

    consumo_diario_promedio = consumo_total / dias_transcurridos
    return balance_actual / consumo_diario_promedio


def enviar_alerta_telegram(mensaje: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.warning("Sin TELEGRAM_BOT_TOKEN o chat_id: no se pudo enviar la alerta de saldo.")
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"},
            timeout=20,
        ).raise_for_status()
    except Exception:
        logger.exception("No se pudo enviar la alerta de saldo a Telegram.")


def main() -> None:
    load_env()

    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        logger.error("Falta DEEPSEEK_API_KEY: no se puede consultar el saldo.")
        sys.exit(1)

    supabase = get_supabase()

    try:
        data = consultar_saldo_deepseek(api_key)
    except Exception as error:
        logger.exception("No se pudo consultar el saldo de DeepSeek: %s", error)
        enviar_alerta_telegram(
            "⚠️ <b>No se pudo consultar el saldo de DeepSeek</b>\n\n"
            f"Error: {error}\n\n"
            "Revisar manualmente en https://platform.deepseek.com/"
        )
        sys.exit(1)

    balance_usd = extraer_balance_usd(data)
    is_available = bool(data.get("is_available", balance_usd is not None and balance_usd > 0))

    logger.info("Saldo DeepSeek: %s USD | disponible: %s", balance_usd, is_available)

    try:
        guardar_snapshot(supabase, balance_usd, is_available, data)
    except Exception:
        logger.exception(
            "No se pudo guardar el snapshot (¿falta correr la migración "
            "2026_07_23_create_deepseek_balance_historial.sql en Supabase?). "
            "Se continúa con el saldo ya obtenido."
        )

    if balance_usd is None:
        logger.warning("No se pudo interpretar el saldo devuelto por DeepSeek: %s", data)
        return

    dias_restantes = proyectar_dias_restantes(supabase, balance_usd)

    saldo_critico = balance_usd <= UMBRAL_SALDO_CRITICO_USD
    dias_bajos = dias_restantes is not None and dias_restantes <= UMBRAL_DIAS_RESTANTES

    if not is_available or saldo_critico or dias_bajos:
        lineas = ["🔋 <b>Alerta de saldo DeepSeek</b>", ""]

        if not is_available:
            lineas.append("❌ La cuenta ya no tiene saldo disponible para hacer consultas.")
        else:
            lineas.append(f"Saldo actual: <b>${balance_usd:.2f} USD</b>")

        if dias_restantes is not None:
            lineas.append(f"Proyección al ritmo de consumo actual: <b>~{dias_restantes:.1f} días restantes</b>")

        lineas.append("")
        lineas.append(
            "Recarga antes de que se agote para no dejar a los usuarios sin "
            "consultas de IA: https://platform.deepseek.com/"
        )

        enviar_alerta_telegram("\n".join(lineas))
        logger.warning("Alerta de saldo enviada (saldo_critico=%s, dias_bajos=%s).", saldo_critico, dias_bajos)
    else:
        logger.info("Saldo saludable, sin alerta.")


if __name__ == "__main__":
    main()
