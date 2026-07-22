"""Script puntual (uso unico): envia una vista previa de como se ve el
mensaje de una alerta, solo al chat admin (TELEGRAM_CHAT_ID), sin tocar a
los suscriptores (no llama send_individual_alerts)."""

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from dotenv import load_dotenv

from agents.agent_notify import NotifyAgent
from agents.agent_register import RegisterAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()

    register = RegisterAgent()
    response = (
        register.supabase
        .table(register.table_name)
        .select("*")
        .eq("source_type", "alerta")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    docs = response.data or []

    if not docs:
        logger.error("No se encontro ninguna alerta en Supabase.")
        sys.exit(1)

    logger.info("Vista previa con: %s", docs[0].get("document_key"))

    notifier = NotifyAgent()
    ok = notifier.send_summary(docs)

    if not ok:
        logger.error("Fallo el envio de la vista previa.")
        sys.exit(1)

    logger.info("Vista previa enviada correctamente (solo chat admin).")


if __name__ == "__main__":
    main()
