import logging
import sys
from dotenv import load_dotenv

from agents.agent_monitor import MonitorAgent
from agents.agent_register import RegisterAgent
from agents.agent_notify import NotifyAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    load_dotenv()

    logger.info("--- Iniciando monitoreo DIGEMID ---")

    monitor = MonitorAgent()
    detected_docs = monitor.get_latest_alerts()

    logger.info("Documentos detectados: %s", len(detected_docs))

    if not detected_docs:
        logger.info("No se detectaron documentos en la fuente.")
        return

    register = RegisterAgent()
    new_docs = register.process_and_save(detected_docs)

    logger.info("Documentos nuevos registrados: %s", len(new_docs))

    if not new_docs:
        logger.info("No hay documentos nuevos para notificar.")
        return

    notifier = NotifyAgent()
    notifier.send_summary(new_docs)

    logger.info("--- Proceso finalizado correctamente ---")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as error:
        logger.exception("Error crítico en el pipeline: %s", error)
        sys.exit(1)