import logging
import sys
from pathlib import Path

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


def main():
    load_dotenv()

    fake_doc = {
        "document_key": "PRUEBA-000-2026",
        "title": "[PRUEBA] Este es un mensaje de prueba - no es una alerta oficial de DIGEMID",
        "detail_url": "https://www.digemid.minsa.gob.pe/webDigemid/alertas-modificaciones/",
    }

    NotifyAgent().send_summary([fake_doc])
    logger.info("Notificacion de prueba enviada.")


if __name__ == "__main__":
    main()
