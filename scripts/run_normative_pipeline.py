import logging
import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.agent_normative_monitor import NormativeMonitorAgent
from agents.agent_normative_pdf_detector import NormativePdfDetectorAgent
from agents.agent_normative_register import NormativeRegisterAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def run_metadata_phase() -> None:
    logger.info("=== Fase metadata normativa DIGEMID ===")
    monitor = NormativeMonitorAgent()
    documents = monitor.collect_documents()

    logger.info("Registros normativos detectados por scraping: %s", len(documents))

    register = NormativeRegisterAgent()
    summary = register.process_and_save(documents)

    logger.info(
        "Resumen metadata | encontrados=%s | nuevos=%s | actualizados=%s | guardados=%s",
        summary["found"],
        summary["new"],
        summary["updated"],
        summary["saved"],
    )


def run_detect_pdf_phase() -> None:
    logger.info("=== Fase deteccion PDF normativa DIGEMID ===")
    detector = NormativePdfDetectorAgent()
    detector.process()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=["metadata", "detect-pdf", "all"],
        default="all",
    )
    args = parser.parse_args()

    load_dotenv()

    logger.info("=== Iniciando pipeline normativo DIGEMID ===")

    if args.phase in ("metadata", "all"):
        run_metadata_phase()

    if args.phase in ("detect-pdf", "all"):
        run_detect_pdf_phase()

    logger.info("=== Pipeline normativo DIGEMID finalizado ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.exception("Error critico en pipeline normativo: %s", error)
        sys.exit(1)
