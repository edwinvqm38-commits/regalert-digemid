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
from agents.agent_notify import NotifyAgent

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

    try:
        notificar_normativa_pendiente(register)
    except Exception:
        # Un fallo notificando (ej. credenciales de Telegram faltantes) no
        # debe tumbar la fase de metadata: el descubrimiento y registro ya
        # quedaron guardados: es lo que importa que corra a diario sin falla.
        logger.exception("No se pudo notificar la normativa nueva por Telegram.")


def _deduplicar_por_detalle(docs: list[dict]) -> list[dict]:
    """DIGEMID a veces lista la misma norma bajo dos secciones distintas (ej.
    normas-legales y resolucion-ministerial); como el document_key de
    respaldo se arma por seccion cuando no se puede parsear un numero de
    norma, terminan como dos filas para el mismo documento real. Se colapsan
    por detail_url para no avisar dos veces lo mismo."""
    vistos: set[str] = set()
    unicos: list[dict] = []

    for doc in docs:
        detalle = doc.get("detail_url")
        if detalle:
            if detalle in vistos:
                continue
            vistos.add(detalle)
        unicos.append(doc)

    return unicos


def notificar_normativa_pendiente(register: NormativeRegisterAgent) -> None:
    pending_docs = register.get_pending_notification_docs()
    logger.info("Normativa pendiente de notificar: %s", len(pending_docs))

    if not pending_docs:
        return

    docs_a_enviar = _deduplicar_por_detalle(pending_docs)
    if len(docs_a_enviar) < len(pending_docs):
        logger.info(
            "Normativa duplicada colapsada por detail_url: %s documento(s) -> %s a notificar.",
            len(pending_docs),
            len(docs_a_enviar),
        )

    notifier = NotifyAgent()
    summary_ok = notifier.send_normativa_summary(docs_a_enviar)
    notifier.send_normativa_individual(docs_a_enviar)

    if not summary_ok:
        logger.error(
            "Fallo el envio del resumen de normativa a Telegram; %s documento(s) quedan "
            "pendientes para reintentarse en la proxima corrida.",
            len(pending_docs),
        )
        return

    register.mark_notified(
        [doc["document_key"] for doc in pending_docs if doc.get("document_key")]
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
