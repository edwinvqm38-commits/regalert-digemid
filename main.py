import logging
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

from agents.agent_monitor import MonitorAgent
from agents.agent_register import RegisterAgent
from agents.agent_notify import NotifyAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

DIAS_SIN_ALERTAS_SOSPECHOSO = 5


def check_staleness(register: RegisterAgent, max_days: int = DIAS_SIN_ALERTAS_SOSPECHOSO) -> None:
    """Avisa (sin depender de Telegram) si hace demasiados dias que no se
    registra ninguna alerta nueva: puede ser DIGEMID sin novedades, o el
    scraper fallando en silencio por un cambio de formato en el sitio."""
    try:
        response = (
            register.supabase
            .table(register.table_name)
            .select("created_at")
            .eq("source_type", "alerta")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = response.data or []
        if not rows or not rows[0].get("created_at"):
            return

        last_created = datetime.fromisoformat(rows[0]["created_at"].replace("Z", "+00:00"))
        gap_days = (datetime.now(timezone.utc) - last_created).days

        if gap_days >= max_days:
            message = (
                f"VIGIA: no se registra ninguna alerta DIGEMID nueva hace {gap_days} dia(s) "
                f"(ultima el {last_created.date().isoformat()}). Puede ser normal, o el scraper "
                "esta fallando en silencio por un cambio de formato en el sitio de DIGEMID."
            )
            logger.warning(message)
            # Annotation de GitHub Actions: se ve en el resumen del run y dispara
            # notificacion por correo a quienes vigilan el repo, sin depender de
            # que Telegram este funcionando.
            print(f"::warning::{message}")
    except Exception:
        logger.exception("No se pudo evaluar el vigia de alertas atrasadas.")


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
    register.process_and_save(detected_docs)

    check_staleness(register)

    pending_docs = register.get_pending_notification_docs()
    logger.info("Documentos pendientes de notificar: %s", len(pending_docs))

    if not pending_docs:
        logger.info("No hay documentos pendientes de notificar.")
        return

    notifier = NotifyAgent()
    summary_ok = notifier.send_summary(pending_docs)
    notifier.send_individual_alerts(pending_docs)

    if not summary_ok:
        pending_keys = [doc["document_key"] for doc in pending_docs if doc.get("document_key")]
        message = (
            f"Fallo el envio del resumen a Telegram; {len(pending_keys)} documento(s) ya estan "
            "en Supabase y quedan pendientes para reintentarse en la proxima corrida."
        )
        logger.error(message)
        print(f"::error::RegAlert DIGEMID: {message}")
        sys.exit(1)

    register.mark_notified(
        [doc["document_key"] for doc in pending_docs if doc.get("document_key")]
    )

    logger.info("--- Proceso finalizado correctamente ---")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as error:
        logger.exception("Error crítico en el pipeline: %s", error)
        sys.exit(1)