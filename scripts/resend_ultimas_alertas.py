"""Script puntual (uso unico): reenvia solo las alertas indicadas y avisa a
los destinatarios que descarten el lote anterior de alertas antiguas que se
reenvio por error durante una corrida de prueba."""

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

DOCUMENT_KEYS = ["80-2026", "81-2026"]

AVISO = (
    "ℹ️ <b>Aviso puntual</b>\n\n"
    "Por una corrida de prueba se reenvió por error un lote de alertas antiguas. "
    "Por favor <b>descarta ese lote anterior</b> y considera únicamente las "
    "siguientes alertas, que son las nuevas de hoy:"
)


def main() -> None:
    load_dotenv()

    register = RegisterAgent()
    response = (
        register.supabase
        .table(register.table_name)
        .select("*")
        .in_("document_key", DOCUMENT_KEYS)
        .execute()
    )
    docs = response.data or []

    if not docs:
        logger.error("No se encontraron los documentos %s en Supabase.", DOCUMENT_KEYS)
        sys.exit(1)

    logger.info(
        "Reenviando %s alerta(s): %s",
        len(docs),
        [doc.get("document_key") for doc in docs],
    )

    notifier = NotifyAgent()

    notifier._send_to_chat(notifier.chat_id, AVISO)

    pagados, en_prueba = notifier._usuarios_elegibles_dm()
    for chat_id in set(pagados) | set(en_prueba.keys()):
        notifier._send_to_chat(chat_id, AVISO)

    summary_ok = notifier.send_summary(docs)
    notifier.send_individual_alerts(docs)

    if not summary_ok:
        logger.error("Fallo el envio del resumen con las alertas reenviadas.")
        sys.exit(1)

    logger.info("Reenvio puntual completado correctamente.")


if __name__ == "__main__":
    main()
