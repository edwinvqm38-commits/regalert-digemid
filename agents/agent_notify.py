import html
import logging
import os
from datetime import datetime, timedelta, timezone

import requests

from agents.agent_utils import es_titulo_generico

logger = logging.getLogger(__name__)

PRUEBA_LIMITE_ALERTAS = 3
PRUEBA_LIMITE_DIAS = 14

NIVEL_PRECIOS = {"basico": 29, "consultoria": 79, "empresarial": 199}

PERU_TZ = timezone(timedelta(hours=-5))

# Prefijo del document_key (ver TYPE_PATTERNS en agent_normative_monitor.py)
# a etiqueta legible. documento_tipo/documento_subtipo solo viven dentro de
# "raw" y no llegan a digemid_documentos como columna, asi que el prefijo es
# la forma confiable de distinguir Ley/Decreto/Resolucion en el mensaje.
NORMATIVA_TIPO_POR_PREFIJO = {
    "LEY": "Ley",
    "DS": "Decreto Supremo",
    "DL": "Decreto Legislativo",
    "DU": "Decreto de Urgencia",
    "RM": "Resolución Ministerial",
    "RD": "Resolución Directoral",
    "RS": "Resolución Suprema",
}

NORMATIVA_TIPO_POR_SECCION = {
    "normas-legales": "Norma Legal",
    "resolucion-ministerial": "Resolución Ministerial",
    "decreto-supremo": "Decreto Supremo",
}


def _tipo_normativa(doc: dict) -> str:
    prefijo = str(doc.get("document_key", "")).split("-", 1)[0].upper()
    if prefijo in NORMATIVA_TIPO_POR_PREFIJO:
        return NORMATIVA_TIPO_POR_PREFIJO[prefijo]

    return NORMATIVA_TIPO_POR_SECCION.get(doc.get("source_section"), "Documento normativo")


def _hora_aproximada_deteccion(doc: dict) -> str | None:
    """DIGEMID no publica la hora exacta en que sube una alerta (solo un
    dia/mes en su tarjeta, sin reloj). Como aproximacion usamos la hora en
    que nuestro propio sistema detecto el documento (raw.scraped_at, en
    UTC), convertida a hora Peru en formato 12h con am/pm (ej. '5:41 pm')."""
    scraped_at = (doc.get("raw") or {}).get("scraped_at")
    if not scraped_at:
        return None

    try:
        momento = datetime.fromisoformat(scraped_at)
    except ValueError:
        return None

    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)

    hora_12h = momento.astimezone(PERU_TZ).strftime("%I:%M %p").lstrip("0")
    return hora_12h.lower()


class NotifyAgent:
    """Agente responsable de notificar novedades por Telegram."""

    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not self.token or not self.chat_id:
            raise ValueError(
                "Faltan variables de entorno TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID"
            )

        self.supabase = None
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if supabase_url and supabase_key:
            from supabase import create_client

            self.supabase = create_client(supabase_url, supabase_key)

    def build_message(self, new_docs: list[dict]) -> str:
        """Construye el mensaje HTML para Telegram."""
        lines = [
            "🚨 <b>Nuevas alertas DIGEMID detectadas</b>",
            "",
        ]

        for doc in new_docs[:10]:
            document_key = str(doc.get("document_key", ""))
            key = html.escape(document_key)
            title_original = str(doc.get("title", ""))
            detail_url = html.escape(str(doc.get("detail_url", "")))
            fecha_publicacion = doc.get("published_date_display")
            hora_deteccion = _hora_aproximada_deteccion(doc)

            # DIGEMID no pone el nombre del producto en el link del listado,
            # solo el numero de alerta ("ALERTA DIGEMID N 81-2026"): mostrar
            # ese titulo junto al document_key es puro relleno repetido.
            if es_titulo_generico(title_original, document_key):
                lines.append(f"📁 <b>Alerta DIGEMID Nº {key}</b>")
            else:
                title = html.escape(title_original)[:250]
                lines.append(f"📁 <b>{key}</b> — {title}")

            if fecha_publicacion:
                fecha_escapada = html.escape(str(fecha_publicacion))
                if hora_deteccion:
                    lines.append(f"🗓️ Fecha: {fecha_escapada} ({hora_deteccion})")
                else:
                    lines.append(f"🗓️ Fecha: {fecha_escapada}")
            lines.append(f"🔗 {detail_url}")
            lines.append("")

        if len(new_docs) > 10:
            lines.append(f"➕ Y {len(new_docs) - 10} documentos más.")

        return "\n".join(lines)

    def send_summary(self, new_docs: list[dict]) -> bool:
        """Envía resumen solo si existen documentos nuevos.

        Devuelve True si Telegram confirmó el envío. Nunca lanza excepción:
        un fallo aquí (ej. TELEGRAM_CHAT_ID invalido) no debe tumbar el resto
        del pipeline ni bloquear el envio de DMs a suscriptores individuales,
        que es un canal independiente.
        """
        if not new_docs:
            logger.info("No hay documentos nuevos para notificar.")
            return True

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": self.build_message(new_docs),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = requests.post(url, json=payload, timeout=20)

            if not response.ok:
                logger.error("Respuesta de Telegram: %s", response.text)

            response.raise_for_status()
        except Exception:
            logger.exception("No se pudo enviar el resumen a Telegram.")
            return False

        logger.info("Notificación enviada a Telegram.")
        return True

    def build_normativa_message(self, new_docs: list[dict]) -> str:
        """Construye el mensaje HTML para normativa nueva (Ley/Decreto/RM/...).

        Solo referencia metadata y los links oficiales (detalle en DIGEMID, y
        el PDF si ya se confirmo): la fase que extrae y verifica el texto de
        cada norma es independiente y hoy no corre sobre lo recien detectado,
        asi que este mensaje nunca debe insinuar que el contenido ya fue
        revisado o transcrito.
        """
        lines = [
            "📜 <b>Nueva normativa DIGEMID detectada</b>",
            "",
        ]

        for doc in new_docs[:10]:
            key = html.escape(str(doc.get("document_key", "")))
            tipo = html.escape(_tipo_normativa(doc))
            title = html.escape(str(doc.get("title", "")))[:250]
            detail_url = html.escape(str(doc.get("detail_url", "")))
            fecha_publicacion = doc.get("published_date_display")

            lines.append(f"📁 <b>{tipo} {key}</b>")
            if title:
                lines.append(title)
            if fecha_publicacion:
                lines.append(f"🗓️ Fecha: {html.escape(str(fecha_publicacion))}")
            lines.append(f"🔗 {detail_url}")

            file_url = doc.get("file_url")
            if file_url:
                lines.append(f"📄 PDF: {html.escape(str(file_url))}")

            lines.append("")

        if len(new_docs) > 10:
            lines.append(f"➕ Y {len(new_docs) - 10} documentos más.")

        lines.append(
            "ℹ️ Detección automática por título/metadata. El contenido aún no "
            "pasó por el proceso de verificación de fidelidad: para citar el "
            "texto, revisa el PDF oficial en el enlace."
        )

        return "\n".join(lines)

    def send_normativa_summary(self, new_docs: list[dict]) -> bool:
        """Envía resumen de normativa nueva. Nunca lanza excepción (ver send_summary)."""
        if not new_docs:
            logger.info("No hay normativa nueva para notificar.")
            return True

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": self.build_normativa_message(new_docs),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = requests.post(url, json=payload, timeout=20)

            if not response.ok:
                logger.error("Respuesta de Telegram (normativa): %s", response.text)

            response.raise_for_status()
        except Exception:
            logger.exception("No se pudo enviar el resumen de normativa a Telegram.")
            return False

        logger.info("Notificación de normativa enviada a Telegram.")
        return True

    def send_pipeline_failure_alert(self, failed_steps: list[str]) -> None:
        """Notifica cuando uno o más pasos del pipeline fallan."""
        if not failed_steps:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        lines = [
            "⚠️ <b>Pipeline DIGEMID: pasos fallidos</b>",
            "",
        ]
        for step_name in failed_steps:
            lines.append(f"• {html.escape(step_name)}")
        lines.append("")
        lines.append("Revisar logs en GitHub Actions.")

        payload = {
            "chat_id": self.chat_id,
            "text": "\n".join(lines),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = requests.post(url, json=payload, timeout=20)
            response.raise_for_status()
            logger.info("Alerta de fallo enviada a Telegram.")
        except Exception:
            logger.exception("No se pudo enviar la alerta de fallo a Telegram.")

    def _send_to_chat(self, chat_id: str, text: str, reply_markup: dict | None = None) -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        response = requests.post(
            f"https://api.telegram.org/bot{self.token}/sendMessage", json=payload, timeout=20
        )
        if not response.ok:
            logger.warning("No se pudo enviar DM a %s: %s", chat_id, response.text)

    def _usuarios_elegibles_dm(self) -> tuple[set[str], dict[str, int]]:
        """Devuelve (chat_ids con suscripcion paga activa, {chat_id: alertas_en_prueba} de los en prueba activa)."""
        pagados: set[str] = set()
        en_prueba: dict[str, int] = {}

        hoy = datetime.now(timezone.utc).date().isoformat()
        suscripciones = (
            self.supabase.table("digemid_suscripciones")
            .select("telegram_chat_id, fecha_fin")
            .eq("estado", "activo")
            .execute()
        )
        for sub in suscripciones.data or []:
            fecha_fin = sub.get("fecha_fin")
            if not fecha_fin or fecha_fin >= hoy:
                pagados.add(sub["telegram_chat_id"])

        en_prueba_rows = (
            self.supabase.table("digemid_bot_usuarios")
            .select("telegram_chat_id, prueba_alertas_enviadas")
            .eq("prueba_estado", "activa")
            .execute()
        )
        for row in en_prueba_rows.data or []:
            en_prueba[row["telegram_chat_id"]] = row.get("prueba_alertas_enviadas") or 0

        return pagados, en_prueba

    def _mensaje_prueba_terminada(self) -> tuple[str, dict]:
        texto = (
            "⏰ <b>Tu prueba gratuita de RegAlert DIGEMID terminó.</b>\n\n"
            "¿Quieres seguir recibiendo alertas automáticas y consultas con IA? Elige un plan:"
        )
        botones = {
            "inline_keyboard": [
                [{"text": f"Solicitar Básico — S/{NIVEL_PRECIOS['basico']}/mes", "callback_data": "plan:basico"}],
                [{"text": f"Solicitar Consultoría — S/{NIVEL_PRECIOS['consultoria']}/mes", "callback_data": "plan:consultoria"}],
                [{"text": f"Solicitar Empresarial — S/{NIVEL_PRECIOS['empresarial']}/mes", "callback_data": "plan:empresarial"}],
            ]
        }
        return texto, botones

    def send_individual_alerts(self, new_docs: list[dict]) -> None:
        """Envia las alertas nuevas por privado a suscriptores pagos y usuarios en prueba activa.

        Los usuarios en prueba activa suman a su contador; al llegar a
        PRUEBA_LIMITE_ALERTAS se cierra la prueba y se les ofrece suscribirse.
        """
        if not new_docs or self.supabase is None:
            return

        texto = self.build_message(new_docs)
        pagados, en_prueba = self._usuarios_elegibles_dm()

        for chat_id in pagados:
            self._send_to_chat(chat_id, texto)

        for chat_id, alertas_previas in en_prueba.items():
            self._send_to_chat(chat_id, texto)

            nuevas_alertas = alertas_previas + len(new_docs)
            actualizacion = {"prueba_alertas_enviadas": nuevas_alertas}

            if nuevas_alertas >= PRUEBA_LIMITE_ALERTAS:
                actualizacion["prueba_estado"] = "finalizada"

            self.supabase.table("digemid_bot_usuarios").update(actualizacion).eq(
                "telegram_chat_id", chat_id
            ).execute()

            if nuevas_alertas >= PRUEBA_LIMITE_ALERTAS:
                texto_fin, botones_fin = self._mensaje_prueba_terminada()
                self._send_to_chat(chat_id, texto_fin, botones_fin)

        logger.info(
            "DM enviados: %s pagados, %s en prueba (de los cuales %s finalizaron su prueba).",
            len(pagados),
            len(en_prueba),
            sum(1 for cid, prev in en_prueba.items() if prev + len(new_docs) >= PRUEBA_LIMITE_ALERTAS),
        )

    def send_normativa_individual(self, new_docs: list[dict]) -> None:
        """DM de normativa nueva a los mismos suscriptores que las alertas.

        A diferencia de send_individual_alerts, no toca prueba_alertas_enviadas
        ni el cierre de la prueba gratuita: ese limite es del producto de
        alertas de producto, y la normativa es un contenido distinto que no
        debe consumir ni terminar esa prueba.
        """
        if not new_docs or self.supabase is None:
            return

        texto = self.build_normativa_message(new_docs)
        pagados, en_prueba = self._usuarios_elegibles_dm()

        for chat_id in pagados:
            self._send_to_chat(chat_id, texto)

        for chat_id in en_prueba:
            self._send_to_chat(chat_id, texto)

        logger.info(
            "DM de normativa enviados: %s pagados, %s en prueba.",
            len(pagados),
            len(en_prueba),
        )
