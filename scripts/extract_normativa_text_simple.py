"""Extrae texto de PDFs de normativa DIGEMID directo desde digemid.minsa.gob.pe
(sin Google Drive) y lo guarda en digemid_norma_paginas con alta fidelidad.

Usa agents.pdf_extract (PyMuPDF -> pdfplumber -> OCR) y guarda un puntaje de
calidad por página para poder marcar transcripciones de baja confiabilidad.
Al terminar, envía un resumen de progreso al Telegram del administrador.

Solo procesa normas con pdf_url directo. Las normas sin pdf_url requieren un
rastreo previo de su página oficial (script aparte).
"""

import argparse
import logging
import os
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from agents.pdf_extract import extract_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PAGE_TABLE = "digemid_norma_paginas"
NORMAS_TABLE = "digemid_normas"
STORAGE_BUCKET = "digemid-documentos"
DELAY_BETWEEN_DESCARGAS_SEGUNDOS = 4.0
MAX_REINTENTOS_429 = 3
UMBRAL_BAJA_CALIDAD = 0.5


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def contar_universo(supabase) -> tuple[int, int]:
    total = supabase.table(NORMAS_TABLE).select("id", count="exact", head=True).execute()
    con_texto = (
        supabase.table(PAGE_TABLE)
        .select("norma_id", count="exact", head=True)
        .execute()
    )
    return (total.count or 0), (con_texto.count or 0)


def get_pending_normas(supabase, limit: int, document_key: str | None = None) -> list[dict]:
    query = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, pdf_url, file_name")
        .not_.is_("pdf_url", "null")
        .neq("pdf_url", "")
    )
    if document_key:
        query = query.eq("document_key", document_key)
    else:
        query = query.order("anio", desc=True)

    response = query.limit(limit).execute()
    normas = response.data or []
    if not normas:
        return []

    # Con un document_key explícito reprocesamos aunque ya tenga páginas.
    if document_key:
        return normas

    norma_ids = [n["id"] for n in normas]
    paginas = (
        supabase.table(PAGE_TABLE)
        .select("norma_id")
        .in_("norma_id", norma_ids)
        .execute()
    )
    con_paginas = {row["norma_id"] for row in (paginas.data or []) if row.get("norma_id")}
    return [n for n in normas if n["id"] not in con_paginas]


def sanitize_file_name(document_key: str, file_name: str | None) -> str:
    base = file_name or f"{document_key}.pdf"
    safe = base.replace("/", "-").replace("\\", "-").strip()
    if not safe.lower().endswith(".pdf"):
        safe = f"{safe}.pdf"
    return safe


def download_pdf(url: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Descargando PDF normativo: %s", url)

    for intento in range(1, MAX_REINTENTOS_429 + 1):
        response = requests.get(
            url,
            timeout=120,
            headers={"User-Agent": "RegAlert-DIGEMID-NormativaText/1.0"},
        )
        if response.status_code == 429 and intento < MAX_REINTENTOS_429:
            espera = float(response.headers.get("Retry-After", 10 * intento))
            logger.warning("429 en %s (intento %s). Espero %.1fs.", url, intento, espera)
            time.sleep(espera)
            continue
        response.raise_for_status()
        local_path.write_bytes(response.content)
        return local_path

    raise RuntimeError(f"No se pudo descargar {url} tras {MAX_REINTENTOS_429} intentos (429)")


def respaldar_pdf(supabase, object_path: str, local_path: Path) -> None:
    """Sube el PDF ya descargado a Supabase Storage como evidencia durable."""
    with local_path.open("rb") as file_obj:
        supabase.storage.from_(STORAGE_BUCKET).upload(
            object_path,
            file_obj,
            file_options={"content-type": "application/pdf", "upsert": "true"},
        )


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFC", text or "").strip()


def write_pages(supabase, norma_id: str, extracciones) -> int:
    """Escribe páginas y devuelve cuántas quedaron con baja calidad."""
    baja_calidad = 0
    for page in extracciones:
        if page.quality < UMBRAL_BAJA_CALIDAD:
            baja_calidad += 1

        payload = {
            "norma_id": norma_id,
            "page_number": page.page_number,
            "text_raw": page.text,
            "text_normalized": normalize_text(page.text),
            "extraction_method": page.method,
            "ocr_used": page.ocr_used,
            "quality_score": page.quality,
            "metadata": {"quality_score": page.quality, "method": page.method},
        }
        supabase.table(PAGE_TABLE).insert(payload).execute()
    return baja_calidad


def mark_norma(supabase, norma_id: str, status: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    supabase.table(NORMAS_TABLE).update(
        {"process_status": status, "updated_at": now}
    ).eq("id", norma_id).execute()


def enviar_progreso_telegram(total: int, con_texto: int, procesadas_ahora: int, normas_baja: int) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.info("Sin TELEGRAM_BOT_TOKEN o chat_id: no se envía progreso.")
        return

    lines = [
        "📚 <b>Progreso normativa DIGEMID</b>",
        "",
        f"Total de normas/reglamentos: <b>{total}</b>",
        f"Con texto extraído: <b>{con_texto}/{total}</b>",
        f"Procesadas en esta corrida: <b>{procesadas_ahora}</b>",
    ]
    if normas_baja:
        lines.append(f"⚠️ Con baja confiabilidad: <b>{normas_baja}</b> (revisar antes de confiar en consultas)")
    else:
        lines.append("✅ Sin páginas de baja confiabilidad en esta corrida.")

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "HTML"},
            timeout=20,
        )
    except Exception:
        logger.exception("No se pudo enviar el progreso a Telegram.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--document-key", default=None,
                        help="Reprocesar SOLO esta norma (borra sus páginas y las regenera).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    total_universo, con_texto_antes = contar_universo(supabase)
    normas = get_pending_normas(supabase, args.limit, args.document_key)
    logger.info(
        "Universo: %s normas | con texto: %s | pendientes con pdf_url en este lote: %s",
        total_universo, con_texto_antes, len(normas),
    )

    procesadas = 0
    errores = 0
    normas_baja_calidad = 0
    temp_dir = Path("tmp") / "normativa_text"

    for index, norma in enumerate(normas):
        if index > 0:
            time.sleep(DELAY_BETWEEN_DESCARGAS_SEGUNDOS)

        document_key = norma["document_key"]
        pdf_url = norma["pdf_url"]
        file_name = sanitize_file_name(document_key, norma.get("file_name"))

        try:
            if args.dry_run:
                logger.info("[dry-run] Extraería texto de %s (%s)", document_key, pdf_url)
                procesadas += 1
                continue

            # Si reprocesamos una norma específica, borramos sus páginas viejas
            # para regenerarlas con el pipeline de alta calidad (sin duplicar).
            if args.document_key:
                supabase.table(PAGE_TABLE).delete().eq("norma_id", norma["id"]).execute()

            local_path = temp_dir / file_name
            download_pdf(pdf_url, local_path)

            # Respaldo del PDF como evidencia durable (reusa el archivo ya descargado).
            object_path = f"normas/{document_key}/{file_name}"
            try:
                respaldar_pdf(supabase, object_path, local_path)
                supabase.table(NORMAS_TABLE).update(
                    {"file_storage_path": object_path}
                ).eq("id", norma["id"]).execute()
            except Exception:
                logger.exception("No se pudo respaldar el PDF de %s (se continúa con el texto).", document_key)

            extracciones = extract_pdf(str(local_path))

            baja = write_pages(supabase, norma["id"], extracciones)
            promedio = sum(p.quality for p in extracciones) / max(1, len(extracciones))
            estado = "text_extracted" if promedio >= UMBRAL_BAJA_CALIDAD else "text_extracted_baja_calidad"
            mark_norma(supabase, norma["id"], estado)

            if baja > 0:
                normas_baja_calidad += 1

            procesadas += 1
            logger.info(
                "%s | páginas: %s | calidad prom: %.2f | páginas baja calidad: %s",
                document_key, len(extracciones), promedio, baja,
            )

        except Exception as error:
            errores += 1
            logger.exception("Error procesando %s: %s", document_key, error)
            if not args.dry_run:
                mark_norma(supabase, norma["id"], "text_extraction_error")

    logger.info("Finalizado. Procesadas: %s | Errores: %s | Con baja calidad: %s",
                procesadas, errores, normas_baja_calidad)

    if not args.dry_run and not args.no_telegram:
        _, con_texto_despues = contar_universo(supabase)
        enviar_progreso_telegram(total_universo, con_texto_despues, procesadas, normas_baja_calidad)


if __name__ == "__main__":
    main()
