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
PENDING_STATUSES = ("inventory_imported",)
RETRYABLE_ERROR_STATUSES = ("text_extraction_error", "pdf_download_error")


class PdfDownloadError(Exception):
    def __init__(self, url: str, status_code: int | None, content_type: str | None, message: str):
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.content_type = content_type


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
    """Devuelve (total de normas, normas completadas con exito). Se cuenta
    por process_status (no trayendo todas las filas de digemid_norma_paginas
    y contando norma_id distintos): esa alternativa trae UNA FILA POR PAGINA
    de TODA la tabla sin paginar, y se trunca silenciosamente si el total de
    paginas supera el limite de filas de PostgREST, subestimando el avance
    real a medida que crece la base."""
    total = supabase.table(NORMAS_TABLE).select("id", count="exact", head=True).execute()
    con_texto = (
        supabase.table(NORMAS_TABLE)
        .select("id", count="exact", head=True)
        .in_("process_status", ["text_extracted", "text_extracted_baja_calidad"])
        .execute()
    )
    return (total.count or 0), (con_texto.count or 0)


def get_pending_normas(
    supabase,
    limit: int,
    document_key: str | None = None,
    document_keys: list[str] | None = None,
    retry_errors: bool = False,
) -> list[dict]:
    query = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, pdf_url, file_name, titulo, process_status, raw")
        .not_.is_("pdf_url", "null")
        .neq("pdf_url", "")
    )
    if document_keys:
        response = query.in_("document_key", document_keys).execute()
        # Con document_keys explicitos reprocesamos aunque ya tengan paginas
        # (ej. backfill de normas con tablas detectadas antes de que
        # extract_page() empezara a convertirlas a Markdown).
        return response.data or []
    if document_key:
        response = query.eq("document_key", document_key).limit(limit).execute()
        # Con un document_key explícito reprocesamos aunque ya tenga páginas.
        return response.data or []

    # "Pendiente" se determina por process_status, NO por si la norma ya
    # tiene alguna fila en digemid_norma_paginas. write_pages() inserta
    # pagina por pagina (no es una transaccion), asi que una norma puede
    # haber fallado a mitad de camino con solo algunas paginas escritas.
    # Las filas en error NO se reintentan en corridas programadas normales:
    # si el pdf_url esta roto, el lote queda atrapado en las mismas normas
    # cada hora y no avanza. Se reintentan solo con --retry-errors o
    # --document-key explicito.
    #
    # Antes esto se resolvia consultando que norma_id ya aparecen en
    # digemid_norma_paginas, pero esa consulta trae UNA FILA POR PAGINA (no
    # por norma) y puede truncarse silenciosamente si el total de paginas ya
    # guardadas supera el limite de filas de PostgREST — con normas de 100+
    # paginas eso pasa rapido, y una norma completa terminaba pareciendo
    # "pendiente" (reintentando y chocando con la pagina 1 ya existente).
    #
    # OJO: el limite se aplica DESPUES del filtro, no antes, para no quedar
    # atascado en las normas mas nuevas (order by anio desc) si esas ya
    # estan listas mientras quedan cientos mas antiguas sin procesar.
    statuses = list(PENDING_STATUSES)
    if retry_errors:
        statuses.extend(RETRYABLE_ERROR_STATUSES)

    status_filters = ["process_status.is.null"]
    status_filters.extend(f"process_status.eq.{status}" for status in statuses)
    response = (
        query
        .or_(",".join(status_filters))
        .order("anio", desc=True)
        .execute()
    )
    normas = response.data or []
    return normas[:limit]


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
        content_type = response.headers.get("Content-Type", "")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise PdfDownloadError(
                url=url,
                status_code=response.status_code,
                content_type=content_type,
                message=f"Descarga PDF fallida: HTTP {response.status_code} en {url}",
            ) from exc

        if not response.content.startswith(b"%PDF"):
            raise PdfDownloadError(
                url=url,
                status_code=response.status_code,
                content_type=content_type,
                message=(
                    "Descarga invalida: la respuesta no parece un PDF "
                    f"(content-type={content_type or 'sin content-type'}) en {url}"
                ),
            )

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


def write_pages(supabase, norma_id: str, extracciones) -> dict:
    """Escribe páginas y devuelve estadísticas de la norma para auditoría:
    cuántas quedaron con baja calidad, cuántas usaron OCR, cuántas traen
    tablas detectadas y cuántas se marcaron con posible fórmula/notación
    técnica (estas dos últimas requieren revisión humana antes de usarse en
    consultas legales, ya que ni el texto plano ni el OCR las reconstruyen
    con fidelidad)."""
    stats = {
        "baja_calidad": 0,
        "ocr_usado": False,
        "con_tablas": 0,
        "con_formula": 0,
        "paginas_baja_calidad": [],
    }

    for page in extracciones:
        if page.quality < UMBRAL_BAJA_CALIDAD:
            stats["baja_calidad"] += 1
            stats["paginas_baja_calidad"].append(page.page_number)
        if page.ocr_used:
            stats["ocr_usado"] = True
        if page.has_tables:
            stats["con_tablas"] += 1
        if page.posible_formula:
            stats["con_formula"] += 1

        payload = {
            "norma_id": norma_id,
            "page_number": page.page_number,
            "text_raw": page.text,
            "text_normalized": normalize_text(page.text),
            "extraction_method": page.method,
            "ocr_used": page.ocr_used,
            "ocr_confidence": page.ocr_confidence,
            "quality_score": page.quality,
            "has_tables": page.has_tables,
            "posible_formula": page.posible_formula,
            "metadata": {
                "quality_score": page.quality,
                "method": page.method,
                "ocr_confidence": page.ocr_confidence,
                "posible_formula": page.posible_formula,
                "tables": page.tables,
            },
        }
        supabase.table(PAGE_TABLE).insert(payload).execute()

    return stats


def mark_norma(
    supabase,
    norma_id: str,
    status: str,
    stats: dict | None = None,
    raw: dict | None = None,
    error_info: dict | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = {"process_status": status, "updated_at": now}

    if stats is not None:
        payload["ocr_required"] = stats["ocr_usado"]
        payload["has_tables"] = stats["con_tablas"] > 0

    if error_info is not None:
        next_raw = dict(raw or {})
        next_raw["text_extraction_last_error"] = {
            **error_info,
            "status": status,
            "occurred_at": now,
        }
        payload["raw"] = next_raw

    supabase.table(NORMAS_TABLE).update(payload).eq("id", norma_id).execute()


def _fetch_norma_status_rows(supabase) -> list[dict]:
    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        response = (
            supabase.table(NORMAS_TABLE)
            .select("id, process_status, pdf_url")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            return rows
        offset += page_size


def contar_estado_normativa(supabase) -> dict:
    rows = _fetch_norma_status_rows(supabase)
    completados = {"text_extracted", "text_extracted_baja_calidad"}
    pendientes = set(PENDING_STATUSES)
    errores_reintento = set(RETRYABLE_ERROR_STATUSES)

    sin_texto = [row for row in rows if row.get("process_status") not in completados]
    return {
        "total": len(rows),
        "con_texto": sum(1 for row in rows if row.get("process_status") in completados),
        "pendientes_con_pdf": sum(
            1
            for row in rows
            if row.get("process_status") in pendientes and (row.get("pdf_url") or "").strip()
        ),
        "errores_con_pdf": sum(
            1
            for row in rows
            if row.get("process_status") in errores_reintento and (row.get("pdf_url") or "").strip()
        ),
        "sin_pdf_sin_texto": sum(
            1
            for row in sin_texto
            if not (row.get("pdf_url") or "").strip()
        ),
    }


def nivel_certeza_norma(promedio: float, stats: dict) -> str:
    if stats.get("baja_calidad", 0) > 0 or promedio < UMBRAL_BAJA_CALIDAD:
        return "baja"
    if promedio < 0.85 or stats.get("ocr_usado") or stats.get("con_tablas", 0) > 0 or stats.get("con_formula", 0) > 0:
        return "media"
    return "alta"


def get_pdf_needs_for_review(supabase, limit: int = 5) -> dict:
    broken = (
        supabase.table(NORMAS_TABLE)
        .select("document_key, titulo, source_url, pdf_url, process_status")
        .in_("process_status", ["pdf_download_error", "text_extraction_error"])
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    missing_rows = (
        supabase.table(NORMAS_TABLE)
        .select("document_key, titulo, source_url, pdf_url, process_status")
        .order("anio", desc=True)
        .execute()
        .data
        or []
    )
    missing = [
        row for row in missing_rows
        if row.get("process_status") not in ("text_extracted", "text_extracted_baja_calidad")
        and not (row.get("pdf_url") or "").strip()
    ][:limit]
    return {"broken": broken, "missing": missing}


def format_review_command(document_key: str) -> str:
    return f"<code>/normarevisar {_escapar_html(document_key)}</code>"


def _escapar_html(texto: str | None) -> str:
    return (texto or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def construir_reporte_html(document_key: str, titulo: str | None, extracciones) -> str:
    """Reporte pagina por pagina para comparar contra el PDF original: color
    por calidad, señales (OCR/formula/tabla), texto completo, y las tablas
    detectadas renderizadas como tabla HTML real (no aplanadas a texto
    corrido), ya que eso es justo lo que se pierde en la extraccion plana y
    donde mas importa la fidelidad para uso legal."""
    secciones = []

    for page in extracciones:
        if page.quality >= 0.85:
            color = "#43a047"
        elif page.quality >= 0.5:
            color = "#fbc02d"
        else:
            color = "#e53935"

        señales = []
        if page.ocr_used:
            señales.append(f"OCR (confianza {page.ocr_confidence})")
        if page.posible_formula:
            señales.append("posible fórmula/notación técnica — revisar manualmente")
        if page.has_tables:
            señales.append("tabla detectada")
        señales_html = " · ".join(_escapar_html(s) for s in señales) if señales else "—"

        tablas_html = ""
        for tabla in (page.tables or []):
            filas_html = "".join(
                "<tr>" + "".join(f"<td>{_escapar_html(str(celda) if celda is not None else '')}</td>" for celda in fila) + "</tr>"
                for fila in tabla
            )
            tablas_html += f'<table class="tabla-detectada">{filas_html}</table>'

        secciones.append(f"""
        <section style="border-left: 6px solid {color}; padding-left: 1rem; margin-bottom: 1.5rem;">
          <h3>Página {page.page_number} — calidad {page.quality} ({_escapar_html(page.method)})</h3>
          <p><b>Señales:</b> {señales_html}</p>
          <pre>{_escapar_html(page.text)}</pre>
          {tablas_html}
        </section>""")

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Extracción — {_escapar_html(document_key)}</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; max-width: 900px; }}
  pre {{ white-space: pre-wrap; word-break: break-word; background: #fafafa; padding: 0.75rem; border-radius: 6px; }}
  table.tabla-detectada {{ border-collapse: collapse; margin: 0.5rem 0; }}
  table.tabla-detectada td {{ border: 1px solid #999; padding: 0.3rem 0.5rem; font-size: 0.85rem; }}
  .nota {{ background: #e3f2fd; border: 1px solid #90caf9; padding: 0.75rem; border-radius: 6px; margin-bottom: 1.5rem; }}
</style>
</head>
<body>
  <h1>{_escapar_html(document_key)}</h1>
  <p>{_escapar_html(titulo)}</p>
  <div class="nota">
    Compara este texto con el PDF adjunto en el mismo mensaje de Telegram.
    Borde verde = calidad alta, amarillo = revisar con cuidado, rojo = baja
    confiabilidad. Usa <code>/normarevisar {_escapar_html(document_key)}</code>
    en Telegram si necesitas corregir alguna página directamente.
  </div>
  {''.join(secciones)}
</body>
</html>"""


def enviar_reporte_extraccion_telegram(document_key: str, html: str, pdf_path: Path) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.info("Sin TELEGRAM_BOT_TOKEN o chat_id: no se envía el reporte de extracción.")
        return

    response_html = requests.post(
        f"https://api.telegram.org/bot{token}/sendDocument",
        data={
            "chat_id": chat_id,
            "caption": f"🔍 Reporte de extracción — {document_key} (revisar tablas/fórmulas/baja calidad vs. el PDF)",
        },
        files={"document": (f"reporte_{document_key}.html", html.encode("utf-8"), "text/html")},
        timeout=30,
    )
    response_html.raise_for_status()

    with pdf_path.open("rb") as file_obj:
        response_pdf = requests.post(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id, "caption": f"📄 PDF original — {document_key}"},
            files={"document": (pdf_path.name, file_obj, "application/pdf")},
            timeout=60,
        )
    response_pdf.raise_for_status()

    logger.info("Reporte de extracción y PDF de %s enviados por Telegram.", document_key)


def enviar_progreso_telegram(
    total: int,
    con_texto: int,
    procesadas_ahora: int,
    errores_ahora: int,
    normas_baja: int,
    pendientes_con_pdf: int = 0,
    errores_con_pdf: int = 0,
    sin_pdf_sin_texto: int = 0,
    normas_con_tablas: int = 0,
    normas_con_formula: int = 0,
    processed_summaries: list[dict] | None = None,
    pdf_needs: dict | None = None,
) -> None:
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
        f"Errores en esta corrida: <b>{errores_ahora}</b>",
    ]

    if pendientes_con_pdf:
        lines.append(f"Pendientes listos con PDF: <b>{pendientes_con_pdf}</b>")
    if errores_con_pdf:
        lines.append(f"Con error previo y PDF: <b>{errores_con_pdf}</b> (no se reintentan en automático)")
    if sin_pdf_sin_texto:
        lines.append(f"Sin PDF directo todavía: <b>{sin_pdf_sin_texto}</b>")

    if normas_baja:
        lines.append(f"⚠️ Con baja confiabilidad: <b>{normas_baja}</b> (revisar antes de confiar en consultas)")
    else:
        lines.append("✅ Sin páginas de baja confiabilidad en esta corrida.")

    if normas_con_tablas:
        lines.append(f"📊 Con tablas detectadas: <b>{normas_con_tablas}</b> (guardadas como estructura, no solo texto plano)")
    if normas_con_formula:
        lines.append(f"🧮 Con posible fórmula/notación técnica: <b>{normas_con_formula}</b> (requieren revisión manual)")

    summaries = processed_summaries or []
    if summaries:
        certeza_counts = {"alta": 0, "media": 0, "baja": 0}
        for item in summaries:
            certeza_counts[item["certeza"]] = certeza_counts.get(item["certeza"], 0) + 1
        lines.extend([
            "",
            "Nivel de certeza en esta corrida:",
            f"Alta: <b>{certeza_counts.get('alta', 0)}</b> | Media: <b>{certeza_counts.get('media', 0)}</b> | Baja: <b>{certeza_counts.get('baja', 0)}</b>",
        ])

        revisar = [
            item for item in summaries
            if item["certeza"] != "alta" or item.get("baja_calidad", 0) > 0
        ][:5]
        if revisar:
            lines.append("")
            lines.append("Revisar/confirmar transcripción:")
            for item in revisar:
                paginas = item.get("paginas_baja_calidad") or []
                paginas_txt = f" págs. {paginas[:8]}" if paginas else ""
                lines.append(
                    f"- {_escapar_html(item['document_key'])}: certeza <b>{item['certeza']}</b>, "
                    f"calidad prom. <b>{item['promedio']:.2f}</b>{_escapar_html(paginas_txt)} → "
                    f"{format_review_command(item['document_key'])}"
                )

    if pdf_needs:
        broken = (pdf_needs.get("broken") or [])[:3]
        missing = (pdf_needs.get("missing") or [])[:3]
        if broken or missing:
            lines.append("")
            lines.append("PDFs por confirmar/subir:")
        for row in broken:
            lines.append(
                f"- URL rota: <b>{_escapar_html(row.get('document_key'))}</b> "
                f"({_escapar_html((row.get('titulo') or '')[:60])})"
            )
        for row in missing:
            lines.append(
                f"- Sin PDF: <b>{_escapar_html(row.get('document_key'))}</b> "
                f"({_escapar_html((row.get('titulo') or '')[:60])})"
            )

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
    parser.add_argument(
        "--document-keys",
        default=None,
        help="Reprocesar varias normas en una sola corrida (document_key separados por coma). "
             "Para backfills, ej. normas con has_tables=true procesadas antes de un cambio en la extracción.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Reintentar normas en text_extraction_error/pdf_download_error. Usar manualmente, no en schedule.",
    )
    args = parser.parse_args()

    load_env()
    supabase = get_supabase()

    estado_antes = contar_estado_normativa(supabase)
    total_universo = estado_antes["total"]
    con_texto_antes = estado_antes["con_texto"]
    document_keys = (
        [key.strip() for key in args.document_keys.split(",") if key.strip()]
        if args.document_keys
        else None
    )
    normas = get_pending_normas(
        supabase,
        args.limit,
        args.document_key,
        document_keys=document_keys,
        retry_errors=args.retry_errors,
    )
    logger.info(
        (
            "Universo: %s normas | con texto: %s | pendientes con pdf_url: %s | "
            "errores con pdf: %s | sin pdf sin texto: %s | lote: %s | retry_errors=%s"
        ),
        total_universo,
        con_texto_antes,
        estado_antes["pendientes_con_pdf"],
        estado_antes["errores_con_pdf"],
        estado_antes["sin_pdf_sin_texto"],
        len(normas),
        args.retry_errors,
    )

    procesadas = 0
    errores = 0
    normas_baja_calidad = 0
    normas_con_tablas = 0
    normas_con_formula = 0
    processed_summaries: list[dict] = []
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

            local_path = temp_dir / file_name
            download_pdf(pdf_url, local_path)

            # Limpia cualquier pagina parcial de un intento anterior fallido
            # solo despues de confirmar que el PDF aun descarga correctamente.
            supabase.table(PAGE_TABLE).delete().eq("norma_id", norma["id"]).execute()

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

            stats = write_pages(supabase, norma["id"], extracciones)
            promedio = sum(p.quality for p in extracciones) / max(1, len(extracciones))
            estado = "text_extracted" if promedio >= UMBRAL_BAJA_CALIDAD else "text_extracted_baja_calidad"
            mark_norma(supabase, norma["id"], estado, stats)
            certeza = nivel_certeza_norma(promedio, stats)

            if stats["baja_calidad"] > 0:
                normas_baja_calidad += 1
            if stats["con_tablas"] > 0:
                normas_con_tablas += 1
            if stats["con_formula"] > 0:
                normas_con_formula += 1

            processed_summaries.append({
                "document_key": document_key,
                "certeza": certeza,
                "promedio": promedio,
                "baja_calidad": stats["baja_calidad"],
                "paginas_baja_calidad": stats["paginas_baja_calidad"],
            })

            procesadas += 1
            logger.info(
                "%s | páginas: %s | certeza: %s | calidad prom: %.2f | baja calidad: %s | con tablas: %s | posible fórmula: %s",
                document_key, len(extracciones), certeza, promedio,
                stats["baja_calidad"], stats["con_tablas"], stats["con_formula"],
            )

            # Reporte automatico para revisar fidelidad vs. el PDF: solo
            # cuando hay algo que amerita ojo humano (tablas, posible
            # formula, o baja calidad), no en cada norma procesada.
            if not args.no_telegram and (stats["con_tablas"] > 0 or stats["con_formula"] > 0 or stats["baja_calidad"] > 0):
                try:
                    html = construir_reporte_html(document_key, norma.get("titulo"), extracciones)
                    enviar_reporte_extraccion_telegram(document_key, html, local_path)
                except Exception:
                    logger.exception("No se pudo enviar el reporte de extracción de %s.", document_key)

        except Exception as error:
            errores += 1
            logger.exception("Error procesando %s: %s", document_key, error)
            if not args.dry_run:
                status = "pdf_download_error" if isinstance(error, PdfDownloadError) else "text_extraction_error"
                error_info = {
                    "message": str(error)[:1000],
                    "type": type(error).__name__,
                    "pdf_url": pdf_url,
                }
                if isinstance(error, PdfDownloadError):
                    error_info["http_status"] = error.status_code
                    error_info["content_type"] = error.content_type
                mark_norma(
                    supabase,
                    norma["id"],
                    status,
                    raw=norma.get("raw") if isinstance(norma.get("raw"), dict) else {},
                    error_info=error_info,
                )

    logger.info("Finalizado. Procesadas: %s | Errores: %s | Con baja calidad: %s",
                procesadas, errores, normas_baja_calidad)

    if not args.dry_run and not args.no_telegram:
        estado_despues = contar_estado_normativa(supabase)
        pdf_needs = get_pdf_needs_for_review(supabase)
        enviar_progreso_telegram(
            total_universo,
            estado_despues["con_texto"],
            procesadas,
            errores,
            normas_baja_calidad,
            estado_despues["pendientes_con_pdf"],
            estado_despues["errores_con_pdf"],
            estado_despues["sin_pdf_sin_texto"],
            normas_con_tablas,
            normas_con_formula,
            processed_summaries,
            pdf_needs,
        )


if __name__ == "__main__":
    main()
