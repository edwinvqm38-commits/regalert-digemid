"""Reprocesa paginas normativas de baja calidad con vision OCR via API.

Uso recomendado:
  1. Correr primero con --report-html para ver un reporte de diferencias
     resaltadas (actual vs transcripcion IA) SIN escribir nada en Supabase.
  2. Revisar el reporte; para las paginas que si conviene aplicar, correr de
     nuevo con --apply para guardar la transcripcion IA en metadata.
  3. Agregar --replace-text solo cuando se quiere que /consulta use ese texto.

La salida del modelo no se trata como verdad absoluta: siempre queda guardada
la extraccion previa en metadata cuando se reemplaza text_raw/text_normalized.
"""

import argparse
import base64
import difflib
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fitz
import requests
from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PAGE_TABLE = "digemid_norma_paginas"
NORMAS_TABLE = "digemid_normas"
STORAGE_BUCKET = "digemid-documentos"
DEFAULT_PROVIDER = "openrouter"
DEFAULT_OPENAI_MODEL = "gpt-5.6"
DEFAULT_OPENROUTER_MODEL = "openrouter/auto"
# gemini-2.5-pro devolvia 404 Not Found con la GEMINI_API_KEY/version de API
# de este proyecto; gemini-flash-latest es el que ya usa el resto del
# proyecto (telegram-bot, _shared/consulta-ia.ts) con exito.
DEFAULT_GEMINI_MODEL = "gemini-flash-latest"
DEFAULT_DETAIL = "original"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"

# Codigos que valen la pena reintentar: sobrecarga/rate-limit transitoria del
# proveedor, no un error real de la request. Confirmado en produccion:
# Gemini devolvio 503 Service Unavailable varias veces seguidas para la
# misma pagina, y un reintento con espera si funciono -- era sobrecarga
# puntual, no un problema de la imagen o el prompt.
_CODIGOS_REINTENTABLES = {429, 500, 502, 503, 504}


def _post_con_reintentos(url: str, intentos: int = 3, espera_base: float = 5.0, **kwargs) -> requests.Response:
    ultimo_error: Exception | None = None
    for intento in range(1, intentos + 1):
        try:
            response = requests.post(url, **kwargs)
            if response.status_code not in _CODIGOS_REINTENTABLES:
                return response
            ultimo_error = requests.HTTPError(
                f"{response.status_code} Server Error: {response.reason} for url: {url}", response=response,
            )
        except requests.RequestException as error:
            ultimo_error = error

        if intento < intentos:
            espera = espera_base * intento
            logger.warning("Intento %s/%s fallo (%s), reintentando en %ss...", intento, intentos, ultimo_error, espera)
            time.sleep(espera)

    raise ultimo_error


def load_env() -> None:
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--document-key")
    parser.add_argument(
        "--page-number", type=int,
        help="Procesar SOLO esta pagina de --document-key (sin importar el filtro de pendientes), "
             "para probar puntualmente un proveedor/modelo en una pagina conocida.",
    )
    parser.add_argument("--quality-below", type=float, default=0.85)
    parser.add_argument("--all-pages", action="store_true")
    parser.add_argument(
        "--filtro",
        choices=["calidad-baja", "tablas-pendientes", "graficos-pendientes", "todas-pendientes"],
        default="calidad-baja",
        help=(
            "calidad-baja: quality_score < --quality-below (comportamiento previo). "
            "tablas-pendientes: has_tables=true y tabla_verificada=false. "
            "graficos-pendientes: posible_grafico=true y revisado_manual=false (una pagina con "
            "grafico/imagen puede tener quality_score alto y sin tablas, asi que sin este filtro "
            "nunca entraba como candidata a la verificacion por vision). "
            "todas-pendientes: union de las tres anteriores (lo que /normaestado marca como pendiente)."
        ),
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--replace-text", action="store_true")
    parser.add_argument(
        "--auto-solo-alta-confianza",
        action="store_true",
        help=(
            "Para corridas automaticas sin supervision (ej. el cron diario): en vez de "
            "reemplazar SIEMPRE que --apply este presente (como --replace-text), solo "
            "reemplaza el texto cuando el propio modelo reporta confianza_estimada >= "
            "--min-confianza y no trae advertencias. Las paginas de baja confianza quedan "
            "solo en metadata para revision humana (via /normarevisar), igual que sin este "
            "flag. NUNCA marca tabla_verificada ni revisado_manual en true -- esos campos "
            "significan 'un humano lo confirmo', y esto es una IA decidiendo sola."
        ),
    )
    parser.add_argument("--min-confianza", type=float, default=0.85)
    parser.add_argument(
        "--telegram-digest",
        action="store_true",
        help="Al terminar, manda un resumen a Telegram (TELEGRAM_BOT_TOKEN + "
             "TELEGRAM_ADMIN_CHAT_ID/TELEGRAM_CHAT_ID) con lo aplicado, lo pendiente de "
             "revision, y el avance total del catalogo (paginas con calidad alta vs bajas).",
    )
    parser.add_argument(
        "--report-html",
        help=(
            "Ruta de salida para un reporte HTML con diff resaltado (actual vs transcripcion IA). "
            "Modo de SOLO LECTURA: llama al modelo de vision pero NUNCA escribe en Supabase, "
            "sin importar --apply. Para aplicar despues de revisar el reporte, correr de nuevo con --apply."
        ),
    )
    parser.add_argument(
        "--provider",
        choices=["openai", "openrouter", "gemini"],
        default=os.getenv("VISION_OCR_PROVIDER", DEFAULT_PROVIDER),
    )
    parser.add_argument("--model")
    parser.add_argument("--detail", default=os.getenv("VISION_OCR_DETAIL", DEFAULT_DETAIL))
    # 150 dpi es legible de sobra para texto/tablas escaneadas y reduce el
    # area de la imagen ~4x contra 300 dpi -> menos tiempo de subida/proceso
    # y menos tokens de imagen por pagina. Se puede subir con --dpi si una
    # norma en particular sale con letra muy chica o mala resolucion.
    parser.add_argument("--dpi", type=int, default=150)
    args = parser.parse_args()
    if args.limit <= 0:
        raise ValueError("--limit debe ser mayor que cero")
    if args.dpi <= 0:
        raise ValueError("--dpi debe ser mayor que cero")
    if args.replace_text and not args.apply:
        raise ValueError("--replace-text requiere --apply")
    if args.page_number is not None and not args.document_key:
        raise ValueError("--page-number requiere --document-key")
    if not args.model:
        if args.provider == "openai":
            args.model = os.getenv("OPENAI_OCR_MODEL", DEFAULT_OPENAI_MODEL)
        elif args.provider == "gemini":
            args.model = os.getenv("GEMINI_OCR_MODEL", DEFAULT_GEMINI_MODEL)
        else:
            args.model = os.getenv("OPENROUTER_OCR_MODEL", DEFAULT_OPENROUTER_MODEL)
    return args


def norm_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


PAGE_SELECT = (
    "id, norma_id, page_number, text_raw, text_normalized, extraction_method, "
    "quality_score, has_tables, tabla_verificada, posible_grafico, revisado_manual, metadata"
)


def aplicar_filtro_pendientes(query, args):
    """Aplica el mismo criterio de 'pendiente' que ya usa /normaestado en el
    bot: calidad-baja (quality_score < umbral), tablas-pendientes (tabla
    detectada sin verificar a mano), graficos-pendientes (imagen/grafico
    detectado sin revisar a mano -- sin este filtro una pagina asi podia
    tener quality_score alto y sin tablas, y nunca entraba como candidata a
    pesar de que el heuristico de posible_grafico existe justo para esto) o
    la union de las tres."""
    if args.filtro == "calidad-baja":
        return query.lt("quality_score", args.quality_below)
    if args.filtro == "tablas-pendientes":
        return query.eq("has_tables", True).eq("tabla_verificada", False)
    if args.filtro == "graficos-pendientes":
        return query.eq("posible_grafico", True).eq("revisado_manual", False)
    return query.or_(
        f"quality_score.lt.{args.quality_below},"
        "and(has_tables.eq.true,tabla_verificada.eq.false),"
        "and(posible_grafico.eq.true,revisado_manual.eq.false)"
    )


def get_candidate_pages(supabase, args) -> list[dict]:
    if args.document_key:
        normas = (
            supabase.table(NORMAS_TABLE)
            .select("id, document_key, titulo, pdf_url, file_storage_path")
            .eq("document_key", args.document_key)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not normas:
            raise ValueError(f"No existe document_key={args.document_key}")
        norma = normas[0]
        query = (
            supabase.table(PAGE_TABLE)
            .select(PAGE_SELECT)
            .eq("norma_id", norma["id"])
            .order("page_number")
        )
        if args.page_number is not None:
            query = query.eq("page_number", args.page_number)
        elif not args.all_pages:
            query = aplicar_filtro_pendientes(query, args)
        pages = query.limit(args.limit).execute().data or []
        for page in pages:
            page["norma"] = norma
        return pages

    query = (
        supabase.table(PAGE_TABLE)
        .select(PAGE_SELECT)
        .order("quality_score")
    )
    if not args.all_pages:
        query = aplicar_filtro_pendientes(query, args)
    pages = query.limit(args.limit).execute().data or []
    norma_ids = sorted({page["norma_id"] for page in pages if page.get("norma_id")})
    if not norma_ids:
        return []

    normas = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, titulo, pdf_url, file_storage_path")
        .in_("id", norma_ids)
        .execute()
        .data
        or []
    )
    normas_by_id = {norma["id"]: norma for norma in normas}
    output = []
    for page in pages:
        norma = normas_by_id.get(page.get("norma_id"))
        if not norma:
            continue
        page["norma"] = norma
        output.append(page)
    return output


def download_pdf_bytes(supabase, norma: dict) -> bytes:
    storage_path = norm_text(norma.get("file_storage_path"))
    if storage_path:
        data = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        if data and data.startswith(b"%PDF"):
            return data
        raise ValueError(f"Storage no devolvio PDF valido: {storage_path}")

    pdf_url = norm_text(norma.get("pdf_url"))
    if not pdf_url:
        raise ValueError(f"{norma.get('document_key')} no tiene file_storage_path ni pdf_url")
    response = requests.get(
        pdf_url,
        timeout=120,
        headers={"User-Agent": "RegAlert-DIGEMID-OpenAI-OCR/1.0"},
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL no devolvio PDF valido: {pdf_url}")
    return response.content


def render_page_png_base64(pdf_path: Path, page_number: int, dpi: int) -> str:
    with fitz.open(pdf_path) as doc:
        page_index = page_number - 1
        if page_index < 0 or page_index >= len(doc):
            raise ValueError(f"Pagina fuera de rango: {page_number}")
        page = doc[page_index]
        pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72), alpha=False)
        png_bytes = pix.tobytes("png")
    return base64.b64encode(png_bytes).decode("ascii")


# Compartido entre transcribe_page_openai y transcribe_page_openrouter. La
# clave "graficos" no le pide a la IA que interprete el significado legal
# del grafico (eso lo hace /consulta despues, con este texto como contexto);
# le pide que registre los datos visuales -- valores, categorias, tendencia --
# como si describiera la tabla de datos equivalente, igual que ya se hace con
# "tablas_markdown". Sin esto, un grafico de barras/circular quedaba fuera
# del texto guardado por completo: solo se transcribia lo que hubiera de
# texto suelto alrededor (titulo, ejes, leyenda).
PROMPT_TRANSCRIPCION = (
    "Transcribe literalmente esta pagina de una norma legal peruana. "
    "No resumas, no corrijas el contenido legal y no inventes texto. "
    "Conserva saltos de linea importantes, numerales, articulos, fechas, "
    "unidades, tablas y encabezados. Si una palabra no es legible escribe "
    "[ilegible]. Si hay un grafico (de barras, circular, de lineas, "
    "diagrama, etc.), registra en 'graficos' su titulo, tipo, y los datos "
    "que muestra (categorias, valores, unidades, tendencia) en texto plano "
    "legible, como si describieras la tabla de datos equivalente -- no hace "
    "falta que interpretes su significado legal, solo que los datos queden "
    "registrados para que se pueda responder preguntas sobre ellos despues. "
    "Devuelve solo JSON valido con estas claves: transcripcion, "
    "tablas_markdown, graficos, advertencias, confianza_estimada."
)


_ESCAPE_INVALIDO_RE = re.compile(r'\\(?!["\\/bfnrtu])')


def _reparar_escapes_invalidos(texto: str) -> str:
    """El modelo a veces mete un backslash suelto dentro de un string JSON
    (ej. una cita legal con formato raro) que no es un escape JSON valido y
    rompe el parser. Se duplica cualquier backslash que no inicie un escape
    reconocido, para que json.loads lo lea como caracter literal."""
    return _ESCAPE_INVALIDO_RE.sub(r"\\\\", texto)


def _parsear_json_tolerante(candidato: str) -> dict:
    """Intenta json.loads en capas cada vez mas permisivas, porque un LLM no
    siempre devuelve JSON estrictamente valido pese a que el prompt lo pida:
    saltos de linea crudos dentro de un string (control character), o un
    backslash que no forma un escape valido, son los casos mas comunes."""
    try:
        return json.loads(candidato)
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(candidato, strict=False)  # permite control chars crudos en strings
    except json.JSONDecodeError:
        pass
    return json.loads(_reparar_escapes_invalidos(candidato), strict=False)


def extract_json(text: str) -> dict:
    clean = (text or "").strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```(?:json)?", "", clean).strip()
        clean = re.sub(r"```$", "", clean).strip()
    try:
        return _parsear_json_tolerante(clean)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        if not match:
            raise
        return _parsear_json_tolerante(match.group(0))


def transcribe_page_openai(
    api_key: str,
    model: str,
    detail: str,
    document_key: str,
    title: str | None,
    page_number: int,
    image_base64: str,
) -> dict:
    prompt = PROMPT_TRANSCRIPCION
    payload = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            f"Documento: {document_key}\n"
                            f"Titulo: {title or ''}\n"
                            f"Pagina: {page_number}\n\n{prompt}"
                        ),
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:image/png;base64,{image_base64}",
                        "detail": detail,
                    },
                ],
            }
        ],
    }
    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    output_text = data.get("output_text")
    if not output_text:
        chunks: list[str] = []
        for item in data.get("output", []) or []:
            for content in item.get("content", []) or []:
                if content.get("type") in {"output_text", "text"} and content.get("text"):
                    chunks.append(content["text"])
        output_text = "\n".join(chunks).strip()
    if not output_text:
        raise ValueError("OpenAI no devolvio output_text")

    # Nota: NO se trata una transcripcion vacia como error. Varias paginas
    # candidatas (quality_score = 0) son portadas o paginas casi en blanco;
    # ahi lo correcto es que el modelo devuelva poco o nada de texto, y
    # tratarlo como fallo generaba falsos "error" en el reporte para
    # paginas donde la IA en realidad acerto (no hay nada que transcribir).
    parsed = extract_json(output_text)
    return {
        "raw_response_id": data.get("id"),
        "transcripcion": parsed.get("transcripcion") or "",
        "tablas_markdown": parsed.get("tablas_markdown") or "",
        "graficos": parsed.get("graficos") or "",
        "advertencias": parsed.get("advertencias") or [],
        "confianza_estimada": parsed.get("confianza_estimada"),
    }


def transcribe_page_openrouter(
    api_key: str,
    model: str,
    document_key: str,
    title: str | None,
    page_number: int,
    image_base64: str,
) -> dict:
    prompt = PROMPT_TRANSCRIPCION
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            f"Documento: {document_key}\n"
                            f"Titulo: {title or ''}\n"
                            f"Pagina: {page_number}\n\n{prompt}"
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64}",
                        },
                    },
                ],
            }
        ],
        "temperature": 0,
    }
    response = requests.post(
        OPENROUTER_CHAT_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/edwinvqm38-commits/regalert-digemid",
            "X-Title": "RegAlert DIGEMID Vision OCR",
        },
        json=payload,
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    output_text = (
        ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
        or ""
    )
    if isinstance(output_text, list):
        output_text = "\n".join(
            item.get("text", "")
            for item in output_text
            if isinstance(item, dict)
        ).strip()
    if not output_text:
        raise ValueError("OpenRouter no devolvio contenido")

    # Nota: NO se trata una transcripcion vacia como error (ver comentario
    # equivalente en transcribe_page_openai) — una pagina casi en blanco
    # devuelve poco o nada de texto legitimamente.
    parsed = extract_json(str(output_text))
    return {
        "raw_response_id": data.get("id"),
        "transcripcion": parsed.get("transcripcion") or "",
        "tablas_markdown": parsed.get("tablas_markdown") or "",
        "graficos": parsed.get("graficos") or "",
        "advertencias": parsed.get("advertencias") or [],
        "confianza_estimada": parsed.get("confianza_estimada"),
    }


def transcribe_page_gemini(
    api_key: str,
    model: str,
    document_key: str,
    title: str | None,
    page_number: int,
    image_base64: str,
) -> dict:
    prompt = (
        f"Documento: {document_key}\nTitulo: {title or ''}\nPagina: {page_number}\n\n"
        f"{PROMPT_TRANSCRIPCION}"
    )
    response = _post_con_reintentos(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{
                "role": "user",
                "parts": [
                    {"text": prompt},
                    {"inline_data": {"mime_type": "image/png", "data": image_base64}},
                ],
            }],
            # 4096 no alcanzaba para una pagina con tabla densa (confirmado
            # en produccion: devolvio 200 pero el JSON venia truncado a
            # mitad de un string -- se quedo sin tokens de salida).
            "generationConfig": {"temperature": 0, "maxOutputTokens": 8192},
        },
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    parts = (data.get("candidates") or [{}])[0].get("content", {}).get("parts") or []
    output_text = "\n".join(p.get("text", "") for p in parts).strip()
    if not output_text:
        raise ValueError("Gemini no devolvio contenido")

    parsed = extract_json(output_text)
    return {
        "transcripcion": parsed.get("transcripcion") or "",
        "tablas_markdown": parsed.get("tablas_markdown") or "",
        "graficos": parsed.get("graficos") or "",
        "advertencias": parsed.get("advertencias") or [],
        "confianza_estimada": parsed.get("confianza_estimada"),
    }


def transcribe_page_with_provider(
    api_key: str,
    provider: str,
    model: str,
    detail: str,
    document_key: str,
    title: str | None,
    page_number: int,
    image_base64: str,
) -> dict:
    if provider == "openai":
        return transcribe_page_openai(
            api_key=api_key,
            model=model,
            detail=detail,
            document_key=document_key,
            title=title,
            page_number=page_number,
            image_base64=image_base64,
        )
    if provider == "openrouter":
        return transcribe_page_openrouter(
            api_key=api_key,
            model=model,
            document_key=document_key,
            title=title,
            page_number=page_number,
            image_base64=image_base64,
        )
    if provider == "gemini":
        return transcribe_page_gemini(
            api_key=api_key,
            model=model,
            document_key=document_key,
            title=title,
            page_number=page_number,
            image_base64=image_base64,
        )
    raise ValueError(f"Proveedor no soportado: {provider}")


def _a_texto(valor) -> str:
    """El modelo deberia devolver string para transcripcion/tablas_markdown,
    pero un LLM no siempre respeta el esquema pedido en el prompt (a veces
    tablas_markdown vino como lista de strings). Se normaliza en vez de
    asumir el tipo, para no tumbar el lote entero por una respuesta con
    forma inesperada en una sola pagina."""
    if valor is None:
        return ""
    if isinstance(valor, str):
        return valor.strip()
    if isinstance(valor, (list, tuple)):
        return "\n\n".join(_a_texto(v) for v in valor if v).strip()
    return str(valor).strip()


def build_replacement_text(ai_result: dict) -> str:
    partes = [_a_texto(ai_result.get("transcripcion"))]

    tables = _a_texto(ai_result.get("tablas_markdown"))
    if tables:
        partes.append(tables)

    # Se guarda como texto plano (no Markdown de tabla) porque un grafico no
    # tiene filas/columnas reales -- es la descripcion que la IA registro de
    # sus datos, para que /consulta pueda citarla igual que citaria un
    # parrafo normal.
    graficos = _a_texto(ai_result.get("graficos"))
    if graficos:
        partes.append(f"Gráfico(s) detectado(s) en esta página:\n{graficos}")

    return "\n\n".join(p for p in partes if p).strip()


def _html_escape(value: str) -> str:
    return (
        (value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def diff_resaltado_html(actual: str, propuesto: str) -> tuple[str, float]:
    """Diff a nivel de palabra entre el texto ya guardado y la transcripcion
    IA, resaltado en HTML (rojo = solo en el actual, verde = solo en la
    transcripcion IA). Devuelve tambien la proporcion de palabras distintas,
    para poder ordenar el reporte por cuanto cambio cada pagina y que el
    admin revise primero las que mas difieren."""
    palabras_actual = (actual or "").split()
    palabras_propuesto = (propuesto or "").split()

    matcher = difflib.SequenceMatcher(a=palabras_actual, b=palabras_propuesto, autojunk=False)
    piezas: list[str] = []
    palabras_distintas = 0

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            piezas.append(_html_escape(" ".join(palabras_actual[i1:i2])))
            continue
        palabras_distintas += max(i2 - i1, j2 - j1)
        if i1 != i2:
            piezas.append(f'<del>{_html_escape(" ".join(palabras_actual[i1:i2]))}</del>')
        if j1 != j2:
            piezas.append(f'<ins>{_html_escape(" ".join(palabras_propuesto[j1:j2]))}</ins>')

    total = max(len(palabras_actual), len(palabras_propuesto), 1)
    proporcion_cambio = palabras_distintas / total
    return " ".join(piezas), proporcion_cambio


def construir_reporte_html(filas_reporte: list[dict]) -> str:
    """filas_reporte: [{document_key, page_number, quality_score, diff_html,
    proporcion_cambio, advertencias, confianza_estimada, error}]"""
    filas_ordenadas = sorted(
        filas_reporte, key=lambda f: f.get("proporcion_cambio") or 0, reverse=True
    )

    bloques = []
    for fila in filas_ordenadas:
        if fila.get("error"):
            bloques.append(
                f"""
        <section class="pagina error">
          <h2>{_html_escape(fila['document_key'])} — pág. {fila['page_number']}</h2>
          <p class="badge badge-error">⚠️ Error al procesar: {_html_escape(fila['error'])}</p>
        </section>"""
            )
            continue

        pct = round((fila.get("proporcion_cambio") or 0) * 100, 1)
        clase_badge = "badge-alto" if pct >= 15 else ("badge-medio" if pct >= 3 else "badge-bajo")
        advertencias = fila.get("advertencias") or []
        advertencias_html = (
            "<ul>" + "".join(f"<li>{_html_escape(str(a))}</li>" for a in advertencias) + "</ul>"
            if advertencias
            else ""
        )

        bloques.append(
            f"""
        <section class="pagina">
          <h2>{_html_escape(fila['document_key'])} — pág. {fila['page_number']}</h2>
          <p>
            <span class="badge {clase_badge}">{pct}% de palabras distintas</span>
            <span class="badge">calidad actual: {fila.get('quality_score')}</span>
            <span class="badge">confianza IA: {_html_escape(str(fila.get('confianza_estimada')))}</span>
          </p>
          {advertencias_html}
          <div class="diff"><pre>{fila['diff_html']}</pre></div>
        </section>"""
        )

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Reporte de diferencias — OCR visión IA</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; max-width: 1100px; }}
  h1 {{ font-size: 1.3rem; }}
  h2 {{ font-size: 1rem; margin-bottom: 0.3rem; }}
  .nota {{ background: #fff8e1; border: 1px solid #ffe082; padding: 0.75rem; border-radius: 6px; margin-bottom: 1.5rem; }}
  section.pagina {{ border: 1px solid #ddd; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }}
  section.error {{ border-color: #e57373; background: #fff5f5; }}
  .badge {{ display: inline-block; font-size: 0.75rem; padding: 0.15rem 0.5rem; border-radius: 999px; background: #eee; margin-right: 0.4rem; }}
  .badge-alto {{ background: #ffcdd2; }}
  .badge-medio {{ background: #fff3b0; }}
  .badge-bajo {{ background: #c8e6c9; }}
  .badge-error {{ background: #ffcdd2; }}
  .diff pre {{ white-space: pre-wrap; word-break: break-word; font-size: 0.85rem; line-height: 1.5; }}
  ins {{ background: #c8f7c5; text-decoration: none; padding: 0 2px; }}
  del {{ background: #ffd0d0; text-decoration: line-through; padding: 0 2px; }}
</style>
</head>
<body>
  <h1>📋 Reporte de diferencias — texto actual vs transcripción con visión IA</h1>
  <div class="nota">
    Ordenado de mayor a menor % de palabras distintas. Verde = lo que agrega/cambia la IA,
    rojo tachado = lo que tenía el texto actual y la IA no repite. Esto NO se guardó en
    Supabase — es solo para decidir qué páginas conviene aplicar con --apply.
    Un % alto no siempre es un error (puede ser el OCR arreglando texto pegado);
    revisa contra el PDF antes de aplicar.
  </div>
  {"".join(bloques)}
</body>
</html>"""


def _debe_reemplazar_texto(ai_result: dict, args) -> bool:
    """--replace-text (manual, quien lo corre ya reviso el reporte) siempre
    reemplaza. --auto-solo-alta-confianza (corridas sin supervision) solo
    reemplaza cuando el propio modelo se declara seguro: confianza_estimada
    numerica >= --min-confianza y sin advertencias. Sin ninguno de los dos,
    la transcripcion queda solo en metadata para revision humana."""
    if args.replace_text:
        return True
    if not args.auto_solo_alta_confianza:
        return False
    confianza = ai_result.get("confianza_estimada")
    if not isinstance(confianza, (int, float)):
        return False
    if ai_result.get("advertencias"):
        return False
    return confianza >= args.min_confianza


def update_page(supabase, page: dict, ai_result: dict, args) -> bool:
    """Devuelve True si se reemplazo el texto (para el resumen de Telegram)."""
    now = datetime.now(timezone.utc).isoformat()
    metadata = page.get("metadata") if isinstance(page.get("metadata"), dict) else {}
    metadata["openai_vision_ocr"] = {
        "provider": args.provider,
        "model": args.model,
        "detail": args.detail,
        "dpi": args.dpi,
        "generated_at": now,
        **ai_result,
    }
    payload: dict[str, Any] = {
        "metadata": metadata,
        "updated_at": now,
    }

    reemplazar = _debe_reemplazar_texto(ai_result, args)
    if reemplazar:
        metadata["previous_extraction_before_openai_vision"] = {
            "text_raw": page.get("text_raw"),
            "text_normalized": page.get("text_normalized"),
            "extraction_method": page.get("extraction_method"),
            "quality_score": page.get("quality_score"),
        }
        payload.update(
            {
                "text_raw": ai_result.get("transcripcion") or "",
                "text_normalized": build_replacement_text(ai_result),
                # NUNCA se toca tabla_verificada/revisado_manual aqui: esos
                # campos significan "un humano lo confirmo", y esto lo decidio
                # una IA sola (aunque con alta confianza declarada por ella
                # misma). Quedan para el flujo de revision manual existente.
                "extraction_method": "openai_vision_ocr",
                "quality_score": 0.9,
            }
        )

    supabase.table(PAGE_TABLE).update(payload).eq("id", page["id"]).execute()
    return reemplazar


def enviar_mensaje_telegram(texto: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.warning("--telegram-digest pedido pero faltan TELEGRAM_BOT_TOKEN/TELEGRAM_ADMIN_CHAT_ID; se omite.")
        return
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": texto, "parse_mode": "HTML"},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as error:
        logger.warning("No se pudo mandar el resumen a Telegram: %s", error)


def contar_avance_catalogo(supabase, quality_below: float) -> dict:
    """Avance total del catalogo (todas las normas, no solo las tocadas en
    esta corrida): cuantas paginas ya tienen calidad alta vs cuantas siguen
    bajas, para poder responder 'que reglamentos tienen avance con alto
    nivel de confianza' sin tener que revisar norma por norma."""
    # head=True: solo pide el conteo, no trae las filas -- con miles de
    # paginas, pedir count sin head igual descarga datos de mas.
    total = supabase.table(PAGE_TABLE).select("id", count="exact", head=True).execute().count or 0
    altas = (
        supabase.table(PAGE_TABLE).select("id", count="exact", head=True)
        .gte("quality_score", quality_below).execute().count or 0
    )
    return {"total": total, "altas": altas, "bajas": total - altas}


def main() -> None:
    args = parse_args()
    load_env()
    if args.provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        api_key_name = "OPENAI_API_KEY"
    elif args.provider == "gemini":
        api_key = os.getenv("GEMINI_API_KEY")
        api_key_name = "GEMINI_API_KEY"
    else:
        api_key = os.getenv("OPENROUTER_API_KEY")
        api_key_name = "OPENROUTER_API_KEY"

    llama_al_modelo = args.apply or bool(args.report_html)
    if not api_key and llama_al_modelo:
        raise ValueError(f"Falta {api_key_name}")

    if args.report_html and args.apply:
        logger.warning(
            "--report-html fuerza modo de solo lectura: NO se escribe en Supabase "
            "en esta corrida aunque --apply este presente. Corre de nuevo solo con "
            "--apply (sin --report-html) para aplicar despues de revisar el reporte."
        )

    supabase = get_supabase()
    pages = get_candidate_pages(supabase, args)
    logger.info("Paginas candidatas (filtro=%s): %s", args.filtro, len(pages))
    if not pages:
        return

    filas_reporte: list[dict] = []
    resumen_digest: list[dict] = []
    pdf_cache: dict[str, Path] = {}

    def escribir_reporte_parcial() -> None:
        if not args.report_html:
            return
        Path(args.report_html).write_text(construir_reporte_html(filas_reporte), encoding="utf-8")

    try:
        for indice, page in enumerate(pages, start=1):
            norma = page["norma"]
            document_key = norma.get("document_key")
            page_number = int(page["page_number"])
            logger.info(
                "%s pagina %s | quality=%s | tablas=%s/%s | method=%s",
                document_key,
                page_number,
                page.get("quality_score"),
                page.get("has_tables"),
                page.get("tabla_verificada"),
                page.get("extraction_method"),
            )

            if not llama_al_modelo:
                continue

            try:
                if document_key not in pdf_cache:
                    pdf_bytes = download_pdf_bytes(supabase, norma)
                    tmp_file = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                    tmp_file.write(pdf_bytes)
                    tmp_file.close()
                    pdf_cache[document_key] = Path(tmp_file.name)

                image_base64 = render_page_png_base64(pdf_cache[document_key], page_number, args.dpi)

                ai_result = transcribe_page_with_provider(
                    api_key=api_key,
                    provider=args.provider,
                    model=args.model,
                    detail=args.detail,
                    document_key=document_key,
                    title=norma.get("titulo"),
                    page_number=page_number,
                    image_base64=image_base64,
                )

                if args.report_html:
                    actual = page.get("text_normalized") or page.get("text_raw") or ""
                    propuesto = build_replacement_text(ai_result)
                    diff_html, proporcion_cambio = diff_resaltado_html(actual, propuesto)
                    filas_reporte.append(
                        {
                            "document_key": document_key,
                            "page_number": page_number,
                            "quality_score": page.get("quality_score"),
                            "diff_html": diff_html,
                            "proporcion_cambio": proporcion_cambio,
                            "advertencias": ai_result.get("advertencias"),
                            "confianza_estimada": ai_result.get("confianza_estimada"),
                        }
                    )

                if args.apply and not args.report_html:
                    reemplazado = update_page(supabase, page, ai_result, args)
                    logger.info("%s pagina %s actualizada con vision IA.", document_key, page_number)
                    resumen_digest.append(
                        {
                            "document_key": document_key,
                            "page_number": page_number,
                            "reemplazado": reemplazado,
                            "confianza_estimada": ai_result.get("confianza_estimada"),
                        }
                    )
            except Exception as error:  # no se pierde el lote entero por una pagina
                logger.warning("%s pagina %s | error: %s", document_key, page_number, error)
                if args.report_html:
                    filas_reporte.append(
                        {
                            "document_key": document_key,
                            "page_number": page_number,
                            "error": str(error),
                        }
                    )
                resumen_digest.append(
                    {"document_key": document_key, "page_number": page_number, "error": str(error)},
                )
                continue
            finally:
                # Escritura periodica: si la corrida se corta (timeout del
                # workflow, cancelacion manual, caida de red) el reporte ya
                # tiene lo procesado hasta el momento en vez de perderse
                # entero por no llegar nunca a la escritura final.
                if indice % 10 == 0:
                    escribir_reporte_parcial()
    finally:
        for tmp_path in pdf_cache.values():
            tmp_path.unlink(missing_ok=True)

    escribir_reporte_parcial()
    if args.report_html:
        logger.info("Reporte escrito en %s (%s paginas)", args.report_html, len(filas_reporte))

    if args.telegram_digest and resumen_digest:
        aplicadas = [f for f in resumen_digest if f.get("reemplazado")]
        pendientes = [f for f in resumen_digest if "error" not in f and not f.get("reemplazado")]
        errores = [f for f in resumen_digest if "error" in f]

        lineas = [f"🔎 <b>Vision OCR ({args.provider}) — resumen del día</b>", ""]
        if aplicadas:
            lineas.append(f"✅ <b>{len(aplicadas)} página(s) aplicada(s)</b> (alta confianza, ya las usa /consulta):")
            for f in aplicadas:
                lineas.append(f"  • {f['document_key']} pág. {f['page_number']} (confianza {f['confianza_estimada']})")
        if pendientes:
            lineas.append(f"\n⏳ <b>{len(pendientes)} página(s) con confianza baja</b> (guardadas en metadata, esperan revisión con /normarevisar):")
            for f in pendientes:
                lineas.append(f"  • {f['document_key']} pág. {f['page_number']} (confianza {f['confianza_estimada']})")
        if errores:
            lineas.append(f"\n⚠️ {len(errores)} página(s) con error, se reintentan en la próxima corrida.")

        try:
            avance = contar_avance_catalogo(supabase, args.quality_below)
            porcentaje = (avance["altas"] / avance["total"] * 100) if avance["total"] else 0
            lineas.append(
                f"\n📊 Avance total del catálogo: {avance['altas']}/{avance['total']} páginas "
                f"con calidad alta ({porcentaje:.0f}%). Detalle por norma: /reportenormas",
            )
        except Exception as error:  # el digest de esta corrida no depende de esto
            logger.warning("No se pudo calcular el avance del catalogo: %s", error)

        enviar_mensaje_telegram("\n".join(lineas))


if __name__ == "__main__":
    main()
