"""Genera un reporte HTML de ejemplo comparando como distintos modelos de
vision (OpenAI, OpenRouter, Gemini) transcriben la MISMA pagina difícil de
una norma (tabla con celdas combinadas, grafico, etc.), lado a lado con el
texto ya guardado en Supabase.

Es una demo/comparacion puntual para decidir que modelo conviene mas para
scripts/ocr_normativa_openai_pages.py -- no escribe nada en Supabase.

Uso:
    python scripts/comparar_vision_ocr_ejemplo.py \
        --document-key DS-020-2024 --page 4 \
        --output comparacion_vision_ocr.html

Requiere las credenciales que ya tengan configuradas (algunas pueden
faltar; el modelo correspondiente simplemente se omite del reporte):
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    OPENAI_API_KEY (opcional)
    OPENROUTER_API_KEY (opcional)
    GEMINI_API_KEY (opcional)
"""

import argparse
import base64
import logging
import os
import tempfile
from pathlib import Path

import requests

from scripts.ocr_normativa_openai_pages import (
    NORMAS_TABLE,
    PAGE_TABLE,
    PROMPT_TRANSCRIPCION,
    build_replacement_text,
    diff_resaltado_html,
    download_pdf_bytes,
    extract_json,
    get_supabase,
    load_env,
    render_page_png_base64,
    transcribe_page_openai,
    transcribe_page_openrouter,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_OPENAI_MODEL = "gpt-5.6"
DEFAULT_OPENROUTER_MODEL = "openrouter/auto"
DEFAULT_GEMINI_MODEL = "gemini-2.5-pro"


def transcribe_page_gemini(
    api_key: str,
    model: str,
    document_key: str,
    title: str | None,
    page_number: int,
    image_base64: str,
) -> dict:
    """Mismo esquema de salida (JSON con transcripcion/tablas_markdown/
    graficos/advertencias/confianza_estimada) que transcribe_page_openai y
    transcribe_page_openrouter, para poder comparar los 3 lado a lado."""
    prompt = (
        f"Documento: {document_key}\nTitulo: {title or ''}\nPagina: {page_number}\n\n"
        f"{PROMPT_TRANSCRIPCION}"
    )
    response = requests.post(
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
            "generationConfig": {"temperature": 0, "maxOutputTokens": 4096},
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--document-key", default="DS-020-2024")
    parser.add_argument("--page", type=int, default=4)
    parser.add_argument("--dpi", type=int, default=150)
    parser.add_argument("--openai-model", default=os.getenv("OPENAI_OCR_MODEL", DEFAULT_OPENAI_MODEL))
    parser.add_argument("--openrouter-model", default=os.getenv("OPENROUTER_OCR_MODEL", DEFAULT_OPENROUTER_MODEL))
    parser.add_argument("--gemini-model", default=os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL))
    parser.add_argument("--output", default="comparacion_vision_ocr.html")
    return parser.parse_args()


def _bloque_modelo(nombre: str, resultado: dict | None, error: str | None, actual: str) -> str:
    if error:
        return f"""
        <section class="modelo error">
          <h2>{nombre}</h2>
          <p class="aviso">⚠️ No se pudo generar: {error}</p>
        </section>"""

    if resultado is None:
        return f"""
        <section class="modelo omitido">
          <h2>{nombre}</h2>
          <p class="aviso">Omitido (no hay API key configurada).</p>
        </section>"""

    propuesto = build_replacement_text(resultado)
    diff_html, proporcion = diff_resaltado_html(actual, propuesto)
    advertencias = resultado.get("advertencias") or []
    confianza = resultado.get("confianza_estimada")

    return f"""
    <section class="modelo">
      <h2>{nombre}</h2>
      <p class="meta">Cambio vs. texto actual: <b>{proporcion:.0%}</b> de palabras distintas
        {f"· Confianza estimada: <b>{confianza}</b>" if confianza is not None else ""}</p>
      {f'<p class="aviso">⚠️ {", ".join(advertencias)}</p>' if advertencias else ""}
      <pre>{diff_html}</pre>
    </section>"""


def construir_reporte(document_key: str, page_number: int, actual: str, resultados: dict) -> str:
    bloques = "\n".join(
        _bloque_modelo(nombre, valor.get("resultado"), valor.get("error"), actual)
        for nombre, valor in resultados.items()
    )

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Comparación vision OCR — {document_key} pág. {page_number}</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; background: #f7f8fa; }}
  h1 {{ font-size: 1.3rem; }}
  .modelo {{ background: #fff; border-radius: 10px; padding: 1.25rem 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 1.5rem; }}
  .modelo.error, .modelo.omitido {{ opacity: 0.7; }}
  .modelo h2 {{ margin-top: 0; font-size: 1.05rem; }}
  .meta {{ color: #555; font-size: 0.85rem; }}
  .aviso {{ background: #fff8e1; border: 1px solid #ffe082; padding: 0.5rem 0.75rem; border-radius: 6px; font-size: 0.85rem; }}
  pre {{ white-space: pre-wrap; word-break: break-word; font-size: 0.85rem; background: #fafafa; padding: 0.75rem; border-radius: 6px; }}
  del {{ background: #fee2e2; text-decoration: line-through; }}
  ins {{ background: #dcfce7; text-decoration: none; }}
  .actual {{ background: #eef2ff; }}
</style>
</head>
<body>
  <h1>📊 Comparación de vision OCR — {document_key}, página {page_number}</h1>
  <p>Rojo tachado = solo estaba en el texto ya guardado (probablemente un error de la extracción anterior). Verde = lo que agrega o corrige cada modelo.</p>

  <section class="modelo actual">
    <h2>Texto actual (guardado en Supabase)</h2>
    <pre>{actual or "(vacío)"}</pre>
  </section>

  {bloques}
</body>
</html>"""


def main():
    args = parse_args()
    load_env()
    supabase = get_supabase()

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
        raise SystemExit(f"No existe document_key={args.document_key}")
    norma = normas[0]

    paginas = (
        supabase.table(PAGE_TABLE)
        .select("text_normalized, text_raw")
        .eq("norma_id", norma["id"])
        .eq("page_number", args.page)
        .limit(1)
        .execute()
        .data
        or []
    )
    actual = (paginas[0].get("text_normalized") or paginas[0].get("text_raw") or "") if paginas else ""

    logger.info("Descargando PDF de %s...", args.document_key)
    pdf_bytes = download_pdf_bytes(supabase, norma)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        pdf_path = Path(tmp.name)

    try:
        logger.info("Renderizando página %s a imagen (%s dpi)...", args.page, args.dpi)
        image_base64 = render_page_png_base64(pdf_path, args.page, args.dpi)
    finally:
        pdf_path.unlink(missing_ok=True)

    resultados: dict[str, dict] = {}

    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        logger.info("Transcribiendo con OpenAI (%s)...", args.openai_model)
        try:
            resultados[f"OpenAI ({args.openai_model})"] = {
                "resultado": transcribe_page_openai(
                    openai_key, args.openai_model, "original", args.document_key,
                    norma.get("titulo"), args.page, image_base64,
                ),
            }
        except Exception as error:  # noqa: BLE001 -- se reporta, no se detiene el resto
            resultados[f"OpenAI ({args.openai_model})"] = {"error": str(error)}
    else:
        resultados[f"OpenAI ({args.openai_model})"] = {}

    openrouter_key = os.getenv("OPENROUTER_API_KEY")
    if openrouter_key:
        logger.info("Transcribiendo con OpenRouter (%s)...", args.openrouter_model)
        try:
            resultados[f"OpenRouter ({args.openrouter_model})"] = {
                "resultado": transcribe_page_openrouter(
                    openrouter_key, args.openrouter_model, args.document_key,
                    norma.get("titulo"), args.page, image_base64,
                ),
            }
        except Exception as error:  # noqa: BLE001
            resultados[f"OpenRouter ({args.openrouter_model})"] = {"error": str(error)}
    else:
        resultados[f"OpenRouter ({args.openrouter_model})"] = {}

    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        logger.info("Transcribiendo con Gemini (%s)...", args.gemini_model)
        try:
            resultados[f"Gemini ({args.gemini_model})"] = {
                "resultado": transcribe_page_gemini(
                    gemini_key, args.gemini_model, args.document_key,
                    norma.get("titulo"), args.page, image_base64,
                ),
            }
        except Exception as error:  # noqa: BLE001
            resultados[f"Gemini ({args.gemini_model})"] = {"error": str(error)}
    else:
        resultados[f"Gemini ({args.gemini_model})"] = {}

    html = construir_reporte(args.document_key, args.page, actual, resultados)
    Path(args.output).write_text(html, encoding="utf-8")
    logger.info("Reporte escrito en %s", args.output)


if __name__ == "__main__":
    main()
