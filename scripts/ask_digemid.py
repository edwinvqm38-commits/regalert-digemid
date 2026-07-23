import argparse
import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

DEEPSEEK_MODEL = "deepseek-chat"
GEMINI_MODEL = "gemini-2.0-flash"
MAX_CHUNKS = 4

SYSTEM_PROMPT = """Eres un asistente que responde preguntas sobre alertas y \
normativa de DIGEMID (Peru) usando UNICAMENTE el texto de los documentos que \
se te entregan como contexto.

Reglas estrictas:
- No inventes datos que no esten en el contexto.
- Si el contexto no contiene la respuesta, dilo explicitamente en vez de adivinar.
- Cita siempre el numero de alerta/norma y la fecha del documento.
- No reemplazas al Director Tecnico ni a la autoridad sanitaria; tu respuesta \
es informativa, no una decision regulatoria."""


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def search_chunks(supabase, query: str, limit: int = MAX_CHUNKS) -> list[dict]:
    """Busqueda por palabra clave sobre el texto ya extraido, via RPC
    buscar_paginas_texto (filtra palabras vacias y ordena por relevancia -
    websearch_to_tsquery/plainto_tsquery exigen que aparezcan todas las
    palabras, lo cual falla con preguntas en lenguaje natural).

    No es busqueda semantica (no hay embeddings todavia) - suficiente para
    validar el flujo end-to-end con costo casi nulo. Se puede mejorar despues
    con pgvector si hace falta precision semantica."""
    response = supabase.rpc(
        "buscar_paginas_texto",
        {"query_texto": query, "limite": limit},
    ).execute()

    return response.data or []


def build_context(chunks: list[dict]) -> str:
    blocks = []

    for chunk in chunks:
        blocks.append(
            f"[Documento {chunk.get('document_key')} - {chunk.get('title')} - "
            f"{chunk.get('published_date')} - pagina {chunk.get('page_number')}]\n"
            f"{chunk.get('text_content')}\n"
            f"Link oficial: {chunk.get('detail_url')}"
        )

    return "\n\n---\n\n".join(blocks)


def call_deepseek(api_key: str, user_content: str) -> str:
    response = requests.post(
        "https://api.deepseek.com/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": DEEPSEEK_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "max_tokens": 1024,
        },
        timeout=60,
    )
    response.raise_for_status()

    return response.json()["choices"][0]["message"]["content"]


def call_gemini(api_key: str, user_content: str) -> str:
    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
        params={"key": api_key},
        json={
            "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "contents": [{"role": "user", "parts": [{"text": user_content}]}],
            "generationConfig": {"maxOutputTokens": 1024},
        },
        timeout=60,
    )
    response.raise_for_status()

    data = response.json()
    candidatos = data.get("candidates") or []
    if not candidatos:
        raise RuntimeError(f"Gemini no devolvió respuesta utilizable: {data}")

    partes = candidatos[0].get("content", {}).get("parts") or []
    return "".join(p.get("text", "") for p in partes)


def call_llm(user_content: str) -> str:
    """DeepSeek como proveedor principal; Gemini Flash (nivel gratuito) como
    respaldo de emergencia si DeepSeek falla (sin saldo, caído, error de
    red), para no dejar a los usuarios sin poder hacer consultas."""
    deepseek_key = os.getenv("DEEPSEEK_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")

    if deepseek_key:
        try:
            logger.info("Usando DeepSeek (%s)", DEEPSEEK_MODEL)
            return call_deepseek(deepseek_key, user_content)
        except Exception as error:
            logger.warning("DeepSeek falló (%s); probando respaldo Gemini.", error)

    if gemini_key:
        logger.info("Usando Gemini (%s) como respaldo", GEMINI_MODEL)
        return call_gemini(gemini_key, user_content)

    raise RuntimeError(
        "Falta configurar DEEPSEEK_API_KEY (principal) o GEMINI_API_KEY (respaldo)."
    )


def ask(question: str, dry_run: bool = False) -> str:
    supabase = get_supabase()
    chunks = search_chunks(supabase, question)

    if not chunks:
        return "No encontre documentos relacionados con esa consulta en la base de datos."

    context = build_context(chunks)

    if dry_run:
        return f"[dry-run] Contexto recuperado ({len(chunks)} fragmentos):\n\n{context}"

    user_content = f"Contexto:\n\n{context}\n\nPregunta: {question}"

    return call_llm(user_content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="?")
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra el contexto recuperado, no llama a DeepSeek")
    args = parser.parse_args()

    load_env()

    question = args.question or input("Pregunta: ")
    print(ask(question, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
