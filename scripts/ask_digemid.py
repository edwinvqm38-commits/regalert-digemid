import argparse
import logging
import os
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5"
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
    """Busqueda simple por palabra clave sobre el texto ya extraido.

    No es busqueda semantica (no hay embeddings todavia) - suficiente para
    validar el flujo end-to-end con costo casi nulo. Se puede mejorar despues
    con pgvector si hace falta precision semantica."""
    response = (
        supabase
        .table("digemid_documento_paginas")
        .select(
            "text_content, page_number, "
            "digemid_documentos(document_key, title, published_date, detail_url)"
        )
        .text_search("text_content", query, options={"type": "websearch", "config": "spanish"})
        .limit(limit)
        .execute()
    )

    return response.data or []


def build_context(chunks: list[dict]) -> str:
    blocks = []

    for chunk in chunks:
        doc = chunk.get("digemid_documentos") or {}
        blocks.append(
            f"[Documento {doc.get('document_key')} - {doc.get('title')} - "
            f"{doc.get('published_date')} - pagina {chunk.get('page_number')}]\n"
            f"{chunk.get('text_content')}\n"
            f"Link oficial: {doc.get('detail_url')}"
        )

    return "\n\n---\n\n".join(blocks)


def ask(question: str, dry_run: bool = False) -> str:
    supabase = get_supabase()
    chunks = search_chunks(supabase, question)

    if not chunks:
        return "No encontre documentos relacionados con esa consulta en la base de datos."

    context = build_context(chunks)

    if dry_run:
        return f"[dry-run] Contexto recuperado ({len(chunks)} fragmentos):\n\n{context}"

    client = Anthropic()

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Contexto:\n\n{context}\n\nPregunta: {question}",
        }],
    )

    return next((block.text for block in response.content if block.type == "text"), "")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="?")
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra el contexto recuperado, no llama a Claude")
    args = parser.parse_args()

    load_env()

    question = args.question or input("Pregunta: ")
    print(ask(question, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
