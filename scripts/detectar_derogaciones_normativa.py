"""Analiza con IA el texto ya extraido de cada norma para detectar si deroga,
deja sin efecto o modifica a OTRA norma, y deja la relacion "pendiente" para
que un admin la confirme desde Telegram (botones inline). No se aplica sola:
un error de la IA aqui significa citar mal una norma legal, asi que la
confirmacion humana es obligatoria antes de marcar algo como derogado.
"""

import argparse
import json
import logging
import os
import re
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
MAX_CHARS_TEXTO = 15000
TIPOS_RELACION_VALIDOS = {"deroga", "modifica", "deja_sin_efecto"}

SYSTEM_PROMPT = """Eres un asistente legal que analiza el texto de una norma \
peruana (ley, decreto, resolucion, etc.) para detectar si ESTE documento \
deroga, deja sin efecto o modifica a OTRA norma distinta.

Devuelve EXCLUSIVAMENTE un JSON (sin texto adicional, sin markdown, sin \
explicaciones) con esta forma exacta:
{"relaciones": [
  {
    "tipo_relacion": "deroga" | "modifica" | "deja_sin_efecto",
    "tipo_norma": "RM" | "DS" | "LEY" | "RD" (abreviatura corta, o null si no se distingue),
    "numero": "920" (solo el numero, sin barras ni anio, o null),
    "anio": 2004 (numero entero de 4 digitos, o null si no se menciona),
    "descripcion": "texto tal cual aparece en el documento identificando la norma afectada",
    "fragmento": "la frase u oracion exacta del documento donde se menciona la derogacion/modificacion (maximo 300 caracteres)"
  }
]}

Reglas estrictas:
- Solo incluye relaciones donde el documento actual afecta a OTRA norma (nunca \
te refieras a si mismo).
- Si el documento no deroga, modifica ni deja sin efecto ninguna otra norma, \
devuelve {"relaciones": []}.
- No inventes numeros, tipos ni anios que no esten explicitos en el texto.
"""


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def call_deepseek(api_key: str, texto: str) -> dict:
    response = requests.post(
        "https://api.deepseek.com/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": DEEPSEEK_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": texto},
            ],
            "max_tokens": 1024,
            "temperature": 0,
        },
        timeout=90,
    )
    response.raise_for_status()
    contenido = response.json()["choices"][0]["message"]["content"].strip()
    contenido = re.sub(r"^```(?:json)?|```$", "", contenido, flags=re.MULTILINE).strip()

    try:
        data = json.loads(contenido)
    except json.JSONDecodeError:
        logger.warning("Respuesta de DeepSeek no es JSON válido: %s", contenido[:300])
        return {"relaciones": []}

    if not isinstance(data.get("relaciones"), list):
        return {"relaciones": []}
    return data


def normas_pendientes(supabase, limit: int, document_key: str | None) -> list[dict]:
    query = (
        supabase.table("digemid_normas")
        .select("id, document_key, titulo")
        .in_("process_status", ["text_extracted", "text_extracted_baja_calidad"])
        .eq("derogacion_analizada", False)
    )
    if document_key:
        query = query.eq("document_key", document_key)

    response = query.limit(limit).execute()
    return response.data or []


def texto_de_norma(supabase, norma_id: str) -> str:
    response = (
        supabase.table("digemid_norma_paginas")
        .select("page_number, text_normalized, text_raw")
        .eq("norma_id", norma_id)
        .order("page_number")
        .execute()
    )

    partes = []
    total = 0
    for fila in response.data or []:
        texto = (fila.get("text_normalized") or fila.get("text_raw") or "").strip()
        if not texto:
            continue
        partes.append(texto)
        total += len(texto)
        if total >= MAX_CHARS_TEXTO:
            break

    return "\n\n".join(partes)[:MAX_CHARS_TEXTO]


def construir_document_key_candidato(tipo_norma, numero, anio) -> str | None:
    if tipo_norma and numero and anio:
        return f"{str(tipo_norma).upper()}-{numero}-{anio}"
    return None


def buscar_norma_afectada(supabase, tipo_norma, numero, anio, document_key_candidato) -> dict | None:
    if document_key_candidato:
        response = (
            supabase.table("digemid_normas")
            .select("id, document_key")
            .eq("document_key", document_key_candidato)
            .maybe_single()
            .execute()
        )
        if response.data:
            return response.data

    if numero and anio:
        query = (
            supabase.table("digemid_normas")
            .select("id, document_key")
            .eq("anio", anio)
            .ilike("numero", f"%{numero}%")
        )
        if tipo_norma:
            query = query.ilike("tipo_norma", f"%{tipo_norma}%")
        response = query.limit(1).execute()
        if response.data:
            return response.data[0]

    return None


def relacion_ya_registrada(supabase, norma_origen_id: str, descripcion: str) -> bool:
    response = (
        supabase.table("digemid_norma_relaciones")
        .select("id")
        .eq("norma_origen_id", norma_origen_id)
        .eq("descripcion_afectada", descripcion)
        .limit(1)
        .execute()
    )
    return bool(response.data)


def enviar_confirmacion_telegram(
    relacion_id: str,
    origen_document_key: str,
    tipo_relacion: str,
    etiqueta_afectada: str,
    fragmento: str,
) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.info("Sin TELEGRAM_BOT_TOKEN o chat_id: no se notifica la relación %s.", relacion_id)
        return

    verbo = {
        "deroga": "derogaría",
        "deja_sin_efecto": "dejaría sin efecto",
        "modifica": "modificaría",
    }.get(tipo_relacion, tipo_relacion)

    texto = (
        "⚠️ <b>Posible derogación detectada por IA</b>\n\n"
        f"<b>{origen_document_key}</b> {verbo} a:\n"
        f"<b>{etiqueta_afectada}</b>\n\n"
        f"Fragmento: <i>\"{fragmento}\"</i>\n\n"
        "¿Confirmas esta relación?"
    )
    reply_markup = {
        "inline_keyboard": [[
            {"text": "✅ Confirmar", "callback_data": f"derog:confirmar:{relacion_id}"},
            {"text": "❌ Rechazar", "callback_data": f"derog:rechazar:{relacion_id}"},
        ]]
    }

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": texto,
                "parse_mode": "HTML",
                "reply_markup": reply_markup,
            },
            timeout=30,
        )
        response.raise_for_status()
    except Exception as error:
        logger.warning("No se pudo notificar la relación %s por Telegram: %s", relacion_id, error)


def procesar_norma(supabase, norma: dict, deepseek_key: str) -> int:
    texto = texto_de_norma(supabase, norma["id"])
    if not texto:
        supabase.table("digemid_normas").update({"derogacion_analizada": True}).eq("id", norma["id"]).execute()
        return 0

    resultado = call_deepseek(deepseek_key, texto)
    insertadas = 0

    for relacion in resultado.get("relaciones", []):
        tipo_relacion = relacion.get("tipo_relacion")
        if tipo_relacion not in TIPOS_RELACION_VALIDOS:
            continue

        descripcion = (relacion.get("descripcion") or "").strip()
        if not descripcion:
            continue

        if relacion_ya_registrada(supabase, norma["id"], descripcion):
            continue

        tipo_norma = relacion.get("tipo_norma") or None
        numero = relacion.get("numero") or None
        anio = relacion.get("anio")
        candidato = construir_document_key_candidato(tipo_norma, numero, anio)
        afectada = buscar_norma_afectada(supabase, tipo_norma, numero, anio, candidato)
        fragmento = (relacion.get("fragmento") or "")[:500]

        insercion = {
            "norma_origen_id": norma["id"],
            "norma_origen_document_key": norma["document_key"],
            "tipo_relacion": tipo_relacion,
            "norma_afectada_id": afectada["id"] if afectada else None,
            "tipo_norma_afectada": tipo_norma,
            "numero_afectada": numero,
            "anio_afectada": anio,
            "descripcion_afectada": descripcion,
            "fragmento_fuente": fragmento,
        }

        respuesta = supabase.table("digemid_norma_relaciones").insert(insercion).execute()
        fila = (respuesta.data or [None])[0]
        if not fila:
            continue

        insertadas += 1
        etiqueta_afectada = afectada["document_key"] if afectada else descripcion
        enviar_confirmacion_telegram(fila["id"], norma["document_key"], tipo_relacion, etiqueta_afectada, fragmento)

    supabase.table("digemid_normas").update({"derogacion_analizada": True}).eq("id", norma["id"]).execute()
    return insertadas


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20, help="Máximo de normas a analizar en esta corrida")
    parser.add_argument("--document-key", default=None, help="Reanalizar solo esta norma")
    args = parser.parse_args()

    load_env()

    deepseek_key = os.getenv("DEEPSEEK_API_KEY")
    if not deepseek_key:
        raise RuntimeError("Falta configurar DEEPSEEK_API_KEY")

    supabase = get_supabase()
    normas = normas_pendientes(supabase, args.limit, args.document_key)
    logger.info("Normas a analizar: %d", len(normas))

    total_relaciones = 0
    for norma in normas:
        try:
            total_relaciones += procesar_norma(supabase, norma, deepseek_key)
        except Exception as error:
            logger.error("Error analizando %s: %s", norma.get("document_key"), error)

    logger.info("Listo. Relaciones nuevas detectadas: %d", total_relaciones)


if __name__ == "__main__":
    main()
