"""Analiza con IA el texto ya extraido de cada norma para detectar su efecto
juridico exacto sobre OTRA norma (deroga, modifica, sustituye, incorpora,
exonera de aplicacion, suspende, prorroga, o efecto no determinable con
certeza), y deja la relacion "pendiente" para que un admin la confirme desde
Telegram (botones inline). No se aplica sola: un error de la IA aqui
significa citar mal una norma legal, asi que la confirmacion humana es
obligatoria antes de marcar algo como derogado/modificado.

Regla central: MENCION DE UNA NORMA NO ES LO MISMO QUE MODIFICARLA. Antes de
esto, el detector solo distinguia deroga/modifica/deja_sin_efecto y forzaba
ahi cualquier relacion (ej. una "exoneracion de aplicacion de los articulos
10 y 11 de la Ley 29459" se clasifico como "modifica" cuando en realidad no
toca el texto de esos articulos, solo exceptua un supuesto especifico de su
aplicacion). El prompt de abajo exige identificar el verbo juridico exacto y
cae a "pendiente_verificacion" en vez de inventar deroga/modifica cuando el
texto no lo deja claro.
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
TIPOS_RELACION_VALIDOS = {
    "deroga",
    "deja_sin_efecto",
    "modifica",
    "sustituye",
    "incorpora",
    "exonera",
    "suspende",
    "prorroga",
    "pendiente_verificacion",
}

SYSTEM_PROMPT = """Eres un abogado especializado en tecnica legislativa \
peruana. Analizas el texto de una norma (ley, decreto, resolucion, etc.) \
para detectar el efecto juridico EXACTO que tiene sobre OTRA norma distinta.

REGLA CENTRAL: que un documento MENCIONE o CITE a otra norma no significa \
que la modifique, derogue ni afecte de ninguna forma. Solo existe una \
relacion si el texto usa un verbo juridico que produce un efecto concreto. \
Simples referencias o citas ("de acuerdo a la Ley X", "conforme al articulo \
Y de...", "en el marco de...") NO son una relacion.

Identifica el verbo juridico exacto y clasifica segun esta tabla (usa el \
tipo_relacion entre parentesis):
- "Modifícase el artículo...", "se modifica..." → modifica
- "Deróguese...", "queda derogado..." → deroga
- "Déjese sin efecto...", "queda sin efecto..." → deja_sin_efecto
- "Sustitúyase el artículo... por el siguiente texto..." → sustituye
- "Incorpórase el artículo/inciso..." → incorpora
- "Exceptúase...", "se exonera de la aplicación de...", "no será de \
aplicación..." → exonera
- "Suspéndase..." → suspende
- "Prorrógase el plazo..." → prorroga

Si el texto SI identifica una norma concreta afectada pero el efecto \
juridico no es alguno de los anteriores, o es ambiguo, o requeriria \
interpretacion juridica para decidir (ej. una posible derogacion tacita por \
incompatibilidad, sin que el texto lo diga expresamente), usa \
tipo_relacion "pendiente_verificacion" en vez de adivinar deroga/modifica.

Devuelve EXCLUSIVAMENTE un JSON (sin texto adicional, sin markdown, sin \
explicaciones) con esta forma exacta:
{"relaciones": [
  {
    "tipo_relacion": "deroga" | "deja_sin_efecto" | "modifica" | "sustituye" | "incorpora" | "exonera" | "suspende" | "prorroga" | "pendiente_verificacion",
    "tipo_norma": "RM" | "DS" | "LEY" | "RD" (abreviatura corta, o null si no se distingue),
    "numero": "920" (solo el numero, sin barras ni anio, o null),
    "anio": 2004 (numero entero de 4 digitos, o null si no se menciona),
    "descripcion": "texto tal cual aparece en el documento identificando la norma afectada",
    "articulos_afectados": "10 y 11" (articulos/numerales/anexos afectados tal como los nombra el texto, o null si no aplica/no se especifica),
    "alcance": "total" | "parcial" | null (si el texto permite saberlo; parcial si solo toca articulos/incisos puntuales),
    "fragmento": "la frase u oracion exacta del documento que sustenta esta clasificacion (maximo 300 caracteres, cita textual, NO parafraseada)"
  }
]}

Reglas estrictas:
- Solo incluye relaciones donde el documento actual afecta a OTRA norma \
CONCRETA e IDENTIFICABLE (nunca te refieras a si mismo).
- IGNORA por completo las clausulas genericas de cierre que casi todo \
documento legal peruano trae, del tipo "deróguese las disposiciones que se \
opongan al presente Decreto/Resolución/Ley" o "quedan derogadas todas las \
normas que se opongan a la presente norma". Esas NO identifican una norma \
especifica, son texto de cierre estandar: NO son una relacion valida.
- Solo reporta una relacion si el texto identifica la norma afectada por \
numero (ej. "Decreto Supremo N° 013-2005-SA"), o por un nombre propio claro \
(ej. "la Ley de Productos Farmacéuticos"). Si la mencion es generica y no \
identifica cual norma puntual queda afectada, no la incluyas.
- Si el documento no tiene ningun efecto juridico sobre otra norma CONCRETA, \
devuelve {"relaciones": []}.
- El "fragmento" debe ser una cita textual exacta del documento, nunca un \
resumen ni una paráfrasis tuya.
- No inventes numeros, tipos, anios ni articulos que no esten explicitos en \
el texto.
"""

# Filtro de respaldo por si la IA igual reporta la clausula generica de
# cierre (redundante con la regla del prompt, pero barato y determinista):
# sin tipo_norma/numero/anio Y con una de estas frases tipicas, se descarta.
PATRON_CLAUSULA_GENERICA = re.compile(
    r"disposiciones?\s+que\s+se\s+opongan|normas?\s+que\s+se\s+opongan|"
    r"quedan?\s+derogad[ao]s?\s+todas\s+las",
    re.IGNORECASE,
)


def es_clausula_generica(tipo_norma, numero, anio, descripcion: str) -> bool:
    if tipo_norma or numero or anio:
        return False
    return bool(PATRON_CLAUSULA_GENERICA.search(descripcion))


def normalizar_para_comparar(texto: str) -> str:
    """Colapsa espacios/saltos de linea y pasa a minusculas, para poder
    comparar el fragmento citado por la IA contra el texto extraido sin que
    diferencias triviales de formato (OCR, saltos de linea) den un falso
    negativo."""
    return re.sub(r"\s+", " ", texto).strip().lower()


def fragmento_aparece_en_texto(fragmento: str, texto_completo: str) -> bool:
    """Verifica que el 'fragmento' que la IA dice haber citado textualmente
    realmente este en el documento, en vez de confiar ciegamente en que la
    IA no parafraseo ni inserto un dato (ej. un numero de articulo) que no
    esta en el texto real. No es infalible (un fragmento truncado en un
    limite raro podria no calzar), pero atrapa el caso mas peligroso: que la
    cita sea, directamente, inventada."""
    if not fragmento:
        return False
    return normalizar_para_comparar(fragmento) in normalizar_para_comparar(texto_completo)


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


def normalizar_numero(numero) -> str | None:
    """Extrae solo el primer grupo de digitos y le quita ceros a la izquierda,
    para poder comparar "014" con "14" o descartar basura que la IA a veces
    deja pegada (ej. "014-2011-SA" en vez de solo "014")."""
    if not numero:
        return None
    coincidencia = re.search(r"\d+", str(numero))
    if not coincidencia:
        return None
    return str(int(coincidencia.group()))


def construir_document_key_candidato(tipo_norma, numero, anio) -> str | None:
    numero_norm = normalizar_numero(numero)
    if tipo_norma and numero_norm and anio:
        return f"{str(tipo_norma).upper()}-{numero_norm}-{anio}"
    return None


def buscar_norma_afectada(supabase, tipo_norma, numero, anio, document_key_candidato) -> dict | None:
    if document_key_candidato:
        response = (
            supabase.table("digemid_normas")
            .select("id, document_key")
            .eq("document_key", document_key_candidato)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0]

    numero_norm = normalizar_numero(numero)
    if numero_norm and anio:
        # Los document_key existentes no siguen un formato de ceros a la
        # izquierda consistente (hay "DS-14-2002" y "DS-008-2025-SA" en la
        # misma tabla), asi que comparar con LIKE es fragil. Ademas
        # digemid_normas.tipo_norma guarda el nombre completo ("Decreto
        # Supremo"), no la abreviatura que devuelve la IA ("DS"), asi que
        # filtrar por tipo aqui solo descartaria filas validas: se trae todo
        # lo de ese año y se compara el numero ya normalizado en Python
        # (año+numero ya es suficientemente selectivo).
        response = (
            supabase.table("digemid_normas")
            .select("id, document_key, numero")
            .eq("anio", anio)
            .execute()
        )
        for fila in response.data or []:
            if normalizar_numero(fila.get("numero")) == numero_norm:
                return fila

    return None


def relacion_ya_registrada(
    supabase,
    norma_origen_id: str,
    tipo_relacion: str,
    descripcion: str,
    numero: str | None,
    anio: int | None,
) -> bool:
    """Evita registrar dos veces la misma relacion. Cuando la IA identifica
    numero+anio de la norma afectada, compara por eso (mas robusto: la misma
    relacion real puede salir redactada con variaciones minimas de texto
    entre corridas, ej. "artículo 9 de la Ley 29698..." vs "Ley 29698
    incorporado en..."). Si no hay numero+anio, cae a comparar el texto
    exacto de la descripcion."""
    response = (
        supabase.table("digemid_norma_relaciones")
        .select("id, numero_afectada, anio_afectada, descripcion_afectada")
        .eq("norma_origen_id", norma_origen_id)
        .eq("tipo_relacion", tipo_relacion)
        .execute()
    )

    numero_norm = normalizar_numero(numero)
    for fila in response.data or []:
        if numero_norm and anio and fila.get("anio_afectada") == anio:
            if normalizar_numero(fila.get("numero_afectada")) == numero_norm:
                return True
        elif fila.get("descripcion_afectada") == descripcion:
            return True

    return False


VERBOS_RELACION = {
    "deroga": "derogaría",
    "deja_sin_efecto": "dejaría sin efecto",
    "modifica": "modificaría",
    "sustituye": "sustituiría el texto de",
    "incorpora": "incorporaría contenido en",
    "exonera": "exoneraría/exceptuaría de la aplicación de",
    "suspende": "suspendería",
    "prorroga": "prorrogaría un plazo de",
    "pendiente_verificacion": "posiblemente afectaría (efecto jurídico NO determinado con certeza) a",
}


def enviar_confirmacion_telegram(
    relacion_id: str,
    origen_document_key: str,
    tipo_relacion: str,
    etiqueta_afectada: str,
    fragmento: str,
    fragmento_verificado: bool,
    articulos_afectados: str | None = None,
    alcance: str | None = None,
) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.info("Sin TELEGRAM_BOT_TOKEN o chat_id: no se notifica la relación %s.", relacion_id)
        return

    verbo = VERBOS_RELACION.get(tipo_relacion, tipo_relacion)

    aviso_cita = (
        ""
        if fragmento_verificado
        else "\n⚠️ <b>Esta cita NO se pudo verificar textualmente contra el documento</b> — revísala antes de confirmar.\n"
    )
    aviso_pendiente = (
        "\n⚠️ <b>La IA no pudo determinar el efecto jurídico exacto con certeza</b> — requiere revisión legal, "
        "no asumas que deroga o modifica.\n"
        if tipo_relacion == "pendiente_verificacion"
        else ""
    )
    detalle_alcance = ""
    if articulos_afectados:
        detalle_alcance += f"\nArtículos/numerales afectados: {articulos_afectados}"
    if alcance:
        detalle_alcance += f"\nAlcance: {alcance}"

    texto = (
        "⚠️ <b>Posible relación normativa detectada por IA</b>\n\n"
        f"<b>{origen_document_key}</b> {verbo}:\n"
        f"<b>{etiqueta_afectada}</b>{detalle_alcance}\n"
        f"{aviso_cita}{aviso_pendiente}\n"
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

        tipo_norma = relacion.get("tipo_norma") or None
        numero = relacion.get("numero") or None
        anio = relacion.get("anio")

        if es_clausula_generica(tipo_norma, numero, anio, descripcion):
            logger.info("Ignorando cláusula genérica de %s: %s", norma["document_key"], descripcion[:80])
            continue

        if relacion_ya_registrada(supabase, norma["id"], tipo_relacion, descripcion, numero, anio):
            continue
        candidato = construir_document_key_candidato(tipo_norma, numero, anio)
        afectada = buscar_norma_afectada(supabase, tipo_norma, numero, anio, candidato)
        fragmento = (relacion.get("fragmento") or "")[:500]
        verificado = fragmento_aparece_en_texto(fragmento, texto)
        if not verificado:
            logger.warning(
                "Fragmento de %s no se pudo verificar textualmente contra el documento: %r",
                norma["document_key"],
                fragmento[:120],
            )

        articulos_afectados = (relacion.get("articulos_afectados") or None)
        alcance = relacion.get("alcance")
        if alcance not in ("total", "parcial"):
            alcance = None

        insercion = {
            "norma_origen_id": norma["id"],
            "norma_origen_document_key": norma["document_key"],
            "tipo_relacion": tipo_relacion,
            "norma_afectada_id": afectada["id"] if afectada else None,
            "tipo_norma_afectada": tipo_norma,
            "numero_afectada": numero,
            "anio_afectada": anio,
            "descripcion_afectada": descripcion,
            "articulos_afectados": articulos_afectados,
            "alcance": alcance,
            "fragmento_fuente": fragmento,
            "fragmento_verificado": verificado,
        }

        respuesta = supabase.table("digemid_norma_relaciones").insert(insercion).execute()
        fila = (respuesta.data or [None])[0]
        if not fila:
            continue

        insertadas += 1
        etiqueta_afectada = afectada["document_key"] if afectada else descripcion
        enviar_confirmacion_telegram(
            fila["id"], norma["document_key"], tipo_relacion, etiqueta_afectada, fragmento, verificado,
            articulos_afectados, alcance,
        )

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
