"""Analiza con IA el texto ya extraido de cada norma para detectar su efecto
juridico exacto sobre OTRA norma (deroga, modifica, sustituye, incorpora,
exonera de aplicacion, suspende, prorroga, o efecto no determinable con
certeza), y deja la relacion "pendiente" en digemid_norma_relaciones. No
manda nada a Telegram en el momento (para no llenar el chat cada hora): el
admin las revisa y confirma/rechaza cuando quiere con /derogacionespendientes
desde el bot. No se aplica sola: un error de la IA aqui significa citar mal
una norma legal, asi que la confirmacion humana es obligatoria antes de
marcar algo como derogado/modificado.

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

ALERTA - PROYECTOS ANEXADOS: es MUY comun que una Resolucion Ministerial \
real solo disponga PUBLICAR UN PROYECTO de una futura norma para recibir \
comentarios (ej. "Articulo 1.- Disponer la publicacion del proyecto de \
resolucion..."), y que el PDF completo incluya, a continuacion, el texto \
completo de ese proyecto como anexo -con su propia numeracion de articulos, \
que puede repetir "Articulo 1", "Articulo 2", etc. Esas secciones suelen \
estar marcadas con leyendas como "PROYECTO PARA PUBLICACION" o "PROYECTO DE \
RESOLUCION". El articulado de un PROYECTO asi anexado NO es la parte \
dispositiva de la norma real: NO tiene efecto juridico vigente todavia. Si \
detectas que el texto contiene un proyecto anexado de este tipo, analiza \
UNICAMENTE la parte dispositiva de la norma real (antes del anexo) e IGNORA \
por completo cualquier "deroga/modifica/etc." que aparezca dentro del texto \
del proyecto anexado.

ALERTA - LINAJE DE APROBACION/MODIFICACION NO ES EL OBJETO MODIFICADO: es \
MUY comun que un articulo diga algo como "Modificar el numeral 2 del Anexo \
02 de la Directiva N° 165-MINSA/DIGEMID-V.01, aprobada por Resolucion \
Ministerial N° 737-2010/MINSA, modificada por Resolucion Ministerial N° \
615-2024/MINSA". En una frase asi, el OBJETO que se esta modificando AHORA \
es la Directiva N° 165 (el sustantivo principal, justo despues de \
"Modificar el/la..."). Las resoluciones citadas despues con "aprobada por" \
o "modificada por" SOLO describen el linaje/historial de esa Directiva \
(quien la creo, quien la modifico antes) -NO son el destino de esta \
modificacion, aunque sean lo ultimo mencionado en la oracion-. Nunca \
reportes esas resoluciones de linaje como si fueran el objeto afectado: la \
relacion correcta ahi es "modifica" hacia la Directiva N° 165, no hacia la \
Resolucion N° 615-2024. Presta atencion a CUAL es el sustantivo que sigue \
inmediatamente al verbo ("Modificar EL/LA ..."), no al ultimo numero de \
norma que aparezca en la oracion.

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
- El "fragmento" y el tipo_relacion DEBEN basarse en la PARTE DISPOSITIVA \
del documento (los articulos que vienen despues de "SE RESUELVE:" o \
"DECRETA:"), nunca en los "CONSIDERANDO" / "Que, ..." previos. Los \
considerandos solo narran el contexto o antecedentes (ej. "Que, mediante \
Resolucion X se aprobo Y...") y pueden mencionar de pasada una norma que \
MAS ADELANTE, en la parte dispositiva, se deroga o modifica -pero el \
considerando en si NO es la relacion, y describir su contenido con tus \
propias palabras no es una cita valida. Busca siempre el articulo real \
("Articulo N°.- Modificar/Derogar/...") que sustenta cada relacion y cita \
ESE texto, no el considerando que lo antecede.
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

# Caso real (RM-419-2025/MINSA): una RM que solo dispone publicar un
# PROYECTO de norma para comentarios trae el proyecto completo anexado en el
# mismo PDF, con su propio "Articulo 2.- Derogar..." que la IA puede leer
# como si fuera la parte dispositiva de la RM real. Se corta el texto antes
# de la primera aparicion de esta leyenda -que DIGEMID/MINSA estampa en cada
# pagina del anexo- para que ese articulado ni siquiera llegue a la IA. Es
# la defensa principal (determinista); la regla del prompt de arriba es
# solo respaldo por si el marcador no aparece o aparece distinto.
PATRON_MARCADOR_PROYECTO = re.compile(r"proyecto\s+para\s+publicaci[oó]n", re.IGNORECASE)


def recortar_antes_de_proyecto_anexado(texto: str, document_key: str) -> str:
    coincidencia = PATRON_MARCADOR_PROYECTO.search(texto)
    if not coincidencia:
        return texto
    logger.warning(
        "%s: se detectó un proyecto anexado (marca 'PROYECTO PARA PUBLICACIÓN'); "
        "se recorta el texto a analizar antes de esa sección.",
        document_key,
    )
    return texto[: coincidencia.start()]


# Caso real (RM-727-2025/MINSA): la RM dispone publicar un proyecto de
# Decreto Supremo para comentarios, y el proyecto completo (con su propio
# articulado "Modificar los articulos 3, 6, 10...") queda pegado despues,
# sin la leyenda "PROYECTO PARA PUBLICACION" (ese marcador no es universal).
# Una Resolucion (RM/RD/RS/RVM/RJ) SIEMPRE cierra su parte dispositiva con
# "SE RESUELVE:", nunca con "DECRETA:" -eso es propio de un Decreto-, asi
# que un "DECRETA:" dentro del texto de una Resolucion es una señal muy
# confiable de que ahi empieza un Decreto anexado (real o proyecto).
TIPOS_RESOLUCION = {"RM", "RD", "RS", "RVM", "RJ"}
PATRON_DECRETA = re.compile(r"\bDECRETA\s*:", re.IGNORECASE)


def recortar_decreto_anexado(texto: str, tipo_norma: str | None, document_key: str) -> str:
    if (tipo_norma or "").strip().upper() not in TIPOS_RESOLUCION:
        return texto
    coincidencia = PATRON_DECRETA.search(texto)
    if not coincidencia:
        return texto
    logger.warning(
        "%s: es una Resolución pero su texto contiene 'DECRETA:' (propio de un Decreto); "
        "se recorta, probablemente hay un Decreto anexado (real o proyecto).",
        document_key,
    )
    return texto[: coincidencia.start()]


# Caso real (RM-883-2024/MINSA): el PDF es un escaneo de El Peruano y su
# primera pagina trae, antes de que empiece la norma real, la cola de OTRA
# resolucion distinta publicada justo antes en la misma edicion del diario.
# La norma real siempre trae su propio encabezado oficial
# ("RESOLUCION MINISTERIAL\nNo 883-2024/MINSA"), asi que si ese encabezado
# con el numero/año propios aparece en medio del texto (no al inicio), todo
# lo de antes es de otro documento y se descarta.
def recortar_antes_del_encabezado_propio(
    texto: str, tipo_norma: str | None, numero: str | None, anio: int | None, document_key: str,
) -> str:
    numero_norm = normalizar_numero(numero)
    if not numero_norm or not anio or not tipo_norma:
        return texto

    palabra_tipo = {
        "RM": "RESOLUCI[OÓ]N MINISTERIAL",
        "RD": "RESOLUCI[OÓ]N DIRECTORAL",
        "RS": "RESOLUCI[OÓ]N SUPREMA",
        "RVM": "RESOLUCI[OÓ]N VICEMINISTERIAL",
        "DS": "DECRETO SUPREMO",
        "DL": "DECRETO LEGISLATIVO",
        "DU": "DECRETO DE URGENCIA",
    }.get((tipo_norma or "").strip().upper())
    if not palabra_tipo:
        return texto

    patron = re.compile(
        rf"{palabra_tipo}\s*\n?\s*N[°ºo.]*\s*0*{numero_norm}[-/]{anio}",
        re.IGNORECASE,
    )
    coincidencia = patron.search(texto)
    # Solo recorta si el encabezado aparece bien adentro del texto (no al
    # inicio, que es lo normal): un match cerca de la posicion 0 es la norma
    # empezando correctamente, no contaminacion de otro documento.
    if not coincidencia or coincidencia.start() < 50:
        return texto

    logger.warning(
        "%s: se encontró el encabezado propio de la norma en medio del texto "
        "(posición %d); se descarta todo lo anterior (probablemente es el final "
        "de otra norma distinta en el mismo PDF de El Peruano).",
        document_key,
        coincidencia.start(),
    )
    return texto[coincidencia.start():]


def es_clausula_generica(tipo_norma, numero, anio, descripcion: str) -> bool:
    if tipo_norma or numero or anio:
        return False
    return bool(PATRON_CLAUSULA_GENERICA.search(descripcion))


# Caso real (RM-899-2025/MINSA): "Modificar el numeral 2 del Anexo 02 de la
# Directiva N° 165..., aprobada por Resolución Ministerial N° 737-2010/MINSA,
# modificada por Resolución Ministerial N° 615-2024/MINSA" -> el objeto que
# se modifica es la Directiva N° 165, no la RM 615-2024. La IA reporto la RM
# 615-2024 (el ultimo numero mencionado en la oracion) como si fuera el
# objeto, cuando solo describe el linaje de modificacion previa de la
# Directiva. Respaldo deterministico: si en el fragmento citado el
# numero/año que la IA reporto aparece justo despues de "aprobado/a por" o
# "modificado/a por", es una cita de linaje, no el objeto de la relacion.
def es_cita_de_linaje(fragmento: str, numero, anio) -> bool:
    numero_norm = normalizar_numero(numero)
    if not numero_norm or not anio or not fragmento:
        return False
    patron = re.compile(
        rf"(?:aprobad[oa]|modificad[oa])\s+por\s+\w[\w\s]{{0,30}}?N[°ºo.]*\s*0*{numero_norm}[-/]{anio}",
        re.IGNORECASE,
    )
    return bool(patron.search(fragmento))


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
        .select("id, document_key, titulo, tipo_norma, numero, anio")
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


def procesar_norma(supabase, norma: dict, deepseek_key: str) -> int:
    texto = texto_de_norma(supabase, norma["id"])
    if not texto:
        supabase.table("digemid_normas").update({"derogacion_analizada": True}).eq("id", norma["id"]).execute()
        return 0

    texto = recortar_antes_del_encabezado_propio(
        texto, norma.get("tipo_norma"), norma.get("numero"), norma.get("anio"), norma["document_key"],
    )
    texto = recortar_antes_de_proyecto_anexado(texto, norma["document_key"])
    texto = recortar_decreto_anexado(texto, norma.get("tipo_norma"), norma["document_key"])
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
        fragmento = (relacion.get("fragmento") or "")[:500]

        if es_clausula_generica(tipo_norma, numero, anio, descripcion):
            logger.info("Ignorando cláusula genérica de %s: %s", norma["document_key"], descripcion[:80])
            continue

        if es_cita_de_linaje(fragmento, numero, anio):
            logger.info(
                "Ignorando cita de linaje (aprobado/modificado por) de %s: %s",
                norma["document_key"], descripcion[:80],
            )
            continue

        if relacion_ya_registrada(supabase, norma["id"], tipo_relacion, descripcion, numero, anio):
            continue
        candidato = construir_document_key_candidato(tipo_norma, numero, anio)
        afectada = buscar_norma_afectada(supabase, tipo_norma, numero, anio, candidato)
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

        # No se notifica por Telegram en el momento: queda "pendiente" en la
        # base y el admin la revisa cuando quiere con /derogacionespendientes,
        # para no llenar el chat de mensajes cada vez que corre este script
        # (cada hora, en lotes de hasta 20 normas).
        insertadas += 1

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
