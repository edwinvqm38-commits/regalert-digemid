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
import time
from datetime import datetime, timezone
from pathlib import Path

import sys

import requests
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent))
from identidad_normativa import (  # noqa: E402
    AMBIGUA,
    clave_dedupe,
    construir_identidad,
    identidad_de_norma,
    normalizar_numero,
    resolver_identidad,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEEPSEEK_MODEL = "deepseek-chat"
MAX_CHARS_TEXTO = 15000

# Version del analizador. Si se cambia el prompt, la segmentacion o las reglas
# deterministas, SUBIR este numero: las normas analizadas con una version
# anterior vuelven a entrar en la cola automaticamente (antes quedaban
# congeladas para siempre con el resultado viejo).
ANALYZER_VERSION = 2

MAX_TOKENS_RESPUESTA = 4096
TIMEOUT_DEEPSEEK = 120
INTENTOS_DEEPSEEK = 3
MIN_CHARS_ANALIZABLE = 200

# Estados explicitos de la llamada al modelo. SOLO ESTADO_OK permite marcar una
# norma como analizada; cualquier otro la deja pendiente para el proximo intento.
ESTADO_OK = "OK"
ESTADO_ERROR_API = "ERROR_API"
ESTADO_ERROR_JSON = "ERROR_JSON"
ESTADO_TIMEOUT = "TIMEOUT"
ESTADO_TEXTO_INSUFICIENTE = "TEXTO_INSUFICIENTE"
ESTADO_RESPUESTA_INCOMPLETA = "RESPUESTA_INCOMPLETA"
ESTADOS_REINTENTABLES = {ESTADO_ERROR_API, ESTADO_TIMEOUT, ESTADO_RESPUESTA_INCOMPLETA}
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
    """SOLO "modificado/a por" denota linaje (una enmienda previa del objeto).

    "aprobado/a por" NO es linaje: en la formula estandar peruana "Modificar el
    articulo N del Reglamento ..., aprobado por Decreto Supremo X", el objeto
    modificado ES ese Decreto -el reglamento vive dentro del instrumento que lo
    aprobo-. Incluir "aprobado por" aqui descartaba relaciones reales y ya
    confirmadas (DS-15-2025 y DS-008-2025 -> DS-014-2011-SA). Ver H-02.
    """
    numero_norm = normalizar_numero(numero)
    if not numero_norm or not anio or not fragmento:
        return False
    patron = re.compile(
        rf"modificad[oa]s?\s+por\s+\w[\w\s]{{0,30}}?N[°ºo.]*\s*0*{numero_norm}[-/]{anio}",
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


def _una_llamada_deepseek(api_key: str, texto: str) -> tuple[str, dict]:
    """Una sola llamada. Devuelve (estado, data) sin lanzar excepciones."""
    try:
        response = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": texto},
                ],
                # 1024 truncaba el JSON justo en las normas con MAS relaciones
                # (las mas ricas juridicamente), y esa truncadura terminaba
                # clasificada como "sin relaciones". Ver ESTADO_RESPUESTA_INCOMPLETA.
                "max_tokens": MAX_TOKENS_RESPUESTA,
                "temperature": 0,
            },
            timeout=TIMEOUT_DEEPSEEK,
        )
    except requests.exceptions.Timeout:
        return ESTADO_TIMEOUT, {}
    except requests.exceptions.RequestException as error:
        logger.warning("Error de red hablando con DeepSeek: %s", error)
        return ESTADO_ERROR_API, {}

    if response.status_code >= 400:
        logger.warning("DeepSeek respondio HTTP %s", response.status_code)
        return ESTADO_ERROR_API, {}

    try:
        cuerpo = response.json()
        eleccion = cuerpo["choices"][0]
        contenido = eleccion["message"]["content"].strip()
    except (ValueError, KeyError, IndexError) as error:
        logger.warning("Respuesta de DeepSeek con forma inesperada: %s", error)
        return ESTADO_ERROR_API, {}

    # finish_reason="length" = el modelo se quedo sin tokens: la respuesta esta
    # cortada aunque por casualidad parsee. Nunca es un "no hay relaciones".
    if eleccion.get("finish_reason") == "length":
        logger.warning("DeepSeek corto la respuesta por limite de tokens.")
        return ESTADO_RESPUESTA_INCOMPLETA, {}

    contenido = re.sub(r"^```(?:json)?|```$", "", contenido, flags=re.MULTILINE).strip()

    try:
        data = json.loads(contenido)
    except json.JSONDecodeError:
        logger.warning("Respuesta de DeepSeek no es JSON válido: %s", contenido[:300])
        return ESTADO_ERROR_JSON, {}

    if not isinstance(data.get("relaciones"), list):
        logger.warning("El JSON de DeepSeek no trae la lista 'relaciones'.")
        return ESTADO_ERROR_JSON, {}

    return ESTADO_OK, data


def call_deepseek(api_key: str, texto: str) -> tuple[str, dict]:
    """Consulta a DeepSeek devolviendo un ESTADO EXPLICITO.

    Antes, cualquier fallo de parseo devolvia {"relaciones": []}, que es
    indistinguible de "el modelo analizo y no encontro nada" -y el llamador
    marcaba la norma como analizada para siempre-. Un error del modelo NO es
    una respuesta valida: solo ESTADO_OK autoriza a dar la norma por analizada.
    """
    if len((texto or "").strip()) < MIN_CHARS_ANALIZABLE:
        return ESTADO_TEXTO_INSUFICIENTE, {}

    estado, data = ESTADO_ERROR_API, {}
    for intento in range(1, INTENTOS_DEEPSEEK + 1):
        estado, data = _una_llamada_deepseek(api_key, texto)
        if estado == ESTADO_OK or estado not in ESTADOS_REINTENTABLES:
            break
        if intento < INTENTOS_DEEPSEEK:
            logger.info(
                "Reintentando DeepSeek (intento %d de %d, estado previo %s)...",
                intento + 1, INTENTOS_DEEPSEEK, estado,
            )
            time.sleep(2 * intento)

    return estado, data


def normas_pendientes(
    supabase, limit: int, document_key: str | None, force: bool = False
) -> list[dict]:
    """Cola de analisis.

    Una norma entra si (a) nunca se analizo, o (b) se analizo con una version
    ANTERIOR del analizador. Sin esto, cada mejora del motor dejaba fuera para
    siempre a las normas ya procesadas -que son justo las que arrastran los
    errores de la version vieja-. Con --force se reanaliza aunque este al dia.
    """
    columnas = (
        "id, document_key, titulo, tipo_norma, numero, anio, "
        "derogacion_analizada, relaciones_analyzer_version"
    )
    query = (
        supabase.table("digemid_normas")
        .select(columnas)
        .in_("process_status", ["text_extracted", "text_extracted_baja_calidad"])
    )
    if document_key:
        query = query.eq("document_key", document_key)

    if force:
        return (query.limit(limit).execute().data or [])

    # PostgREST no expresa comodamente "version < N OR version IS NULL" junto al
    # resto de filtros, asi que se pide un margen y se filtra en Python.
    candidatas = query.limit(max(limit * 5, limit)).execute().data or []
    pendientes = [
        n for n in candidatas
        if not n.get("derogacion_analizada")
        or (n.get("relaciones_analyzer_version") or 0) < ANALYZER_VERSION
    ]
    return pendientes[:limit]


# Marcadores de inicio de la parte dispositiva. Todo el efecto juridico de una
# norma peruana vive despues de uno de estos; lo anterior son vistos y
# considerandos (contexto, no efecto).
PATRON_INICIO_DISPOSITIVA = re.compile(r"\b(SE\s+RESUELVE|DECRETA|SE\s+DECRETA|RESUELVE)\s*:", re.IGNORECASE)


def texto_de_norma(supabase, norma_id: str) -> str:
    response = (
        supabase.table("digemid_norma_paginas")
        .select("page_number, text_normalized, text_raw")
        .eq("norma_id", norma_id)
        .order("page_number")
        .execute()
    )

    partes = [
        (fila.get("text_normalized") or fila.get("text_raw") or "").strip()
        for fila in response.data or []
    ]
    return "\n\n".join(p for p in partes if p)


def seleccionar_texto_relevante(texto: str, document_key: str = "") -> str:
    """Recorta a la ventana del modelo SIN perder la parte dispositiva.

    El truncado anterior (`texto[:15000]`) cortaba por el principio, y en
    tecnica legislativa peruana las disposiciones complementarias DEROGATORIAS
    y MODIFICATORIAS van AL FINAL. Con normas de 44k chars de media, eso
    amputaba la evidencia juridica del 57,5% del corpus: RM-894-2024 tenia su
    "Articulo 3.- Derogar la RM 339-2023" en el offset ~17.900 y el detector
    nunca lo vio, asi que cito un considerando. Ver H-01.

    Estrategia: se prioriza la parte dispositiva y SIEMPRE se conserva el final
    del documento. Si no cabe entera, se toman su inicio y su cola.
    """
    if len(texto) <= MAX_CHARS_TEXTO:
        return texto

    inicio = PATRON_INICIO_DISPOSITIVA.search(texto)
    if inicio:
        # Un poco de encabezado da contexto de que norma es; el resto del
        # presupuesto se gasta en la parte dispositiva, no en considerandos.
        encabezado = texto[: min(inicio.start(), 1500)]
        dispositiva = texto[inicio.start():]
        presupuesto = MAX_CHARS_TEXTO - len(encabezado) - 100

        if len(dispositiva) <= presupuesto:
            cuerpo = dispositiva
        else:
            # Cabeza (articulos iniciales) + cola (disposiciones finales).
            mitad = presupuesto // 2
            cuerpo = (
                dispositiva[:mitad]
                + "\n\n[... fragmento intermedio omitido por longitud ...]\n\n"
                + dispositiva[-mitad:]
            )
        logger.info(
            "%s: segmentacion estructural (parte dispositiva localizada; %d chars totales).",
            document_key, len(texto),
        )
        return encabezado + "\n\n" + cuerpo

    # Sin marcador legible (OCR pobre): cabeza + cola, nunca solo la cabeza.
    logger.warning(
        "%s: no se localizo 'SE RESUELVE/DECRETA'; se analizan inicio y final del documento.",
        document_key,
    )
    mitad = (MAX_CHARS_TEXTO - 100) // 2
    return (
        texto[:mitad]
        + "\n\n[... fragmento intermedio omitido por longitud ...]\n\n"
        + texto[-mitad:]
    )


def cargar_catalogo(supabase) -> list[dict]:
    """Catalogo completo de normas, leido UNA vez por corrida.

    La resolucion de identidad necesita ver todas las candidatas a la vez para
    poder declarar IDENTIDAD_AMBIGUA; con consultas puntuales por numero+año
    -lo que se hacia antes- era imposible distinguir "una candidata" de
    "varias, elegi la primera" (hallazgo H-06).
    """
    return (
        supabase.table("digemid_normas")
        .select("id, document_key, tipo_norma, numero, anio")
        .execute()
        .data
        or []
    )


def identidad_para_dedupe(resultado, citada):
    """Identidad con la que se construye la clave estable: la de la norma REAL
    cuando se pudo resolver (asi "Ley 29459" y "Ley N° 29459-2009" convergen),
    y la de la cita cuando no. Es el mismo criterio que usa el DRY-RUN."""
    if resultado.resuelta:
        return identidad_de_norma(resultado.norma)
    return citada


def claves_ya_registradas(supabase, norma_origen_id: str, catalogo: list[dict]) -> set[str]:
    """H-07 · Indice de deduplicacion de las relaciones YA existentes de esta
    norma origen.

    Se recomputa la clave en memoria a partir de los campos de cada fila; NO se
    escribe nada sobre las filas historicas (muchas estan confirmadas). Asi el
    reanalisis con --force es idempotente incluso contra relaciones creadas
    antes de que existiera la columna clave_dedupe.
    """
    filas = (
        supabase.table("digemid_norma_relaciones")
        .select("id, tipo_relacion, tipo_norma_afectada, numero_afectada, anio_afectada, "
                "articulos_afectados, descripcion_afectada")
        .eq("norma_origen_id", norma_origen_id)
        .execute()
        .data
        or []
    )

    claves = set()
    for fila in filas:
        citada = construir_identidad(
            fila.get("tipo_norma_afectada"), fila.get("numero_afectada"), fila.get("anio_afectada")
        )
        identidad = identidad_para_dedupe(resolver_identidad(citada, catalogo), citada)
        claves.add(
            clave_dedupe(
                norma_origen_id,
                fila.get("tipo_relacion"),
                identidad,
                fila.get("articulos_afectados"),
                fila.get("descripcion_afectada"),
            )
        )
    return claves


def marcar_analizada(supabase, norma_id: str) -> None:
    """Solo se llama cuando el analisis fue realmente concluyente (ESTADO_OK o
    documento sin texto util). Deja constancia de CON QUE VERSION se analizo,
    para que una mejora futura del motor vuelva a encolar la norma."""
    supabase.table("digemid_normas").update(
        {
            "derogacion_analizada": True,
            "relaciones_analyzer_version": ANALYZER_VERSION,
            "relaciones_analizadas_en": datetime.now(timezone.utc).isoformat(),
        }
    ).eq("id", norma_id).execute()


def procesar_norma(supabase, norma: dict, deepseek_key: str, catalogo: list[dict]) -> int:
    texto = texto_de_norma(supabase, norma["id"])
    if not texto:
        marcar_analizada(supabase, norma["id"])
        return 0

    texto = recortar_antes_del_encabezado_propio(
        texto, norma.get("tipo_norma"), norma.get("numero"), norma.get("anio"), norma["document_key"],
    )
    texto = recortar_antes_de_proyecto_anexado(texto, norma["document_key"])
    texto = recortar_decreto_anexado(texto, norma.get("tipo_norma"), norma["document_key"])
    if not texto:
        marcar_analizada(supabase, norma["id"])
        return 0

    # La ventana se aplica AL FINAL, y de forma estructural: primero se limpia
    # la contaminacion sobre el texto completo, despues se elige que parte
    # entra al modelo sin perder las disposiciones finales.
    texto = seleccionar_texto_relevante(texto, norma["document_key"])

    estado, resultado = call_deepseek(deepseek_key, texto)

    if estado != ESTADO_OK:
        # Un fallo del modelo NO es "no hay relaciones": la norma queda
        # pendiente para el proximo intento en vez de cerrarse en falso.
        logger.error(
            "%s: analisis NO concluyente (%s). Se deja pendiente, sin marcar como analizada.",
            norma["document_key"], estado,
        )
        return 0

    insertadas = 0
    claves_previas = claves_ya_registradas(supabase, norma["id"], catalogo)

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

        # --- IDENTIDAD NORMATIVA (H-05/H-06): quien es exactamente la norma
        # afectada. Ante varias candidatas NO se elige ninguna: la relacion se
        # guarda con la identidad sin resolver para que la decida un humano.
        citada = construir_identidad(tipo_norma, numero, anio)
        resolucion = resolver_identidad(citada, catalogo)
        afectada = resolucion.norma if resolucion.resuelta else None
        if resolucion.nivel == AMBIGUA:
            logger.warning(
                "%s: identidad AMBIGUA para %s -> candidatas: %s. No se vincula.",
                norma["document_key"], citada,
                ", ".join(c.get("document_key", "?") for c in resolucion.candidatas),
            )

        # --- DEDUPLICACION (H-07): clave estable, sin el fragmento.
        clave = clave_dedupe(
            norma["id"], tipo_relacion,
            identidad_para_dedupe(resolucion, citada),
            relacion.get("articulos_afectados"), descripcion,
        )
        if clave in claves_previas:
            logger.info(
                "%s: relacion ya registrada (clave %s). Se omite.",
                norma["document_key"], clave,
            )
            continue
        claves_previas.add(clave)

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
            "clave_dedupe": clave,
            "identidad_nivel": resolucion.nivel,
            "identidad_confianza": resolucion.confianza,
            "identidad_candidatas": ", ".join(
                c.get("document_key", "?") for c in resolucion.candidatas
            ) or None,
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

    marcar_analizada(supabase, norma["id"])
    return insertadas


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20, help="Máximo de normas a analizar en esta corrida")
    parser.add_argument("--document-key", default=None, help="Reanalizar solo esta norma")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reanaliza aunque ya se haya analizado con la version actual",
    )
    args = parser.parse_args()

    load_env()

    deepseek_key = os.getenv("DEEPSEEK_API_KEY")
    if not deepseek_key:
        raise RuntimeError("Falta configurar DEEPSEEK_API_KEY")

    supabase = get_supabase()
    catalogo = cargar_catalogo(supabase)
    normas = normas_pendientes(supabase, args.limit, args.document_key, force=args.force)
    logger.info(
        "Normas a analizar: %d (analyzer v%d%s)",
        len(normas), ANALYZER_VERSION, ", --force" if args.force else "",
    )

    total_relaciones = 0
    for norma in normas:
        try:
            total_relaciones += procesar_norma(supabase, norma, deepseek_key, catalogo)
        except Exception as error:
            logger.error("Error analizando %s: %s", norma.get("document_key"), error)

    logger.info("Listo. Relaciones nuevas detectadas: %d", total_relaciones)


if __name__ == "__main__":
    main()
