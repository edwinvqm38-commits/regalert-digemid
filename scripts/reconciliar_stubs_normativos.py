"""Reconciliacion de stubs normativos (H-08). DRY-RUN por defecto.

Un "stub" es un registro minimo que el bot creo solo para dejar constancia de
una norma citada que no estaba en la base. El problema: cuando la norma REAL si
existia, el stub la duplico, y la relacion juridica quedo colgada del duplicado
en vez de la norma real (caso LEY-29459).

Este script NO decide identidad por su cuenta: usa scripts/identidad_normativa.py,
el mismo motor del detector y -por paridad verificada en tests- del bot.

Reglas que respeta:

  * Sin --apply no escribe absolutamente nada.
  * Nunca elige "la primera" candidata: ante varias, requiere humano.
  * Nunca traslada estado_vigencia del stub a la norma real. Una afectacion
    parcial (art. 9 de una ley) no deroga la ley entera.
  * Nunca borra nada: la reconciliacion propuesta deja el stub como alias
    trazable, no lo elimina.
  * Las relaciones rechazadas se preservan como historial.

Uso:
    python scripts/reconciliar_stubs_normativos.py                  # DRY-RUN
    python scripts/reconciliar_stubs_normativos.py --desde-json dir # DRY-RUN offline
    python scripts/reconciliar_stubs_normativos.py --apply          # requiere autorizacion explicita
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from identidad_normativa import (  # noqa: E402
    AMBIGUA,
    CONFIANZA,
    SPEC,
    NIVEL_TIPO_NUMERO,
    DATOS_INSUFICIENTES,
    NIVEL_EXACTA,
    NIVEL_TIPO_NUMERO_ANIO,
    NO_ENCONTRADA,
    clave_dedupe,
    construir_identidad,
    identidad_de_norma,
    normalizar_articulos,
    resolver_identidad,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RECONCILIADOR_VERSION = 1

# --- Clasificacion de cada stub ------------------------------------------
MATCH_EXACTO_UNICO = "MATCH_EXACTO_UNICO"
MATCH_PROBABLE_REQUIERE_HUMANO = "MATCH_PROBABLE_REQUIERE_HUMANO"
IDENTIDAD_AMBIGUA = "IDENTIDAD_AMBIGUA"
SIN_NORMA_REAL = "SIN_NORMA_REAL"
STUB_UNIDAD_PARCIAL = "STUB_QUE_REPRESENTA_UNIDAD_PARCIAL"
NO_ES_STUB_REAL = "NO_ES_STUB_REAL"

# process_status de normas REALES a las que solo les falta el documento. No son
# stubs: tienen identidad propia y pueden ser destino legitimo de una relacion.
ESTADOS_INCOMPLETOS = {"pdf_download_error", "inventory_imported", "text_extraction_error"}

# Tipos de relacion que SI alteran el estado de la norma afectada, y con que
# valor. Debe coincidir con ESTADO_VIGENCIA_POR_RELACION del bot.
VIGENCIA_POR_RELACION = {
    "deroga": "derogada",
    "deja_sin_efecto": "derogada",
    "modifica": "modificada",
    "sustituye": "modificada",
    "incorpora": "modificada",
    "suspende": "suspendida",
    "exonera": None,
    "prorroga": None,
    "pendiente_verificacion": None,
}


def es_stub(fila: dict) -> bool:
    """Stub DECLARADO. Se acepta cualquier variante historica de process_status
    que empiece por 'stub', y la convencion de clave 'NORM-' que usaba el bot
    cuando no podia construir un document_key con tipo+numero+año."""
    estado = str(fila.get("process_status") or "")
    return estado.startswith("stub") or str(fila.get("document_key") or "").startswith("NORM-")


# Tipos cuya numeracion es unica a nivel nacional: "Ley 29459" identifica una
# sola norma aunque la cita no traiga año. Para DS/RM/RD el numero se reinicia
# cada año, asi que sin año NO hay identidad suficiente.
TIPOS_NUMERO_UNICO = set(SPEC.get("tipos_numero_unico_nacional", []))

# El titulo del stub empieza describiendo una PARTE, no una norma.
PATRON_UNIDAD_EN_TITULO = re.compile(
    r"^\s*(art[ií]culos?|numerales?|incisos?|literales?|anexos?|"
    r"disposici[oó]n(?:es)?\s+(?:complementaria|transitoria|final))\b",
    re.IGNORECASE,
)


def representa_unidad_parcial(stub: dict) -> bool:
    """El stub no representa una NORMA sino una PARTE de una norma.

    Es una propiedad del STUB (su clave y su titulo), NO de la relacion: que
    una relacion sea parcial es normal -"exonera de los articulos 10 y 11 de
    la Ley 29459" afecta parcialmente a una norma que existe entera-. Lo que
    impide reconciliar es que el registro creado sea el "articulo 9" en vez de
    "la Ley 29698": fusionarlo trasladaria a toda la ley un efecto que solo
    alcanza a esa parte.
    """
    if re.search(r"-ART\.?\d", str(stub.get("document_key") or ""), re.IGNORECASE):
        return True
    return bool(PATRON_UNIDAD_EN_TITULO.match(str(stub.get("titulo") or "")))


def clasificar(stub: dict, relaciones: list[dict], catalogo: list[dict]) -> tuple[str, object, str]:
    """Devuelve (clasificacion, resultado_identidad, motivo)."""
    # Un stub nunca puede reconciliarse contra otro stub.
    reales = [f for f in catalogo if not es_stub(f) and f["id"] != stub["id"]]
    resolucion = resolver_identidad(identidad_de_norma(stub), reales)

    if resolucion.nivel == DATOS_INSUFICIENTES:
        return SIN_NORMA_REAL, resolucion, "el stub no tiene numero: no hay identidad que reconciliar"

    if resolucion.nivel == NO_ENCONTRADA:
        return SIN_NORMA_REAL, resolucion, "no existe una norma real con esa identidad: el stub es legitimo"

    if resolucion.nivel == AMBIGUA:
        return IDENTIDAD_AMBIGUA, resolucion, "varias normas reales comparten la identidad del stub"

    if representa_unidad_parcial(stub):
        return (
            STUB_UNIDAD_PARCIAL,
            resolucion,
            "el stub representa una PARTE de la norma (articulo/numeral), no la norma entera: "
            f"apunta a {resolucion.norma['document_key'] if resolucion.norma else '?'} pero "
            "fusionarlo trasladaria a toda la norma un efecto parcial (H-09)",
        )

    identidad_completa = resolucion.nivel in (NIVEL_EXACTA, NIVEL_TIPO_NUMERO_ANIO) or (
        # Para una Ley, tipo+numero YA es la identidad completa.
        resolucion.nivel == NIVEL_TIPO_NUMERO
        and identidad_de_norma(stub).tipo in TIPOS_NUMERO_UNICO
    )
    if identidad_completa:
        return MATCH_EXACTO_UNICO, resolucion, f"norma real unica ({resolucion.nivel})"

    return (
        MATCH_PROBABLE_REQUIERE_HUMANO,
        resolucion,
        f"norma real unica pero por identidad incompleta ({resolucion.nivel}): confirmar a mano",
    )


def accion_sobre_vigencia(relacion: dict, stub: dict, real: dict | None) -> tuple[str, str]:
    """NUNCA se traslada la vigencia del stub. Se evalua de cero, y ante
    cualquier duda se deja en manos de un humano."""
    tipo = relacion.get("tipo_relacion")
    parcial = bool(relacion.get("articulos_afectados")) or relacion.get("alcance") == "parcial"
    esperado = VIGENCIA_POR_RELACION.get(tipo, "DESCONOCIDO")

    if parcial:
        return "no_tocar", (
            f"afectacion parcial ({relacion.get('articulos_afectados') or 'alcance parcial'}): "
            "no cambia el estado de la norma completa"
        )
    if esperado == "DESCONOCIDO":
        return "requiere_humano", f"tipo_relacion desconocido: {tipo}"
    if esperado is None:
        return "no_tocar", f"'{tipo}' no altera la vigencia de la norma citada"
    if real is None:
        return "no_tocar", "no hay norma real a la que aplicar vigencia"
    if real.get("estado_vigencia") == esperado:
        return "no_tocar", f"la norma real ya esta '{esperado}'"
    return "requiere_humano", (
        f"la relacion es '{tipo}' TOTAL y la norma real esta '{real.get('estado_vigencia')}'; "
        f"pasar a '{esperado}' es una decision juridica, no automatica"
    )


def cargar_desde_supabase() -> tuple[list[dict], list[dict]]:
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    supabase = create_client(url, key)

    normas = (
        supabase.table("digemid_normas")
        .select("id, document_key, tipo_norma, numero, anio, titulo, estado_vigencia, process_status")
        .execute().data or []
    )
    relaciones = supabase.table("digemid_norma_relaciones").select("*").execute().data or []
    return normas, relaciones


def cargar_desde_json(directorio: str) -> tuple[list[dict], list[dict]]:
    """Snapshot de solo lectura, para auditar sin credenciales y para poder
    reproducir despues exactamente el mismo DRY-RUN."""
    base = Path(directorio)
    normas = json.loads((base / "normas.json").read_text(encoding="utf-8"))
    relaciones = json.loads((base / "relaciones.json").read_text(encoding="utf-8"))
    return normas, relaciones


def analizar(normas: list[dict], relaciones: list[dict]) -> tuple[list[dict], list[dict]]:
    por_id = {n["id"]: n for n in normas}
    rel_por_afectada: dict[str, list[dict]] = {}
    for r in relaciones:
        if r.get("norma_afectada_id"):
            rel_por_afectada.setdefault(r["norma_afectada_id"], []).append(r)

    filas, sospechosos = [], []

    for norma in normas:
        if not es_stub(norma):
            # Normas INCOMPLETAS que no son stubs: se inventarian para que el
            # humano las vea, pero no se tocan. Son normas reales, identificadas
            # y citables; lo unico que les falta es el PDF. Confundirlas con
            # stubs seria el error inverso al que persigue H-08.
            if (norma.get("process_status") or "") in ESTADOS_INCOMPLETOS:
                sospechosos.append({
                    "stub_id": norma["id"],
                    "stub_key": norma["document_key"],
                    "stub_titulo": (norma.get("titulo") or "")[:120],
                    "estado_vigencia_stub": norma.get("estado_vigencia"),
                    "clasificacion": NO_ES_STUB_REAL,
                    "relaciones_entrantes": sum(
                        1 for r in relaciones if r.get("norma_afectada_id") == norma["id"]
                    ),
                    "accion_sobre_stub": "no tocar: norma real sin PDF, no es un stub",
                    "motivo": f"process_status={norma.get('process_status')}",
                    "requiere_humano": "no",
                })
            continue

        rels = rel_por_afectada.get(norma["id"], [])
        clasificacion, resolucion, motivo = clasificar(norma, rels, normas)
        real = resolucion.norma if getattr(resolucion, "resuelta", False) else None

        if not rels:
            filas.append(_fila(norma, None, clasificacion, resolucion, real, motivo))
            continue

        for rel in rels:
            filas.append(_fila(norma, rel, clasificacion, resolucion, real, motivo))

    return filas, sospechosos


def _fila(stub, rel, clasificacion, resolucion, real, motivo) -> dict:
    rel = rel or {}
    accion_vig, motivo_vig = accion_sobre_vigencia(rel, stub, real)

    reconciliable = clasificacion == MATCH_EXACTO_UNICO and rel.get("estado") != "rechazada"
    requiere_humano = not reconciliable

    if clasificacion == SIN_NORMA_REAL:
        accion_stub = "conservar (es la unica constancia de esa norma)"
    elif clasificacion == MATCH_EXACTO_UNICO:
        accion_stub = "marcar como alias reconciliado (NO borrar)"
    elif clasificacion == STUB_UNIDAD_PARCIAL:
        accion_stub = "conservar hasta modelar afectaciones parciales (H-09)"
    else:
        accion_stub = "no tocar: decision humana"

    return {
        # --- ANTES ---
        "relacion_id": rel.get("id"),
        "stub_id": stub["id"],
        "stub_key": stub["document_key"],
        "stub_titulo": (stub.get("titulo") or "")[:120],
        "norma_real_id": real["id"] if real else None,
        "norma_real_key": real["document_key"] if real else None,
        "estado_relacion": rel.get("estado"),
        "tipo_relacion": rel.get("tipo_relacion"),
        "norma_origen": rel.get("norma_origen_document_key"),
        "articulos_afectados": rel.get("articulos_afectados"),
        "alcance": rel.get("alcance"),
        "fragmento_verificado": rel.get("fragmento_verificado"),
        "estado_vigencia_stub": stub.get("estado_vigencia"),
        "estado_vigencia_real": real.get("estado_vigencia") if real else None,
        # --- PROPUESTA ---
        "clasificacion": clasificacion,
        "nuevo_norma_afectada_id": real["id"] if reconciliable else None,
        "accion_sobre_stub": accion_stub,
        "accion_sobre_vigencia": accion_vig,
        "motivo": f"{motivo}; vigencia: {motivo_vig}",
        "confianza": CONFIANZA.get(resolucion.nivel, "nula"),
        "requiere_humano": "si" if requiere_humano else "no",
        "candidatas": " | ".join(c["document_key"] for c in resolucion.candidatas),
        "nivel_identidad": resolucion.nivel,
    }


def colisiones_dedupe(filas: list[dict], normas: list[dict], relaciones: list[dict]) -> list[dict]:
    """Que pasaria con clave_dedupe DESPUES de reconciliar identidades.

    No se rellena nada: se simula. Interesa saber si dos relaciones del mismo
    origen convergerian a la misma clave (duplicado real) o si solo se parecen
    pero afectan articulos distintos (NO deben fusionarse).
    """
    nuevo_destino = {
        f["relacion_id"]: f["nuevo_norma_afectada_id"]
        for f in filas
        if f.get("relacion_id") and f.get("nuevo_norma_afectada_id")
    }
    por_id = {n["id"]: n for n in normas}

    claves: dict[str, list[dict]] = {}
    for rel in relaciones:
        destino_id = nuevo_destino.get(rel["id"], rel.get("norma_afectada_id"))
        destino = por_id.get(destino_id) if destino_id else None
        identidad = (
            identidad_de_norma(destino) if destino
            else construir_identidad(
                rel.get("tipo_norma_afectada"), rel.get("numero_afectada"), rel.get("anio_afectada")
            )
        )
        clave = clave_dedupe(
            rel["norma_origen_id"], rel.get("tipo_relacion"), identidad,
            rel.get("articulos_afectados"), rel.get("descripcion_afectada"),
        )
        claves.setdefault(clave, []).append(rel)

    colisiones = []
    for clave, rels in claves.items():
        if len(rels) < 2:
            continue
        # Se comparan las unidades NORMALIZADAS, no la redaccion: "articulos 10
        # y 11" y "arts. 10 y 11" son las mismas unidades.
        articulos = {normalizar_articulos(r.get("articulos_afectados")) for r in rels}
        colisiones.append({
            "clave_dedupe": clave,
            "relaciones": ", ".join(r["id"] for r in rels),
            "origen": rels[0].get("norma_origen_document_key"),
            "tipo_relacion": rels[0].get("tipo_relacion"),
            "estados": ", ".join(sorted({r.get("estado", "?") for r in rels})),
            "articulos_distintos": "si" if len(articulos) > 1 else "no",
            "veredicto": (
                "REVISAR: misma clave pero articulos distintos"
                if len(articulos) > 1 else "duplicado real: convergen tras reconciliar"
            ),
        })
    return colisiones


def escribir(filas: list[dict], colisiones: list[dict], sospechosos: list[dict], out_dir: str) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "MATRIZ_RECONCILIACION_STUBS.csv"
    if filas:
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
            w.writeheader()
            w.writerows(filas)
    (out / "MATRIZ_RECONCILIACION_STUBS.json").write_text(
        json.dumps(
            {"stubs": filas, "colisiones_dedupe": colisiones, "no_son_stubs": sospechosos},
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )
    return csv_path


def imprimir(filas: list[dict], colisiones: list[dict], sospechosos: list[dict], aplicar: bool) -> None:
    print("\n" + "=" * 74)
    print("RECONCILIACION DE STUBS — " + ("APLICANDO" if aplicar else "DRY-RUN (no se escribe nada)"))
    print("=" * 74)

    for f in filas:
        print(f"\n■ {f['stub_key']}  [{f['clasificacion']}]  confianza={f['confianza']}")
        print(f"    ANTES     stub_id={f['stub_id']}  vigencia_stub={f['estado_vigencia_stub']}")
        print(f"              relacion={f['relacion_id']} ({f['estado_relacion']}, {f['tipo_relacion']}) "
              f"origen={f['norma_origen']}")
        print(f"              articulos={f['articulos_afectados']} alcance={f['alcance']} "
              f"cita_verificada={f['fragmento_verificado']}")
        print(f"              norma_real={f['norma_real_key']} vigencia_real={f['estado_vigencia_real']}")
        print(f"    PROPUESTA nuevo_norma_afectada_id={f['nuevo_norma_afectada_id']}")
        print(f"              stub: {f['accion_sobre_stub']}")
        print(f"              vigencia: {f['accion_sobre_vigencia']}")
        print(f"              requiere_humano={f['requiere_humano']}")
        print(f"              motivo: {f['motivo']}")
        if f["candidatas"]:
            print(f"              candidatas: {f['candidatas']}")

    print("\n" + "-" * 74)
    print(f"COLISIONES DE clave_dedupe tras reconciliar: {len(colisiones)}")
    for c in colisiones:
        print(f"    {c['origen']} [{c['tipo_relacion']}] {c['veredicto']}")
        print(f"      relaciones: {c['relaciones']}  estados: {c['estados']}")

    print("\n" + "-" * 74)
    print(f"NORMAS INCOMPLETAS QUE NO SON STUBS (no se tocan): {len(sospechosos)}")
    con_relaciones = [s for s in sospechosos if s["relaciones_entrantes"]]
    for s in con_relaciones:
        print(f"    {s['stub_key']}  ({s['motivo']})  relaciones_entrantes={s['relaciones_entrantes']}")
    if len(sospechosos) > len(con_relaciones):
        print(f"    ... y {len(sospechosos) - len(con_relaciones)} mas sin relaciones entrantes")

    total = len(filas)
    auto = sum(1 for f in filas if f["requiere_humano"] == "no")
    print("\n" + "-" * 74)
    print(f"Filas: {total} | reconciliables automaticamente: {auto} | requieren humano: {total - auto}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--dry-run", action="store_true", default=True, help="(por defecto) no escribe nada")
    grupo.add_argument("--apply", action="store_true", help="aplica los cambios propuestos")
    parser.add_argument("--desde-json", default=None, help="Directorio con normas.json y relaciones.json")
    parser.add_argument("--out-dir", default="reportes")
    args = parser.parse_args()

    normas, relaciones = (
        cargar_desde_json(args.desde_json) if args.desde_json else cargar_desde_supabase()
    )
    logger.info("Normas: %d | Relaciones: %d", len(normas), len(relaciones))

    filas, sospechosos = analizar(normas, relaciones)
    colisiones = colisiones_dedupe(filas, normas, relaciones)
    csv_path = escribir(filas, colisiones, sospechosos, args.out_dir)
    imprimir(filas, colisiones, sospechosos, args.apply)

    if not args.apply:
        print(f"\nDRY-RUN: no se modifico nada. Matriz: {csv_path}")
        return 0

    raise SystemExit(
        "--apply todavia no esta habilitado: la fase actual termina en DRY-RUN "
        "y los cambios de datos juridicos requieren autorizacion explicita."
    )


if __name__ == "__main__":
    sys.exit(main())
