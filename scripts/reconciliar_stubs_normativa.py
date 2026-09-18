"""Repunta digemid_norma_relaciones.norma_afectada_id desde un "stub"
duplicado hacia la norma real que representa la misma identidad juridica.

Contexto (H-08, docs/AUDITORIA_RELACIONES_NORMATIVAS.md): antes de la
correccion en supabase/functions/telegram-bot/index.ts
(buscarNormaExistentePorIdentidad), confirmar una relacion cuya norma
afectada no calzaba con el document_key exacto esperado creaba una fila
NUEVA en digemid_normas ("stub") en vez de encontrar la norma real que ya
existia con otro document_key. Eso dejaba relaciones confirmadas apuntando
al stub, invisibles para /consulta sobre la norma real.

Este script SOLO corrige el dato ya duplicado. NO evita que se generen mas
stubs (eso ya lo hace el bot desde la correccion de H-08) y NO borra nada:
- Repunta cada digemid_norma_relaciones.norma_afectada_id que apuntaba al
  stub hacia la norma real.
- Marca el stub con process_status="<algo>_reconciliada" (conserva el
  prefijo "stub" para que scripts/auditar_relaciones_normativas.py lo siga
  clasificando como stub) y anota la norma real en su columna raw.

NUNCA toca estado_vigencia/titulo de la norma REAL. El par
"LEY-29698-ART9" -> "LEY-29698" es un caso de H-09 (afectacion PARCIAL -solo
el articulo 9- modelada como si fuera otra norma completa): repuntar la
relacion es correcto (asi /consulta sobre LEY-29698 ve esa advertencia), pero
la norma real sigue "vigente" a proposito -no se le copia el
estado_vigencia="derogada" del stub, porque eso si seria el bug de H-09.

Uso:
  python scripts/reconciliar_stubs_normativa.py                 # dry-run, pares documentados
  python scripts/reconciliar_stubs_normativa.py --apply          # aplica los pares documentados
  python scripts/reconciliar_stubs_normativa.py --par STUB:REAL --apply
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

NORMAS_TABLE = "digemid_normas"
RELACIONES_TABLE = "digemid_norma_relaciones"

# Pares (stub, norma_real) documentados en docs/AUDITORIA_RELACIONES_NORMATIVAS.md
# seccion H-08. Ampliar esta lista exige la misma verificacion humana que ahi
# se describe: NO es seguro deducir pares nuevos por texto/similitud.
PARES_DOCUMENTADOS = [
    ("NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT", "LEY-29459"),
    ("LEY-29698-ART9", "LEY-29698"),
]


def get_supabase():
    load_dotenv()
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


def _norma_por_document_key(supabase, document_key: str) -> dict | None:
    filas = (
        supabase.table(NORMAS_TABLE)
        .select("id, document_key, process_status, raw")
        .eq("document_key", document_key)
        .limit(1)
        .execute()
        .data
        or []
    )
    return filas[0] if filas else None


def _proximo_process_status(actual: str | None) -> str:
    base = actual or "stub"
    if not base.startswith("stub"):
        # Un document_key que no es realmente un stub (segun su propio
        # process_status) no deberia estar en la lista de pares: no se
        # inventa un prefijo "stub" que no tenia.
        base = f"stub_{base}"
    if "reconciliad" in base:
        return base
    return f"{base}_reconciliada"


def reconciliar_par(supabase, stub_key: str, real_key: str, apply_: bool) -> None:
    stub = _norma_por_document_key(supabase, stub_key)
    real = _norma_por_document_key(supabase, real_key)

    if not stub:
        logger.warning("Stub '%s' no existe (ya reconciliado, o nunca se creo). Nada que hacer.", stub_key)
        return
    if not real:
        logger.error("Norma real '%s' no existe todavia: no se puede reconciliar '%s'.", real_key, stub_key)
        return
    if stub["id"] == real["id"]:
        logger.warning("'%s' y '%s' ya son la misma fila (nada que reconciliar).", stub_key, real_key)
        return

    afectadas = (
        supabase.table(RELACIONES_TABLE)
        .select("id, norma_origen_document_key, tipo_relacion, descripcion_afectada")
        .eq("norma_afectada_id", stub["id"])
        .execute()
        .data
        or []
    )

    logger.info(
        "%s -> %s: %d relacion(es) a repuntar%s",
        stub_key,
        real_key,
        len(afectadas),
        "" if apply_ else " (dry-run, no se aplica nada)",
    )
    for rel in afectadas:
        logger.info(
            "  - %s %s %s (relacion id=%s)",
            rel.get("norma_origen_document_key"),
            rel.get("tipo_relacion"),
            rel.get("descripcion_afectada"),
            rel.get("id"),
        )

    if not apply_:
        return

    for rel in afectadas:
        supabase.table(RELACIONES_TABLE).update({"norma_afectada_id": real["id"]}).eq("id", rel["id"]).execute()

    nuevo_status = _proximo_process_status(stub.get("process_status"))
    nuevo_raw = {**(stub.get("raw") or {}), "reconciliado_con_document_key": real_key}
    supabase.table(NORMAS_TABLE).update({"process_status": nuevo_status, "raw": nuevo_raw}).eq(
        "id", stub["id"]
    ).execute()
    logger.info("  -> aplicado. Stub '%s' marcado como '%s' (sin borrar).", stub_key, nuevo_status)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--par",
        action="append",
        dest="pares",
        metavar="STUB:REAL",
        help="Par document_key_stub:document_key_real a reconciliar. Repetible. "
        "Si se omite, usa los pares documentados en H-08.",
    )
    parser.add_argument("--apply", action="store_true", help="Aplica los cambios (por defecto es dry-run).")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.pares:
        pares = []
        for par in args.pares:
            if ":" not in par:
                raise ValueError(f"--par debe tener la forma STUB:REAL, recibido: {par!r}")
            stub_key, real_key = par.split(":", 1)
            pares.append((stub_key.strip(), real_key.strip()))
    else:
        pares = PARES_DOCUMENTADOS

    supabase = get_supabase()
    for stub_key, real_key in pares:
        reconciliar_par(supabase, stub_key, real_key, args.apply)

    if not args.apply:
        logger.info("Dry-run: nada se escribio. Repetir con --apply para aplicar los cambios de arriba.")


if __name__ == "__main__":
    main()
