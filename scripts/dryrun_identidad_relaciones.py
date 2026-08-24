"""DRY-RUN de la capa de identidad canonica sobre las relaciones existentes.

SOLO LECTURA: no modifica ninguna relacion, ningun estado_vigencia y ningun
stub. Aplica el resolvedor nuevo (scripts/identidad_normativa.py) a TODAS las
relaciones -incluidas las confirmadas- y produce la matriz comparativa
"identidad actual vs identidad esperada".

Uso:
    python scripts/dryrun_identidad_relaciones.py --out-dir reportes/
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent))
from identidad_normativa import (  # noqa: E402
    AMBIGUA,
    DATOS_INSUFICIENTES,
    NO_ENCONTRADA,
    clave_dedupe,
    construir_identidad,
    resolver_identidad,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_supabase():
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="reportes")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_supabase()

    catalogo = (
        supabase.table("digemid_normas")
        .select("id, document_key, tipo_norma, numero, anio, process_status, estado_vigencia")
        .execute()
        .data
        or []
    )
    relaciones = supabase.table("digemid_norma_relaciones").select("*").execute().data or []
    por_id = {n["id"]: n for n in catalogo}
    logger.info("Catalogo: %d normas | Relaciones: %d", len(catalogo), len(relaciones))

    filas, claves_vistas, duplicados = [], {}, []

    for rel in relaciones:
        citada = construir_identidad(
            rel.get("tipo_norma_afectada"), rel.get("numero_afectada"), rel.get("anio_afectada")
        )
        res = resolver_identidad(citada, catalogo)

        actual = por_id.get(rel.get("norma_afectada_id"))
        actual_key = actual["document_key"] if actual else None
        propuesta_key = res.norma["document_key"] if res.resuelta else None

        # Discrepancia: la relacion YA apunta a una norma distinta de la que el
        # resolvedor nuevo elegiria. No se corrige aqui.
        discrepancia = bool(actual_key and propuesta_key and actual_key != propuesta_key)

        if rel.get("norma_afectada_id") is None:
            if res.resuelta:
                accion = f"proponer vinculo -> {propuesta_key}"
            elif res.nivel == AMBIGUA:
                accion = "revision humana: varias candidatas"
            elif res.nivel == NO_ENCONTRADA:
                accion = "norma no esta en la base (posible stub futuro)"
            else:
                accion = "datos insuficientes en la cita"
        elif discrepancia:
            accion = "DISCREPANCIA_IDENTIDAD: revisar antes de tocar"
        else:
            accion = "sin cambios"

        clave = clave_dedupe(
            rel.get("norma_origen_id"), rel.get("tipo_relacion"),
            res.norma and construir_identidad(
                res.norma.get("tipo_norma"), res.norma.get("numero"), res.norma.get("anio")
            ) or citada,
            rel.get("articulos_afectados"), rel.get("descripcion_afectada"),
        )
        if clave in claves_vistas:
            duplicados.append((claves_vistas[clave], rel["id"], clave))
        else:
            claves_vistas[clave] = rel["id"]

        filas.append({
            "relacion_id": rel["id"],
            "origen": rel.get("norma_origen_document_key"),
            "tipo_relacion": rel.get("tipo_relacion"),
            "estado": rel.get("estado"),
            "descripcion_actual": (rel.get("descripcion_afectada") or "")[:160],
            "tipo_citado": rel.get("tipo_norma_afectada"),
            "numero_citado": rel.get("numero_afectada"),
            "anio_citado": rel.get("anio_afectada"),
            "identidad_citada": str(citada),
            "candidata_actual": actual_key,
            "candidatas_encontradas": " | ".join(
                c["document_key"] for c in res.candidatas
            ) or (propuesta_key or ""),
            "identidad_propuesta": propuesta_key,
            "nivel_resolucion": res.nivel,
            "confianza": res.confianza,
            "discrepancia_identidad": discrepancia,
            "accion_propuesta": accion,
            "clave_dedupe": clave,
        })

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "MATRIZ_IDENTIDAD_RELACIONES.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)
    (out / "MATRIZ_IDENTIDAD_RELACIONES.json").write_text(
        json.dumps(filas, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    sin_resolver = [f for f in filas if f["candidata_actual"] is None]
    confirmadas = [f for f in filas if f["estado"] == "confirmada"]

    def cuenta(subset):
        r = {}
        for f in subset:
            r[f["nivel_resolucion"]] = r.get(f["nivel_resolucion"], 0) + 1
        return r

    print("\n" + "=" * 66)
    print("DRY-RUN DE IDENTIDAD — SOLO LECTURA, PRODUCCION INTACTA")
    print("=" * 66)
    print(f"\nA) Relaciones SIN norma_afectada_id: {len(sin_resolver)}")
    for k, v in sorted(cuenta(sin_resolver).items(), key=lambda kv: -kv[1]):
        print(f"     {v:3d}  {k}")
    resolubles = sum(1 for f in sin_resolver if f["identidad_propuesta"])
    print(f"   -> resolubles automaticamente con seguridad: {resolubles}/{len(sin_resolver)}")

    print(f"\nB) Relaciones CONFIRMADAS auditadas: {len(confirmadas)}")
    disc = [f for f in confirmadas if f["discrepancia_identidad"]]
    print(f"   -> DISCREPANCIA_IDENTIDAD: {len(disc)}")
    for f in disc:
        print(f"      {f['origen']} {f['tipo_relacion']}: "
              f"actual={f['candidata_actual']} vs propuesta={f['identidad_propuesta']}")

    ambiguas = [f for f in filas if f["nivel_resolucion"] == AMBIGUA]
    print(f"\nC) Casos AMBIGUOS (requieren humano): {len(ambiguas)}")
    for f in ambiguas[:15]:
        print(f"      {f['origen']} -> {f['identidad_citada']}  candidatas: {f['candidatas_encontradas']}")

    print(f"\nD) Duplicados por clave estable: {len(duplicados)}")
    for a, b, k in duplicados[:10]:
        print(f"      {a} == {b}   clave={k[:70]}")

    print(f"\nMatriz: {csv_path}")


if __name__ == "__main__":
    main()
