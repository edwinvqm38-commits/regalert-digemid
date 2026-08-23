"""Auditoría SOLO LECTURA de digemid_norma_relaciones.

Genera la MATRIZ_AUDITORIA_RELACIONES (CSV + JSON) clasificando cada relación
-pendiente, confirmada o rechazada- sin modificar absolutamente nada en
produccion. Es la base del DRY-RUN de reauditoria: primero se mira, despues se
decide.

Uso:
    python scripts/auditar_relaciones_normativas.py --out-dir reportes/

No recibe credenciales por argumento: usa SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY
del entorno, igual que el resto de scripts del repo.
"""

import argparse
import csv
import json
import logging
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Ventana que usa hoy el detector. Se replica aqui para poder medir cuantas
# relaciones se decidieron sobre texto amputado (hallazgo H-01).
MAX_CHARS_TEXTO = 15000

VERBOS_DISPOSITIVOS = re.compile(
    r"(der[oó]gu?[ea]se|der[oó]gase|derogar|modif[ií]case|modificar|sustit[uú]yase|"
    r"incorp[oó]rase|d[eé]jese sin efecto|susp[eé]ndase|prorr[oó]gase|except[uú]ase|exon[eé]rase)",
    re.IGNORECASE,
)

TIPOS_CANONICOS = {
    "resolucion ministerial": "RM",
    "resolucion directoral": "RD",
    "resolucion suprema": "RS",
    "resolucion viceministerial": "RVM",
    "resolucion jefatural": "RJ",
    "decreto supremo": "DS",
    "decreto legislativo": "DL",
    "decreto de urgencia": "DU",
    "ley": "LEY",
}


def normalizar_tipo_norma(valor) -> str | None:
    """Normalizacion canonica unica (hallazgo H-05). Acepta tanto la forma
    larga ("Decreto Supremo") como la abreviatura ("DS")."""
    if not valor:
        return None
    bruto = str(valor).strip()
    sin_acentos = bruto.translate(str.maketrans("áéíóúÁÉÍÓÚ", "aeiouAEIOU"))
    plano = re.sub(r"\s+", " ", sin_acentos).strip().lower()
    if plano in TIPOS_CANONICOS:
        return TIPOS_CANONICOS[plano]
    corto = plano.upper().replace(".", "")
    return corto if corto in set(TIPOS_CANONICOS.values()) else bruto.upper()


def normalizar_numero(numero) -> str | None:
    if not numero:
        return None
    m = re.search(r"\d+", str(numero))
    return str(int(m.group())) if m else None


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def cargar(supabase):
    relaciones = supabase.table("digemid_norma_relaciones").select("*").execute().data or []
    normas = (
        supabase.table("digemid_normas")
        .select("id, document_key, tipo_norma, numero, anio, estado_vigencia, process_status")
        .execute()
        .data
        or []
    )
    paginas = (
        supabase.table("digemid_norma_paginas")
        .select("norma_id, page_number, text_normalized, text_raw")
        .execute()
        .data
        or []
    )
    return relaciones, normas, paginas


def indexar_texto(paginas: list[dict]) -> dict[str, dict]:
    """Devuelve, por norma, el texto completo y el prefijo que realmente vio el
    detector, para poder distinguir evidencia analizada de evidencia amputada."""
    por_norma: dict[str, list[tuple[int, str]]] = {}
    for p in paginas:
        texto = (p.get("text_normalized") or p.get("text_raw") or "").strip()
        if texto:
            por_norma.setdefault(p["norma_id"], []).append((p.get("page_number") or 0, texto))

    indice = {}
    for norma_id, pags in por_norma.items():
        completo = "\n\n".join(t for _, t in sorted(pags))
        indice[norma_id] = {
            "completo": completo,
            "analizado": completo[:MAX_CHARS_TEXTO],
            "chars": len(completo),
        }
    return indice


def normaliza_espacios(t: str) -> str:
    return re.sub(r"\s+", " ", t).strip().lower()


def clasificar(rel, normas_por_id, normas_por_key, indice) -> tuple[str, str, str]:
    """Devuelve (resultado_auditoria, motivo, confianza)."""
    motivos = []
    origen = normas_por_key.get(rel.get("norma_origen_document_key"))
    texto = indice.get(origen["id"]) if origen else None

    # ¿La cita existe en el documento? ¿Estaba dentro de la ventana analizada?
    frag = (rel.get("fragmento_fuente") or "").split(" [RECHAZADO")[0]
    fuera_de_ventana = False
    if texto and frag:
        f = normaliza_espacios(frag)
        en_completo = f in normaliza_espacios(texto["completo"])
        en_analizado = f in normaliza_espacios(texto["analizado"])
        if en_completo and not en_analizado:
            fuera_de_ventana = True
        if not en_completo and rel.get("fragmento_verificado"):
            motivos.append("fragmento_verificado=true pero la cita no aparece en el texto")

    if texto and texto["chars"] > MAX_CHARS_TEXTO:
        motivos.append(f"norma origen truncada ({texto['chars']} chars > {MAX_CHARS_TEXTO})")

    afectada = normas_por_id.get(rel.get("norma_afectada_id"))

    # Identidad
    if not rel.get("norma_afectada_id"):
        motivos.append("sin norma_afectada_id (identidad no resuelta)")
        resultado = "IDENTIDAD_AMBIGUA"
    elif afectada and (afectada.get("process_status") or "").startswith("stub"):
        motivos.append(f"apunta a stub {afectada.get('document_key')}")
        resultado = "CORRECTA_PERO_DATOS_INCOMPLETOS"
    else:
        resultado = "CORRECTA_VERIFICADA"

    # Afectación parcial tratada como total (H-09)
    parcial = bool(rel.get("articulos_afectados")) or rel.get("alcance") == "parcial"
    if (
        parcial
        and rel.get("tipo_relacion") in ("deroga", "deja_sin_efecto")
        and afectada
        and afectada.get("estado_vigencia") == "derogada"
    ):
        motivos.append("afectacion PARCIAL pero la norma quedo DEROGADA total")
        resultado = "ALCANCE_INCORRECTO"

    # Cita no verificable
    if not rel.get("fragmento_verificado"):
        motivos.append("fragmento no verificado contra el texto")
        if resultado == "CORRECTA_VERIFICADA":
            resultado = "FRAGMENTO_NO_VERIFICADO"

    # Verbo dispositivo ausente en la cita -> posible considerando (H-01/PR#65)
    if frag and not VERBOS_DISPOSITIVOS.search(frag):
        motivos.append("la cita no contiene verbo dispositivo (posible considerando)")
        resultado = "REQUIERE_REVISION_HUMANA"

    if fuera_de_ventana:
        motivos.append("la cita esta FUERA de la ventana analizada (no pudo sustentar la decision)")
        resultado = "REQUIERE_REVISION_HUMANA"

    confianza = "alta" if resultado == "CORRECTA_VERIFICADA" else (
        "baja" if resultado in ("IDENTIDAD_AMBIGUA", "REQUIERE_REVISION_HUMANA", "ALCANCE_INCORRECTO") else "media"
    )
    return resultado, "; ".join(motivos) or "sin observaciones", confianza


ACCIONES = {
    "CORRECTA_VERIFICADA": "mantener",
    "CORRECTA_PERO_DATOS_INCOMPLETOS": "reconciliar stub con norma real",
    "IDENTIDAD_AMBIGUA": "resolver identidad antes de confirmar",
    "FRAGMENTO_NO_VERIFICADO": "reverificar cita contra texto completo",
    "ALCANCE_INCORRECTO": "corregir estado global (parcial != total)",
    "REQUIERE_REVISION_HUMANA": "revision juridica con fuente oficial",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="reportes", help="Directorio de salida")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_supabase()

    relaciones, normas, paginas = cargar(supabase)
    logger.info("Relaciones: %d | Normas: %d | Paginas: %d", len(relaciones), len(normas), len(paginas))

    normas_por_id = {n["id"]: n for n in normas}
    normas_por_key = {n["document_key"]: n for n in normas}
    indice = indexar_texto(paginas)

    filas = []
    for rel in relaciones:
        resultado, motivo, confianza = clasificar(rel, normas_por_id, normas_por_key, indice)
        afectada = normas_por_id.get(rel.get("norma_afectada_id")) or {}
        filas.append(
            {
                "id": rel["id"],
                "norma_origen": rel.get("norma_origen_document_key"),
                "tipo_relacion": rel.get("tipo_relacion"),
                "norma_afectada": afectada.get("document_key"),
                "tipo_norma_afectada": rel.get("tipo_norma_afectada"),
                "tipo_norma_canonico": normalizar_tipo_norma(rel.get("tipo_norma_afectada")),
                "numero": rel.get("numero_afectada"),
                "numero_canonico": normalizar_numero(rel.get("numero_afectada")),
                "anio": rel.get("anio_afectada"),
                "articulos_afectados": rel.get("articulos_afectados"),
                "alcance": rel.get("alcance"),
                "fragmento_verificado": rel.get("fragmento_verificado"),
                "estado_actual": rel.get("estado"),
                "norma_afectada_id": rel.get("norma_afectada_id"),
                "estado_vigencia_actual": afectada.get("estado_vigencia"),
                "resultado_de_auditoria": resultado,
                "motivo": motivo,
                "confianza": confianza,
                "accion_recomendada": ACCIONES.get(resultado, "revisar"),
                "fragmento_fuente": (rel.get("fragmento_fuente") or "")[:300],
            }
        )

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_path, json_path = out / "MATRIZ_AUDITORIA_RELACIONES.csv", out / "MATRIZ_AUDITORIA_RELACIONES.json"

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(filas[0].keys()) if filas else [])
        writer.writeheader()
        writer.writerows(filas)
    json_path.write_text(json.dumps(filas, ensure_ascii=False, indent=2), encoding="utf-8")

    resumen: dict[str, int] = {}
    for f in filas:
        resumen[f["resultado_de_auditoria"]] = resumen.get(f["resultado_de_auditoria"], 0) + 1

    print("\n=== RESUMEN DE AUDITORIA (solo lectura, nada fue modificado) ===")
    for k, v in sorted(resumen.items(), key=lambda kv: -kv[1]):
        print(f"  {v:4d}  {k}")
    print(f"\nMatriz escrita en:\n  {csv_path}\n  {json_path}")


if __name__ == "__main__":
    main()
