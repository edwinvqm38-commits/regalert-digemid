"""F-04-A: manifest del piloto de fidelidad de transcripción. SOLO LECTURA.

    IDENTIDAD NORMATIVA -> PDF CORRECTO -> DOCUMENTO COMPLETO
      -> TRANSCRIPCION FIEL -> INTERPRETACION JURIDICA

Antes de poder preguntar "¿esta transcripción es fiel al PDF?" (F-04) hay que
tener resueltos los tres escalones anteriores para la página concreta que se
va a mirar. Este script NO decide eso por su cuenta: para cada norma corre la
MISMA auditoría F-03 real (`auditar_identidad_documental.analizar_norma`,
que descarga el PDF de Storage y lee sus encabezados) y aplica el gate de
`agents/f04_seleccion_muestra.py` -F-03 aprobado, SHA256 conocido, documento
completo (y, en multinorma, completo DENTRO del rango probado)-.

No escribe nada en Supabase. Solo hace SELECT y descargas de Storage para
poder abrir los PDF con PyMuPDF y leer sus encabezados -exactamente lo mismo
que ya hace `scripts/auditar_identidad_documental.py`-.

Uso:
    python scripts/f04_generar_manifest_piloto.py --out-dir reportes
    python scripts/f04_generar_manifest_piloto.py --solo RM-100-2020,DS-5-2019
    python scripts/f04_generar_manifest_piloto.py --limite-normas 40 --limite-paginas 50
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from auditar_identidad_documental import analizar_norma  # noqa: E402
from f04_seleccion_muestra import (  # noqa: E402
    apto_para_piloto_f04,
    completitud_para_f04,
    diagnostico_ocr_pool,
    fila_manifest,
    pagina_pertenece_a_norma,
    razones_de_riesgo,
    resumen_pool,
    seleccionar_muestra,
    seleccionar_muestra_estratificada,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MANIFEST_BASENAME = "F04_MANIFEST_PILOTO"


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def cargar_normas_y_paginas(supabase, claves: list[str] | None):
    consulta = supabase.table("digemid_normas").select(
        "id, document_key, tipo_norma, numero, anio, source_url, pdf_url, file_storage_path"
    )
    if claves:
        consulta = consulta.in_("document_key", claves)
    normas = [n for n in (consulta.execute().data or []) if n.get("file_storage_path")]

    paginas_por_norma: dict[str, list[dict]] = {}
    desde = 0
    while True:
        lote = (
            supabase.table("digemid_norma_paginas")
            .select("norma_id, page_number, text_normalized, text_raw, extraction_method, "
                    "ocr_used, ocr_confidence, quality_score, has_tables, posible_formula, "
                    "posible_grafico, revisado_manual")
            .range(desde, desde + 999).execute().data or []
        )
        for p in lote:
            paginas_por_norma.setdefault(p["norma_id"], []).append(p)
        if len(lote) < 1000:
            break
        desde += 1000

    return normas, paginas_por_norma


def construir_candidatas(
    supabase, normas: list[dict], paginas_por_norma: dict[str, list[dict]], con_ocr: bool,
) -> tuple[list[dict], list[dict]]:
    """Devuelve (candidatas_manifest, excluidas). `excluidas` documenta POR
    QUÉ cada norma no aportó páginas -para que la exclusión sea auditable, no
    silenciosa-."""
    candidatas: list[dict] = []
    excluidas: list[dict] = []

    for norma in normas:
        stored = paginas_por_norma.get(norma["id"], [])
        if not stored:
            excluidas.append({"document_key": norma["document_key"],
                              "motivo": "sin paginas en digemid_norma_paginas"})
            continue

        numeros = sorted({p["page_number"] for p in stored if p.get("page_number")})
        fila_identidad = analizar_norma(supabase, norma, len(stored), con_ocr)
        completitud_estado, completitud_motivo = completitud_para_f04(fila_identidad, numeros)
        apto, motivo = apto_para_piloto_f04(fila_identidad, completitud_estado)

        logger.info("%s -> F03=%s completitud=%s apto=%s", norma["document_key"],
                    fila_identidad.get("classification"), completitud_estado, apto)

        if not apto:
            excluidas.append({
                "document_key": norma["document_key"],
                "f03_classification": fila_identidad.get("classification"),
                "completitud": completitud_estado,
                "completitud_motivo": completitud_motivo,
                "motivo": motivo,
            })
            continue

        inicio, fin = fila_identidad.get("start_page"), fila_identidad.get("end_page")
        total_paginas = fila_identidad.get("pdf_page_count") or (numeros[-1] if numeros else 0)
        for pagina in stored:
            numero = pagina.get("page_number")
            if numero is None or not pagina_pertenece_a_norma(numero, inicio, fin):
                continue
            razones = razones_de_riesgo(pagina, numero, total_paginas, inicio, fin)
            candidatas.append(fila_manifest(fila_identidad, pagina, razones))

    return candidatas, excluidas


def _resumen_seleccion(seleccion: list[dict], candidatas: list[dict], excluidas: list[dict]) -> dict:
    """Resumen de UNA seleccion final (V1 o V2): ademas de la composicion
    (que ya da `resumen_pool`), documenta cuantas normas quedaron excluidas
    y por que -para que la exclusion sea auditable, no silenciosa-."""
    por_motivo_exclusion: dict[str, int] = {}
    for e in excluidas:
        clave = e.get("f03_classification") or e["motivo"]
        por_motivo_exclusion[clave] = por_motivo_exclusion.get(clave, 0) + 1

    por_extraction_method: dict[str, int] = {}
    for fila in seleccion:
        clave = fila.get("extraction_method") or "SIN_METODO"
        por_extraction_method[clave] = por_extraction_method.get(clave, 0) + 1

    base = resumen_pool(seleccion)
    base.update({
        "normas_excluidas": len(excluidas),
        "por_motivo_exclusion": por_motivo_exclusion,
        "paginas_candidatas_tras_gate_f03": len(candidatas),
        "paginas_seleccionadas": base["total_paginas"],
        "normas_en_muestra": base["normas_distintas"],
        "distribucion_por_metodo_extraccion": dict(
            sorted(por_extraction_method.items(), key=lambda kv: -kv[1])
        ),
        "multinorma_en_muestra": base["por_f03_classification"].get(
            "PDF_CONTIENE_NORMA_EN_MULTINORMA", 0),
        "f03_classification_en_muestra": base["por_f03_classification"],
    })
    return base


def _imprimir_resumen(titulo: str, resumen: dict) -> None:
    print("\n" + "=" * 72)
    print(titulo)
    print("=" * 72)
    for k, v in resumen.items():
        print(f"  {k:34} {v}")
    # Bloque JSON explicito y delimitado: pensado para leerse directamente
    # desde los logs del job de GitHub Actions (get_job_logs), sin depender
    # de poder descargar el artifact -algunos entornos no tienen salida a
    # donde el artifact realmente se almacena-.
    print("\n=== RESUMEN (JSON) ===")
    print(json.dumps(resumen, ensure_ascii=False, indent=2))
    print("=== FIN RESUMEN ===")


def _escribir_csv(ruta: Path, filas: list[dict]) -> None:
    if not filas:
        return
    with ruta.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modo", choices=["manifest", "pool_post_gate", "manifest_v2"], default="manifest",
                        help="manifest: V1, top-N por puntaje de riesgo (comportamiento original). "
                             "pool_post_gate: NO selecciona nada, vuelca el pool COMPLETO que paso "
                             "el gate F-03+F-02+SHA256, para auditar que existe antes de elegir 50. "
                             "manifest_v2: selector estratificado por cuotas (OCR/digital/tablas/"
                             "exacta-multinorma), solo tiene sentido correrlo despues de revisar "
                             "pool_post_gate.")
    parser.add_argument("--out-dir", default="reportes")
    parser.add_argument("--limite-normas", type=int, default=None,
                        help="Maximo de normas a auditar contra F-03 (control de costo/tiempo)")
    parser.add_argument("--limite-paginas", type=int, default=50,
                        help="Tamano objetivo de la muestra final (modos manifest/manifest_v2; "
                             "ignorado en pool_post_gate, que no trunca nada)")
    parser.add_argument("--solo", default="",
                        help="document_key separadas por coma: audita SOLO esas normas")
    parser.add_argument("--con-ocr", action="store_true", default=True,
                        help="Ademas del texto embebido, lee el encabezado del render (300 DPI) "
                             "para resolver F-03 en documentos escaneados. Activado por defecto: "
                             "un PDF escaneado sin OCR de encabezado nunca podria calificar.")
    parser.add_argument("--sin-ocr", dest="con_ocr", action="store_false")
    args = parser.parse_args()

    claves = [k.strip() for k in args.solo.split(",") if k.strip()] or None

    supabase = get_supabase()
    normas, paginas_por_norma = cargar_normas_y_paginas(supabase, claves)
    if args.limite_normas and not claves:
        normas = normas[: args.limite_normas]
    logger.info("Normas con PDF guardado a auditar contra F-03: %d", len(normas))

    inicio = time.time()
    candidatas, excluidas = construir_candidatas(supabase, normas, paginas_por_norma, args.con_ocr)
    duracion = time.time() - inicio

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    if args.modo == "pool_post_gate":
        # Sin seleccionar nada: el pool COMPLETO que ya paso el gate real,
        # para decidir -con datos, no a ciegas- si hace falta un selector
        # estratificado y con que cuotas son realmente alcanzables.
        _escribir_csv(out / "F04_POOL_POST_GATE.csv", candidatas)
        resumen = resumen_pool(candidatas)
        resumen["normas_excluidas"] = len(excluidas)
        resumen["por_motivo_exclusion_f03"] = {
            (e.get("f03_classification") or e["motivo"]): 0 for e in excluidas
        }
        for e in excluidas:
            clave = e.get("f03_classification") or e["motivo"]
            resumen["por_motivo_exclusion_f03"][clave] = resumen["por_motivo_exclusion_f03"].get(clave, 0) + 1
        resumen["diagnostico_ocr"] = diagnostico_ocr_pool(resumen)
        resumen["segundos"] = round(duracion, 1)
        (out / "F04_POOL_POST_GATE.json").write_text(
            json.dumps({"resumen": resumen, "paginas": candidatas, "normas_excluidas": excluidas},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (out / "F04_POOL_POST_GATE_RESUMEN.json").write_text(
            json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        _imprimir_resumen(
            "F-04-A.1 — POOL POST-GATE (SIN SELECCIONAR) — SOLO LECTURA, NADA ESCRITO EN SUPABASE",
            resumen,
        )
        print(f"\nPool completo: {out / 'F04_POOL_POST_GATE.csv'} ({len(candidatas)} paginas)")
        print(f"Resumen:       {out / 'F04_POOL_POST_GATE_RESUMEN.json'}")
        return 0

    if args.modo == "manifest_v2":
        seleccion, diagnostico_seleccion = seleccionar_muestra_estratificada(candidatas, args.limite_paginas)
        basename = "F04_MANIFEST_PILOTO_V2"
        resumen_basename = "F04_RESUMEN_SELECCION_V2"
        titulo = "F-04-A.1 — MANIFEST V2 (ESTRATIFICADO) — SOLO LECTURA, NADA ESCRITO EN SUPABASE"
    else:
        seleccion = seleccionar_muestra(candidatas, args.limite_paginas)
        diagnostico_seleccion = None
        basename = MANIFEST_BASENAME
        resumen_basename = "F04_RESUMEN_SELECCION"
        titulo = "F-04-A — MANIFEST DEL PILOTO — SOLO LECTURA, NADA ESCRITO EN SUPABASE"

    _escribir_csv(out / f"{basename}.csv", seleccion)

    resumen = _resumen_seleccion(seleccion, candidatas, excluidas)
    if diagnostico_seleccion is not None:
        resumen["diagnostico_cuotas"] = diagnostico_seleccion
    resumen["segundos"] = round(duracion, 1)
    (out / f"{basename}.json").write_text(
        json.dumps({"resumen": resumen, "paginas": seleccion, "normas_excluidas": excluidas},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    # Resumen aparte, liviano: para auditar el gate F-03/F-02/SHA256 sin
    # tener que abrir el manifest completo (que trae el texto de cada pagina).
    (out / f"{resumen_basename}.json").write_text(
        json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    _imprimir_resumen(titulo, resumen)
    print(f"\nManifest: {out / f'{basename}.csv'}")
    print(f"Detalle:  {out / f'{basename}.json'} ({len(excluidas)} normas excluidas, con motivo)")
    print(f"Resumen:  {out / f'{resumen_basename}.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
