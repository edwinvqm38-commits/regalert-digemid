"""F-04-B.1: paquete visual autocontenido para la revisión humana (golden).

SOLO LECTURA. Dados el Manifest V2 YA CONGELADO y la comparación de motores
YA GENERADA (ambos de corridas anteriores -no se regenera ni se vuelve a
correr ninguna comparación aquí-), descarga cada PDF una sola vez (con caché
por documento), verifica que su SHA256 sigue siendo EXACTAMENTE el que el
gate F-03 congeló en el manifest, y renderiza -solo si el SHA coincide- la
página exacta seleccionada a PNG 300 DPI, con nombre determinista.

Arma un paquete autocontenido en `--out-dir` con: las imágenes, un índice
que enlaza cada (document_key, page_number) con su imagen y su SHA256, una
copia de `herramientas/f04b_revision_humana.html`, copias de los dos JSON de
entrada, y un README con instrucciones. Todo pensado para funcionar sin
internet una vez descomprimido.

Un SHA256 que no coincide con el manifest congelado NUNCA se sustituye en
silencio: esa página se excluye del render y queda registrada como
GOLDEN_PDF_SHA_MISMATCH en el resumen.

No escribe nada en Supabase, no usa ningún modelo de pago.

Uso:
    python scripts/f04b_generar_paquete_golden.py \
        --manifest reportes/F04_MANIFEST_PILOTO_V2.json \
        --comparacion reportes/F04_COMPARACION_MOTORES.json \
        --out-dir reportes/golden_package
"""

import argparse
import hashlib
import io
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from f04b_paquete_golden import (  # noqa: E402
    ERROR_RENDER,
    GOLDEN_PDF_SHA_MISMATCH,
    PAGINA_FUERA_DEL_PDF,
    RENDERIZADO_OK,
    construir_plan_render,
    resumen_control_calidad,
    sha_coincide,
)
from piloto_verificacion_paginas import DPI_RENDER, descargar_pdf  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DOCUMENTOS_REQUERIDOS = [("RM-250-2019", 1), ("DS-5-2019", 1), ("RM-862-2019", 1)]
OCR_ESPERADAS = 12
HERRAMIENTA_HTML = RAIZ / "herramientas" / "f04b_revision_humana.html"


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def _cargar_paginas(ruta: str) -> list[dict]:
    datos = json.loads(Path(ruta).read_text(encoding="utf-8"))
    paginas = datos.get("paginas")
    if not isinstance(paginas, list):
        raise ValueError(f'{ruta}: no tiene una lista "paginas" -no es un JSON de F-04 valido')
    return paginas


def renderizar_pagina(supabase, entrada: dict, cache: dict) -> dict:
    """Descarga (con cache por PDF), verifica el SHA256 contra el manifest
    congelado y renderiza UNA pagina a PNG. Nunca escribe nada en Supabase."""
    resultado = dict(entrada)
    storage_path = entrada["storage_path"]
    try:
        if storage_path not in cache:
            cache[storage_path] = descargar_pdf(supabase, storage_path)
        datos = cache[storage_path]
    except Exception as error:
        resultado.update(estado=ERROR_RENDER, error=f"descarga fallo: {error}"[:200])
        return resultado

    sha_real = hashlib.sha256(datos).hexdigest()
    resultado["sha256_real"] = sha_real
    if not sha_coincide(entrada.get("pdf_sha256_esperado"), sha_real):
        resultado.update(estado=GOLDEN_PDF_SHA_MISMATCH)
        logger.error(
            "SHA MISMATCH %s p.%s: manifest congelado=%s descargado=%s -pagina EXCLUIDA, no se renderiza-",
            entrada["document_key"], entrada["page_number"],
            entrada.get("pdf_sha256_esperado"), sha_real,
        )
        return resultado

    try:
        import fitz

        with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
            indice0 = entrada["page_number"] - 1
            if indice0 < 0 or indice0 >= doc.page_count:
                resultado.update(estado=PAGINA_FUERA_DEL_PDF, pdf_page_count=doc.page_count)
                return resultado
            pagina = doc[indice0]
            pix = pagina.get_pixmap(matrix=fitz.Matrix(DPI_RENDER / 72, DPI_RENDER / 72))
            ruta_imagen = Path(resultado["_out_dir"]) / entrada["archivo_imagen"]
            ruta_imagen.parent.mkdir(parents=True, exist_ok=True)
            pix.save(str(ruta_imagen))
            resultado.update(
                estado=RENDERIZADO_OK, ancho_px=pix.width, alto_px=pix.height,
                bytes_imagen=ruta_imagen.stat().st_size, dpi_render=DPI_RENDER,
            )
    except Exception as error:
        resultado.update(estado=ERROR_RENDER, error=f"render fallo: {error}"[:200])
    return resultado


def _escribir_readme(out: Path) -> None:
    contenido = """# F-04-B.1 — Paquete visual autocontenido de revisión humana

Este paquete trae TODO lo necesario para revisar las 50 páginas del Manifest
V2 sin abrir el PDF por separado ni buscar páginas a mano: la herramienta, los
datos y las 50 imágenes ya están juntos en esta misma carpeta.

## Abrir la herramienta

**Opción A (probar primero):** doble clic en `f04b_revision_humana.html`.

**Opción B (si "Exportar decisiones" no descarga nada):** en PowerShell,
parado en esta misma carpeta:

```powershell
python -m http.server 8000
```

y abre en el navegador `http://localhost:8000/f04b_revision_humana.html`.
(`Ctrl+C` en esa ventana para detenerlo al terminar.)

## Cargar los datos

Dentro de la herramienta, en la barra superior, carga en orden:

1. `F04_COMPARACION_MOTORES.json`
2. `F04_MANIFEST_PILOTO_V2.json`
3. `F04B_INDICE_IMAGENES.json` — esto es lo nuevo: habilita que la imagen de
   cada página se muestre DENTRO de la herramienta, sin salir a buscar el PDF.

El enlace al PDF original queda disponible como respaldo, pero ya no hace
falta abrirlo para revisar: la imagen de la página exacta está justo ahí.

## Todo lo demás

Las instrucciones completas -qué decisión marcar, qué diferencias son
críticas, cómo guardar y reanudar el trabajo, cómo entregar el resultado-
están dentro de la propia herramienta, en el panel "Instrucciones" (se abre
solo al cargar la página), y también en
`docs/F04B_REVISION_HUMANA.md` del repositorio.

## Qué NO hace este paquete

No se conecta a Supabase ni a ninguna base de datos. No escribe nada. No usa
ningún modelo de pago. La única conexión a internet posible es la que tú
generes voluntariamente si decides abrir el enlace de respaldo al PDF.
"""
    (out / "README.md").write_text(contenido, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="reportes/F04_MANIFEST_PILOTO_V2.json")
    parser.add_argument("--comparacion", default="reportes/F04_COMPARACION_MOTORES.json")
    parser.add_argument("--out-dir", default="reportes/golden_package")
    args = parser.parse_args()

    filas_manifest = _cargar_paginas(args.manifest)
    filas_comparacion = _cargar_paginas(args.comparacion)
    logger.info("Manifest V2: %d paginas. Comparacion: %d paginas.", len(filas_manifest), len(filas_comparacion))

    plan = construir_plan_render(filas_manifest)

    out = Path(args.out_dir)
    (out / "paginas").mkdir(parents=True, exist_ok=True)

    supabase = get_supabase()
    cache: dict[str, bytes] = {}
    inicio = time.time()
    resultados = []
    for entrada in plan:
        entrada_con_dir = dict(entrada, _out_dir=str(out))
        resultados.append(renderizar_pagina(supabase, entrada_con_dir, cache))
    duracion = time.time() - inicio
    for r in resultados:
        r.pop("_out_dir", None)

    resumen = resumen_control_calidad(
        plan, resultados, filas_comparacion, DOCUMENTOS_REQUERIDOS, OCR_ESPERADAS,
    )
    resumen["segundos_total"] = round(duracion, 1)
    resumen["costo_api_usd"] = 0.0
    resumen["modelo_pagado_usado"] = None
    tamano_total = sum((out / "paginas" / Path(r["archivo_imagen"]).name).stat().st_size
                        for r in resultados if r.get("estado") == RENDERIZADO_OK)
    resumen["tamano_total_imagenes_bytes"] = tamano_total

    indice_imagenes = [
        {
            "document_key": r["document_key"], "page_number": r["page_number"],
            "pdf_sha256": r.get("pdf_sha256_esperado"), "estado": r.get("estado"),
            "archivo_imagen": r.get("archivo_imagen") if r.get("estado") == RENDERIZADO_OK else None,
        }
        for r in resultados
    ]
    (out / "F04B_INDICE_IMAGENES.json").write_text(
        json.dumps({"resumen": resumen, "imagenes": indice_imagenes}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    shutil.copy2(args.manifest, out / "F04_MANIFEST_PILOTO_V2.json")
    shutil.copy2(args.comparacion, out / "F04_COMPARACION_MOTORES.json")
    if HERRAMIENTA_HTML.exists():
        shutil.copy2(HERRAMIENTA_HTML, out / HERRAMIENTA_HTML.name)
    else:
        logger.warning("No se encontro %s -el paquete quedara sin la herramienta HTML-", HERRAMIENTA_HTML)
    _escribir_readme(out)

    print("\n" + "=" * 72)
    print("F-04-B.1 — PAQUETE VISUAL GOLDEN — SOLO LECTURA, $0 EN LLAMADAS PAGADAS")
    print("=" * 72)
    for k, v in resumen.items():
        print(f"  {k:34} {v}")
    print("\n=== RESUMEN (JSON) ===")
    print(json.dumps(resumen, ensure_ascii=False, indent=2))
    print("=== FIN RESUMEN ===")
    print(f"\nPaquete: {out}")

    if not resumen["todos_los_controles_pasan"]:
        logger.error("Uno o mas controles obligatorios NO pasaron. Ver resumen arriba.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
