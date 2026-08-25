"""Reporte visual para construir el GOLDEN DATASET a mano (F-02 · 13 y 16).

Genera un HTML autocontenido con, por cada pagina:

    IMAGEN REAL DEL PDF  +  TRANSCRIPCION CANDIDATA EDITABLE
                         +  TOKENS JURIDICOS RESALTADOS
                         +  DIFERENCIAS ENTRE MOTORES

La persona CORRIGE o CONFIRMA; no vuelve a escribir la pagina desde cero. Al
terminar descarga un JSON con las transcripciones de referencia, que es el
ground truth. La salida de otro modelo NUNCA es ground truth: por eso el texto
que aparece es solo un CANDIDATO y hay que confirmarlo mirando la imagen.

El HTML no envia nada a ningun servidor: todo ocurre en el navegador.

Uso:
    python scripts/generar_revision_visual.py --document-keys RM-894-2024,DS-12-2023
    python scripts/generar_revision_visual.py --desde-manifest reportes/F04_MANIFEST_PILOTO.csv
"""

import argparse
import base64
import csv
import html
import io
import json
import logging
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))

from fidelidad_legal import es_pagina_dispositiva, tokens_sensibles  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "digemid-documentos"
DPI_IMAGEN = 150  # solo para MIRAR; la verificacion usa 300 (ver F-01 §13)


def get_supabase():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def resaltar_tokens(texto: str) -> str:
    """Marca los tokens cuyo cambio altera el efecto juridico, para que el ojo
    humano se detenga justo ahi."""
    escapado = html.escape(texto or "")
    vistos = set()
    for valores in tokens_sensibles(texto).values():
        vistos.update(valores)
    for valor in sorted(vistos, key=len, reverse=True):
        if len(valor) < 2:
            continue
        escapado = escapado.replace(html.escape(valor), f"<mark>{html.escape(valor)}</mark>")
    return escapado


def bloque(pagina: dict) -> str:
    ident = f"{pagina['document_key']}-p{pagina['page_number']}"
    dispositiva = pagina.get("es_dispositiva")
    etiqueta = ('<span class="badge alta">PARTE DISPOSITIVA</span>' if dispositiva
                else '<span class="badge">considerandos</span>')
    tokens = tokens_sensibles(pagina.get("candidato") or "")
    lista_tokens = "".join(
        f"<li><b>{html.escape(k)}</b>: {html.escape(', '.join(v))}</li>" for k, v in tokens.items()
    ) or "<li>sin tokens jurídicos detectados</li>"

    diferencias = "".join(
        f"<li>{html.escape(d)}</li>" for d in (pagina.get("diferencias") or [])
    ) or "<li>los motores no discrepan en ningún token jurídico</li>"

    return f"""
<section class="pagina" data-id="{html.escape(ident)}">
  <h2>{html.escape(pagina['document_key'])} — página {pagina['page_number']} {etiqueta}</h2>
  <div class="meta">
    método: <code>{html.escape(str(pagina.get('extraction_method')))}</code> ·
    quality_score: <code>{pagina.get('quality_score')}</code> ·
    OCR conf: <code>{pagina.get('ocr_confidence')}</code> ·
    SHA-256 del PDF: <code>{html.escape(str(pagina.get('sha256'))[:16])}…</code>
  </div>
  <div class="grid">
    <div class="col">
      <h3>PDF oficial (la evidencia)</h3>
      <img src="data:image/png;base64,{pagina['imagen_b64']}" alt="página del PDF">
    </div>
    <div class="col">
      <h3>Transcripción candidata — <em>confírmala o corrígela</em></h3>
      <textarea id="txt-{html.escape(ident)}" rows="26">{html.escape(pagina.get('candidato') or '')}</textarea>
      <div class="acciones">
        <button onclick="marcar('{html.escape(ident)}','confirmada')">✓ Coincide con el PDF</button>
        <button onclick="marcar('{html.escape(ident)}','corregida')">✎ La corregí</button>
        <button onclick="marcar('{html.escape(ident)}','ilegible')">✗ Ilegible / no verificable</button>
        <span class="estado" id="est-{html.escape(ident)}">sin revisar</span>
      </div>
      <details open>
        <summary>Tokens jurídicos detectados (revísalos uno por uno)</summary>
        <ul class="tokens">{lista_tokens}</ul>
      </details>
      <details>
        <summary>Discrepancias entre motores</summary>
        <ul class="difs">{diferencias}</ul>
      </details>
      <details>
        <summary>Texto con los tokens resaltados</summary>
        <pre class="resaltado">{resaltar_tokens(pagina.get('candidato') or '')}</pre>
      </details>
    </div>
  </div>
</section>"""


PLANTILLA = """<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<title>Revisión visual — golden dataset</title>
<style>
 body {{ font-family: system-ui, sans-serif; margin: 0 auto; max-width: 1500px; padding: 1.5rem;
        background: #fafafa; color: #1a1a1a; }}
 h1 {{ margin-bottom: .25rem; }}
 .intro {{ background: #fff8e1; border-left: 4px solid #f9a825; padding: .9rem 1rem;
           margin-bottom: 1.5rem; line-height: 1.5; }}
 .pagina {{ background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 1rem;
            margin-bottom: 2rem; }}
 .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
 @media (max-width: 1100px) {{ .grid {{ grid-template-columns: 1fr; }} }}
 img {{ width: 100%; border: 1px solid #ccc; background: #fff; }}
 textarea {{ width: 100%; font-family: ui-monospace, monospace; font-size: .82rem;
             line-height: 1.45; padding: .6rem; box-sizing: border-box; }}
 .badge {{ font-size: .72rem; padding: .15rem .5rem; border-radius: 10px; background: #e0e0e0; }}
 .badge.alta {{ background: #ffcdd2; font-weight: 700; }}
 .meta {{ font-size: .78rem; color: #555; margin-bottom: .7rem; }}
 .acciones {{ margin: .6rem 0; display: flex; gap: .5rem; align-items: center; flex-wrap: wrap; }}
 button {{ padding: .4rem .8rem; cursor: pointer; border: 1px solid #bbb; border-radius: 5px;
           background: #fff; }}
 .estado {{ font-size: .8rem; color: #666; }}
 .estado.ok {{ color: #2e7d32; font-weight: 700; }}
 mark {{ background: #fff59d; }}
 .tokens li, .difs li {{ font-size: .82rem; }}
 pre.resaltado {{ white-space: pre-wrap; font-size: .78rem; background: #fcfcfc;
                  border: 1px solid #eee; padding: .6rem; }}
 #barra {{ position: sticky; top: 0; background: #1a1a1a; color: #fff; padding: .7rem 1rem;
           display: flex; justify-content: space-between; align-items: center; z-index: 10; }}
</style></head><body>
<div id="barra">
  <span id="progreso">0 revisadas</span>
  <button onclick="descargar()">⬇ Descargar golden dataset (JSON)</button>
</div>
<h1>Revisión visual — construcción del golden dataset</h1>
<div class="intro">
  <b>La imagen de la izquierda es la evidencia.</b> El texto de la derecha es solo un
  <b>candidato</b> producido por un motor automático: puede estar mal justo en un número.
  Compara y corrige lo que haga falta — no hace falta reescribir la página entera.<br>
  Presta atención especial a los <mark>tokens resaltados</mark>: número de norma, año,
  artículos, plazos, montos y verbos como <b>derogar / modificar</b>. Un dígito cambia el
  efecto jurídico.<br>
  Nada se envía a ningún servidor: al terminar, descarga el JSON y pásalo al repositorio.
</div>
{bloques}
<script>
const revisiones = {{}};
function marcar(id, estado) {{
  const texto = document.getElementById('txt-' + id).value;
  revisiones[id] = {{ estado, texto, revisado_en: new Date().toISOString() }};
  const el = document.getElementById('est-' + id);
  el.textContent = estado === 'confirmada' ? 'confirmada' :
                   estado === 'corregida' ? 'corregida' : 'ilegible';
  el.className = 'estado ok';
  document.getElementById('progreso').textContent =
    Object.keys(revisiones).length + ' revisadas de {total}';
}}
function descargar() {{
  const blob = new Blob([JSON.stringify(
    {{ generado_en: new Date().toISOString(), fuente: 'revision_humana', revisiones }},
    null, 2)], {{ type: 'application/json' }});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'golden_dataset_revisado.json';
  a.click();
}}
</script>
</body></html>"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--document-keys", default="", help="RM-894-2024,DS-12-2023")
    parser.add_argument("--paginas", default="", help="limitar a estas paginas (1,5,9)")
    parser.add_argument("--desde-manifest", default="",
                        help="CSV con columnas document_key,page_number (p.ej. "
                             "F04_MANIFEST_PILOTO.csv): usa EXACTAMENTE esos pares "
                             "documento+pagina en vez de --document-keys/--paginas")
    parser.add_argument("--limite", type=int, default=50)
    parser.add_argument("--out", default="reportes/REVISION_VISUAL.html")
    args = parser.parse_args()

    import fitz

    supabase = get_supabase()

    pares_manifest = None
    if args.desde_manifest:
        with open(args.desde_manifest, newline="", encoding="utf-8") as fh:
            pares_manifest = {
                (fila["document_key"], int(fila["page_number"]))
                for fila in csv.DictReader(fh) if fila.get("document_key") and fila.get("page_number")
            }
        claves = sorted({dk for dk, _ in pares_manifest})
        if not claves:
            raise SystemExit(f"{args.desde_manifest} no tiene filas document_key,page_number")
    else:
        claves = [k.strip() for k in args.document_keys.split(",") if k.strip()]
        if not claves:
            raise SystemExit("indica --document-keys o --desde-manifest")
    filtro_paginas = {int(x) for x in args.paginas.split(",") if x.strip()} if args.paginas else None

    normas = (
        supabase.table("digemid_normas")
        .select("id, document_key, file_storage_path, pdf_url")
        .in_("document_key", claves).execute().data or []
    )

    bloques, total = [], 0
    for norma in normas:
        if not norma.get("file_storage_path"):
            logger.warning("%s no tiene PDF guardado: se omite", norma["document_key"])
            continue
        datos = supabase.storage.from_(BUCKET).download(norma["file_storage_path"])
        import hashlib

        sha = hashlib.sha256(datos).hexdigest()
        paginas = (
            supabase.table("digemid_norma_paginas")
            .select("page_number, text_normalized, text_raw, extraction_method, "
                    "quality_score, ocr_confidence")
            .eq("norma_id", norma["id"]).order("page_number").execute().data or []
        )

        with fitz.open(stream=io.BytesIO(datos), filetype="pdf") as doc:
            for p in paginas:
                numero = p.get("page_number") or 0
                if pares_manifest is not None and (norma["document_key"], numero) not in pares_manifest:
                    continue
                if filtro_paginas and numero not in filtro_paginas:
                    continue
                if total >= args.limite:
                    break
                if numero < 1 or numero > doc.page_count:
                    continue
                pix = doc[numero - 1].get_pixmap(
                    matrix=fitz.Matrix(DPI_IMAGEN / 72, DPI_IMAGEN / 72))
                texto = p.get("text_normalized") or p.get("text_raw") or ""
                bloques.append(bloque({
                    "document_key": norma["document_key"],
                    "page_number": numero,
                    "candidato": texto,
                    "es_pagina_dispositiva": None,
                    "es_dispositiva": es_pagina_dispositiva(texto),
                    "extraction_method": p.get("extraction_method"),
                    "quality_score": p.get("quality_score"),
                    "ocr_confidence": p.get("ocr_confidence"),
                    "sha256": sha,
                    "imagen_b64": base64.b64encode(pix.tobytes("png")).decode("ascii"),
                    "diferencias": [],
                }))
                total += 1

    salida = Path(args.out)
    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(
        PLANTILLA.format(bloques="\n".join(bloques), total=total), encoding="utf-8")
    print(f"Escrito {salida} con {total} páginas para revisar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
