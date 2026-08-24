"""Mide el efecto del DPI de render sobre la fidelidad del OCR (F-01 · 12).

El workflow de visión IA usa 150 DPI por defecto, mientras que el OCR con
Tesseract de agents/pdf_extract.py renderiza a 300. Es decir: las páginas MÁS
difíciles -las que Tesseract ya no pudo leer- se le mandan al modelo con la
MITAD de resolución. Este script mide cuánto cuesta eso, en vez de suponerlo.

Compara, sobre las mismas páginas, la transcripción a 150 / 200 / 300 DPI, y
no se queda en CER/WER: cuenta cuántos TOKENS JURÍDICOS (números de norma,
artículos, años, plazos, montos, dosis) cambian, que es lo único que altera el
efecto legal.

Uso:
    python scripts/comparar_dpi_ocr.py --pdf ruta.pdf --paginas 1,5,9
    python scripts/comparar_dpi_ocr.py --document-key RM-894-2024   # baja el PDF de Supabase
"""

import argparse
import io
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))

from fidelidad_legal import comparar_fidelidad, tokens_sensibles  # noqa: E402

DPIS = (150, 200, 300)


def ocr_a_dpi(pdf: Path, indice: int, dpi: int) -> tuple[str, float | None]:
    import fitz
    import pytesseract
    from PIL import Image

    with fitz.open(pdf) as doc:
        pix = doc[indice].get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72))
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    texto = pytesseract.image_to_string(img, lang="spa") or ""
    datos = pytesseract.image_to_data(img, lang="spa", output_type=pytesseract.Output.DICT)
    confs = []
    for valor in datos.get("conf", []):
        try:
            c = float(valor)
        except (TypeError, ValueError):
            continue
        if c >= 0:
            confs.append(c)
    return texto, (sum(confs) / len(confs) / 100.0 if confs else None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", required=True, type=Path)
    parser.add_argument("--paginas", default="", help="1,5,9 (por defecto todas)")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    import fitz

    with fitz.open(args.pdf) as doc:
        total = doc.page_count
    indices = (
        [int(x) - 1 for x in args.paginas.split(",") if x.strip()]
        if args.paginas else list(range(total))
    )

    filas = []
    print(f"{'pag':>4} {'DPI':>4} {'conf':>6} {'CER vs 300':>11} {'tokens_juridicos_distintos':>27}")
    print("-" * 60)
    for indice in indices:
        textos = {dpi: ocr_a_dpi(args.pdf, indice, dpi) for dpi in DPIS}
        referencia = textos[300][0]  # 300 DPI como mejor disponible, NO como verdad
        for dpi in DPIS:
            texto, conf = textos[dpi]
            r = comparar_fidelidad(referencia, texto)
            errores = r.errores_token + r.verbos_cambiados
            filas.append({
                "pagina": indice + 1, "dpi": dpi,
                "ocr_confidence": conf, "cer_vs_300": round(r.cer, 4),
                "legal_token_error_rate": round(r.legal_token_error_rate, 4),
                "tokens_distintos": [str(e) for e in errores],
                "tokens_juridicos_detectados": sum(len(v) for v in tokens_sensibles(texto).values()),
            })
            c = f"{conf:.3f}" if conf else "  n/a"
            print(f"{indice+1:>4} {dpi:>4} {c:>6} {r.cer:>11.4f} {len(errores):>27}")
            for e in errores[:4]:
                print(f"{'':>17}   ! {e}")

    if args.out:
        Path(args.out).write_text(json.dumps(filas, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nEscrito {args.out}")

    print("\nOJO: 300 DPI se usa como REFERENCIA por ser la mejor disponible, no")
    print("como verdad. Un token que coincide a 150 y a 300 puede estar mal en")
    print("las dos. La verdad es el PDF oficial leído por una persona.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
