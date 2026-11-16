"""Mide la precision del OCR de RegAlert con una verdad conocida.

Toma un PDF CON capa de texto (la verdad), rasteriza unas paginas, las degrada
como un mal escaneo (inclinacion, ruido, desenfoque, bajo contraste, JPEG) y
compara la lectura del pipeline con el texto real.

Uso: python herramientas/bench_ocr.py POES.pdf [--paginas 7,30,100] [--nivel 1|2|3]
Salida: precision de caracteres y de palabras (0-100) por pagina y promedio.
"""

from __future__ import annotations

import argparse
import difflib
import io
import re
import sys
import unicodedata
from pathlib import Path

import cv2
import fitz
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agents import pdf_extract  # noqa: E402

NIVELES = {  # angulo, ruido, blur, contraste, calidad jpeg
    1: (1.0, 6, 0.0, 0.9, 80),
    2: (2.5, 12, 1.0, 0.75, 60),
    3: (4.0, 20, 1.6, 0.6, 45),
}


def norm(t: str) -> str:
    t = unicodedata.normalize("NFKD", t.lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", t)).strip()


def degradar(gris: np.ndarray, nivel: int, semilla: int = 7) -> np.ndarray:
    ang, ruido, blur, contraste, q = NIVELES[nivel]
    rng = np.random.default_rng(semilla)
    h, w = gris.shape
    m = cv2.getRotationMatrix2D((w / 2, h / 2), ang, 1.0)
    img = cv2.warpAffine(gris, m, (w, h), borderValue=255)
    if blur:
        img = cv2.GaussianBlur(img, (0, 0), blur)
    img = (255 - (255 - img.astype(np.float32)) * contraste)
    img = np.clip(img + rng.normal(0, ruido, img.shape), 0, 255).astype(np.uint8)
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, q])
    return cv2.imdecode(buf, cv2.IMREAD_GRAYSCALE)


def pdf_de_imagen(gris: np.ndarray) -> fitz.Document:
    ok, png = cv2.imencode(".png", gris)
    doc = fitz.open()
    pagina = doc.new_page(width=595, height=842)
    pagina.insert_image(pagina.rect, stream=png.tobytes())
    return fitz.open("pdf", doc.tobytes())


def puntuar(verdad: str, lectura: str) -> tuple[float, float]:
    a, b = norm(verdad), norm(lectura)
    c = difflib.SequenceMatcher(None, a, b, autojunk=False).ratio() * 100
    wa, wb = a.split(), b.split()
    w = difflib.SequenceMatcher(None, wa, wb, autojunk=False).ratio() * 100
    return round(c, 1), round(w, 1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf")
    ap.add_argument("--paginas", default="7,30,100")
    ap.add_argument("--nivel", type=int, default=2, choices=NIVELES)
    args = ap.parse_args()
    doc = fitz.open(args.pdf)
    tot_c, tot_w, n = 0.0, 0.0, 0
    for num in [int(x) for x in args.paginas.split(",")]:
        pag = doc[num - 1]
        verdad = pag.get_text()
        pix = pag.get_pixmap(matrix=fitz.Matrix(200 / 72, 200 / 72), colorspace=fitz.csGRAY)
        gris = np.frombuffer(pix.samples, np.uint8).reshape(pix.height, pix.width)
        sucia = pdf_de_imagen(degradar(gris, args.nivel))[0]
        lectura, conf = pdf_extract._ocr_page(sucia)
        c, w = puntuar(verdad, lectura)
        print(f"pág {num}: caracteres {c}% | palabras {w}% | confianza OCR {conf}")
        tot_c, tot_w, n = tot_c + c, tot_w + w, n + 1
    print(f"PROMEDIO nivel {args.nivel}: caracteres {tot_c / n:.1f}% | palabras {tot_w / n:.1f}%")


if __name__ == "__main__":
    main()
