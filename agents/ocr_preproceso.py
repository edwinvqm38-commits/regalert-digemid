"""Preprocesado de imagenes de paginas escaneadas antes del OCR.

Tesseract rinde mucho mejor con una imagen enderezada, sin ruido y con buen
contraste que con el render crudo de un escaneo (inclinado, con manchas o
JPEG muy comprimido). Este modulo ofrece variantes de la imagen; el llamador
corre OCR sobre cada una y se queda con la de mayor confianza.

OpenCV es una dependencia OPCIONAL: si no esta instalada, todo degrada al
comportamiento anterior (imagen cruda) sin romper el pipeline.
"""

import logging

logger = logging.getLogger(__name__)

try:
    import cv2
    import numpy as np
    HAS_CV2 = True
except Exception:  # pragma: no cover - depende del entorno
    HAS_CV2 = False

# Un angulo mayor se considera falso positivo (tablas, lineas largas) y no se
# corrige: enderezar de mas gira bien una pagina que ya estaba recta.
_MAX_ANGULO_GRADOS = 8.0
# Inclinaciones menores no justifican la interpolacion (que difumina texto).
_MIN_ANGULO_GRADOS = 0.3


def estimar_angulo(gris) -> float:
    """Estima la inclinacion del texto en grados (positivo = antihorario)
    a partir del rectangulo minimo que envuelve los pixeles de tinta."""
    if not HAS_CV2:
        return 0.0
    _, binaria = cv2.threshold(gris, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    coords = cv2.findNonZero(binaria)
    if coords is None or len(coords) < 200:
        return 0.0
    angulo = cv2.minAreaRect(coords)[-1]
    # OpenCV >= 4.5 devuelve (0, 90]; se normaliza a [-45, 45].
    if angulo > 45:
        angulo -= 90
    if abs(angulo) > _MAX_ANGULO_GRADOS:
        return 0.0
    return float(angulo)


def enderezar(gris, angulo: float):
    if abs(angulo) < _MIN_ANGULO_GRADOS:
        return gris
    alto, ancho = gris.shape[:2]
    matriz = cv2.getRotationMatrix2D((ancho / 2, alto / 2), angulo, 1.0)
    return cv2.warpAffine(
        gris, matriz, (ancho, alto),
        flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE,
    )


def variantes_para_ocr(png_bytes: bytes) -> list[tuple[str, bytes]]:
    """Devuelve [(nombre, png)] con las variantes a probar, de la mas
    conservadora a la mas agresiva (hoy solo una). Si OpenCV no esta, lista vacia."""
    if not HAS_CV2:
        return []
    try:
        gris = cv2.imdecode(np.frombuffer(png_bytes, np.uint8), cv2.IMREAD_GRAYSCALE)
        if gris is None:
            return []

        gris = enderezar(gris, estimar_angulo(gris))
        limpia = cv2.fastNlMeansDenoising(gris, None, h=12, templateWindowSize=7, searchWindowSize=21)
        contraste = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(limpia)
        binaria = cv2.adaptiveThreshold(
            contraste, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 41, 15,
        )

        def a_png(imagen) -> bytes:
            ok, buf = cv2.imencode(".png", imagen)
            return buf.tobytes() if ok else b""

        # Solo se ofrece la binarizada: en las pruebas, la variante solo
        # enderezada/con contraste empeoraba el texto en la mayoria de los
        # documentos frente al render crudo.
        png_binaria = a_png(binaria)
        return [("binarizada", png_binaria)] if png_binaria else []
    except Exception as error:
        logger.warning("Preprocesado de imagen para OCR fallo: %s", error)
        return []
