import warnings

try:
    import pytest

    _skipif = pytest.mark.skipif
except ImportError:  # el CI usa unittest y no instala pytest
    import unittest

    def _skipif(condicion, reason=""):
        return unittest.skipIf(condicion, reason)

warnings.filterwarnings("ignore")

from agents import ocr_preproceso
from agents.pdf_extract import elegir_por_consenso


def test_consenso_elige_la_lectura_mas_parecida_al_resto():
    buena = "Retiro del mercado del lote 123 del producto Paracetamol 500 mg tabletas"
    similar = "Retiro del mercado del lote 123 del producto Paracetamol 500 mg tabletas."
    con_error = "Rctiro dcl mcrcado dcl lotc 123 dcl producto Paracetamol 5O0 mq"

    indice = elegir_por_consenso([con_error, buena, similar])

    assert indice in (1, 2)


def test_consenso_con_pocas_lecturas_devuelve_la_primera():
    assert elegir_por_consenso(["uno"]) == 0
    assert elegir_por_consenso(["uno", "dos"]) == 0


@_skipif(not ocr_preproceso.HAS_CV2, reason="OpenCV no instalado")
def test_estimar_angulo_detecta_pagina_inclinada():
    import cv2
    import numpy as np

    pagina = np.full((800, 600), 255, np.uint8)
    for fila in range(100, 700, 40):
        cv2.putText(pagina, "TEXTO DE PRUEBA DIGEMID", (50, fila), cv2.FONT_HERSHEY_SIMPLEX, 0.9, 0, 2)
    matriz = cv2.getRotationMatrix2D((300, 400), 3.0, 1.0)
    inclinada = cv2.warpAffine(pagina, matriz, (600, 800), borderValue=255)

    angulo = ocr_preproceso.estimar_angulo(inclinada)

    assert abs(abs(angulo) - 3.0) < 1.0


def test_sin_opencv_no_hay_variantes(monkeypatch):
    monkeypatch.setattr(ocr_preproceso, "HAS_CV2", False)
    assert ocr_preproceso.variantes_para_ocr(b"cualquier cosa") == []
