"""Tablas de escalas de infracciones (Anexo 01 DIGEMID) con encabezados que
hacen wrap a varias lineas dentro de una celda: pdfplumber a veces detecta
una linea de grilla espuria ahi y parte celdas de mas, produciendo
encabezados sin sentido (ej. "NE IO CÉ cul lec" en vez de nada, "BOTIQUÍN"
partido en "B" / "OTI" / "QUÍN"). Confirmado en DS-020-2024, paginas 4-5
(digemid_norma_paginas, norma_id f1a39a0d-a3bd-479c-bc27-3e046fbc79aa).

Estos tests fijan el comportamiento de las funciones puras que limpian ese
resultado ANTES de que se convierta a Markdown y llegue al flujo de revision
humana de Telegram (tablaATextoEditable/etiquetaColumna en index.ts, que ya
sabe mostrar "Columna N" para un encabezado vacio): un encabezado vacio con
esa clave interna estable es preferible a uno con texto incorrecto que un
revisor podria no notar que esta mal.
"""

import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from agents.pdf_extract import (  # noqa: E402
    _celda_parece_fragmento,
    _fila_parece_fantasma,
    _fraccion_celdas_fragmentadas,
    _limpiar_encabezado_sospechoso,
    _limpiar_filas_fantasma,
    _tabla_a_markdown,
    tablas_a_markdown,
)


# Reproduccion fiel de lo que pdfplumber.extract_tables() devolvio para la
# tabla de infracciones 20/42 de DS-020-2024 pagina 4 (estrategia "lines").
TABLA_ANEXO01_CORRUPTA = [
    ["", "", "", "", "NE IO CÉ cul lec", "X N U o 5 im", "", "", "", "", ""],
    [
        "", "INFRACCIÓN", "FARMACIA BOTICA",
        "FARMACIA DE LOS ESTABLECIMIENTOS DE SALUD",
        "B", "OTI", "QUÍN", "DROGUERÍAS", "ALMACÉN ESPECIALIZADO",
        "LABORATORIO", "NO FARMACÉUTICO",
    ],
    [
        "20", "Por comercializar productos no comprendidos en la Ley N° 29459",
        "1 UIT", "1 UIT", "0", ".1", "UIT", "1 UIT", "1 UIT", "1 UIT", "NA",
    ],
]


class CeldaPareceFragmentoTest(unittest.TestCase):
    def test_fragmento_de_palabra_partida_mayuscula(self):
        # "BOTIQUÍN" partido en 3 celdas por una linea de grilla espuria.
        self.assertTrue(_celda_parece_fragmento("B"))
        self.assertTrue(_celda_parece_fragmento("OTI"))
        self.assertTrue(_celda_parece_fragmento("QUÍN"))

    def test_fragmento_de_palabra_partida_minuscula(self):
        self.assertTrue(_celda_parece_fragmento("cul"))
        self.assertTrue(_celda_parece_fragmento("lec"))

    def test_fragmento_decimal_partido(self):
        # "0.1 UIT" partido en "0" / ".1" / "UIT".
        self.assertTrue(_celda_parece_fragmento("0"))
        self.assertTrue(_celda_parece_fragmento(".1"))

    def test_abreviaturas_conocidas_no_son_fragmento(self):
        self.assertFalse(_celda_parece_fragmento("UIT"))
        self.assertFalse(_celda_parece_fragmento("NA"))

    def test_celda_vacia_no_es_fragmento(self):
        self.assertFalse(_celda_parece_fragmento(""))
        self.assertFalse(_celda_parece_fragmento("   "))

    def test_encabezado_real_no_es_fragmento(self):
        self.assertFalse(_celda_parece_fragmento("FARMACIA BOTICA"))
        self.assertFalse(_celda_parece_fragmento("DROGUERÍAS"))
        self.assertFalse(_celda_parece_fragmento("INFRACCIÓN"))


class FilaFantasmaTest(unittest.TestCase):
    def test_fila_casi_vacia_con_fragmentos_sueltos_es_fantasma(self):
        fila = ["", "", "", "", "NE IO CÉ cul lec", "X N U o 5 im", "", "", "", "", ""]
        self.assertTrue(_fila_parece_fantasma(fila))

    def test_fila_de_datos_real_no_es_fantasma(self):
        fila = TABLA_ANEXO01_CORRUPTA[2]
        self.assertFalse(_fila_parece_fantasma(fila))

    def test_fila_totalmente_vacia_no_se_marca_fantasma(self):
        # Una fila 100% vacia es un caso distinto (separador de layout real
        # entre secciones de la tabla); no se toca aca.
        self.assertFalse(_fila_parece_fantasma(["", "", "", "", "", ""]))


class LimpiarFilasFantasmaTest(unittest.TestCase):
    def test_descarta_fila_fantasma_de_encabezado(self):
        limpia = _limpiar_filas_fantasma(TABLA_ANEXO01_CORRUPTA)
        self.assertEqual(len(limpia), 2)
        self.assertEqual(limpia[0][1], "INFRACCIÓN")

    def test_no_toca_tablas_de_2_filas_o_menos(self):
        tabla = [["a", "b"], ["1", "2"]]
        self.assertEqual(_limpiar_filas_fantasma(tabla), tabla)

    def test_no_descarta_fila_casi_vacia_en_medio_de_los_datos(self):
        tabla = [
            ["INFRACCIÓN", "FARMACIA"],
            ["20", "1 UIT"],
            ["", ""],
            ["42", "1 UIT"],
        ]
        self.assertEqual(len(_limpiar_filas_fantasma(tabla)), 4)


class LimpiarEncabezadoSospechosoTest(unittest.TestCase):
    def test_vacia_solo_las_celdas_fragmentadas(self):
        encabezado = TABLA_ANEXO01_CORRUPTA[1]
        limpio = _limpiar_encabezado_sospechoso(encabezado)
        self.assertEqual(limpio[4], "")  # "B"
        self.assertEqual(limpio[5], "")  # "OTI"
        self.assertEqual(limpio[6], "")  # "QUÍN"
        # Los encabezados reales y legibles se conservan intactos.
        self.assertEqual(limpio[1], "INFRACCIÓN")
        self.assertEqual(limpio[2], "FARMACIA BOTICA")
        self.assertEqual(limpio[7], "DROGUERÍAS")
        self.assertEqual(limpio[10], "NO FARMACÉUTICO")


class FraccionCeldasFragmentadasTest(unittest.TestCase):
    def test_tabla_corrupta_tiene_fragmentacion_mayor_que_una_limpia(self):
        tabla_limpia = [
            ["INFRACCIÓN", "FARMACIA BOTICA", "BOTIQUÍN", "DROGUERÍAS"],
            ["20", "1 UIT", "0.1 UIT", "1 UIT"],
        ]
        frag_corrupta = _fraccion_celdas_fragmentadas([TABLA_ANEXO01_CORRUPTA])
        frag_limpia = _fraccion_celdas_fragmentadas([tabla_limpia])
        self.assertGreater(frag_corrupta, frag_limpia)
        self.assertEqual(frag_limpia, 0.0)


class TablaAMarkdownIntegracionTest(unittest.TestCase):
    def test_pipeline_completo_produce_encabezado_sin_basura(self):
        """Reproduce lo que hace _pdfplumber_tables antes de convertir a
        Markdown: descarta la fila fantasma y vacia las celdas de
        encabezado fragmentadas, en vez de propagar "NE IO CÉ cul lec" o
        "BOTIQUÍN" partido en 3 columnas hasta /consulta o la revision
        humana en Telegram."""
        tabla_limpia = _limpiar_filas_fantasma(TABLA_ANEXO01_CORRUPTA)
        markdown = tablas_a_markdown([tabla_limpia])

        self.assertNotIn("NE IO CÉ", markdown)
        self.assertNotIn("cul lec", markdown)
        # La fila de encabezado (segunda linea del bloque Markdown, despues
        # de "Tabla:") no debe traer los fragmentos "B" / "OTI" / "QUÍN"
        # como celdas propias (serian sustrings validos dentro de otras
        # palabras, ej. "BOTICA" contiene "OTI"; por eso se revisa la fila
        # ya partida en celdas, no el texto completo).
        fila_encabezado = markdown.splitlines()[1]
        celdas_encabezado = [c.strip() for c in fila_encabezado.strip("|").split("|")]
        self.assertNotIn("B", celdas_encabezado)
        self.assertNotIn("OTI", celdas_encabezado)
        self.assertNotIn("QUÍN", celdas_encabezado)
        # Los encabezados juridicamente relevantes sobreviven intactos.
        self.assertIn("INFRACCIÓN", markdown)
        self.assertIn("FARMACIA BOTICA", markdown)
        self.assertIn("DROGUERÍAS", markdown)
        self.assertIn("ALMACÉN ESPECIALIZADO", markdown)
        self.assertIn("NO FARMACÉUTICO", markdown)

    def test_tabla_sin_corrupcion_no_cambia(self):
        tabla = [
            ["INFRACCIÓN", "FARMACIA BOTICA", "BOTIQUÍN"],
            ["66", "0.5 UIT", "NA"],
        ]
        markdown = _tabla_a_markdown(tabla)
        self.assertIn("INFRACCIÓN", markdown)
        self.assertIn("FARMACIA BOTICA", markdown)
        self.assertIn("BOTIQUÍN", markdown)
        self.assertIn("0.5 UIT", markdown)


if __name__ == "__main__":
    unittest.main()
