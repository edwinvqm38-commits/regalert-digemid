"""Regresiones de selección conservadora de tablas regulatorias.

Los fixtures mínimos reproducen geometrías y fragmentos observados al ejecutar
pdfplumber sobre las páginas PDF 4 y 5 del caso real local. No reconstruyen la
tabla jurídica: fijan las señales por las que un candidato inseguro debe
rechazarse, preservando el texto plano.
"""

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from agents.pdf_extract import (  # noqa: E402
    PageExtraction,
    _agregar_acuerdo_entre_extractores,
    _celda_parece_fragmento,
    _consolidar_deteccion_candidatos,
    _evaluar_candidato_tabla,
    _fila_parece_fantasma,
    _fraccion_celdas_fragmentadas,
    _limpiar_filas_fantasma,
    _seleccionar_candidatos,
    _tabla_a_markdown,
    extract_pdf,
    extract_page,
    tablas_a_markdown,
)
from scripts.extract_normativa_text_simple import write_pages  # noqa: E402


# Fragmento del candidato pdfplumber/text observado en la página PDF 4: la
# grilla empieza en el encabezado editorial y parte ambas columnas de prosa.
PAGINA_EDITORIAL_DOS_COLUMNAS = [
    ["22", "", "", "NORMAS L", "EGALES V", "iernes 25 de octubre de 20", "24/ El Peruano"],
    ["", "", "", "", "", "", ""],
    ["Esta publicidad in", "cluye", "información", "comparativa en", "Artículo sobre", "la información", ""],
    ["cuanto a calidad y pre", "cio si", "existe un refe", "rente", "texto de la", "segunda columna", "editorial"],
    ["Los anuncios de", "productos", "autorizados para", "venta directa", "se sujetan a", "medios audiovisuales", "e impresos"],
    ["La autoridad puede", "publicar", "información para", "los usuarios", "cuando resulte", "necesario para", "su protección"],
    ["Las disposiciones", "se aplican", "de acuerdo con", "la normativa", "sin alterar el", "contenido oficial", "publicado"],
]


# Subconjunto rectangular 16x19 derivado del candidato pdfplumber/lines de la
# página PDF 5. Conserva las columnas vacías intercaladas y la fila fantasma
# central, sin pretender reconstruir asociaciones encabezado-valor.
PAGINA_SOBRESEGMENTADA_19_COLUMNAS = [
    ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
    ["66", "Descripción regulatoria extensa", "", "0.5 ABC", "", "0.5 ABC", "", "", "", "N", "A", "2 ABC", "", "NA", "", "2 ABC", "", "NA", ""],
    ["68", "Otra descripción regulatoria extensa", "", "1 ABC o medida temporal", "", "1 ABC o medida temporal", "", "", "", "N", "A", "2 ABC", "", "NA", "", "2 ABC", "", "NA", ""],
    ["", "", "", "", "", "", "", "", "4, -2 de nt E O É ulo ec", "7 01 l R e XO NE UT 5 im", "", "", "", "", "", "", "", "", ""],
    ["", "DESCRIPCIÓN", "", "", "CATEGORÍA PRINCIPAL", "", "OTRA CATEGORÍA", "", "S OS", "B", "OTIQUÍN", "", "TIPO A", "", "TIPO B", "", "TIPO C", "", "TIPO D"],
    ["72", "Texto descriptivo de la fila", "Primera condición", "", "Medida", "", "Medida", "", "", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA"],
    ["", "", "Segunda condición", "", "1 ABC", "", "1 ABC", "", "", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA"],
    ["", "", "", "", "", "", "", "", "n d -20", "e I 19", "", "", "", "", "", "", "", "", ""],
]

# Completa las 16 bandas observadas manteniendo contenido en las columnas
# útiles y vacíos en las separaciones geométricas artificiales.
PAGINA_SOBRESEGMENTADA_19_COLUMNAS.extend([
    [str(numero), "Texto descriptivo de la fila", "Condición", "", "Medida", "", "Medida", "", "", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA"]
    for numero in range(73, 81)
])


TABLA_SIMPLE_VALIDA = [
    ["Código", "Descripción", "Estado"],
    ["A-10", "Registro inicial", "Vigente"],
    ["A-11", "Registro complementario", "Cerrado"],
]


TABLA_30_POR_CIENTO_COLUMNAS_VACIAS = [
    [f"dato-{fila}-{columna}" for columna in range(7)] + ["", "", ""]
    for fila in range(10)
]


TABLA_FRAGMENTADA_EN_LIMITE = [
    ["CON", "SER", "VAR", "Estado"],
    ["A-1", "Registro", "Inicial", "Vigente"],
    ["A-2", "Registro", "Adicional", "Vigente"],
    ["A-3", "Registro", "Temporal", "Cerrado"],
    ["A-4", "Registro", "Final", "Cerrado"],
]


def evaluar(
    tabla,
    *,
    strategy="lines",
    bbox=(50.0, 100.0, 450.0, 400.0),
    page_width=500.0,
    page_height=800.0,
    extractor="pdfplumber",
):
    return _evaluar_candidato_tabla(
        tabla,
        extractor=extractor,
        strategy=strategy,
        bbox=bbox,
        page_width=page_width,
        page_height=page_height,
    )


class FragmentacionGenericaTest(unittest.TestCase):
    def test_celda_aislada_solo_se_marca_si_es_inequivoca(self):
        self.assertTrue(_celda_parece_fragmento(".1"))
        self.assertTrue(_celda_parece_fragmento("B"))
        self.assertFalse(_celda_parece_fragmento("ABC"))
        self.assertFalse(_celda_parece_fragmento("7"))

    def test_palabra_partida_entre_columnas_eleva_fragmentacion(self):
        corrupta = [
            ["Código", "Descripción", "CON", "SER", "VAR", "Estado"],
            ["10", "Registro inicial", "", "", "", "Vigente"],
        ]
        limpia = [
            ["Código", "Descripción", "CONSERVAR", "Estado"],
            ["10", "Registro inicial", "", "Vigente"],
        ]
        self.assertGreater(
            _fraccion_celdas_fragmentadas([corrupta]),
            _fraccion_celdas_fragmentadas([limpia]),
        )

    def test_decimal_y_unidad_partidos_elevan_fragmentacion(self):
        corrupta = [
            ["Código", "Valor", "Unidad", ""],
            ["A-1", "0", ".1", "ABC"],
        ]
        limpia = [
            ["Código", "Valor"],
            ["A-1", "0.1 ABC"],
        ]
        self.assertGreaterEqual(_fraccion_celdas_fragmentadas([corrupta]), 0.4)
        self.assertEqual(_fraccion_celdas_fragmentadas([limpia]), 0.0)


class FilaFantasmaTest(unittest.TestCase):
    def test_fila_fantasma_real_se_detecta_aunque_aparezca_en_medio(self):
        fila = PAGINA_SOBRESEGMENTADA_19_COLUMNAS[3]
        self.assertTrue(_fila_parece_fantasma(fila))

        limpia = _limpiar_filas_fantasma(PAGINA_SOBRESEGMENTADA_19_COLUMNAS)
        self.assertNotIn(fila, limpia)

    def test_fila_escasa_pero_semantica_no_se_elimina(self):
        fila = ["", "Sección general", "", "", "Vigencia aplicable", ""]
        tabla = [
            TABLA_SIMPLE_VALIDA[0] + ["", "", ""],
            fila,
            TABLA_SIMPLE_VALIDA[1] + ["", "", ""],
        ]
        self.assertFalse(_fila_parece_fantasma(fila))
        self.assertIn(fila, _limpiar_filas_fantasma(tabla))

    def test_fila_totalmente_vacia_no_es_fantasma(self):
        self.assertFalse(_fila_parece_fantasma(["", "", "", "", "", ""]))


class EvaluacionCandidatosTest(unittest.TestCase):
    def test_pagina_editorial_no_se_acepta_como_tabla_gigante(self):
        candidato = evaluar(
            PAGINA_EDITORIAL_DOS_COLUMNAS,
            strategy="text",
            bbox=(28.0, 62.0, 454.0, 752.0),
            page_width=489.0,
            page_height=779.0,
        )
        self.assertFalse(candidato["accepted"])
        self.assertGreaterEqual(candidato["whole_page_table_penalty"], 0.55)
        self.assertGreaterEqual(candidato["prose_density_penalty"], 0.35)
        self.assertTrue(candidato["captures_both_editorial_columns"])

    def test_19_columnas_vacias_intercaladas_reciben_penalizacion_fuerte(self):
        candidato = evaluar(
            PAGINA_SOBRESEGMENTADA_19_COLUMNAS,
            bbox=(36.0, 78.0, 461.0, 648.0),
            page_width=489.0,
            page_height=779.0,
        )
        self.assertEqual(candidato["columns"], 19)
        self.assertGreaterEqual(candidato["empty_column_ratio"], 0.30)
        self.assertGreaterEqual(candidato["alternating_empty_columns_score"], 0.20)
        self.assertGreaterEqual(candidato["oversegmentation_score"], 0.30)
        self.assertFalse(candidato["accepted"])

    def test_fragmentacion_excesiva_rechaza_el_candidato(self):
        candidato = evaluar([
            ["Código", "Descripción", "B", "OTI", "QUÍN", "Estado"],
            ["20", "Texto de la disposición", "0", ".1", "ABC", "NA"],
            ["21", "Otro texto de disposición", "0", ".5", "ABC", "NA"],
        ])
        self.assertGreaterEqual(candidato["fragmentation_score"], 0.15)
        self.assertFalse(candidato["accepted"])
        self.assertIn("fragmentation_excessive", candidato["reasons"])

    def test_tabla_simple_valida_sigue_aceptada(self):
        candidato = evaluar(TABLA_SIMPLE_VALIDA)
        self.assertTrue(candidato["accepted"])
        self.assertGreaterEqual(candidato["candidate_confidence"], 0.68)

    def test_tabla_sin_lineas_bien_alineada_puede_aceptarse(self):
        candidato = evaluar(
            TABLA_SIMPLE_VALIDA,
            strategy="text",
            bbox=(60.0, 180.0, 430.0, 330.0),
        )
        tablas, diagnosticos, regiones = _seleccionar_candidatos([candidato])
        self.assertEqual(tablas, [TABLA_SIMPLE_VALIDA])
        self.assertTrue(diagnosticos[0]["selected"])
        self.assertEqual(regiones["accepted_regions"], 1)
        self.assertEqual(regiones["unresolved_regions"], 0)

    def test_30_por_ciento_columnas_vacias_es_blocker_aunque_supere_umbral(self):
        candidato = evaluar(TABLA_30_POR_CIENTO_COLUMNAS_VACIAS)

        self.assertAlmostEqual(candidato["candidate_confidence"], 0.69, places=2)
        self.assertIn("column_oversegmentation", candidato["structural_blockers"])
        self.assertIn("too_many_empty_columns", candidato["structural_blockers"])
        self.assertFalse(candidato["accepted"])

    def test_fragmentacion_en_limite_es_blocker_aunque_confianza_sea_alta(self):
        candidato = evaluar(TABLA_FRAGMENTADA_EN_LIMITE)

        self.assertAlmostEqual(candidato["fragmentation_score"], 0.15, places=2)
        self.assertGreater(candidato["candidate_confidence"], 0.68)
        self.assertIn("fragmentation_excessive", candidato["structural_blockers"])
        self.assertFalse(candidato["accepted"])

    def test_bonus_entre_motores_no_rescata_blocker(self):
        pdfplumber = evaluar(
            TABLA_30_POR_CIENTO_COLUMNAS_VACIAS,
            extractor="pdfplumber",
        )
        pymupdf = evaluar(
            TABLA_30_POR_CIENTO_COLUMNAS_VACIAS,
            extractor="pymupdf",
        )

        _agregar_acuerdo_entre_extractores([pdfplumber, pymupdf])

        self.assertGreaterEqual(pdfplumber["candidate_confidence"], 0.68)
        self.assertTrue(pdfplumber["structural_blockers"])
        self.assertFalse(pdfplumber["accepted"])

    def test_region_aceptada_y_region_insegura_mantienen_revision_pendiente(self):
        segura = evaluar(
            TABLA_SIMPLE_VALIDA,
            bbox=(40.0, 80.0, 240.0, 220.0),
        )
        insegura = evaluar(
            TABLA_30_POR_CIENTO_COLUMNAS_VACIAS,
            bbox=(270.0, 360.0, 470.0, 650.0),
        )

        deteccion = _consolidar_deteccion_candidatos([segura, insegura])

        self.assertEqual(deteccion["tables"], [TABLA_SIMPLE_VALIDA])
        self.assertTrue(deteccion["table_requires_review"])
        self.assertEqual(deteccion["regions"]["detected_regions"], 2)
        self.assertEqual(deteccion["regions"]["accepted_regions"], 1)
        self.assertEqual(deteccion["regions"]["unresolved_regions"], 1)
        self.assertEqual(sum(d["selected"] for d in deteccion["candidates"]), 1)

    def test_acuerdo_independiente_aumenta_confianza_sin_reemplazar_metricas(self):
        pdfplumber = evaluar(TABLA_SIMPLE_VALIDA, extractor="pdfplumber")
        pymupdf = evaluar(TABLA_SIMPLE_VALIDA, extractor="pymupdf")
        confianza_inicial = pdfplumber["candidate_confidence"]

        _agregar_acuerdo_entre_extractores([pdfplumber, pymupdf])

        self.assertEqual(pdfplumber["agreement_extractors"], ["pdfplumber", "pymupdf"])
        self.assertAlmostEqual(
            pdfplumber["candidate_confidence"],
            min(1.0, confianza_inicial + 0.06),
            places=4,
        )


class FronterasEstructuralesTest(unittest.TestCase):
    @staticmethod
    def tabla_con_columnas_casi_vacias(columnas, casi_vacias):
        tabla = [
            [f"H{columna + 1}" for columna in range(columnas)]
        ] + [
            [f"R{fila}C{columna}" for columna in range(columnas)]
            for fila in range(1, 10)
        ]
        for columna in range(columnas - casi_vacias, columnas):
            for fila in range(2, 10):
                tabla[fila][columna] = ""
        return tabla

    def test_20_por_ciento_columnas_casi_vacias_falla_cerrado(self):
        candidato = evaluar(self.tabla_con_columnas_casi_vacias(5, 1))
        self.assertAlmostEqual(candidato["near_empty_column_ratio"], 0.20)
        self.assertIn("sparse_column_pattern", candidato["structural_blockers"])
        self.assertFalse(candidato["accepted"])

    def test_25_por_ciento_columnas_casi_vacias_falla_cerrado(self):
        candidato = evaluar(self.tabla_con_columnas_casi_vacias(8, 2))
        self.assertAlmostEqual(candidato["near_empty_column_ratio"], 0.25)
        self.assertIn("sparse_column_pattern", candidato["structural_blockers"])
        self.assertFalse(candidato["accepted"])

    def test_30_y_40_por_ciento_columnas_vacias_se_bloquean(self):
        for casi_vacias, esperado in ((3, 0.30), (4, 0.40)):
            with self.subTest(casi_vacias=casi_vacias):
                candidato = evaluar(self.tabla_con_columnas_casi_vacias(10, casi_vacias))
                self.assertAlmostEqual(candidato["near_empty_column_ratio"], esperado)
                self.assertFalse(candidato["accepted"])

    def test_columna_extremadamente_dominante_se_bloquea(self):
        tabla = [
            ["Código", "Tipo", "Nivel", "Valor", "Clase", "Descripción"],
            *[
                [
                    f"C{fila}", "X1", "N1", "V1", "K1",
                    "Texto legal extremadamente largo que concentra la extracción " * 12,
                ]
                for fila in range(1, 8)
            ],
        ]
        candidato = evaluar(tabla)
        self.assertGreaterEqual(candidato["dominant_column_ratio"], 0.82)
        self.assertIn("dominant_column_imbalance", candidato["structural_blockers"])
        self.assertFalse(candidato["accepted"])

    def test_20_columnas_densas_legitimas_no_se_rechazan_por_ancho(self):
        tabla = [[f"C{columna:02d}" for columna in range(20)]] + [
            [f"R{fila}C{columna}" for columna in range(20)]
            for fila in range(1, 8)
        ]
        candidato = evaluar(tabla)
        self.assertEqual(candidato["columns"], 20)
        self.assertEqual(candidato["empty_column_ratio"], 0.0)
        self.assertTrue(candidato["accepted"])

    def test_layout_editorial_de_20_columnas_se_rechaza_por_combinacion(self):
        tabla = [[f"Bloque editorial número {columna}" for columna in range(20)]] + [
            [f"Párrafo narrativo extenso de la página oficial en bloque {columna}" for columna in range(20)]
            for _ in range(8)
        ]
        candidato = evaluar(
            tabla,
            strategy="text",
            bbox=(5.0, 5.0, 495.0, 795.0),
        )
        self.assertFalse(candidato["accepted"])
        self.assertIn("editorial_prose_layout", candidato["structural_blockers"])

    def test_tabla_legitima_de_pagina_completa_se_acepta(self):
        tabla = [["Código", "Categoría", "Valor", "Estado", "Fecha"]] + [
            [f"C-{fila}", f"CAT-{fila}", f"{fila}%", "Vigente", f"2026-09-{fila:02d}"]
            for fila in range(1, 20)
        ]
        candidato = evaluar(tabla, bbox=(5.0, 5.0, 495.0, 795.0))
        self.assertTrue(candidato["whole_page_like"])
        self.assertTrue(candidato["accepted"])

    def test_tabla_juridica_multilinea_se_acepta(self):
        tabla = [
            ["Código", "Infracción", "Sanción", "Medida"],
            [
                "A-1",
                "Incumplir la obligación sanitaria descrita\ncuando exista riesgo comprobado",
                "Multa graduada según los criterios legales\ny antecedentes del administrado",
                "Suspensión temporal hasta acreditar\nla subsanación completa",
            ],
            [
                "A-2",
                "No conservar los registros exigidos\ndurante el plazo establecido",
                "Amonestación o multa proporcional\nsegún la gravedad comprobada",
                "Presentar un plan documentado\ndentro del plazo otorgado",
            ],
            [
                "A-3",
                "Omitir la comunicación obligatoria\na la autoridad sanitaria",
                "Multa según riesgo y reincidencia\ndebidamente acreditada",
                "Retiro preventivo y comunicación\na los usuarios involucrados",
            ],
        ]
        candidato = evaluar(tabla)
        self.assertGreater(candidato["multiline_cell_ratio"], 0.0)
        self.assertGreaterEqual(candidato["semantic_anchor_ratio"], 0.70)
        self.assertTrue(candidato["accepted"])

    def test_alternancia_vacio_contenido_se_bloquea(self):
        tabla = self.tabla_con_columnas_casi_vacias(7, 0)
        for columna in (1, 3, 5):
            for fila in range(2, 10):
                tabla[fila][columna] = ""
        candidato = evaluar(tabla)
        self.assertGreaterEqual(candidato["alternating_empty_columns_score"], 0.20)
        self.assertFalse(candidato["accepted"])

    def test_fragmentacion_decimal_unidad_se_bloquea(self):
        candidato = evaluar([
            ["Código", "Valor", "Decimal", "Unidad"],
            ["A-1", "0", ".1", "UIT"],
            ["A-2", "0", ".5", "UIT"],
        ])
        self.assertIn("fragmentation_excessive", candidato["structural_blockers"])
        self.assertFalse(candidato["accepted"])


class AgrupacionMultirregionTest(unittest.TestCase):
    def test_candidata_puente_no_fusiona_regiones_sin_solapamiento(self):
        superior = evaluar(TABLA_SIMPLE_VALIDA, bbox=(0.0, 0.0, 100.0, 100.0))
        puente = evaluar(
            TABLA_30_POR_CIENTO_COLUMNAS_VACIAS,
            bbox=(0.0, 0.0, 100.0, 210.0),
            extractor="pymupdf",
        )
        inferior = evaluar(
            TABLA_30_POR_CIENTO_COLUMNAS_VACIAS,
            bbox=(0.0, 110.0, 100.0, 210.0),
        )
        deteccion = _consolidar_deteccion_candidatos([superior, puente, inferior])
        self.assertGreaterEqual(deteccion["regions"]["detected_regions"], 2)
        self.assertNotEqual(
            deteccion["candidates"][0]["region_id"],
            deteccion["candidates"][2]["region_id"],
        )
        self.assertTrue(deteccion["table_requires_review"])

    def test_misma_tabla_dos_motores_comparte_region(self):
        primero = evaluar(
            TABLA_SIMPLE_VALIDA,
            bbox=(50.0, 100.0, 450.0, 400.0),
            extractor="pdfplumber",
        )
        segundo = evaluar(
            TABLA_SIMPLE_VALIDA,
            bbox=(53.0, 102.0, 447.0, 398.0),
            extractor="pymupdf",
        )
        deteccion = _consolidar_deteccion_candidatos([primero, segundo])
        self.assertEqual(deteccion["regions"]["detected_regions"], 1)

    def test_dos_tablas_verticales_proximas_siguen_separadas(self):
        superior = evaluar(TABLA_SIMPLE_VALIDA, bbox=(40.0, 40.0, 240.0, 190.0))
        inferior = evaluar(TABLA_SIMPLE_VALIDA, bbox=(40.0, 200.0, 240.0, 350.0))
        deteccion = _consolidar_deteccion_candidatos([superior, inferior])
        self.assertEqual(deteccion["regions"]["detected_regions"], 2)

    def test_tabla_anidada_no_se_confunde_automaticamente_con_exterior(self):
        exterior = evaluar(TABLA_SIMPLE_VALIDA, bbox=(0.0, 0.0, 100.0, 100.0))
        interior = evaluar(TABLA_SIMPLE_VALIDA, bbox=(15.0, 15.0, 85.0, 85.0))
        deteccion = _consolidar_deteccion_candidatos([exterior, interior])
        self.assertEqual(deteccion["regions"]["detected_regions"], 2)

    def test_tablas_laterales_siguen_separadas(self):
        izquierda = evaluar(TABLA_SIMPLE_VALIDA, bbox=(20.0, 100.0, 220.0, 350.0))
        derecha = evaluar(TABLA_SIMPLE_VALIDA, bbox=(230.0, 100.0, 430.0, 350.0))
        deteccion = _consolidar_deteccion_candidatos([izquierda, derecha])
        self.assertEqual(deteccion["regions"]["detected_regions"], 2)


class DegradacionConservadoraTest(unittest.TestCase):
    def test_extract_pdf_liga_paginas_al_sha256_de_la_fuente(self):
        contenido = b"pdf oficial de prueba"
        page = PageExtraction(
            page_number=1,
            text="Texto fuente",
            method="pymupdf",
            quality=1.0,
            ocr_used=False,
        )
        with tempfile.TemporaryDirectory() as directorio:
            ruta = Path(directorio) / "fuente.pdf"
            ruta.write_bytes(contenido)
            documento = MagicMock()
            documento.__enter__.return_value = [Mock()]
            with (
                patch("agents.pdf_extract.fitz.open", return_value=documento),
                patch("agents.pdf_extract.extract_page", return_value=page),
            ):
                resultado = extract_pdf(str(ruta))

        self.assertEqual(
            resultado[0].source_pdf_sha256,
            hashlib.sha256(contenido).hexdigest(),
        )

    def test_candidatos_inseguros_no_contaminan_texto_plano(self):
        texto_oficial = "Texto plano oficial conservado sin asociaciones añadidas."
        page = Mock()
        page.get_text.return_value = texto_oficial

        deteccion_rechazada = {
            "tables": [],
            "possible_table": True,
            "table_requires_review": True,
            "table_quality_score": 0.31,
            "layout_quality_score": 0.44,
            "candidates": [{"accepted": False, "reasons": ["below_confidence_threshold"]}],
            "regions": {
                "detected_regions": 1,
                "accepted_regions": 0,
                "unresolved_regions": 1,
                "regions": [],
            },
        }
        with (
            patch("agents.pdf_extract.es_pagina_en_blanco", return_value=False),
            patch("agents.pdf_extract._detectar_tablas", return_value=deteccion_rechazada),
            patch("agents.pdf_extract.posible_grafico", return_value=False),
        ):
            resultado = extract_page("documento-local.pdf", page, 0)

        self.assertEqual(resultado.text, texto_oficial)
        self.assertTrue(resultado.has_tables)
        self.assertTrue(resultado.possible_table)
        self.assertTrue(resultado.table_requires_review)
        self.assertIsNone(resultado.tables)
        self.assertEqual(resultado.quality, resultado.text_quality_score)
        self.assertEqual(resultado.table_quality_score, 0.31)

    def test_tabla_aceptada_no_se_agrega_al_texto_raw(self):
        texto_oficial = "Texto narrativo original de la página."
        page = Mock()
        page.get_text.return_value = texto_oficial
        deteccion_aceptada = {
            "tables": [TABLA_SIMPLE_VALIDA],
            "possible_table": True,
            "table_requires_review": False,
            "table_quality_score": 0.96,
            "layout_quality_score": 1.0,
            "candidates": [{"accepted": True, "selected": True}],
            "regions": {
                "detected_regions": 1,
                "accepted_regions": 1,
                "unresolved_regions": 0,
                "regions": [],
            },
        }
        with (
            patch("agents.pdf_extract.es_pagina_en_blanco", return_value=False),
            patch("agents.pdf_extract._detectar_tablas", return_value=deteccion_aceptada),
            patch("agents.pdf_extract.posible_grafico", return_value=False),
        ):
            resultado = extract_page("documento-local.pdf", page, 0)

        self.assertEqual(resultado.text, texto_oficial)
        self.assertEqual(resultado.tables, [TABLA_SIMPLE_VALIDA])
        self.assertNotIn("| Código", resultado.text)

    def test_write_pages_separa_raw_y_markdown_estructurado(self):
        page = PageExtraction(
            page_number=1,
            text="Texto fuente sin tabla reconstruida.",
            method="pymupdf",
            quality=1.0,
            ocr_used=False,
            has_tables=True,
            tables=[TABLA_SIMPLE_VALIDA],
            text_quality_score=1.0,
            table_quality_score=0.96,
            layout_quality_score=1.0,
            possible_table=True,
            table_requires_review=False,
            table_region_summary={
                "detected_regions": 1,
                "accepted_regions": 1,
                "unresolved_regions": 0,
            },
            source_pdf_sha256="a" * 64,
        )
        supabase = Mock()
        consulta = supabase.table.return_value
        consulta.insert.return_value.execute.return_value = Mock()

        write_pages(supabase, "norma-1", [page])

        payload = consulta.insert.call_args.args[0]
        self.assertEqual(payload["text_raw"], page.text)
        self.assertEqual(payload["text_normalized"], page.text)
        self.assertNotIn("| Código", payload["text_raw"])
        self.assertIn("| Código", payload["metadata"]["tables_markdown"])
        self.assertEqual(payload["metadata"]["tables"], [TABLA_SIMPLE_VALIDA])
        self.assertEqual(payload["metadata"]["table_regions"]["accepted_regions"], 1)
        self.assertEqual(payload["metadata"]["source_pdf_sha256"], "a" * 64)


class TablaAMarkdownTest(unittest.TestCase):
    def test_serializa_tabla_aceptada_sin_reconstruir_encabezados(self):
        markdown = _tabla_a_markdown(TABLA_SIMPLE_VALIDA)
        self.assertIn("Código", markdown)
        self.assertIn("Descripción", markdown)
        self.assertIn("Registro complementario", markdown)

    def test_varias_tablas_conservan_bloques_separados(self):
        markdown = tablas_a_markdown([TABLA_SIMPLE_VALIDA, TABLA_SIMPLE_VALIDA])
        self.assertIn("Tabla 1:", markdown)
        self.assertIn("Tabla 2:", markdown)

    def test_serializa_pipe_y_backslash_sin_ambiguedad(self):
        tabla = [
            ["A|B", r"Ruta\legal"],
            [r"\|", "fin\\"],
        ]
        markdown = _tabla_a_markdown(tabla)

        self.assertIn(r"A\|B", markdown)
        self.assertIn(r"Ruta\\legal", markdown)
        self.assertIn(r"\\\|", markdown)
        self.assertIn(r"fin\\", markdown)

    def test_no_promueve_un_titulo_previo_a_encabezado(self):
        tabla = [
            ["Título general", "", ""],
            ["Código", "Descripción", "Estado"],
            ["A-1", "Registro inicial", "Vigente"],
        ]
        markdown = _tabla_a_markdown(tabla)
        lineas = markdown.splitlines()
        self.assertEqual([celda.strip() for celda in lineas[0].strip("|").split("|")], ["", "", ""])
        self.assertIn("Título general", lineas[2])
        self.assertIn("Código", lineas[3])


if __name__ == "__main__":
    unittest.main()
