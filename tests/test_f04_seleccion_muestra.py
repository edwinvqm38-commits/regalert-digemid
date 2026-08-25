"""Regresión de la selección de muestra F-04-A (fidelidad de transcripción).

F-04 solo puede estudiar la fidelidad de una transcripción si los tres
escalones anteriores de la cadena ya están resueltos: identidad normativa
correcta (F-03), documento completo (F-02) y SHA256 conocido. Cada test aquí
corresponde a una forma concreta en la que una página NO debería poder
colarse en el piloto, o en la que dos transcripciones que se ven parecidas
deberían marcarse como jurídicamente distintas.
"""

import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from custodia_documental import COMPLETO, INCOMPLETO  # noqa: E402
from fidelidad_legal import SenalesPagina, comparar_fidelidad  # noqa: E402
from identidad_documental import (  # noqa: E402
    PDF_CONTIENE_NORMA_EN_MULTINORMA,
    PDF_IDENTIDAD_AMBIGUA,
    PDF_IDENTIDAD_CONTRADICTORIA,
    PDF_IDENTIDAD_EXACTA,
    PDF_NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
)

from f04_seleccion_muestra import (  # noqa: E402
    COMPARACION_INCOMPLETA,
    apto_para_piloto_f04,
    completitud_para_f04,
    estado_verificacion_f04,
    fila_manifest,
    pagina_pertenece_a_norma,
    razones_de_riesgo,
    seleccionar_muestra,
    todas_las_comparaciones_completas,
)


def _fila_identidad(**overrides) -> dict:
    """Fila base con la forma exacta que produce
    scripts.auditar_identidad_documental.analizar_norma() para una norma
    única, ya aprobada por F-03. Los tests parten de esta y sobreescriben
    solo lo que quieren probar."""
    base = {
        "document_key": "RM-100-2020",
        "identity_expected": "RM 100-2020",
        "pdf_url": "https://www.gob.pe/x/rm-100-2020.pdf",
        "storage_path": "normas/rm-100-2020.pdf",
        "pdf_sha256": "a" * 64,
        "pdf_page_count": 5,
        "start_page": None,
        "end_page": None,
        "rango_completo": None,
        "document_type": "DOCUMENTO_NORMA_UNICA",
        "audit_complete": True,
        "classification": PDF_IDENTIDAD_EXACTA,
    }
    base.update(overrides)
    return base


class TestGateF03(unittest.TestCase):
    """Solo documentos F-03 válidos pueden entrar al piloto."""

    def test_pdf_identidad_exacta_es_apto(self):
        fila = _fila_identidad()
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertTrue(apto)

    def test_ambigua_queda_fuera(self):
        fila = _fila_identidad(classification=PDF_IDENTIDAD_AMBIGUA)
        apto, motivo = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)
        self.assertIn("PDF_IDENTIDAD_AMBIGUA", motivo)

    def test_contradictoria_queda_fuera(self):
        fila = _fila_identidad(classification=PDF_IDENTIDAD_CONTRADICTORIA)
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)

    def test_no_encontrada_queda_fuera(self):
        fila = _fila_identidad(classification=PDF_NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA)
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)

    def test_auditoria_f03_incompleta_queda_fuera_aunque_la_clasificacion_sea_apta(self):
        """Un PDF_IDENTIDAD_EXACTA obtenido con la auditoría recortada no
        prueba nada: el encabezado de otra norma pudo estar en la página que
        no se llegó a mirar."""
        fila = _fila_identidad(audit_complete=False)
        apto, motivo = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)
        self.assertIn("incompleta", motivo)

    def test_sin_pdf_disponible_queda_fuera(self):
        fila = _fila_identidad(classification="PDF_NO_DISPONIBLE")
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)


class TestMultinormaRespetaRangos(unittest.TestCase):
    def _fila_multinorma(self, **overrides):
        base = _fila_identidad(
            classification=PDF_CONTIENE_NORMA_EN_MULTINORMA,
            document_type="DOCUMENTO_MULTINORMA",
            rango_completo=True, start_page=12, end_page=18,
        )
        base.update(overrides)
        return base

    def test_multinorma_con_rango_cerrado_es_apto(self):
        fila = self._fila_multinorma()
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertTrue(apto)

    def test_multinorma_sin_rango_cerrado_no_es_apto(self):
        fila = self._fila_multinorma(rango_completo=False)
        apto, motivo = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)
        self.assertIn("rango", motivo)

    def test_multinorma_sin_paginas_resueltas_no_es_apto(self):
        fila = self._fila_multinorma(start_page=None, end_page=None)
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)

    def test_pagina_dentro_del_rango_pertenece(self):
        self.assertTrue(pagina_pertenece_a_norma(15, 12, 18))
        self.assertTrue(pagina_pertenece_a_norma(12, 12, 18))  # borde inicio
        self.assertTrue(pagina_pertenece_a_norma(18, 12, 18))  # borde fin

    def test_pagina_fuera_del_rango_no_pertenece(self):
        """Una página de OTRA norma en el mismo PDF multinorma nunca debe
        entrar al piloto de ESTA norma, aunque comparta norma_id en la
        consulta."""
        self.assertFalse(pagina_pertenece_a_norma(11, 12, 18))
        self.assertFalse(pagina_pertenece_a_norma(19, 12, 18))

    def test_completitud_multinorma_se_mide_dentro_del_rango_no_contra_el_pdf_entero(self):
        fila = self._fila_multinorma()
        estado, _ = completitud_para_f04(fila, [12, 13, 14, 15, 16, 17, 18])
        self.assertEqual(estado, COMPLETO)

    def test_completitud_multinorma_incompleta_si_falta_una_pagina_del_rango(self):
        fila = self._fila_multinorma()
        estado, motivo = completitud_para_f04(fila, [12, 13, 14, 16, 17, 18])  # falta 15
        self.assertEqual(estado, INCOMPLETO)
        self.assertIn("15", motivo)

    def test_multinorma_incompleto_dentro_del_rango_no_es_apto(self):
        fila = self._fila_multinorma()
        estado, _ = completitud_para_f04(fila, [12, 13, 14])  # faltan 15-18
        apto, _ = apto_para_piloto_f04(fila, estado)
        self.assertFalse(apto)


class TestShaObligatorio(unittest.TestCase):
    def test_sin_sha256_no_es_apto(self):
        fila = _fila_identidad(pdf_sha256=None)
        apto, motivo = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)
        self.assertIn("SHA256", motivo)

    def test_sha256_vacio_no_es_apto(self):
        fila = _fila_identidad(pdf_sha256="")
        apto, _ = apto_para_piloto_f04(fila, COMPLETO)
        self.assertFalse(apto)


class TestDocumentoIncompletoQuedaFuera(unittest.TestCase):
    def test_norma_unica_incompleta_no_es_apta_aunque_todo_lo_demas_este_bien(self):
        fila = _fila_identidad()
        apto, motivo = apto_para_piloto_f04(fila, INCOMPLETO)
        self.assertFalse(apto)
        self.assertIn("completo", motivo)

    def test_completitud_norma_unica_usa_evaluar_completitud_de_f02(self):
        fila = _fila_identidad(pdf_page_count=5)
        estado, _ = completitud_para_f04(fila, [1, 2, 3, 4, 5])
        self.assertEqual(estado, COMPLETO)
        estado, _ = completitud_para_f04(fila, [1, 2, 3])
        self.assertEqual(estado, INCOMPLETO)


class TestCalculoDeTokensLegales(unittest.TestCase):
    """F-04 exige detectar diferencias en los tokens jurídicamente sensibles;
    la métrica en sí vive en fidelidad_legal.py (F-01) y aquí se fija como
    contrato explícito de F-04, no se reimplementa."""

    def test_articulo_13_vs_articulo_18_genera_error_legal(self):
        r = comparar_fidelidad(
            "Artículo 13.- Deróguese la presente disposición",
            "Artículo 18.- Deróguese la presente disposición",
        )
        self.assertTrue(r.hay_error_juridico)
        self.assertIn("articulo", [e.categoria for e in r.errores_token])

    def test_rm_339_2023_vs_rm_339_2028_genera_error_legal(self):
        r = comparar_fidelidad(
            "Resolución Ministerial N° 339-2023/MINSA",
            "Resolución Ministerial N° 339-2028/MINSA",
        )
        self.assertTrue(r.hay_error_juridico)

    def test_fechas_diferentes_se_detectan(self):
        r = comparar_fidelidad(
            "publicado el 15 de marzo de 2023", "publicado el 25 de marzo de 2023",
        )
        self.assertTrue(r.hay_error_juridico)
        self.assertIn("fecha", [e.categoria for e in r.errores_token])

    def test_plazos_diferentes_se_detectan(self):
        r = comparar_fidelidad("en un plazo de 10 días hábiles", "en un plazo de 30 días hábiles")
        self.assertTrue(r.hay_error_juridico)

    def test_porcentajes_diferentes_se_detectan(self):
        r = comparar_fidelidad("un descuento del 15%", "un descuento del 50%")
        self.assertTrue(r.hay_error_juridico)

    def test_razones_de_riesgo_incluye_token_sensible_por_categoria(self):
        pagina = {"text_normalized": "Artículo 5.- En un plazo de 10 días, con un 20% de margen"}
        razones = razones_de_riesgo(pagina, page_number=3, total_paginas=20)
        self.assertIn("token_sensible:articulo", razones)
        self.assertIn("token_sensible:plazo", razones)
        self.assertIn("token_sensible:porcentaje", razones)


class TestPaginaSinComparacionCompletaNoSeVerifica(unittest.TestCase):
    def test_todas_las_comparaciones_completas_exige_los_tres_motores(self):
        self.assertTrue(todas_las_comparaciones_completas(
            {"pymupdf": "texto", "pdfplumber": "texto", "ocr_tesseract": "texto"}))
        self.assertFalse(todas_las_comparaciones_completas(
            {"pymupdf": "texto", "pdfplumber": "texto", "ocr_tesseract": None}))
        self.assertFalse(todas_las_comparaciones_completas(
            {"pymupdf": "texto", "pdfplumber": None, "ocr_tesseract": None}))

    def test_texto_vacio_en_los_tres_motores_si_cuenta_como_completa(self):
        """Una página realmente en blanco es '' en los tres motores, y eso
        es una comparación completa -distinto de que un motor haya fallado."""
        self.assertTrue(todas_las_comparaciones_completas(
            {"pymupdf": "", "pdfplumber": "", "ocr_tesseract": ""}))

    def test_motor_faltante_nunca_produce_estado_verificada(self):
        senales = SenalesPagina(extraction_method="pymupdf", quality_score=1.0, texto="Artículo 1.- Deróguese")
        estado, _, motivos = estado_verificacion_f04(
            {"pymupdf": "Artículo 1.- Deróguese", "pdfplumber": "Artículo 1.- Deróguese", "ocr_tesseract": None},
            senales,
        )
        self.assertEqual(estado, COMPARACION_INCOMPLETA)
        self.assertNotIn("VERIFICADA", estado)
        self.assertTrue(any("ocr_tesseract" in m for m in motivos))

    def test_comparacion_completa_y_concordante_si_puede_verificar(self):
        texto = "Artículo 1.- Deróguese la Resolución Ministerial N° 100-2020/MINSA"
        senales = SenalesPagina(extraction_method="pymupdf", quality_score=1.0, texto=texto)
        estado, _, _ = estado_verificacion_f04(
            {"pymupdf": texto, "pdfplumber": texto, "ocr_tesseract": texto}, senales,
        )
        self.assertEqual(estado, "VERIFICADA_AUTOMATICAMENTE")

    def test_comparacion_completa_pero_discordante_en_token_juridico_no_verifica(self):
        senales = SenalesPagina(extraction_method="pymupdf", quality_score=1.0,
                                texto="Artículo 13.- Deróguese la norma")
        estado, _, _ = estado_verificacion_f04(
            {"pymupdf": "Artículo 13.- Deróguese la norma",
             "pdfplumber": "Artículo 18.- Deróguese la norma",
             "ocr_tesseract": "Artículo 13.- Deróguese la norma"},
            senales,
        )
        self.assertNotEqual(estado, "VERIFICADA_AUTOMATICAMENTE")
        self.assertEqual(estado, "DISCREPANCIA_ENTRE_MOTORES")

    def test_pymupdf_y_pdfplumber_de_acuerdo_no_bastan_si_el_render_discrepa(self):
        """PyMuPDF y pdfplumber leen la MISMA capa embebida: que concuerden
        entre sí no es evidencia independiente si esa capa está mal. Solo el
        cruce contra el render (Tesseract, una fuente de lectura distinta)
        puede verificar. Antes de esta corrección, dos lecturas de la misma
        capa que coincidían ya bastaban para VERIFICADA_AUTOMATICAMENTE,
        aunque el render dijera otra cosa."""
        senales = SenalesPagina(extraction_method="pymupdf", quality_score=1.0,
                                texto="Artículo 13.- Deróguese la norma")
        estado, _, _ = estado_verificacion_f04(
            {"pymupdf": "Artículo 13.- Deróguese la norma",
             "pdfplumber": "Artículo 13.- Deróguese la norma",
             "ocr_tesseract": "Artículo 18.- Deróguese la norma"},
            senales,
        )
        self.assertNotEqual(estado, "VERIFICADA_AUTOMATICAMENTE")
        self.assertEqual(estado, "DISCREPANCIA_ENTRE_MOTORES")

    def test_verificacion_exige_acuerdo_con_el_render_no_solo_entre_parsers(self):
        """Caso positivo simétrico: los 3 motores deben concordar -no basta
        con que los dos parsers de la capa embebida concuerden entre sí-."""
        texto = "Artículo 1.- Deróguese la Resolución Ministerial N° 100-2020/MINSA"
        senales = SenalesPagina(extraction_method="pymupdf", quality_score=1.0, texto=texto)
        estado, _, _ = estado_verificacion_f04(
            {"pymupdf": texto, "pdfplumber": texto, "ocr_tesseract": texto}, senales,
        )
        self.assertEqual(estado, "VERIFICADA_AUTOMATICAMENTE")


class TestSeleccionDeMuestra(unittest.TestCase):
    def test_candidatas_sin_razon_de_riesgo_se_descartan(self):
        candidatas = [
            {"document_key": "A", "razon_de_riesgo": ""},
            {"document_key": "B", "razon_de_riesgo": "pagina_dispositiva"},
        ]
        seleccion = seleccionar_muestra(candidatas, limite=10)
        self.assertEqual(len(seleccion), 1)
        self.assertEqual(seleccion[0]["document_key"], "B")

    def test_respeta_el_limite(self):
        candidatas = [
            {"document_key": f"DOC-{i}", "razon_de_riesgo": "pagina_dispositiva"} for i in range(80)
        ]
        seleccion = seleccionar_muestra(candidatas, limite=50)
        self.assertEqual(len(seleccion), 50)

    def test_un_solo_documento_no_agota_el_cupo(self):
        """Muchas páginas de alto riesgo del MISMO documento no deben llenar
        toda la muestra: F-04 necesita diversidad de normas."""
        candidatas = [
            {"document_key": "DOC-LARGO", "razon_de_riesgo": "pagina_dispositiva"} for _ in range(80)
        ]
        seleccion = seleccionar_muestra(candidatas, limite=50)
        self.assertLess(len(seleccion), 50)

    def test_prioriza_mas_senales_de_riesgo(self):
        candidatas = [
            {"document_key": "A", "razon_de_riesgo": "quality_score_bajo"},
            {"document_key": "B", "razon_de_riesgo": "pagina_dispositiva;primera_pagina;ocr_baja_confianza"},
        ]
        seleccion = seleccionar_muestra(candidatas, limite=1)
        self.assertEqual(seleccion[0]["document_key"], "B")


class TestFilaManifest(unittest.TestCase):
    def test_incluye_todas_las_columnas_minimas_requeridas(self):
        fila = _fila_identidad()
        pagina = {
            "page_number": 2, "text_normalized": "Artículo 1.- Deróguese",
            "extraction_method": "pymupdf", "quality_score": 0.95, "ocr_used": False,
        }
        manifest = fila_manifest(fila, pagina, ["pagina_dispositiva"])
        columnas_requeridas = {
            "document_key", "identidad_normativa", "pdf_url", "storage_path",
            "pdf_sha256", "page_number", "pdf_page_count",
            "rango_documental_multinorma", "extraction_method", "quality_score",
            "ocr_used", "razon_de_riesgo", "texto_almacenado", "f03_classification",
        }
        self.assertTrue(columnas_requeridas.issubset(manifest.keys()))

    def test_rango_documental_presente_solo_en_multinorma(self):
        fila_unica = _fila_identidad()
        pagina = {"page_number": 1, "text_normalized": ""}
        self.assertIsNone(fila_manifest(fila_unica, pagina, [])["rango_documental_multinorma"])

        fila_multi = _fila_identidad(classification=PDF_CONTIENE_NORMA_EN_MULTINORMA,
                                     start_page=12, end_page=18)
        self.assertEqual(fila_manifest(fila_multi, pagina, [])["rango_documental_multinorma"], "12-18")


if __name__ == "__main__":
    unittest.main(verbosity=2)
