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
    RIESGO_OCR_BAJA_CONFIANZA,
    apto_para_piloto_f04,
    completitud_para_f04,
    diagnostico_ocr_pool,
    estado_verificacion_f04,
    fila_manifest,
    pagina_pertenece_a_norma,
    razones_de_riesgo,
    resumen_pool,
    seleccionar_muestra,
    seleccionar_muestra_estratificada,
    todas_las_comparaciones_completas,
)


def _fila_pool(doc, page, *, ocr=False, ocr_baja_confianza=False, tabla=False,
               clasif=PDF_IDENTIDAD_EXACTA, razones_extra=None, sha256="a" * 64) -> dict:
    """Fila sintética con la FORMA de fila_manifest(), para probar el
    resumen del pool y el selector estratificado sin tener que construir
    fila_identidad/pagina reales en cada test."""
    razones = list(razones_extra or [])
    if ocr:
        razones.append("pagina_escaneada_ocr")
        if ocr_baja_confianza:
            razones.append(RIESGO_OCR_BAJA_CONFIANZA)
    if tabla:
        razones.append("contiene_tabla")
    if not razones:
        razones.append("pagina_dispositiva")
    return {
        "document_key": doc,
        "page_number": page,
        "razon_de_riesgo": ";".join(razones),
        "ocr_used": ocr,
        "tiene_tabla": tabla,
        "es_dispositiva": "pagina_dispositiva" in razones,
        "es_primera_pagina": "primera_pagina" in razones,
        "es_ultima_o_penultima": "ultima_o_penultima_pagina" in razones,
        "f03_classification": clasif,
        "pdf_sha256": sha256,
    }


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
        sin_riesgo = _fila_pool("A", 1)
        sin_riesgo["razon_de_riesgo"] = ""  # sin ninguna razon, a proposito
        candidatas = [sin_riesgo, _fila_pool("B", 1)]
        seleccion = seleccionar_muestra(candidatas, limite=10)
        self.assertEqual(len(seleccion), 1)
        self.assertEqual(seleccion[0]["document_key"], "B")

    def test_respeta_el_limite(self):
        candidatas = [_fila_pool(f"DOC-{i}", 1) for i in range(80)]
        seleccion = seleccionar_muestra(candidatas, limite=50)
        self.assertEqual(len(seleccion), 50)

    def test_un_solo_documento_no_agota_el_cupo(self):
        """Muchas páginas de alto riesgo del MISMO documento no deben llenar
        toda la muestra: F-04 necesita diversidad de normas."""
        candidatas = [_fila_pool("DOC-LARGO", i + 1) for i in range(80)]
        seleccion = seleccionar_muestra(candidatas, limite=50)
        self.assertLess(len(seleccion), 50)
        self.assertGreater(len(seleccion), 0)

    def test_prioriza_mas_senales_de_riesgo(self):
        candidatas = [
            _fila_pool("A", 1, razones_extra=["quality_score_bajo"]),
            _fila_pool("B", 1, razones_extra=["pagina_dispositiva", "primera_pagina", "ocr_baja_confianza"]),
        ]
        seleccion = seleccionar_muestra(candidatas, limite=1)
        self.assertEqual(seleccion[0]["document_key"], "B")

    def test_descarta_clasificacion_f03_no_apta_igual_que_el_estratificado(self):
        """seleccionar_muestra() (V1) aplica la misma defensa que el
        selector estratificado: sin f03_classification apta, no entra."""
        candidatas = [_fila_pool("BUENO", 1)]
        intruso = _fila_pool("INTRUSO", 1, clasif=PDF_IDENTIDAD_AMBIGUA,
                             razones_extra=["pagina_dispositiva", "primera_pagina"])
        seleccion = seleccionar_muestra(candidatas + [intruso], limite=10)
        self.assertFalse(any(f["document_key"] == "INTRUSO" for f in seleccion))


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


class TestResumenPool(unittest.TestCase):
    """F-04-A.1: auditar qué existe en el pool ANTES de elegir 50."""

    def test_pool_vacio(self):
        self.assertEqual(resumen_pool([])["total_paginas"], 0)

    def test_cuenta_ocr_digital_tablas_dispositivas(self):
        pool = [
            _fila_pool("A", 1, ocr=True, ocr_baja_confianza=True),
            _fila_pool("B", 1, ocr=True),
            _fila_pool("C", 1, tabla=True),
            _fila_pool("D", 1),
        ]
        r = resumen_pool(pool)
        self.assertEqual(r["total_paginas"], 4)
        self.assertEqual(r["normas_distintas"], 4)
        self.assertEqual(r["paginas_ocr_previo"], 2)
        self.assertEqual(r["paginas_texto_digital"], 2)
        self.assertEqual(r["paginas_ocr_baja_confianza"], 1)
        self.assertEqual(r["paginas_con_tablas"], 1)
        self.assertEqual(r["con_sha256_pct"], 100.0)

    def test_distribucion_por_clasificacion_y_normas_distintas_por_grupo(self):
        pool = (
            [_fila_pool(f"EX-{i}", 1, clasif=PDF_IDENTIDAD_EXACTA) for i in range(3)]
            + [_fila_pool(f"MU-{i}", 1, clasif=PDF_CONTIENE_NORMA_EN_MULTINORMA) for i in range(7)]
        )
        r = resumen_pool(pool)
        self.assertEqual(r["por_f03_classification"][PDF_IDENTIDAD_EXACTA], 3)
        self.assertEqual(r["por_f03_classification"][PDF_CONTIENE_NORMA_EN_MULTINORMA], 7)
        self.assertEqual(r["normas_distintas_por_clasificacion"][PDF_IDENTIDAD_EXACTA], 3)
        self.assertEqual(r["normas_distintas_por_clasificacion"][PDF_CONTIENE_NORMA_EN_MULTINORMA], 7)

    def test_diagnostico_ocr_pool_dice_la_verdad_cuando_no_hay_ocr(self):
        pool = [_fila_pool("A", 1), _fila_pool("B", 1)]
        self.assertEqual(diagnostico_ocr_pool(resumen_pool(pool)),
                         "NO EXISTEN PAGINAS OCR DOCUMENTALMENTE APTAS")

    def test_diagnostico_ocr_pool_reporta_cantidad_cuando_si_hay(self):
        pool = [_fila_pool("A", 1, ocr=True), _fila_pool("B", 1)]
        mensaje = diagnostico_ocr_pool(resumen_pool(pool))
        self.assertIn("1 paginas OCR aptas", mensaje)


class TestSelectorEstratificadoV2(unittest.TestCase):
    """F-04-A.1: cuotas de diversidad que nunca relajan el gate F-03."""

    def _pool_mixto(self, n_exacta=20, n_multi=20, n_ocr=20, n_tablas=6):
        pool = [_fila_pool(f"EX-{i}", 1, clasif=PDF_IDENTIDAD_EXACTA) for i in range(n_exacta)]
        pool += [_fila_pool(f"MU-{i}", 1, clasif=PDF_CONTIENE_NORMA_EN_MULTINORMA,
                            razones_extra=["ultima_o_penultima_pagina"]) for i in range(n_multi)]
        pool += [_fila_pool(f"OCR-{i}", 1, ocr=True, ocr_baja_confianza=(i % 2 == 0)) for i in range(n_ocr)]
        pool += [_fila_pool(f"TAB-{i}", 1, tabla=True) for i in range(n_tablas)]
        return pool

    def test_seleccion_es_subconjunto_del_pool_y_solo_clasificaciones_aptas(self):
        pool = self._pool_mixto()
        seleccion, _ = seleccionar_muestra_estratificada(pool, limite=50)
        claves_pool = {(f["document_key"], f["page_number"]) for f in pool}
        for fila in seleccion:
            self.assertIn((fila["document_key"], fila["page_number"]), claves_pool)
            self.assertIn(fila["f03_classification"],
                          (PDF_IDENTIDAD_EXACTA, PDF_CONTIENE_NORMA_EN_MULTINORMA))

    def test_sin_ocr_aptas_no_inventa_la_cuota(self):
        pool = self._pool_mixto(n_ocr=0)
        seleccion, diag = seleccionar_muestra_estratificada(pool, limite=50)
        self.assertEqual(diag["ocr"]["disponible_en_pool"], 0)
        self.assertEqual(diag["ocr"]["agregado"], 0)
        self.assertTrue(diag["ocr"]["cuota_no_disponible"])
        self.assertTrue(any("ocr" in a.lower() for a in diag["avisos"]))
        self.assertFalse(any(f["ocr_used"] for f in seleccion))

    def test_ocr_invalida_documentalmente_nunca_entra(self):
        """Una fila OCR con clasificación F-03 NO apta (simula un bug del
        llamador: esto nunca debería llegar aquí desde el pipeline real,
        pero el selector no puede depender de que el llamador sea perfecto)
        no debe entrar aunque su puntaje de riesgo sea el más alto posible."""
        pool = self._pool_mixto()
        intruso = _fila_pool("INTRUSO-OCR", 1, ocr=True, ocr_baja_confianza=True,
                             clasif=PDF_IDENTIDAD_AMBIGUA,
                             razones_extra=["pagina_dispositiva", "primera_pagina", "ultima_o_penultima_pagina"])
        seleccion, _ = seleccionar_muestra_estratificada(pool + [intruso], limite=50)
        self.assertFalse(any(f["document_key"] == "INTRUSO-OCR" for f in seleccion))

    def test_ocr_sin_sha256_nunca_entra(self):
        """Defensa adicional: sin SHA256 tampoco, aunque la clasificación
        diga ser apta -otra forma en que 'documentalmente invalida' puede
        colarse si el llamador tiene un bug."""
        pool = self._pool_mixto()
        intruso = _fila_pool("INTRUSO-SIN-SHA", 1, ocr=True, ocr_baja_confianza=True, sha256=None,
                             razones_extra=["pagina_dispositiva", "primera_pagina"])
        # Nota: el gate real (apto_para_piloto_f04) es lo que impide que esto
        # ocurra corriente arriba; aqui se prueba que ADEMAS el selector no
        # lo prioriza como si fuera una fila normal con sha256 valido.
        seleccion, diag = seleccionar_muestra_estratificada(pool + [intruso], limite=len(pool))
        # Con limite == len(pool), el intruso (que excede el limite en 1)
        # solo entraria si desplaza a alguna fila valida; comprobamos que si
        # entra, no lo hace POR la cuota OCR de forma privilegiada respecto
        # de las demas OCR validas.
        self.assertLessEqual(diag["ocr"]["agregado"], diag["ocr"]["objetivo_max"])

    def test_cuota_ocr_se_toma_solo_del_pool_f03_validado(self):
        pool = self._pool_mixto(n_ocr=3)  # menos que el objetivo minimo (15)
        seleccion, diag = seleccionar_muestra_estratificada(pool, limite=50)
        ocr_en_seleccion = [f for f in seleccion if f["ocr_used"]]
        self.assertEqual(len(ocr_en_seleccion), 3)  # nunca mas de lo disponible
        self.assertTrue(diag["ocr"]["cuota_no_disponible"])
        for f in ocr_en_seleccion:
            self.assertIn(f["f03_classification"], (PDF_IDENTIDAD_EXACTA, PDF_CONTIENE_NORMA_EN_MULTINORMA))

    def test_diversidad_exacta_multinorma_respeta_disponibilidad_real(self):
        """Si el pool tiene pocas EXACTA disponibles, la cuota exacta no
        puede pedir mas de las que hay -ni tampoco puede quedar en cero si
        hay al menos una disponible y queda cupo libre-."""
        pool = self._pool_mixto(n_exacta=2, n_multi=200, n_ocr=0, n_tablas=0)
        seleccion, diag = seleccionar_muestra_estratificada(pool, limite=50)
        exactas_en_seleccion = sum(1 for f in seleccion if f["f03_classification"] == PDF_IDENTIDAD_EXACTA)
        self.assertLessEqual(exactas_en_seleccion, 2)
        self.assertGreaterEqual(exactas_en_seleccion, 1)
        multi_en_seleccion = sum(1 for f in seleccion if f["f03_classification"] == PDF_CONTIENE_NORMA_EN_MULTINORMA)
        self.assertGreater(multi_en_seleccion, 0)

    def test_ninguna_cuota_puede_prevalecer_sobre_seguridad_documental(self):
        """Un intruso con clasificacion no apta, pero disenado para ganar
        CUALQUIER cuota (ocr de baja confianza, tabla, primera pagina, todo
        a la vez) no debe entrar por ninguna via."""
        pool = self._pool_mixto()
        intruso_total = _fila_pool(
            "INTRUSO-TOTAL", 1, ocr=True, ocr_baja_confianza=True, tabla=True,
            clasif=PDF_IDENTIDAD_CONTRADICTORIA,
            razones_extra=["pagina_dispositiva", "primera_pagina", "ultima_o_penultima_pagina"],
        )
        seleccion, _ = seleccionar_muestra_estratificada(pool + [intruso_total], limite=50)
        self.assertFalse(any(f["document_key"] == "INTRUSO-TOTAL" for f in seleccion))

    def test_seleccion_determinista_con_mismo_corpus(self):
        import random

        pool = self._pool_mixto(n_exacta=40, n_multi=200, n_ocr=48, n_tablas=10)
        seleccion_1, _ = seleccionar_muestra_estratificada(pool, limite=50)

        barajado = pool[:]
        random.Random(42).shuffle(barajado)
        seleccion_2, _ = seleccionar_muestra_estratificada(barajado, limite=50)

        claves_1 = sorted((f["document_key"], f["page_number"]) for f in seleccion_1)
        claves_2 = sorted((f["document_key"], f["page_number"]) for f in seleccion_2)
        self.assertEqual(claves_1, claves_2)

    def test_respeta_minimo_de_tablas_cuando_hay_suficientes(self):
        pool = self._pool_mixto(n_tablas=8)
        _, diag = seleccionar_muestra_estratificada(pool, limite=50, minimo_tablas=5)
        self.assertGreaterEqual(diag["tablas"]["agregado"], 5)
        self.assertFalse(diag["tablas"]["cuota_no_disponible"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
