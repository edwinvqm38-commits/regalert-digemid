"""Regresión de fidelidad documental (F-01).

Cada test corresponde a una forma concreta de corromper el sentido jurídico de
una norma. Todos deben FALLAR si el pipeline vuelve a aceptar como buena una
transcripción que cambia el efecto legal.
"""

import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))

from fidelidad_legal import (  # noqa: E402
    DISCREPANCIA_ENTRE_MOTORES,
    DOCUMENTO_INCOMPLETO,
    EXTRACCION_DIGITAL_ALTA_CONCORDANCIA,
    ILEGIBLE_PARCIAL,
    NO_EVALUADA,
    OCR_PENDIENTE_VERIFICACION,
    PDF_NO_DISPONIBLE,
    REQUIERE_REVISION_HUMANA,
    RIESGO_ALTO,
    RIESGO_CRITICO,
    SenalesPagina,
    VERIFICADA_AUTOMATICAMENTE,
    VERIFICADA_HUMANO,
    cer,
    comparar_fidelidad,
    discrepancia_entre_motores,
    es_pagina_dispositiva,
    evaluar_pagina,
    puede_alimentar_detector,
    puede_citarse_como_fuente_legal,
    tokens_sensibles,
    verbos_normativos,
    wer,
)

DISPOSITIVA = (
    "SE RESUELVE:\n"
    "Artículo 18.- Derógase la Resolución Ministerial N° 339-2023/MINSA, "
    "en un plazo de 10 días hábiles, hasta por S/ 10 000,00."
)


class TestTokensSensibles(unittest.TestCase):
    """Un dígito distinto en cualquiera de estos cambia el efecto jurídico."""

    def _lter(self, a, b):
        return comparar_fidelidad(a, b).legal_token_error_rate

    def test_numero_de_articulo_cambiado(self):
        r = comparar_fidelidad("Artículo 18.- Deróguese", "Artículo 13.- Deróguese")
        self.assertTrue(r.hay_error_juridico)
        self.assertIn("articulo", [e.categoria for e in r.errores_token])

    def test_numero_de_norma_cambiado(self):
        r = comparar_fidelidad("Resolución Ministerial N° 339-2023", "Resolución Ministerial N° 349-2023")
        self.assertTrue(r.hay_error_juridico)

    def test_anio_cambiado(self):
        r = comparar_fidelidad("RM N° 339-2023/MINSA", "RM N° 339-2028/MINSA")
        self.assertTrue(r.hay_error_juridico)
        self.assertIn("anio", [e.categoria for e in r.errores_token])

    def test_plazo_cambiado(self):
        r = comparar_fidelidad("en un plazo de 10 días", "en un plazo de 100 días")
        self.assertTrue(r.hay_error_juridico)

    def test_monto_cambiado(self):
        r = comparar_fidelidad("hasta por S/ 10 000,00", "hasta por S/ 100 000,00")
        self.assertTrue(r.hay_error_juridico)

    def test_dosis_o_concentracion_cambiada(self):
        r = comparar_fidelidad("concentración 0,5 mg/mL", "concentración 0,8 mg/mL")
        self.assertTrue(r.hay_error_juridico)

    def test_verbo_juridico_cambiado_es_error_aunque_el_texto_se_parezca(self):
        """WER puede ser bajísimo y aun así ser jurídicamente inaceptable."""
        a = "Artículo 1.- Derógase el Decreto Supremo N° 014-2011-SA"
        b = "Artículo 1.- Modifícase el Decreto Supremo N° 014-2011-SA"
        r = comparar_fidelidad(a, b)
        self.assertLess(r.wer, 0.2, "el texto es casi idéntico")
        self.assertTrue(r.verbos_cambiados, "pero el efecto jurídico cambió por completo")
        self.assertTrue(r.hay_error_juridico)

    def test_texto_equivalente_sin_tokens_no_genera_falso_positivo(self):
        r = comparar_fidelidad(
            "El presente decreto entra en vigencia al día siguiente de su publicación.",
            "El presente decreto entra en vigencia al dia siguiente de su publicacion.",
        )
        self.assertFalse(r.hay_error_juridico)
        self.assertEqual(r.legal_token_error_rate, 0.0)

    def test_una_sustitucion_cuenta_como_un_error_no_como_dos(self):
        r = comparar_fidelidad("Artículo 18", "Artículo 13")
        self.assertEqual(len(r.errores_token), 1)
        self.assertEqual(r.legal_token_error_rate, 1.0)


class TestMetricas(unittest.TestCase):
    def test_cer_y_wer_son_cero_en_texto_identico(self):
        self.assertEqual(cer(DISPOSITIVA, DISPOSITIVA), 0.0)
        self.assertEqual(wer(DISPOSITIVA, DISPOSITIVA), 0.0)

    def test_wer_bajo_no_implica_fidelidad_juridica(self):
        r = comparar_fidelidad(DISPOSITIVA, DISPOSITIVA.replace("18", "13"))
        self.assertLess(r.wer, 0.1)
        self.assertGreater(r.legal_token_error_rate, 0.0)

    def test_ocr_vacio_sobre_pagina_con_contenido(self):
        r = comparar_fidelidad(DISPOSITIVA, "")
        self.assertEqual(r.cer, 1.0)
        self.assertTrue(r.hay_error_juridico)


class TestParteDispositiva(unittest.TestCase):
    def test_detecta_las_formas_reales(self):
        for texto in ["SE RESUELVE:", "DECRETA:", "DISPOSICIONES COMPLEMENTARIAS DEROGATORIAS",
                      "Deróguese el artículo 9", "Déjese sin efecto la RM 097-2000",
                      "Modifícase el artículo 43", "Exonérase de la aplicación"]:
            self.assertTrue(es_pagina_dispositiva(texto), texto)

    def test_un_considerando_no_es_dispositivo(self):
        self.assertFalse(es_pagina_dispositiva(
            "Que, mediante el Decreto Supremo N° 014-2011-SA se aprobó el Reglamento"))

    def test_clasifica_el_verbo_por_clase_no_por_cadena(self):
        for variante in ["Deróguese", "Derógase", "derogar", "DEROGA"]:
            self.assertIn("DEROGA", verbos_normativos(variante), variante)


class TestEstadosDeVerificacion(unittest.TestCase):
    def test_confianza_alta_de_ocr_no_alcanza_para_verificada(self):
        """Ni Tesseract ni un LLM verifican por sí solos."""
        estado, _, _ = evaluar_pagina(SenalesPagina(
            extraction_method="ocr_tesseract", ocr_used=True, ocr_confidence=0.99,
            quality_score=1.0, texto=DISPOSITIVA))
        self.assertEqual(estado, OCR_PENDIENTE_VERIFICACION)
        self.assertFalse(puede_citarse_como_fuente_legal(estado))

    def test_quality_score_perfecto_no_alcanza_para_verificada(self):
        estado, _, _ = evaluar_pagina(SenalesPagina(
            extraction_method="pymupdf", quality_score=1.0, texto=DISPOSITIVA))
        self.assertEqual(estado, EXTRACCION_DIGITAL_ALTA_CONCORDANCIA)
        self.assertFalse(puede_citarse_como_fuente_legal(estado))

    def test_dos_motores_que_difieren_en_un_token_dan_discrepancia_critica(self):
        cmp_ = discrepancia_entre_motores(DISPOSITIVA, DISPOSITIVA.replace("18", "13"))
        estado, riesgo, _ = evaluar_pagina(SenalesPagina(
            extraction_method="pymupdf", quality_score=1.0, texto=DISPOSITIVA,
            comparacion_motores=cmp_))
        self.assertEqual(estado, DISCREPANCIA_ENTRE_MOTORES)
        self.assertEqual(riesgo, RIESGO_CRITICO)

    def test_dos_motores_coincidentes_si_verifican(self):
        estado, _, _ = evaluar_pagina(SenalesPagina(
            extraction_method="pymupdf", quality_score=1.0, texto=DISPOSITIVA,
            comparacion_motores=discrepancia_entre_motores(DISPOSITIVA, DISPOSITIVA)))
        self.assertEqual(estado, VERIFICADA_AUTOMATICAMENTE)
        self.assertTrue(puede_citarse_como_fuente_legal(estado))

    def test_coincidencia_textual_no_verifica_una_tabla(self):
        estado, riesgo, _ = evaluar_pagina(SenalesPagina(
            extraction_method="pymupdf", quality_score=1.0, texto=DISPOSITIVA, has_tables=True,
            comparacion_motores=discrepancia_entre_motores(DISPOSITIVA, DISPOSITIVA)))
        self.assertEqual(estado, REQUIERE_REVISION_HUMANA)
        self.assertEqual(riesgo, RIESGO_ALTO)

    def test_documento_incompleto_gana_a_cualquier_calidad(self):
        estado, riesgo, _ = evaluar_pagina(SenalesPagina(
            extraction_method="pymupdf", quality_score=1.0, texto=DISPOSITIVA,
            documento_completo=False))
        self.assertEqual(estado, DOCUMENTO_INCOMPLETO)
        self.assertEqual(riesgo, RIESGO_CRITICO)

    def test_sin_pdf_no_hay_nada_que_verificar(self):
        estado, _, _ = evaluar_pagina(SenalesPagina(texto=DISPOSITIVA, pdf_disponible=False))
        self.assertEqual(estado, PDF_NO_DISPONIBLE)

    def test_marca_de_ilegible_se_respeta(self):
        estado, _, _ = evaluar_pagina(SenalesPagina(
            extraction_method="ocr_tesseract", ocr_used=True, ocr_confidence=0.95,
            texto="Resolución N° [ilegible] del año 2023"))
        self.assertEqual(estado, ILEGIBLE_PARCIAL)

    def test_pagina_en_blanco_no_se_da_por_buena(self):
        """quality_score=1.0 para una página sin texto es una heurística de
        píxel, no una verificación."""
        estado, _, _ = evaluar_pagina(SenalesPagina(
            extraction_method="pagina_en_blanco", quality_score=1.0, texto=""))
        self.assertEqual(estado, NO_EVALUADA)

    def test_revision_humana_es_el_unico_atajo(self):
        estado, _, _ = evaluar_pagina(SenalesPagina(
            extraction_method="ocr_tesseract", ocr_used=True, ocr_confidence=0.4,
            texto=DISPOSITIVA, revisado_manual=True))
        self.assertEqual(estado, VERIFICADA_HUMANO)


class TestPuertas(unittest.TestCase):
    def test_dispositiva_no_verificada_no_alimenta_al_detector(self):
        for estado in (OCR_PENDIENTE_VERIFICACION, REQUIERE_REVISION_HUMANA,
                       DISCREPANCIA_ENTRE_MOTORES, ILEGIBLE_PARCIAL,
                       EXTRACCION_DIGITAL_ALTA_CONCORDANCIA):
            self.assertFalse(puede_alimentar_detector(estado, dispositiva=True), estado)

    def test_considerando_no_verificado_si_puede_leerse(self):
        self.assertTrue(puede_alimentar_detector(OCR_PENDIENTE_VERIFICACION, dispositiva=False))

    def test_documento_incompleto_nunca_alimenta_al_detector(self):
        """La disposición derogatoria peruana va al final: justo la página que
        falta suele ser la decisiva."""
        self.assertFalse(puede_alimentar_detector(DOCUMENTO_INCOMPLETO, dispositiva=False))
        self.assertFalse(puede_alimentar_detector(PDF_NO_DISPONIBLE, dispositiva=False))

    def test_solo_lo_verificado_se_cita_como_fuente_legal(self):
        for estado in (VERIFICADA_HUMANO, VERIFICADA_AUTOMATICAMENTE):
            self.assertTrue(puede_citarse_como_fuente_legal(estado))
        for estado in (NO_EVALUADA, OCR_PENDIENTE_VERIFICACION, EXTRACCION_DIGITAL_ALTA_CONCORDANCIA,
                       REQUIERE_REVISION_HUMANA, DISCREPANCIA_ENTRE_MOTORES, ILEGIBLE_PARCIAL,
                       DOCUMENTO_INCOMPLETO, PDF_NO_DISPONIBLE):
            self.assertFalse(puede_citarse_como_fuente_legal(estado), estado)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestNivelesDeUso(unittest.TestCase):
    """Buscar y citar son cosas distintas (F-02 · 11)."""

    def test_buscar_se_permite_en_todos_los_niveles(self):
        from fidelidad_legal import (NIVEL_0_SOLO_INDICE, NIVEL_1_DIGITAL_CONCORDANTE,
                                     NIVEL_2_AUTO_VERIFICADA, NIVEL_3_VERIFICADA_HUMANO,
                                     USO_BUSQUEDA, usos_permitidos)
        for nivel in (NIVEL_0_SOLO_INDICE, NIVEL_1_DIGITAL_CONCORDANTE,
                      NIVEL_2_AUTO_VERIFICADA, NIVEL_3_VERIFICADA_HUMANO):
            self.assertIn(USO_BUSQUEDA, usos_permitidos(nivel), nivel)

    def test_citar_exige_nivel_2_o_3(self):
        from fidelidad_legal import (NIVEL_0_SOLO_INDICE, NIVEL_1_DIGITAL_CONCORDANTE,
                                     NIVEL_2_AUTO_VERIFICADA, NIVEL_3_VERIFICADA_HUMANO,
                                     USO_CITA_LEGAL, usos_permitidos)
        self.assertNotIn(USO_CITA_LEGAL, usos_permitidos(NIVEL_0_SOLO_INDICE))
        self.assertNotIn(USO_CITA_LEGAL, usos_permitidos(NIVEL_1_DIGITAL_CONCORDANTE))
        self.assertIn(USO_CITA_LEGAL, usos_permitidos(NIVEL_2_AUTO_VERIFICADA))
        self.assertIn(USO_CITA_LEGAL, usos_permitidos(NIVEL_3_VERIFICADA_HUMANO))

    def test_una_dispositiva_de_nivel_1_no_alimenta_al_detector(self):
        from fidelidad_legal import (NIVEL_1_DIGITAL_CONCORDANTE, USO_DETECTOR_RELACIONES,
                                     usos_permitidos)
        self.assertNotIn(USO_DETECTOR_RELACIONES,
                         usos_permitidos(NIVEL_1_DIGITAL_CONCORDANTE, dispositiva=True))
        self.assertIn(USO_DETECTOR_RELACIONES,
                      usos_permitidos(NIVEL_1_DIGITAL_CONCORDANTE, dispositiva=False))

    def test_un_documento_incompleto_baja_la_pagina_a_nivel_0(self):
        from fidelidad_legal import (NIVEL_0_SOLO_INDICE, VERIFICADA_AUTOMATICAMENTE,
                                     nivel_de_uso)
        self.assertEqual(nivel_de_uso(VERIFICADA_AUTOMATICAMENTE, documento_completo=False),
                         NIVEL_0_SOLO_INDICE)

    def test_la_verificacion_humana_sobrevive_a_un_documento_incompleto(self):
        from fidelidad_legal import NIVEL_3_VERIFICADA_HUMANO, VERIFICADA_HUMANO, nivel_de_uso
        self.assertEqual(nivel_de_uso(VERIFICADA_HUMANO, documento_completo=False),
                         NIVEL_3_VERIFICADA_HUMANO)

    def test_los_niveles_bajos_obligan_a_advertir_en_consulta(self):
        from fidelidad_legal import (NIVEL_0_SOLO_INDICE, NIVEL_1_DIGITAL_CONCORDANTE,
                                     NIVEL_2_AUTO_VERIFICADA, advertencia_para_consulta)
        self.assertIsNotNone(advertencia_para_consulta(NIVEL_0_SOLO_INDICE))
        self.assertIsNotNone(advertencia_para_consulta(NIVEL_1_DIGITAL_CONCORDANTE))
        self.assertIsNone(advertencia_para_consulta(NIVEL_2_AUTO_VERIFICADA))
