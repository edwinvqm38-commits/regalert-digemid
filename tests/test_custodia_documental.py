"""Cadena de custodia documental y completitud (F-02).

Cada test corresponde a una forma de perder la trazabilidad entre el PDF
oficial y lo que la base afirma que dice.
"""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))

from custodia_documental import (  # noqa: E402
    COMPLETO,
    COPIA_LOCAL_SIN_PROCEDENCIA,
    DESCONOCIDO,
    EjecucionMotor,
    FUENTE_NO_OFICIAL,
    FUENTE_OFICIAL_DECLARADA_NO_REVALIDADA,
    FUENTE_OFICIAL_VERIFICADA,
    INCOMPLETO,
    PDF_CORRUPTO,
    PDF_NO_DISPONIBLE,
    SenalesBlanco,
    VersionDocumental,
    clasificar_procedencia,
    detectar_documentos_compartidos,
    es_auditable,
    es_pagina_alto_riesgo,
    evaluar_completitud,
    evaluar_pagina_en_blanco,
    registrar_version,
    sha256_de,
)

SHA_A = sha256_de(b"pdf oficial version A")
SHA_B = sha256_de(b"pdf oficial version B")


def version(sha, actual=True, paginas=35):
    return VersionDocumental(
        norma_id="n1", sha256=sha, byte_size=1000, pdf_page_count=paginas,
        source_url="https://www.digemid.minsa.gob.pe/x.pdf",
        storage_path="normas/X/X.pdf", is_current=actual,
        downloaded_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
    )


class TestVersionadoInmutable(unittest.TestCase):
    def test_un_sha_distinto_crea_version_nueva_y_conserva_la_anterior(self):
        versiones, accion = registrar_version([version(SHA_A)], version(SHA_B))
        self.assertEqual(accion, "nueva_version")
        self.assertEqual(len(versiones), 2, "la version anterior NO se pierde")
        self.assertEqual([v.sha256 for v in versiones if v.is_current], [SHA_B])
        self.assertIn(SHA_A, [v.sha256 for v in versiones], "la evidencia previa sigue ahi")

    def test_el_mismo_sha_no_duplica_ni_altera_nada(self):
        original = [version(SHA_A)]
        versiones, accion = registrar_version(original, version(SHA_A))
        self.assertEqual(accion, "sin_cambios")
        self.assertEqual(versiones, original)

    def test_volver_a_una_version_previa_la_reactiva_sin_duplicar(self):
        historial = [version(SHA_A, actual=False), version(SHA_B, actual=True)]
        versiones, accion = registrar_version(historial, version(SHA_A))
        self.assertEqual(accion, "reactivada_version_previa")
        self.assertEqual(len(versiones), 2)
        self.assertEqual([v.sha256 for v in versiones if v.is_current], [SHA_A])

    def test_no_se_admite_una_version_sin_hash_completo(self):
        with self.assertRaises(ValueError):
            registrar_version([], VersionDocumental("n1", "abc", 10, 3))


class TestCompletitud(unittest.TestCase):
    def test_documento_completo(self):
        c = evaluar_completitud(3, [1, 2, 3])
        self.assertEqual(c.estado, COMPLETO)
        self.assertTrue(c.habilita_confirmar_relaciones)

    def test_falta_una_pagina_aunque_las_demas_sean_perfectas(self):
        """PDF 35 + Supabase 34 = DOCUMENTO NO VERIFICADO."""
        c = evaluar_completitud(35, list(range(1, 35)))
        self.assertEqual(c.estado, INCOMPLETO)
        self.assertEqual(c.faltantes, [35])
        self.assertFalse(c.habilita_confirmar_relaciones)

    def test_avisa_expresamente_cuando_falta_la_ultima_pagina(self):
        c = evaluar_completitud(10, list(range(1, 10)))
        self.assertIn("ULTIMA", c.motivo)

    def test_sin_page_count_del_pdf_el_estado_es_desconocido_no_completo(self):
        c = evaluar_completitud(None, [1, 2, 3])
        self.assertEqual(c.estado, DESCONOCIDO)
        self.assertFalse(c.habilita_confirmar_relaciones,
                         "DESCONOCIDO no habilita confirmar relaciones")

    def test_paginas_duplicadas(self):
        c = evaluar_completitud(3, [1, 2, 2, 3])
        self.assertEqual(c.estado, INCOMPLETO)
        self.assertEqual(c.duplicadas, [2])

    def test_paginas_extra_que_el_pdf_no_tiene(self):
        c = evaluar_completitud(3, [1, 2, 3, 4])
        self.assertEqual(c.estado, INCOMPLETO)
        self.assertEqual(c.extras, [4])

    def test_numeracion_fuera_de_secuencia(self):
        c = evaluar_completitud(None, [2, 3, 4])
        self.assertTrue(c.fuera_de_secuencia)

    def test_documento_vacio(self):
        self.assertEqual(evaluar_completitud(5, []).estado, INCOMPLETO)

    def test_pdf_corrupto_y_no_disponible(self):
        self.assertEqual(evaluar_completitud(None, [1], pdf_corrupto=True).estado, PDF_CORRUPTO)
        self.assertEqual(evaluar_completitud(None, [1], pdf_disponible=False).estado, PDF_NO_DISPONIBLE)


class TestProcedencia(unittest.TestCase):
    def test_dominio_oficial_sin_revalidar_no_es_verificado(self):
        estado, _ = clasificar_procedencia("https://www.digemid.minsa.gob.pe/a.pdf")
        self.assertEqual(estado, FUENTE_OFICIAL_DECLARADA_NO_REVALIDADA)

    def test_solo_revalidando_el_hash_se_afirma_oficial_verificada(self):
        estado, _ = clasificar_procedencia("https://www.digemid.minsa.gob.pe/a.pdf",
                                           revalidado_contra_origen=True)
        self.assertEqual(estado, FUENTE_OFICIAL_VERIFICADA)

    def test_una_url_de_nuestro_propio_storage_no_es_procedencia(self):
        estado, motivo = clasificar_procedencia(
            "https://xxx.supabase.co/storage/v1/object/sign/b/x.pdf?token=abc")
        self.assertEqual(estado, COPIA_LOCAL_SIN_PROCEDENCIA)
        self.assertIn("caduca", motivo)

    def test_sin_url_no_hay_procedencia(self):
        self.assertEqual(clasificar_procedencia(None)[0], COPIA_LOCAL_SIN_PROCEDENCIA)

    def test_dominio_desconocido(self):
        self.assertEqual(clasificar_procedencia("https://drive.google.com/x.pdf")[0], FUENTE_NO_OFICIAL)


class TestDocumentosCompartidos(unittest.TestCase):
    """Caso REAL del corpus: dos normas distintas transcritas del mismo PDF."""

    def test_texto_identico_entre_normas_distintas_es_critico(self):
        hallazgos = detectar_documentos_compartidos([
            {"document_key": "DS-9-2015", "pdf_url": "u/DS_009-2015.pdf", "hash_texto": "abc"},
            {"document_key": "DS-10-2015", "pdf_url": "u/DS_009-2015.pdf", "hash_texto": "abc"},
        ])
        criticos = [h for h in hallazgos if h["gravedad"] == "CRITICO"]
        self.assertTrue(criticos)
        self.assertEqual(criticos[0]["normas"], ["DS-10-2015", "DS-9-2015"])

    def test_normas_distintas_con_documentos_distintos_no_alertan(self):
        self.assertEqual(detectar_documentos_compartidos([
            {"document_key": "A", "pdf_url": "a.pdf", "hash_texto": "1"},
            {"document_key": "B", "pdf_url": "b.pdf", "hash_texto": "2"},
        ]), [])


class TestPaginaEnBlanco(unittest.TestCase):
    def test_falso_blanco_una_linea_de_texto_sobre_A4(self):
        """Un poco de tinta deja el promedio de pixel casi en blanco, pero la
        pagina TIENE contenido."""
        es_blanco, _, motivo = evaluar_pagina_en_blanco(
            SenalesBlanco(ratio_pixeles_no_blancos=0.004))
        self.assertFalse(es_blanco)
        self.assertIn("contenido", motivo)

    def test_pagina_realmente_vacia(self):
        es_blanco, confianza, _ = evaluar_pagina_en_blanco(
            SenalesBlanco(ratio_pixeles_no_blancos=0.0001, bloques_texto=0, objetos_dibujo=0))
        self.assertTrue(es_blanco)
        self.assertEqual(confianza, "alta")

    def test_texto_embebido_descarta_el_blanco(self):
        self.assertFalse(evaluar_pagina_en_blanco(SenalesBlanco(texto_embebido="Artículo 1"))[0])

    def test_bloques_u_objetos_descartan_el_blanco(self):
        self.assertFalse(evaluar_pagina_en_blanco(SenalesBlanco(bloques_texto=3))[0])
        self.assertFalse(evaluar_pagina_en_blanco(SenalesBlanco(objetos_dibujo=12))[0])

    def test_la_ultima_pagina_nunca_se_declara_en_blanco_por_heuristica(self):
        es_blanco, confianza, _ = evaluar_pagina_en_blanco(
            SenalesBlanco(ratio_pixeles_no_blancos=0.0001, es_ultima_pagina=True))
        self.assertFalse(es_blanco)
        self.assertEqual(confianza, "baja")

    def test_sin_analisis_de_pixeles_no_se_afirma_nada(self):
        es_blanco, confianza, _ = evaluar_pagina_en_blanco(SenalesBlanco())
        self.assertFalse(es_blanco)
        self.assertEqual(confianza, "baja")


class TestAltoRiesgoPorPosicion(unittest.TestCase):
    def test_primera_ultima_y_penultima_son_de_alto_riesgo(self):
        for pagina in (1, 9, 10):
            self.assertTrue(es_pagina_alto_riesgo(pagina, 10, texto_dispositivo=False)[0], pagina)

    def test_una_pagina_intermedia_sin_marcadores_no_lo_es(self):
        self.assertFalse(es_pagina_alto_riesgo(5, 10, texto_dispositivo=False)[0])

    def test_el_texto_dispositivo_manda_aunque_sea_intermedia(self):
        self.assertTrue(es_pagina_alto_riesgo(5, 10, texto_dispositivo=True)[0])

    def test_un_ocr_que_destruyo_el_marcador_no_baja_el_riesgo_en_la_ultima(self):
        """Es el motivo de la regla por posición."""
        self.assertTrue(es_pagina_alto_riesgo(10, 10, texto_dispositivo=False)[0])


class TestAuditabilidadDelMotor(unittest.TestCase):
    def _completa(self, **cambios):
        base = dict(provider="openai", model_solicitado="gpt-4.1-mini", model_real="gpt-4.1-mini",
                    response_id="resp_123", prompt_version="v1", dpi=300, pdf_sha256=SHA_A,
                    page_number=3)
        base.update(cambios)
        return EjecucionMotor(**base)

    def test_una_ejecucion_completa_es_auditable(self):
        ok, faltan = es_auditable(self._completa())
        self.assertTrue(ok, faltan)

    def test_modelo_auto_no_es_auditable(self):
        ok, faltan = es_auditable(self._completa(model_solicitado="openrouter/auto"))
        self.assertFalse(ok)
        self.assertTrue(any("auto" in f for f in faltan))

    def test_sin_response_id_ni_prompt_version_no_es_auditable(self):
        self.assertFalse(es_auditable(self._completa(response_id=None))[0])
        self.assertFalse(es_auditable(self._completa(prompt_version=None))[0])

    def test_sin_hash_del_pdf_no_se_sabe_que_documento_se_transcribio(self):
        ok, faltan = es_auditable(self._completa(pdf_sha256=None))
        self.assertFalse(ok)
        self.assertIn("pdf_sha256", faltan)


if __name__ == "__main__":
    unittest.main(verbosity=2)
