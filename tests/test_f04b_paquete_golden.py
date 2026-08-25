import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agents"))

from f04b_paquete_golden import (  # noqa: E402
    GOLDEN_PDF_SHA_MISMATCH,
    RENDERIZADO_OK,
    clave_pagina,
    construir_plan_render,
    contar_ocr,
    nombre_archivo_imagen,
    orden_paginas,
    resumen_control_calidad,
    sha_coincide,
    verificar_cobertura_paginas,
    verificar_enlace_render,
    verificar_presencia,
)


def _fila_manifest(document_key, page_number, sha256="a" * 64, storage_path=None):
    return {
        "document_key": document_key,
        "page_number": page_number,
        "pdf_sha256": sha256,
        "storage_path": storage_path or f"normas/{document_key}.pdf",
    }


def _fila_comparacion(document_key, page_number, same_engine=False):
    return {"document_key": document_key, "page_number": page_number, "same_engine_as_stored": same_engine}


def _manifest_50_realista():
    """50 filas sintéticas que incluyen los 3 documentos de control obligatorio
    y exactamente 12 páginas 'OCR' (same_engine_as_stored), como en el
    Manifest V2 real."""
    filas = [
        _fila_manifest("RM-250-2019", 1),
        _fila_manifest("DS-5-2019", 1),
        _fila_manifest("RM-862-2019", 1),
    ]
    for i in range(9):
        filas.append(_fila_manifest(f"OCR-A", i + 1))
    for i in range(3):
        filas.append(_fila_manifest(f"OCR-B", i + 1))
    for i in range(35):
        filas.append(_fila_manifest(f"EX-{i}", 1))
    assert len(filas) == 50
    return filas


class TestNombreArchivoImagen(unittest.TestCase):
    def test_formato_exacto(self):
        self.assertEqual(nombre_archivo_imagen(1, "RM-250-2019", 1), "001_RM-250-2019_p1.png")

    def test_indice_de_3_digitos(self):
        self.assertEqual(nombre_archivo_imagen(42, "X", 7), "042_X_p7.png")
        self.assertEqual(nombre_archivo_imagen(123, "X", 7), "123_X_p7.png")

    def test_sanitiza_caracteres_inseguros_para_archivo(self):
        nombre = nombre_archivo_imagen(1, "RM/250:2019", 1)
        self.assertNotIn("/", nombre)
        self.assertNotIn(":", nombre)


class TestOrdenPaginas(unittest.TestCase):
    def test_orden_determinista_sin_importar_entrada(self):
        filas = _manifest_50_realista()
        barajadas = filas[:]
        random.Random(7).shuffle(barajadas)
        self.assertEqual(orden_paginas(filas), orden_paginas(barajadas))

    def test_orden_por_document_key_y_luego_page_number(self):
        filas = [_fila_manifest("B", 2), _fila_manifest("A", 5), _fila_manifest("A", 1)]
        ordenadas = orden_paginas(filas)
        claves = [(f["document_key"], f["page_number"]) for f in ordenadas]
        self.assertEqual(claves, [("A", 1), ("A", 5), ("B", 2)])


class TestConstruirPlanRender(unittest.TestCase):
    def test_50_filas_producen_50_entradas_indices_1_a_50(self):
        plan = construir_plan_render(_manifest_50_realista())
        self.assertEqual(len(plan), 50)
        self.assertEqual([p["indice"] for p in plan], list(range(1, 51)))

    def test_archivos_unicos_sin_duplicados(self):
        plan = construir_plan_render(_manifest_50_realista())
        archivos = [p["archivo_imagen"] for p in plan]
        self.assertEqual(len(archivos), len(set(archivos)))

    def test_archivo_coincide_con_nombre_archivo_imagen(self):
        plan = construir_plan_render(_manifest_50_realista())
        for entrada in plan:
            esperado = "paginas/" + nombre_archivo_imagen(
                entrada["indice"], entrada["document_key"], entrada["page_number"])
            self.assertEqual(entrada["archivo_imagen"], esperado)

    def test_sha_esperado_es_el_del_manifest_no_uno_recalculado(self):
        plan = construir_plan_render([_fila_manifest("A", 1, sha256="deadbeef" * 8)])
        self.assertEqual(plan[0]["pdf_sha256_esperado"], "deadbeef" * 8)


class TestShaCoincide(unittest.TestCase):
    def test_coincide_exacto(self):
        self.assertTrue(sha_coincide("abc123", "abc123"))

    def test_coincide_sin_importar_mayusculas(self):
        self.assertTrue(sha_coincide("ABC123", "abc123"))

    def test_no_coincide(self):
        self.assertFalse(sha_coincide("abc123", "def456"))

    def test_ninguno_esperado_o_calculado_es_none_o_vacio(self):
        self.assertFalse(sha_coincide(None, "abc123"))
        self.assertFalse(sha_coincide("abc123", None))
        self.assertFalse(sha_coincide("", ""))


class TestVerificarCoberturaPaginas(unittest.TestCase):
    def test_coincide_exactamente_cuando_son_las_mismas_50(self):
        manifest = _manifest_50_realista()
        comparacion = [_fila_comparacion(f["document_key"], f["page_number"]) for f in manifest]
        resultado = verificar_cobertura_paginas(manifest, comparacion)
        self.assertTrue(resultado["coincide_exactamente"])
        self.assertEqual(resultado["total_manifest"], 50)
        self.assertEqual(resultado["total_comparacion"], 50)
        self.assertEqual(resultado["duplicados_en_manifest"], 0)
        self.assertEqual(resultado["duplicados_en_comparacion"], 0)

    def test_detecta_pagina_extra_en_comparacion(self):
        manifest = [_fila_manifest("A", 1)]
        comparacion = [_fila_comparacion("A", 1), _fila_comparacion("B", 9)]
        resultado = verificar_cobertura_paginas(manifest, comparacion)
        self.assertFalse(resultado["coincide_exactamente"])
        self.assertEqual(resultado["solo_en_comparacion"], [("B", 9)])

    def test_detecta_pagina_faltante_en_comparacion(self):
        manifest = [_fila_manifest("A", 1), _fila_manifest("B", 9)]
        comparacion = [_fila_comparacion("A", 1)]
        resultado = verificar_cobertura_paginas(manifest, comparacion)
        self.assertFalse(resultado["coincide_exactamente"])
        self.assertEqual(resultado["solo_en_manifest"], [("B", 9)])

    def test_detecta_duplicado_dentro_del_mismo_lado(self):
        manifest = [_fila_manifest("A", 1), _fila_manifest("A", 1)]
        comparacion = [_fila_comparacion("A", 1)]
        resultado = verificar_cobertura_paginas(manifest, comparacion)
        self.assertEqual(resultado["duplicados_en_manifest"], 1)
        self.assertFalse(resultado["coincide_exactamente"])


class TestVerificarPresencia(unittest.TestCase):
    def test_los_3_documentos_de_control_presentes(self):
        plan = construir_plan_render(_manifest_50_realista())
        presencia = verificar_presencia(
            plan, [("RM-250-2019", 1), ("DS-5-2019", 1), ("RM-862-2019", 1)])
        self.assertEqual(presencia, {
            "RM-250-2019_p1": True, "DS-5-2019_p1": True, "RM-862-2019_p1": True,
        })

    def test_documento_ausente_se_reporta_false(self):
        plan = construir_plan_render([_fila_manifest("X", 1)])
        presencia = verificar_presencia(plan, [("RM-250-2019", 1)])
        self.assertEqual(presencia, {"RM-250-2019_p1": False})


class TestContarOcr(unittest.TestCase):
    def test_cuenta_exactamente_las_marcadas_same_engine(self):
        comparacion = (
            [_fila_comparacion(f"D{i}", 1, same_engine=True) for i in range(12)]
            + [_fila_comparacion(f"E{i}", 1, same_engine=False) for i in range(38)]
        )
        self.assertEqual(contar_ocr(comparacion), 12)

    def test_cero_si_ninguna_es_ocr(self):
        comparacion = [_fila_comparacion("A", 1, same_engine=False)]
        self.assertEqual(contar_ocr(comparacion), 0)


class TestVerificarEnlaceRender(unittest.TestCase):
    def test_enlace_correcto(self):
        entrada = {"document_key": "RM-250-2019", "page_number": 1,
                   "archivo_imagen": "paginas/001_RM-250-2019_p1.png"}
        self.assertTrue(verificar_enlace_render(entrada))

    def test_detecta_document_key_cruzado(self):
        entrada = {"document_key": "RM-250-2019", "page_number": 1,
                   "archivo_imagen": "paginas/001_OTRA-NORMA_p1.png"}
        self.assertFalse(verificar_enlace_render(entrada))

    def test_detecta_page_number_cruzado(self):
        entrada = {"document_key": "RM-250-2019", "page_number": 1,
                   "archivo_imagen": "paginas/001_RM-250-2019_p2.png"}
        self.assertFalse(verificar_enlace_render(entrada))

    def test_nombre_con_formato_invalido(self):
        entrada = {"document_key": "RM-250-2019", "page_number": 1,
                   "archivo_imagen": "paginas/no-sigue-el-formato.png"}
        self.assertFalse(verificar_enlace_render(entrada))


class TestResumenControlCalidad(unittest.TestCase):
    REQUERIDAS = [("RM-250-2019", 1), ("DS-5-2019", 1), ("RM-862-2019", 1)]

    def _resultados_render_ok(self, plan):
        return [
            {**entrada, "estado": RENDERIZADO_OK, "sha256_real": entrada["pdf_sha256_esperado"]}
            for entrada in plan
        ]

    def test_paquete_completo_pasa_todos_los_controles(self):
        manifest = _manifest_50_realista()
        plan = construir_plan_render(manifest)
        comparacion = [
            _fila_comparacion(f["document_key"], f["page_number"],
                               same_engine=f["document_key"].startswith("OCR"))
            for f in manifest
        ]
        resultados = self._resultados_render_ok(plan)
        resumen = resumen_control_calidad(plan, resultados, comparacion, self.REQUERIDAS, ocr_esperadas=12)
        self.assertTrue(resumen["todos_los_controles_pasan"], resumen)
        self.assertEqual(resumen["total_renders_generados"], 50)
        self.assertEqual(resumen["renders_duplicados"], 0)
        self.assertEqual(resumen["renders_fuera_de_manifest"], 0)
        self.assertEqual(resumen["sha_mismatch_count"], 0)
        self.assertEqual(resumen["enlace_render_incorrecto_count"], 0)
        self.assertEqual(resumen["paginas_ocr_identificadas"], 12)
        self.assertTrue(all(resumen["documentos_requeridos_presentes"].values()))

    def test_sha_mismatch_excluye_la_pagina_y_falla_el_control(self):
        manifest = _manifest_50_realista()
        plan = construir_plan_render(manifest)
        comparacion = [_fila_comparacion(f["document_key"], f["page_number"]) for f in manifest]
        resultados = self._resultados_render_ok(plan)
        # una pagina "falla" el SHA: no se renderiza, se marca el estado especifico
        resultados[0] = {
            **plan[0], "estado": GOLDEN_PDF_SHA_MISMATCH, "sha256_real": "otro-hash-completamente-distinto",
        }
        resumen = resumen_control_calidad(plan, resultados, comparacion, self.REQUERIDAS, ocr_esperadas=0)
        self.assertFalse(resumen["todos_los_controles_pasan"])
        self.assertEqual(resumen["sha_mismatch_count"], 1)
        self.assertEqual(resumen["total_renders_generados"], 49)
        self.assertEqual(
            resumen["paginas_sha_mismatch"],
            [{"document_key": plan[0]["document_key"], "page_number": plan[0]["page_number"]}],
        )

    def test_documento_requerido_ausente_falla_el_control(self):
        manifest = [f for f in _manifest_50_realista() if f["document_key"] != "RM-250-2019"]
        manifest.append(_fila_manifest("RELLENO", 1))
        self.assertEqual(len(manifest), 50)
        plan = construir_plan_render(manifest)
        comparacion = [_fila_comparacion(f["document_key"], f["page_number"]) for f in manifest]
        resultados = self._resultados_render_ok(plan)
        resumen = resumen_control_calidad(plan, resultados, comparacion, self.REQUERIDAS, ocr_esperadas=12)
        self.assertFalse(resumen["todos_los_controles_pasan"])
        self.assertFalse(resumen["documentos_requeridos_presentes"]["RM-250-2019_p1"])

    def test_conteo_ocr_incorrecto_falla_el_control(self):
        manifest = _manifest_50_realista()
        plan = construir_plan_render(manifest)
        comparacion = [_fila_comparacion(f["document_key"], f["page_number"]) for f in manifest]
        resultados = self._resultados_render_ok(plan)
        resumen = resumen_control_calidad(plan, resultados, comparacion, self.REQUERIDAS, ocr_esperadas=12)
        self.assertFalse(resumen["todos_los_controles_pasan"])
        self.assertEqual(resumen["paginas_ocr_identificadas"], 0)

    def test_enlace_incorrecto_falla_el_control(self):
        manifest = _manifest_50_realista()
        plan = construir_plan_render(manifest)
        comparacion = [_fila_comparacion(f["document_key"], f["page_number"]) for f in manifest]
        resultados = self._resultados_render_ok(plan)
        resultados[3]["archivo_imagen"] = "paginas/999_OTRA-COSA_p1.png"
        resumen = resumen_control_calidad(plan, resultados, comparacion, self.REQUERIDAS, ocr_esperadas=12)
        self.assertFalse(resumen["todos_los_controles_pasan"])
        self.assertEqual(resumen["enlace_render_incorrecto_count"], 1)

    def test_menos_de_50_renders_falla_el_control(self):
        manifest = _manifest_50_realista()
        plan = construir_plan_render(manifest)
        comparacion = [_fila_comparacion(f["document_key"], f["page_number"]) for f in manifest]
        resultados = self._resultados_render_ok(plan)[:49]
        resumen = resumen_control_calidad(plan, resultados, comparacion, self.REQUERIDAS, ocr_esperadas=12)
        self.assertFalse(resumen["todos_los_controles_pasan"])
        self.assertEqual(resumen["total_renders_generados"], 49)


if __name__ == "__main__":
    unittest.main()
