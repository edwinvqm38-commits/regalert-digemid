import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
spec = importlib.util.spec_from_file_location("auditar_fidelidad", RAIZ / "scripts" / "auditar_fidelidad_documental.py")
auditoria = importlib.util.module_from_spec(spec)
spec.loader.exec_module(auditoria)


def pagina(norma_id, n, texto):
    return {"norma_id": norma_id, "page_number": n, "text_normalized": texto, "extraction_method": "pymupdf",
            "quality_score": 0.9, "ocr_used": False}


def arts(nums):
    return "\n".join(f"Artículo {n}°.- Texto del artículo número {n} de la norma." for n in nums)


class AuditoriaSecuenciaTest(unittest.TestCase):
    def test_resumen_incluye_secuencia_y_marca_la_norma_con_salto(self):
        normas = [
            {"id": "a", "document_key": "DS-1-2020", "pdf_url": "x"},
            {"id": "b", "document_key": "DS-2-2020", "pdf_url": "x"},
        ]
        paginas = [
            pagina("a", 1, arts([1, 2, 3])), pagina("a", 2, arts([4, 5, 6])),
            pagina("b", 1, arts([1, 2, 3])), pagina("b", 2, arts([8, 9])),
        ]
        _, resumen = auditoria.auditar(normas, paginas, verificar_pdf=False)
        sec = resumen["secuencia_articulos"]
        self.assertEqual((sec["evaluables"], sec["con_senales"]), (2, 1))
        self.assertEqual(sec["normas"][0]["document_key"], "DS-2-2020")
        self.assertIn("salto", sec["normas"][0]["senales"][0])

    def test_texto_telegram_es_corto_y_orientativo(self):
        resumen = {"paginas": 10, "normas": 2, "aptas_para_consulta": 4, "no_aptas_para_consulta": 6,
                   "secuencia_articulos": {"evaluables": 2, "con_senales": 1,
                                           "normas": [{"document_key": "DS-2-2020", "senales": ["pág. 2: salto - ..."]}]}}
        t = auditoria.texto_resumen_telegram(resumen)
        self.assertIn("DS-2-2020", t)
        self.assertIn("Orientativo", t)
        self.assertLess(len(t), 4000)

    def test_sin_credenciales_no_envia(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertFalse(auditoria.enviar_resumen_telegram({"paginas": 0, "normas": 0, "aptas_para_consulta": 0, "no_aptas_para_consulta": 0}))


if __name__ == "__main__":
    unittest.main()
