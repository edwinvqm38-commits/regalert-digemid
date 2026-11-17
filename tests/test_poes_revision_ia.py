import json
import unittest

from agents.poes_costo import Tarifa, cotizar
from agents.poes_revision_ia import formatear_reporte_ia, parsear_hallazgos, revisar_poes
from agents.poes_analisis import analizar_poes

P = lambda n, k, t, tit: f"POE-B N° {n:03d}\nPÁG: {k} de {t}\n{tit}\n" + "paso del procedimiento " * 10
PAGINAS = [P(1, 1, 1, "RECEPCIÓN DE PRODUCTOS FARMACÉUTICOS"), P(2, 1, 1, "LIMPIEZA DEL ESTABLECIMIENTO")]
NORMAS = [{"document_key": "RM-132-2015", "titulo": "BPA", "extracto": "texto", "estado_vigencia": "vigente"}]


class RevisionIaTest(unittest.TestCase):
    def test_descarta_hallazgos_sin_norma_del_contexto(self):
        resp = json.dumps({"hallazgos": [
            {"tipo": "falta", "severidad": "alta", "descripcion": "Falta X", "norma_ref": "RM-132-2015"},
            {"tipo": "falta", "severidad": "alta", "descripcion": "Inventada", "norma_ref": "LEY-999"},
            {"tipo": "otro", "severidad": "alta", "descripcion": "Tipo raro", "norma_ref": "RM-132-2015"},
        ]})
        h, d = parsear_hallazgos(resp, {"RM-132-2015"}, "P")
        self.assertEqual((len(h), d), (1, 2))
        self.assertEqual(parsear_hallazgos("no json", {"a"}, "P"), ([], 0))

    def test_flujo_completo_con_llm_falso(self):
        r = analizar_poes(PAGINAS, {})
        llamadas = []

        def llm(system, user):
            llamadas.append(user)
            return json.dumps({"hallazgos": [{"tipo": "desactualizado", "severidad": "media",
                                              "descripcion": "Cita norma anterior", "norma_ref": "RM-132-2015"}]})

        rep = revisar_poes(PAGINAS, r.procedimientos, lambda t: NORMAS, llm, normas_recientes=lambda: NORMAS)
        self.assertEqual(len(llamadas), 3)  # 2 procedimientos + faltantes
        self.assertEqual(len(rep.hallazgos), 3)
        self.assertIn("RECEPCIÓN", rep.hallazgos[0].procedimiento)
        self.assertIn("Orientativo", formatear_reporte_ia(rep))

    def test_sin_normas_o_con_error_no_tumba_la_revision(self):
        r = analizar_poes(PAGINAS, {})

        def llm(s, u):
            raise TimeoutError()

        rep = revisar_poes(PAGINAS, r.procedimientos, lambda t: NORMAS if "LIMPIEZA" not in t else [], llm)
        self.assertEqual(len(rep.errores), 2)
        self.assertEqual(rep.hallazgos, [])


class CostoTest(unittest.TestCase):
    def test_cotizacion_crece_con_paginas_y_respeta_minimo(self):
        t = Tarifa(0.3, 1.2, 3.8, 50.0, 1.0)
        chico = cotizar(["x" * 500], 1, t)
        grande = cotizar(["x" * 3000] * 140, 17, t)
        self.assertGreaterEqual(chico.precio_pen, 1.0)
        self.assertGreater(grande.precio_pen, chico.precio_pen)
        self.assertGreater(grande.tokens_entrada, 140 * 900)
        self.assertEqual(grande.precio_pen % 0.5, 0)


if __name__ == "__main__":
    unittest.main()
