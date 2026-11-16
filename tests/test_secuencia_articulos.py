import unittest

from agents.secuencia_articulos import analizar_secuencia


def arts(nums, extra=""):
    return "\n".join(f"Artículo {n}°.- Texto del artículo {n}. {extra}" for n in nums)


class SecuenciaArticulosTest(unittest.TestCase):
    def test_secuencia_completa_sin_senales(self):
        r = analizar_secuencia([arts([1, 2, 3]), arts([4, 5, 6])])
        self.assertTrue(r.evaluable)
        self.assertEqual(r.senales, [])

    def test_salto_por_pagina_perdida(self):
        r = analizar_secuencia([arts([1, 2, 3]), arts([7, 8])])
        self.assertEqual([s.tipo for s in r.senales], ["salto"])
        self.assertEqual(r.senales[0].pagina, 2)
        self.assertIn("faltan 4, 5, 6", r.senales[0].detalle)

    def test_retroceso_y_repetido(self):
        r = analizar_secuencia([arts([1, 2, 3, 4, 5]), arts([5, 3])])
        self.assertEqual([s.tipo for s in r.senales], ["repetido", "retroceso"])

    def test_numero_leido_con_letras(self):
        txt = arts([1, 2, 3, 4]) + "\nArtículo 1O°.- texto\n"
        r = analizar_secuencia([txt])
        tipos = [s.tipo for s in r.senales]
        self.assertIn("numero_dudoso", tipos)

    def test_referencias_dentro_de_frases_no_cuentan(self):
        txt = arts([1, 2, 3, 4, 5]) + "\nsegún lo dispuesto en el artículo 40° de la Ley, se aplica.\n"
        self.assertEqual(analizar_secuencia([txt]).senales, [])

    def test_bis_no_rompe_la_secuencia(self):
        txt = "\n".join(["Artículo 1°.- a", "Artículo 2°.- b", "Artículo 2-A.- c", "Artículo 3°.- d", "Artículo 4°.- e", "Artículo 5°.- f"])
        self.assertEqual(analizar_secuencia([txt]).senales, [])

    def test_reinicio_en_anexo_no_es_error(self):
        r = analizar_secuencia([arts([1, 2, 3, 4, 5]), arts([1, 2, 3])])
        self.assertEqual(r.senales, [])

    def test_numero_atipico_se_marca_sin_romper_el_resto(self):
        r = analizar_secuencia([arts([1, 2, 3, 4]), arts([659]), arts([5, 6])])
        self.assertEqual([s.tipo for s in r.senales], ["numero_dudoso"])

    def test_norma_modificatoria_no_se_evalua(self):
        txt = "Modifíquese el artículo 28 de la Ley.\n" + arts([28, 30, 32, 39, 46])
        r = analizar_secuencia([txt])
        self.assertFalse(r.evaluable)
        self.assertEqual(r.senales, [])
        self.assertIn("modificatoria", r.motivo_no_evaluable)

    def test_pocos_articulos_no_se_evaluan(self):
        r = analizar_secuencia([arts([1, 9])])
        self.assertFalse(r.evaluable)
        self.assertEqual(r.senales, [])


if __name__ == "__main__":
    unittest.main()
