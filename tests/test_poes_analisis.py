import unittest
from datetime import date

from agents.poes_analisis import (
    analizar_poes,
    extraer_citas,
    formatear_reporte,
    normalizar_clave,
)


def pagina(poe: int, n: int, total: int, extra: str = "") -> str:
    return f"BOTICA X POE-B N° {poe:03d}\nPÁG: {n} de {total}\n{extra} " + "texto del procedimiento " * 5


INDICE = "LISTADO DE PROCEDIMIENTOS\nPOE- BF - 001 A\nPOE- BF - 002 B\nPOE- BF - 004 C\n"


class PoesAnalisisTest(unittest.TestCase):
    def test_normalizar_clave(self):
        self.assertEqual(normalizar_clave("DS-014-2011-SA"), ("DS", 14, 2011))
        self.assertEqual(normalizar_clave("LEY-29459"), ("LEY", 29459, None))
        self.assertEqual(normalizar_clave("NORM-LEY-29459-LEY-DE-LOS"), ("LEY", 29459, None))
        self.assertIsNone(normalizar_clave("XYZ"))

    def test_extraer_citas(self):
        t = "Decreto Supremo N° 014-2011-SA y la Ley N° 29459; R.M. N° 585-99-SA/DM"
        self.assertEqual(
            extraer_citas(t),
            [("DS", 14, 2011), ("LEY", 29459, None), ("RM", 585, 1999)],
        )

    def test_completitud_indice_vigencia_y_normas(self):
        paginas = [
            "portada " * 10,
            INDICE,
            pagina(1, 1, 2, "Vigencia Hasta: 01-06-2024 Decreto Supremo N° 014-2011-SA"),
            pagina(1, 2, 2),
            pagina(2, 1, 3, "Ley N° 26842"),
            pagina(2, 2, 3),
            pagina(3, 1, 1),
        ]
        catalogo = {"DS-014-2011-SA": "modificada"}
        r = analizar_poes(paginas, catalogo, hoy=date(2026, 11, 16))
        self.assertEqual([p.numero for p in r.procedimientos], [1, 2, 3])
        self.assertEqual(r.incompletos, ["POE 002: trae 2 de 3 páginas"])
        self.assertEqual(len(r.indice_inconsistente), 2)  # sobra 004 y falta 003
        self.assertIn("Vigencia Hasta: 01-06-2024", r.vigencia_vencida)
        self.assertEqual(r.normas["modificada"], ["DS 14-2011"])
        self.assertEqual(r.normas["no_esta_en_base"], ["LEY 26842"])
        self.assertIn("Orientativo", formatear_reporte(r))

    def test_paginas_sin_texto_bajan_la_lectura(self):
        r = analizar_poes(["", "x" * 100], {}, hoy=date(2026, 1, 1))
        self.assertEqual(r.paginas_sin_texto, [1])
        self.assertEqual(r.lectura_pct, 50.0)


if __name__ == "__main__":
    unittest.main()
