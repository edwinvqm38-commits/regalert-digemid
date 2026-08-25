"""La política documental canónica: cuándo se puede escribir un pdf_url (F-03B).

Una sola regla para las cinco rutas que hoy escriben por posición. Si estos
tests pasan a fallar, alguna ruta volvió a elegir "el mejor candidato".
"""

import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from identidad_documental import (  # noqa: E402
    AUDITORIA_INCOMPLETA,
    EvidenciaDocumental,
    identidades_en_texto,
)
from identidad_normativa import NormaIdentity, construir_identidad  # noqa: E402
from politica_documental import (  # noqa: E402
    REQUIERE_HUMANO,
    REQUIERE_REAUDITORIA,
    decidir,
)


def encabezado(tipo_largo, numero):
    return f"MINISTERIO DE SALUD\n\n{tipo_largo}\nN° {numero}\n\nLima, 3 de junio de 2024\n"


def candidato(objetivo, texto, url, **kw):
    ev = EvidenciaDocumental(
        identidad_objetivo=objetivo,
        apariciones=identidades_en_texto(texto, 1),
        total_paginas=1,
        texto_completo=texto,
        filename=url.rsplit("/", 1)[-1],
        url=url,
        **kw,
    )
    return ev


class TestPoliticaCanonica(unittest.TestCase):

    OBJETIVO = construir_identidad("RM", "1000-2016-MINSA")

    def test_un_solo_candidato_probado_si_se_escribe(self):
        d = decidir(
            [candidato(self.OBJETIVO,
                       encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA"),
                       "https://x/rm1000.pdf")],
            self.OBJETIVO,
        )
        self.assertTrue(d.escribir)
        self.assertEqual(d.url, "https://x/rm1000.pdf")

    def test_dos_candidatos_plausibles_van_a_un_humano(self):
        """No hay desempate automático. Dos documentos que dicen ser la misma
        norma es exactamente el caso que un score resolvería mal."""
        d = decidir(
            [
                candidato(self.OBJETIVO,
                          encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA"),
                          "https://x/a.pdf"),
                candidato(self.OBJETIVO,
                          encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA"),
                          "https://x/b.pdf"),
            ],
            self.OBJETIVO,
        )
        self.assertFalse(d.escribir)
        self.assertIsNone(d.url)
        self.assertEqual(d.seguimiento, REQUIERE_HUMANO)

    def test_el_nombre_del_archivo_no_gana_al_contenido(self):
        """`RM_1000-2016.pdf` que por dentro es la RM 1001: no se escribe."""
        d = decidir(
            [candidato(self.OBJETIVO,
                       encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA"),
                       "https://x/RM_1000-2016.pdf")],
            self.OBJETIVO,
        )
        self.assertFalse(d.escribir)
        self.assertEqual(d.seguimiento, REQUIERE_HUMANO)

    def test_candidatos_omitidos_bloquean_por_incompletitud_no_por_ausencia(self):
        d = decidir(
            [candidato(self.OBJETIVO,
                       encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA"),
                       "https://x/a.pdf")],
            self.OBJETIVO,
            candidatos_omitidos=14,
            motivo_omision="la pagina listaba mas de 40 PDF",
        )
        self.assertFalse(d.escribir)
        self.assertEqual(d.estado, AUDITORIA_INCOMPLETA)
        self.assertTrue(d.bloqueada_por_incompletitud)
        self.assertEqual(d.seguimiento, REQUIERE_REAUDITORIA)

    def test_sin_identidad_objetivo_no_se_escribe_nada(self):
        d = decidir([], NormaIdentity(tipo=None, numero=None, anio=None, sector=None))
        self.assertFalse(d.escribir)
        self.assertEqual(d.seguimiento, REQUIERE_HUMANO)
        self.assertIn("identidad", d.motivo)

    def test_la_evidencia_de_cada_candidato_queda_registrada(self):
        d = decidir(
            [candidato(self.OBJETIVO,
                       encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA"),
                       "https://x/a.pdf")],
            self.OBJETIVO,
        )
        self.assertIsNotNone(d.evidencia)
        self.assertEqual(len(d.evidencia["candidatos"]), 1)
        self.assertIn("clasificacion", d.evidencia["candidatos"][0])


if __name__ == "__main__":
    unittest.main()
