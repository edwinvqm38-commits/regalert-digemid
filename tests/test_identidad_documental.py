"""Identidad DOCUMENTAL: ¿este PDF es el de esta norma? (F-03)

Cada test corresponde a una forma real en que el crawler asoció un PDF a la
norma equivocada. Todos deben fallar si se vuelve a elegir un documento por su
posición en el HTML o por el nombre del archivo.
"""

import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from identidad_documental import (  # noqa: E402
    AMBIGUO,
    CONTRADICTORIO,
    DOCUMENTO_INDETERMINADO,
    DOCUMENTO_MULTINORMA,
    DOCUMENTO_NORMA_UNICA,
    DOCUMENTO_PROYECTO,
    EvidenciaDocumental,
    MATCH_EXACTO,
    MATCH_MULTINORMA,
    NO_ENCONTRADO,
    PDF_IDENTIDAD_AMBIGUA,
    PDF_IDENTIDAD_CONTRADICTORIA,
    PDF_IDENTIDAD_EXACTA,
    PDF_CONTIENE_NORMA_EN_MULTINORMA,
    clasificar_identidad_documental,
    identidades_en_texto,
    rango_de_paginas,
    resolver_pdf_para_norma,
    tipo_de_documento,
)
from identidad_normativa import construir_identidad  # noqa: E402


def encabezado(tipo_largo, numero):
    return f"MINISTERIO DE SALUD\n\n{tipo_largo}\nN° {numero}\n\nLima, 3 de junio de 2024\n"


def evidencia(objetivo, paginas_texto, **kw):
    """paginas_texto: {numero_pagina: texto}"""
    apariciones = []
    for numero, texto in paginas_texto.items():
        apariciones.extend(identidades_en_texto(texto, numero))
    return EvidenciaDocumental(
        identidad_objetivo=objetivo,
        apariciones=apariciones,
        total_paginas=max(paginas_texto) if paginas_texto else 0,
        texto_completo="\n".join(paginas_texto.values()),
        **kw,
    )


class TestEncabezadoVsCita(unittest.TestCase):
    """Que un PDF MENCIONE una norma no significa que SEA esa norma."""

    def test_distingue_el_encabezado_propio_de_una_cita(self):
        texto = (encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")
                 + "CONSIDERANDO:\nQue, mediante Decreto Supremo N° 014-2011-SA se aprobo...")
        apariciones = identidades_en_texto(texto, 1)
        propios = [a for a in apariciones if a.es_encabezado]
        citas = [a for a in apariciones if not a.es_encabezado]
        self.assertEqual([a.identidad.tipo for a in propios], ["RM"])
        self.assertEqual([a.identidad.tipo for a in citas], ["DS"])

    def test_un_pdf_que_solo_cita_la_norma_no_es_esa_norma(self):
        objetivo = construir_identidad("DS", "014-2011-SA")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")
                                  + "Que, el Decreto Supremo N° 014-2011-SA establece..."})
        clasificacion, _, _ = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_CONTRADICTORIA)


class TestClasificacionDeDocumento(unittest.TestCase):
    def test_norma_unica(self):
        texto = encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")
        self.assertEqual(tipo_de_documento(identidades_en_texto(texto), texto),
                         DOCUMENTO_NORMA_UNICA)

    def test_multinorma_el_peruano(self):
        texto = (encabezado("DECRETO SUPREMO", "009-2015-SA")
                 + encabezado("DECRETO SUPREMO", "010-2015-SA"))
        self.assertEqual(tipo_de_documento(identidades_en_texto(texto), texto),
                         DOCUMENTO_MULTINORMA)

    def test_un_proyecto_no_es_una_norma_aprobada(self):
        texto = ("PROYECTO PARA PUBLICACION\n\n"
                 + encabezado("DECRETO SUPREMO", "001-2025-SA"))
        self.assertEqual(tipo_de_documento(identidades_en_texto(texto), texto),
                         DOCUMENTO_PROYECTO)

    def test_sin_encabezado_es_indeterminado(self):
        texto = "Documento sin encabezado normativo alguno."
        self.assertEqual(tipo_de_documento(identidades_en_texto(texto), texto),
                         DOCUMENTO_INDETERMINADO)


class TestSeleccionDePdf(unittest.TestCase):
    """El resolvedor que sustituye a `return candidatos[0]`."""

    def test_1_dos_pdf_en_la_pagina_solo_uno_coincide(self):
        objetivo = construir_identidad("RM", "1000-2016")
        a = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA")},
                      url="a.pdf")
        b = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA")},
                      url="b.pdf")
        r = resolver_pdf_para_norma([b, a], objetivo)   # b va primero a proposito
        self.assertEqual(r.estado, MATCH_EXACTO)
        self.assertEqual(r.url, "a.pdf", "no puede ganar por aparecer primero")
        self.assertTrue(r.puede_escribirse)

    def test_2_dos_pdf_que_coinciden_es_ambiguo_y_no_escribe(self):
        objetivo = construir_identidad("RM", "1000-2016")
        a = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA")}, url="a.pdf")
        b = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA")}, url="b.pdf")
        r = resolver_pdf_para_norma([a, b], objetivo)
        self.assertEqual(r.estado, AMBIGUO)
        self.assertIsNone(r.url)
        self.assertFalse(r.puede_escribirse)

    def test_3_ningun_pdf_coincide(self):
        objetivo = construir_identidad("RM", "1000-2016")
        a = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA")}, url="a.pdf")
        r = resolver_pdf_para_norma([a], objetivo)
        self.assertEqual(r.estado, NO_ENCONTRADO)
        self.assertFalse(r.puede_escribirse)

    def test_4_filename_correcto_pero_contenido_incorrecto_se_rechaza(self):
        """El caso RM-1000/RM-1001: el nombre miente, el contenido manda."""
        objetivo = construir_identidad("RM", "1000-2016")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA")},
                       filename="RM_1000-2016.pdf", url="x.pdf")
        clasificacion, _, motivo = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_CONTRADICTORIA)
        self.assertIn("nombre", motivo)
        self.assertTrue(ev.filename_match, "el nombre SI coincide, y aun asi se rechaza")

    def test_5_filename_generico_pero_contenido_correcto_se_acepta(self):
        objetivo = construir_identidad("RM", "1000-2016")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA")},
                       filename="documento.pdf", url="x.pdf")
        clasificacion, confianza, _ = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_EXACTA)
        self.assertEqual(confianza, "alta")
        self.assertFalse(ev.filename_match)

    def test_6_caso_real_rm_1000_y_rm_1001_intercambiadas(self):
        """Ambas normas apuntan al PDF de la otra: se detecta en las dos."""
        for numero_norma, numero_pdf in (("1000-2016", "1001-2016"), ("1001-2016", "1000-2016")):
            objetivo = construir_identidad("RM", numero_norma)
            ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", f"{numero_pdf}/MINSA")},
                           filename=f"RM_{numero_norma}.pdf", url="x.pdf")
            self.assertEqual(clasificar_identidad_documental(ev)[0],
                             PDF_IDENTIDAD_CONTRADICTORIA, numero_norma)

    def test_7_multinorma_con_rangos_de_pagina(self):
        objetivo = construir_identidad("DS", "010-2015-SA")
        paginas = {
            1: encabezado("DECRETO SUPREMO", "009-2015-SA"),
            2: "continuacion del decreto anterior",
            3: encabezado("DECRETO SUPREMO", "010-2015-SA"),
            4: "continuacion",
        }
        ev = evidencia(objetivo, paginas, url="peruano.pdf")
        clasificacion, _, _ = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_CONTIENE_NORMA_EN_MULTINORMA)

        r = resolver_pdf_para_norma([ev], objetivo)
        self.assertEqual(r.estado, MATCH_MULTINORMA)
        self.assertEqual((r.start_page, r.end_page), (3, 4),
                         "no se pueden guardar las 4 paginas como si la norma fuera todo el PDF")

    def test_7b_rango_de_la_primera_norma_termina_donde_empieza_la_segunda(self):
        objetivo = construir_identidad("DS", "009-2015-SA")
        paginas = {1: encabezado("DECRETO SUPREMO", "009-2015-SA"),
                   2: "sigue",
                   3: encabezado("DECRETO SUPREMO", "010-2015-SA")}
        ev = evidencia(objetivo, paginas, url="p.pdf")
        self.assertEqual(rango_de_paginas(ev.apariciones, objetivo, 3), (1, 2))

    def test_8_un_proyecto_anexado_no_se_confunde_con_la_norma(self):
        objetivo = construir_identidad("DS", "001-2025-SA")
        texto = "PROYECTO PARA PUBLICACION\n\n" + encabezado("DECRETO SUPREMO", "001-2025-SA")
        ev = evidencia(objetivo, {1: texto}, url="proyecto.pdf")
        self.assertEqual(tipo_de_documento(ev.apariciones, ev.texto_completo), DOCUMENTO_PROYECTO)

    def test_9_pagina_con_varios_pdf_no_elige_el_primero(self):
        """El defecto exacto de elegir_pdf(): candidatos[0] tras ordenar por ruta."""
        objetivo = construir_identidad("LEY", "29698")
        primero = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")},
                            url="https://x/Archivos/Normatividad/2024/RM_373-2024-MINSA.pdf")
        segundo = evidencia(objetivo, {1: encabezado("LEY", "29698")},
                            url="https://x/otro/ley29698.pdf")
        r = resolver_pdf_para_norma([primero, segundo], objetivo)
        self.assertEqual(r.url, "https://x/otro/ley29698.pdf")

    def test_10_ocr_del_render_contradice_la_capa_de_texto(self):
        objetivo = construir_identidad("RM", "373-2024/MINSA")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")},
                       url="x.pdf")
        ev.identidad_visual = construir_identidad("RM", "376-2023/MINSA")
        clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_AMBIGUA)
        self.assertEqual(confianza, "nula")
        self.assertIn("DISCREPANCIA_IDENTIDAD_CRITICA", motivo)


class TestNoEscribirSinPrueba(unittest.TestCase):
    def test_solo_un_match_probado_autoriza_a_escribir_pdf_url(self):
        objetivo = construir_identidad("RM", "1000-2016")
        for candidatos, esperado in (
            ([], NO_ENCONTRADO),
            ([evidencia(objetivo, {1: "sin encabezado"}, url="a.pdf")], NO_ENCONTRADO),
        ):
            r = resolver_pdf_para_norma(candidatos, objetivo)
            self.assertEqual(r.estado, esperado)
            self.assertFalse(r.puede_escribirse)

    def test_el_nombre_solo_no_alcanza_para_escribir(self):
        objetivo = construir_identidad("RM", "1000-2016")
        ev = evidencia(objetivo, {1: "documento sin encabezado normativo"},
                       filename="RM_1000-2016.pdf", url="a.pdf")
        clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_AMBIGUA)
        self.assertIn("el nombre no es evidencia", motivo)
        self.assertFalse(resolver_pdf_para_norma([ev], objetivo).puede_escribirse)


if __name__ == "__main__":
    unittest.main(verbosity=2)
