"""Identidad DOCUMENTAL: ¿este PDF es el de esta norma? (F-03)

Cada test corresponde a una forma real en que el crawler asoció un PDF a la
norma equivocada. Todos deben fallar si se vuelve a elegir un documento por su
posición en el HTML o por el nombre del archivo.
"""

import re
import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "agents"))
sys.path.insert(0, str(RAIZ / "scripts"))

from identidad_documental import (  # noqa: E402
    AMBIGUO,
    AUDITORIA_INCOMPLETA,
    CONTRADICTORIO,
    DOCUMENTO_INDETERMINADO,
    DOCUMENTO_MULTINORMA,
    DOCUMENTO_NORMA_UNICA,
    DOCUMENTO_PROYECTO,
    EvidenciaDocumental,
    MATCH_EXACTO,
    MATCH_MULTINORMA,
    NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
    PDF_IDENTIDAD_AMBIGUA,
    PDF_IDENTIDAD_CONTRADICTORIA,
    PDF_IDENTIDAD_EXACTA,
    PDF_AUDITORIA_INCOMPLETA,
    PDF_CONTIENE_NORMA_EN_MULTINORMA,
    PDF_NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
    clasificar_identidad_documental,
    identidades_en_texto,
    rango_de_paginas,
    resolver_pdf_para_norma,
    segmento_multinorma,
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
        self.assertEqual(r.estado, NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA)
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
            ([], NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA),
            ([evidencia(objetivo, {1: "sin encabezado"}, url="a.pdf")], NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA),
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


# ===========================================================================
# F-03 · 9 — Sector / sufijo: información adicional vs información distinta
# ===========================================================================
class TestSectorYSufijo(unittest.TestCase):
    """`RM-373-2024` (ficha) y `RM 373-2024/MINSA` (PDF) son la MISMA norma.

    `RM 373-2024/MINSA` y `RM 373-2024-SA` no lo son. La diferencia está en si
    el sector falta o si está y dice otra cosa: lo primero es una ficha
    incompleta, lo segundo es una contradicción.
    """

    def test_bd_sin_sector_y_pdf_con_sector_son_compatibles(self):
        objetivo = construir_identidad("RM", "373-2024")  # ficha sin sufijo
        self.assertIsNone(objetivo.sector or None)

        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")})
        clasificacion, confianza, _ = clasificar_identidad_documental(ev)

        self.assertEqual(clasificacion, PDF_IDENTIDAD_EXACTA)
        self.assertEqual(confianza, "alta")

    def test_bd_con_sector_y_pdf_sin_sector_son_compatibles(self):
        objetivo = construir_identidad("RM", "373-2024-MINSA")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "373-2024")})

        clasificacion, _, _ = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_EXACTA)

    def test_dos_sectores_explicitos_y_distintos_son_discrepancia(self):
        objetivo = construir_identidad("RM", "373-2024-MINSA")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "373-2024-SA")})

        clasificacion, _, motivo = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_CONTRADICTORIA)
        self.assertIn("373", motivo)

    def test_el_sector_no_rescata_a_un_numero_distinto(self):
        """Compartir sector no acerca a dos normas: 1000 y 1001 siguen siendo
        distintas aunque ambas sean /MINSA."""
        objetivo = construir_identidad("RM", "1000-2016-MINSA")
        ev = evidencia(objetivo, {1: encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA")})

        clasificacion, _, _ = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_IDENTIDAD_CONTRADICTORIA)


# ===========================================================================
# F-03 · 10 — Encabezado propio vs las siete formas de MENCIÓN
# ===========================================================================
class TestSieteFormasDeMencion(unittest.TestCase):
    """Ninguna de estas siete formas convierte a un PDF en la norma citada."""

    OBJETIVO = construir_identidad("DS", "14-2011-SA")

    def _no_es_encabezado_propio(self, texto, etiqueta):
        ev = evidencia(self.OBJETIVO, {1: texto})
        self.assertFalse(
            ev.content_match,
            f"{etiqueta}: se tomó una mención por encabezado propio",
        )

    def test_a_encabezado_propio_si_cuenta(self):
        ev = evidencia(self.OBJETIVO, {1: encabezado("DECRETO SUPREMO", "014-2011-SA")})
        self.assertTrue(ev.content_match)
        self.assertEqual(clasificar_identidad_documental(ev)[0], PDF_IDENTIDAD_EXACTA)

    def test_b_considerando(self):
        self._no_es_encabezado_propio(
            "Que, mediante Decreto Supremo N° 014-2011-SA se aprobó el Reglamento "
            "de Establecimientos Farmacéuticos, el cual establece los requisitos;",
            "considerando",
        )

    def test_c_pie_de_pagina(self):
        self._no_es_encabezado_propio(
            "El presente documento desarrolla los alcances descritos.\n"
            "____________________\n"
            "1 Véase el Decreto Supremo N° 014-2011-SA, artículo 12.\n",
            "pie de página",
        )

    def test_d_bibliografia_o_referencia(self):
        self._no_es_encabezado_propio(
            "VII. REFERENCIAS BIBLIOGRAFICAS\n"
            "1. Ministerio de Salud. Decreto Supremo N° 014-2011-SA. Lima; 2011.\n"
            "2. Organización Mundial de la Salud. Buenas prácticas. Ginebra; 2015.\n",
            "bibliografía",
        )

    def test_e_anexo_que_cita(self):
        self._no_es_encabezado_propio(
            "ANEXO 2\nFormato de solicitud\n\n"
            "Declaro conocer las obligaciones del Decreto Supremo N° 014-2011-SA "
            "y me comprometo a cumplirlas.\n",
            "anexo",
        )

    def test_f_proyecto_que_cita(self):
        texto = (
            "PROYECTO PARA PUBLICACION\n\n"
            "Propuesta de modificación del Decreto Supremo N° 014-2011-SA que "
            "aprueba el Reglamento de Establecimientos Farmacéuticos.\n"
        )
        self._no_es_encabezado_propio(texto, "proyecto")
        # Además, un proyecto nunca debe pasar por norma aprobada.
        self.assertEqual(tipo_de_documento([], texto), DOCUMENTO_PROYECTO)

    def test_g_disposicion_que_cita_otra_norma(self):
        """Una RM cuyo artículo deroga un DS: el DS aparece en la parte
        dispositiva, pero el documento sigue siendo la RM."""
        texto = (
            encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA")
            + "\nSE RESUELVE:\n\n"
            "Artículo 2.- Derógase el Decreto Supremo N° 014-2011-SA.\n"
        )
        ev = evidencia(construir_identidad("DS", "14-2011-SA"), {1: texto})
        self.assertFalse(ev.content_match, "una derogación no convierte al PDF en la derogada")

        # Y la identidad del documento sí es la RM que lo encabeza.
        propio = evidencia(construir_identidad("RM", "373-2024-MINSA"), {1: texto})
        self.assertTrue(propio.content_match)
        self.assertEqual(clasificar_identidad_documental(propio)[0], PDF_IDENTIDAD_EXACTA)


# ===========================================================================
# F-03 · 7 — Una auditoría incompleta no concluye nada en contra
# ===========================================================================
class TestAuditoriaIncompleta(unittest.TestCase):

    OBJETIVO = construir_identidad("RM", "734-2025-MINSA")

    def test_no_leer_todas_las_paginas_no_es_no_encontrada(self):
        """Se leyeron 2 de 20 páginas y no apareció la norma. Eso NO prueba
        que no esté: puede estar en la página 15."""
        ev = evidencia(
            self.OBJETIVO,
            {1: "Portada\n", 2: "Indice general\n"},
        )
        ev.total_paginas = 20
        ev.paginas_analizadas = 2

        clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_AUDITORIA_INCOMPLETA)
        self.assertEqual(confianza, "nula")
        self.assertIn("2/20", motivo)
        self.assertNotEqual(clasificacion, PDF_NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA)

    def test_leer_todas_y_no_hallar_encabezado_si_es_concluyente(self):
        ev = evidencia(self.OBJETIVO, {1: "Portada\n", 2: "Indice general\n"})
        ev.paginas_analizadas = 2

        clasificacion, confianza, _ = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA)
        self.assertEqual(confianza, "alta")

    def test_hallazgo_positivo_sobrevive_a_la_incompletitud_pero_no_afirma_exclusividad(self):
        """Ver el encabezado prueba que el documento la contiene. No prueba que
        sea el único encabezado: para eso hay que leerlo entero."""
        ev = evidencia(self.OBJETIVO, {1: encabezado("RESOLUCION MINISTERIAL", "734-2025/MINSA")})
        ev.total_paginas = 20
        ev.paginas_analizadas = 1

        clasificacion, confianza, motivo = clasificar_identidad_documental(ev)
        self.assertEqual(clasificacion, PDF_CONTIENE_NORMA_EN_MULTINORMA)
        self.assertEqual(confianza, "media")
        self.assertIn("unico encabezado", motivo)

    def test_truncar_candidatos_impide_concluir_no_encontrada(self):
        """La página listaba 20 PDF y el auditor miró 6. Con esa lista truncada
        no puede decirse que la norma no esté en ninguno."""
        candidatos = [
            evidencia(self.OBJETIVO, {1: encabezado("RESOLUCION MINISTERIAL", "733-2025/MINSA")},
                      url="https://x/a.pdf"),
            evidencia(self.OBJETIVO, {1: encabezado("RESOLUCION MINISTERIAL", "735-2025/MINSA")},
                      url="https://x/b.pdf"),
        ]
        r = resolver_pdf_para_norma(
            candidatos, self.OBJETIVO,
            candidatos_omitidos=18,
            motivo_omision="la página listaba 20 PDF y el límite era 2",
        )

        self.assertEqual(r.estado, AUDITORIA_INCOMPLETA)
        self.assertFalse(r.puede_escribirse)
        self.assertFalse(r.auditoria_completa)
        self.assertIn("18 candidato", r.motivo)

    def test_sin_omisiones_si_se_concluye_tras_auditoria_completa(self):
        candidatos = [
            evidencia(self.OBJETIVO, {1: encabezado("RESOLUCION MINISTERIAL", "733-2025/MINSA")},
                      url="https://x/a.pdf"),
        ]
        r = resolver_pdf_para_norma(candidatos, self.OBJETIVO, candidatos_omitidos=0)

        self.assertEqual(r.estado, NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA)
        self.assertTrue(r.auditoria_completa)
        self.assertFalse(r.puede_escribirse)


# ===========================================================================
# F-03 · 11 — El rango de un multinorma necesita evidencia, no solo números
# ===========================================================================
class TestSegmentoMultinorma(unittest.TestCase):

    def _peruano(self, objetivo):
        ev = evidencia(objetivo, {
            1: encabezado("RESOLUCION MINISTERIAL", "1000-2016/MINSA"),
            2: "Artículo 3.- Vigencia.\n",
            3: encabezado("RESOLUCION MINISTERIAL", "1001-2016/MINSA"),
            4: "Artículo 2.- Publicación.\n",
            5: encabezado("DECRETO SUPREMO", "010-2017-SA"),
        })
        ev.pdf_sha256 = "a" * 64
        return ev

    def test_el_rango_trae_evidencia_de_inicio_y_de_fin(self):
        objetivo = construir_identidad("RM", "1001-2016-MINSA")
        seg = segmento_multinorma(self._peruano(objetivo), objetivo)

        self.assertEqual((seg.start_page, seg.end_page), (3, 4))
        self.assertEqual(seg.pdf_sha256, "a" * 64)
        self.assertIn("p.3", seg.evidencia_inicio)
        self.assertIn("1001", seg.evidencia_inicio)
        # El fin se prueba con el encabezado de la SIGUIENTE norma.
        self.assertIn("p.5", seg.evidencia_fin)
        self.assertIn("DS", seg.evidencia_fin)
        self.assertTrue(seg.rango_completo)
        self.assertTrue(seg.es_utilizable)

    def test_la_ultima_norma_del_documento_necesita_haberlo_leido_entero(self):
        objetivo = construir_identidad("DS", "10-2017-SA")
        ev = self._peruano(objetivo)
        ev.total_paginas = 12          # el PDF tiene 12 páginas...
        ev.paginas_analizadas = 5      # ...y solo se leyeron 5

        seg = segmento_multinorma(ev, objetivo)
        self.assertEqual(seg.start_page, 5)
        self.assertFalse(seg.rango_completo, "no se puede cerrar el rango sin leer el final")
        self.assertFalse(seg.es_utilizable)

    def test_un_rango_sin_cerrar_no_autoriza_a_escribir(self):
        objetivo = construir_identidad("DS", "10-2017-SA")
        ev = self._peruano(objetivo)
        ev.url = "https://x/peruano.pdf"
        ev.total_paginas = 12
        ev.paginas_analizadas = 5

        r = resolver_pdf_para_norma([ev], objetivo)
        self.assertEqual(r.estado, MATCH_MULTINORMA)
        self.assertFalse(r.puede_escribirse, "un rango sin evidencia de fin no se escribe")

    def test_norma_ausente_no_produce_rango(self):
        objetivo = construir_identidad("RM", "999-2016-MINSA")
        seg = segmento_multinorma(self._peruano(objetivo), objetivo)

        self.assertIsNone(seg.start_page)
        self.assertFalse(seg.es_utilizable)


class TestRecomendacionDeLaMatriz(unittest.TestCase):
    """La recomendación se calcula sobre la fila YA actualizada.

    Regresión: `fila.update(..., recommended_action=recomendar(..., fila))`
    evaluaba `recomendar` con la fila anterior, así que un multinorma con el
    rango resuelto salía recomendando "solo las paginas None-None".
    """

    def test_el_multinorma_recomienda_el_rango_real(self):
        import auditar_identidad_documental as auditor

        fila = {"start_page": 3, "end_page": 4,
                "pdf_page_count": 20, "stored_page_count": 20}
        texto = auditor.recomendar(PDF_CONTIENE_NORMA_EN_MULTINORMA,
                                   DOCUMENTO_MULTINORMA, fila)

        self.assertIn("3-4", texto)
        self.assertNotIn("None", texto)

    def test_recomendar_no_se_llama_dentro_del_update(self):
        """La prueba de arriba no reproduce el fallo real, que era de ORDEN:
        Python evalua los argumentos de `fila.update(...)` antes de aplicarla.
        Esto si lo reproduce."""
        fuente = (RAIZ / "scripts" / "auditar_identidad_documental.py").read_text(
            encoding="utf-8")
        dentro_del_update = re.search(
            r"fila\.update\((?:[^()]|\([^()]*\))*recommended_action\s*=\s*recomendar",
            fuente,
        )
        self.assertIsNone(
            dentro_del_update,
            "recomendar() debe llamarse DESPUES de fila.update(), no como argumento: "
            "si no, lee la fila vieja y reporta 'paginas None-None'",
        )


if __name__ == "__main__":
    unittest.main()
