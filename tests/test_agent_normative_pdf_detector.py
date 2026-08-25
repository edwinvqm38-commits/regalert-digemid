"""Regresion de NormativePdfDetectorAgent (F-03B).

Hasta ahora este agente elegia `candidate_links[0]` tras ordenar por SCORE.
El bloqueo agregado despues -"si el mejor score empata con otro, no
escribas"- tapaba solo un sintoma: un UNICO candidato de score maximo seguia
pudiendo ganar sin que nadie hubiera comprobado que su CONTENIDO fuera la
norma objetivo.

Estos tests prueban que ahora:

  1. El score sigue existiendo -filtra basura y ordena que se inspecciona
     primero-, pero la decision de ESCRIBIR pasa siempre por
     `agents.politica_documental.decidir()`, la misma politica que usa el
     crawler (scripts/crawl_normativa_pdf_urls.py).
  2. Un score alto nunca gana sobre un contenido probado, y un score bajo
     nunca pierde contra un contenido probado.
  3. AMBIGUO, CONTRADICTORIO (via NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA),
     NO_ENCONTRADO, AUDITORIA_INCOMPLETA e identidad objetivo insuficiente
     nunca autorizan a escribir.

No se descarga ningun PDF real: `EvidenciaDocumental.apariciones` se
construye directo desde texto, igual que en test_identidad_documental.py y
test_politica_documental.py -asi los tests no dependen de red ni de pymupdf-.
"""

import re
import sys
import types
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))


def _stub_module(name, **attrs):
    """Registra un modulo falso en sys.modules SOLO si el real no esta
    instalado, para poder importar el agente sin credenciales de Supabase ni
    las dependencias de scraping/PDF que no hacen falta para estos tests."""
    if name in sys.modules:
        return sys.modules[name]
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


_stub_module("dotenv", load_dotenv=lambda *a, **k: None)


class _StubSupabaseClient:
    def table(self, *_a, **_k):
        raise AssertionError("Estos tests no deben tocar Supabase.")


_stub_module(
    "supabase",
    Client=_StubSupabaseClient,
    create_client=lambda *a, **k: _StubSupabaseClient(),
)

if "bs4" not in sys.modules:
    # Stub minimo: solo entiende <a href="...">texto</a>, que es todo lo que
    # _enlaces_candidatos necesita. Si el bs4 real esta instalado (como en
    # CI) este bloque ni se ejecuta.
    class _StubAnchor:
        def __init__(self, href: str, texto: str):
            self._href = href
            self._texto = texto

        def get(self, key, default=None):
            return self._href if key == "href" else default

        def get_text(self, _sep=" "):
            return self._texto

    class _StubBeautifulSoup:
        def __init__(self, html, *_a, **_k):
            self._anchors = [
                _StubAnchor(href, texto)
                for href, texto in re.findall(
                    r'<a\s+href="([^"]*)"[^>]*>([^<]*)</a>', html or "", re.IGNORECASE
                )
            ]

        def find_all(self, _tag=None, href=False, **_k):
            return self._anchors

    _stub_module("bs4", BeautifulSoup=_StubBeautifulSoup)

import agents.agent_normative_pdf_detector as detector  # noqa: E402
import agents.politica_documental as politica_documental  # noqa: E402
from identidad_documental import (  # noqa: E402
    AMBIGUO,
    AUDITORIA_INCOMPLETA,
    EvidenciaDocumental,
    NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
    identidades_en_texto,
)
from identidad_normativa import construir_identidad  # noqa: E402


def encabezado(tipo_largo: str, numero: str) -> str:
    return f"MINISTERIO DE SALUD\n\n{tipo_largo}\nN° {numero}\n\nLima, 3 de junio de 2024\n"


def candidato(objetivo, texto: str, url: str, **kw) -> EvidenciaDocumental:
    return EvidenciaDocumental(
        identidad_objetivo=objetivo,
        apariciones=identidades_en_texto(texto, 1),
        total_paginas=kw.pop("total_paginas", 1),
        texto_completo=texto,
        filename=url.rsplit("/", 1)[-1],
        url=url,
        **kw,
    )


class TestReutilizaLaPoliticaCanonica(unittest.TestCase):
    """El detector no reimplementa la regla: usa la misma funcion.

    `agent_normative_pdf_detector.py` importa `decidir` con un import "bare"
    (mismo patron que identidad_documental.py/politica_documental.py, que se
    insertan a si mismos en sys.path para no depender de que el paquete
    `agents` este en el path del llamador). Eso hace que Python la registre
    bajo el nombre de modulo "politica_documental" en vez de
    "agents.politica_documental", asi que `is` compara dos objetos-funcion
    distintos aunque vengan del mismo archivo. Se compara por bytecode -mismo
    codigo fuente- en vez de por identidad de objeto.
    """

    def test_decidir_es_la_misma_funcion_que_politica_documental(self):
        self.assertEqual(
            detector.decidir.__code__.co_filename,
            politica_documental.decidir.__code__.co_filename,
        )
        self.assertEqual(detector.decidir.__code__, politica_documental.decidir.__code__)


class TestIdentidadObjetivoDeDocumento(unittest.TestCase):
    """digemid_documentos no tiene tipo_norma/numero/anio: se parsea del
    titulo, igual que se parsea un encabezado dentro de un PDF."""

    def test_construye_identidad_desde_el_titulo(self):
        identidad = detector.identidad_objetivo_de_documento(
            "Resolución Ministerial N° 793-2025/MINSA",
            "NORM-RESOLUCION-MINISTERIAL-2025-3EEE0B42",
        )
        self.assertIsNotNone(identidad)
        self.assertEqual(identidad.tipo, "RM")
        self.assertEqual(identidad.numero, "793")
        self.assertEqual(identidad.anio, 2025)

    def test_document_key_hash_no_sirve_de_identidad(self):
        # Sin titulo, el document_key de esta norma (un hash) tampoco produce
        # una identidad util.
        identidad = detector.identidad_objetivo_de_documento(
            None, "NORM-RESOLUCION-MINISTERIAL-2025-3EEE0B42"
        )
        self.assertIsNone(identidad)

    def test_sin_titulo_ni_document_key_util_no_hay_identidad(self):
        self.assertIsNone(detector.identidad_objetivo_de_documento("", ""))
        self.assertIsNone(detector.identidad_objetivo_de_documento(None, None))


class TestNoConfundirNormaCitadaConLaPropia(unittest.TestCase):
    """Hallazgo de la SHADOW VALIDATION F-03B.

    La primera version de `identidad_objetivo_de_documento` tomaba la
    PRIMERA identidad que encontraba en el titulo, sin importar su posicion.
    Cuando el titulo declara su propio numero primero eso da la respuesta
    correcta -pura casualidad de que el numero propio venga antes-, pero un
    titulo que SOLO cita a otra norma sin declarar la suya hacia que la
    funcion adoptara el numero de la norma CITADA como si fuera la identidad
    de este documento. Exactamente la confusion que F-03 existe para evitar,
    trasladada del contenido del PDF a la fuente del titulo.

    La correccion exige que la identidad aparezca ANCLADA al inicio del
    texto (`PATRON_ENCABEZADO.match`, no `finditer` en cualquier posicion).
    """

    def test_titulo_propio_seguido_de_cita_toma_la_identidad_propia(self):
        identidad = detector.identidad_objetivo_de_documento(
            "Resolución Ministerial N° 793-2025/MINSA que modifica la "
            "Resolución Ministerial N° 419-2025/MINSA"
        )
        self.assertIsNotNone(identidad)
        self.assertEqual(identidad.numero, "793")

    def test_decreto_supremo_propio_seguido_de_cita_toma_la_identidad_propia(self):
        identidad = detector.identidad_objetivo_de_documento(
            "Decreto Supremo N° 020-2024-SA que modifica el Decreto "
            "Supremo N° 014-2011-SA"
        )
        self.assertIsNotNone(identidad)
        self.assertEqual(identidad.numero, "20")

    def test_titulo_que_solo_cita_sin_declarar_la_suya_no_produce_identidad(self):
        """El titulo NUNCA dice su propio numero: solo dice que "modifica" a
        otra norma. Aceptar el numero citado como identidad propia asociaria
        este documento con el PDF de OTRA norma."""
        identidad = detector.identidad_objetivo_de_documento(
            "Resolución Ministerial que aprueba el TUPA, modificado por "
            "Resolución Ministerial N° 100-2020/MINSA"
        )
        self.assertIsNone(identidad)

    def test_titulo_puramente_una_cita_no_produce_identidad(self):
        identidad = detector.identidad_objetivo_de_documento(
            "Modifican la Resolución Ministerial N° 419-2025/MINSA"
        )
        self.assertIsNone(identidad)

    def test_deroga_sin_declarar_la_propia_no_produce_identidad(self):
        identidad = detector.identidad_objetivo_de_documento(
            "Derogan diversos artículos del Decreto Supremo N° 014-2011-SA"
        )
        self.assertIsNone(identidad)


class TestSinIdentidadObjetivoNoEscribe(unittest.TestCase):
    """Regresion obligatoria: sin identidad objetivo verificable → no escribe.

    Se prueba sobre `detect_pdf_url` directamente: con identidad None debe
    devolver de inmediato, sin tocar la red -por eso es seguro correrlo aqui
    sin credenciales ni mocks de requests-.
    """

    def test_detect_pdf_url_no_escribe_sin_identidad(self):
        agente = detector.NormativePdfDetectorAgent.__new__(
            detector.NormativePdfDetectorAgent
        )
        resultado = agente.detect_pdf_url("https://x/detalle", None)

        self.assertEqual(resultado["status"], "pdf_identity_unknown")
        self.assertIsNone(resultado["pdf_url"])
        self.assertNotEqual(resultado["status"], "pdf_detected")


class TestElScoreOrdenaPeroNuncaDecide(unittest.TestCase):
    """Las seis regresiones que pidio el usuario, una por una."""

    OBJETIVO = construir_identidad("RM", "373-2024-MINSA")

    def test_score_alto_con_contenido_incorrecto_pierde_contra_score_menor_exacto(self):
        # "wrong.pdf" es lo que el score antiguo habria elegido: URL bajo
        # /archivos/normatividad/ con texto de enlace "Descargar PDF" (score
        # alto). "right.pdf" es un enlace generico (score bajo). El contenido
        # decide, no el score.
        alto_score_incorrecto = candidato(
            self.OBJETIVO,
            encabezado("LEY", "29698"),  # otra norma, no la objetivo
            "https://x/archivos/normatividad/wrong.pdf",
        )
        bajo_score_exacto = candidato(
            self.OBJETIVO,
            encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA"),
            "https://x/generic/right.pdf",
        )

        decision = politica_documental.decidir(
            [alto_score_incorrecto, bajo_score_exacto], self.OBJETIVO
        )

        self.assertTrue(decision.escribir)
        self.assertEqual(decision.url, "https://x/generic/right.pdf")

    def test_unico_candidato_score_alto_pero_identidad_contradictoria_no_escribe(self):
        unico = candidato(
            self.OBJETIVO,
            encabezado("LEY", "29698"),  # el documento ES otra norma
            "https://x/archivos/normatividad/wrong.pdf",
        )

        decision = politica_documental.decidir([unico], self.OBJETIVO)

        self.assertFalse(decision.escribir)
        self.assertIsNone(decision.url)
        self.assertEqual(decision.estado, NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA)

    def test_filename_correcto_pero_contenido_incorrecto_no_escribe(self):
        ev = candidato(
            self.OBJETIVO,
            encabezado("LEY", "29698"),
            "https://x/RM_373-2024-MINSA.pdf",  # el nombre SI sugiere la norma
        )
        self.assertTrue(ev.filename_match)
        self.assertFalse(ev.content_match)

        decision = politica_documental.decidir([ev], self.OBJETIVO)

        self.assertFalse(decision.escribir)

    def test_filename_incorrecto_pero_encabezado_exacto_puede_aceptar(self):
        ev = candidato(
            self.OBJETIVO,
            encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA"),
            "https://x/documento.pdf",  # nombre generico, no sugiere nada
        )
        self.assertFalse(ev.filename_match)
        self.assertTrue(ev.content_match)

        decision = politica_documental.decidir([ev], self.OBJETIVO)

        self.assertTrue(decision.escribir)
        self.assertEqual(decision.url, "https://x/documento.pdf")

    def test_empate_de_identidad_no_escribe(self):
        # Dos documentos DISTINTOS afirman -por contenido, no por score- ser
        # la norma objetivo. Nadie desempata solo; es un caso para un humano.
        primero = candidato(
            self.OBJETIVO,
            encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA"),
            "https://x/a.pdf",
        )
        segundo = candidato(
            self.OBJETIVO,
            encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA"),
            "https://x/b.pdf",
        )

        decision = politica_documental.decidir([primero, segundo], self.OBJETIVO)

        self.assertFalse(decision.escribir)
        self.assertEqual(decision.estado, AMBIGUO)

    def test_auditoria_incompleta_no_escribe(self):
        ev = candidato(
            self.OBJETIVO,
            "Portada\n",  # sin encabezado alguno en lo leido
            "https://x/grande.pdf",
            total_paginas=40,
        )
        ev.paginas_analizadas = 3  # se corto antes de terminar de leer

        decision = politica_documental.decidir([ev], self.OBJETIVO)

        self.assertFalse(decision.escribir)
        self.assertEqual(decision.estado, AUDITORIA_INCOMPLETA)

    def test_score_empatado_no_bloquea_si_el_contenido_desempata(self):
        """La prueba directa del defecto original: dos enlaces con el MISMO
        score (el bloqueo por empate de score de la version anterior los
        habria descartado a los dos sin mirar el contenido). Con contenido
        se sabe cual es: se escribe.
        """
        mismo_score_incorrecto = candidato(
            self.OBJETIVO, "Portada sin encabezado.\n", "https://x/archivos/a.pdf",
        )
        mismo_score_correcto = candidato(
            self.OBJETIVO,
            encabezado("RESOLUCION MINISTERIAL", "373-2024/MINSA"),
            "https://x/archivos/b.pdf",
        )

        decision = politica_documental.decidir(
            [mismo_score_incorrecto, mismo_score_correcto], self.OBJETIVO
        )

        self.assertTrue(decision.escribir)
        self.assertEqual(decision.url, "https://x/archivos/b.pdf")


class TestEnlacesCandidatosNoDecide(unittest.TestCase):
    """`_enlaces_candidatos` solo filtra y ORDENA por score; no elimina
    candidatos por comparacion relativa entre ellos (eso quedaria decidiendo
    por score otra vez)."""

    def test_devuelve_ambos_candidatos_aunque_el_score_sea_muy_distinto(self):
        html = (
            '<a href="/archivos/normatividad/wrong.pdf">Descargar PDF</a>'
            '<a href="/otro/right.pdf">enlace</a>'
        )
        agente = detector.NormativePdfDetectorAgent.__new__(
            detector.NormativePdfDetectorAgent
        )
        agente.ignored_link_connection_errors = 0

        candidatos = agente._enlaces_candidatos("https://x/detalle", html)
        urls = [url for url, _texto in candidatos]

        self.assertEqual(len(candidatos), 2)
        self.assertIn("https://x/archivos/normatividad/wrong.pdf", urls)
        self.assertIn("https://x/otro/right.pdf", urls)
        # El de mayor score va primero -eso SI es su trabajo-, pero eso no
        # significa que sea el elegido: decidir() ignora este orden.
        self.assertEqual(urls[0], "https://x/archivos/normatividad/wrong.pdf")


class TestMapeoDeEstados(unittest.TestCase):
    """Ningun estado que no sea de escritura puede terminar mapeado a
    "pdf_detected"."""

    def test_ningun_estado_no_escribible_mapea_a_detected(self):
        for estado, status in detector._ESTADO_A_STATUS.items():
            self.assertNotEqual(
                status, "pdf_detected",
                f"{estado} no debe poder producir process_status=pdf_detected",
            )


if __name__ == "__main__":
    unittest.main()
