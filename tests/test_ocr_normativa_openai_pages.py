"""aplicar_filtro_pendientes decide que paginas entran como candidatas a la
verificacion por vision (scripts/ocr_normativa_openai_pages.py). El filtro
"graficos-pendientes" existe porque una pagina con posible_grafico=true puede
tener quality_score alto y sin tablas -- sin ese filtro nunca entraba como
candidata a pesar de que el heuristico existe justo para marcarla.
"""

import sys
import unittest
from argparse import Namespace
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from unittest.mock import MagicMock, patch  # noqa: E402

from scripts.ocr_normativa_openai_pages import (  # noqa: E402
    _post_con_reintentos,
    aplicar_filtro_pendientes,
    get_candidate_pages,
    transcribe_page_with_provider,
)


class FakeQuery:
    """Registra las llamadas encadenadas (.lt/.eq/.or_) en vez de pegarle a
    Supabase, para poder afirmar QUE filtro se armo sin red ni credenciales."""

    def __init__(self):
        self.llamadas: list[tuple[str, tuple]] = []

    def lt(self, *args):
        self.llamadas.append(("lt", args))
        return self

    def eq(self, *args):
        self.llamadas.append(("eq", args))
        return self

    def or_(self, *args):
        self.llamadas.append(("or_", args))
        return self


class TestAplicarFiltroPendientes(unittest.TestCase):
    def test_calidad_baja_filtra_por_quality_score(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="calidad-baja", quality_below=0.85))
        self.assertEqual(query.llamadas, [("lt", ("quality_score", 0.85))])

    def test_tablas_pendientes_filtra_por_tabla_no_verificada(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="tablas-pendientes", quality_below=0.85))
        self.assertEqual(
            query.llamadas,
            [("eq", ("has_tables", True)), ("eq", ("tabla_verificada", False))],
        )

    def test_graficos_pendientes_filtra_por_grafico_no_revisado(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="graficos-pendientes", quality_below=0.85))
        self.assertEqual(
            query.llamadas,
            [("eq", ("posible_grafico", True)), ("eq", ("revisado_manual", False))],
        )

    def test_todas_pendientes_incluye_las_tres_condiciones(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="todas-pendientes", quality_below=0.85))
        self.assertEqual(len(query.llamadas), 1)
        _, (clausula,) = query.llamadas[0]
        self.assertIn("quality_score.lt.0.85", clausula)
        self.assertIn("has_tables.eq.true,tabla_verificada.eq.false", clausula)
        self.assertIn("posible_grafico.eq.true,revisado_manual.eq.false", clausula)


class FakeQueryPaginas:
    """Fake fluido para get_candidate_pages: registra si aplicar_filtro_pendientes
    llego a ejecutarse (via .lt/.or_, que solo ese aplica) y si se filtro por
    page_number, sin pegarle a Supabase."""

    def __init__(self, paginas):
        self.paginas = paginas
        self.uso_filtro_pendientes = False
        self.page_number_filtrado = None

    def select(self, *_a):
        return self

    def eq(self, campo, valor):
        if campo == "page_number":
            self.page_number_filtrado = valor
        return self

    def order(self, *_a):
        return self

    def lt(self, *_a):
        self.uso_filtro_pendientes = True
        return self

    def or_(self, *_a):
        self.uso_filtro_pendientes = True
        return self

    def limit(self, _n):
        return self

    def execute(self):
        return MagicMock(data=self.paginas)


class FakeSupabasePaginas:
    def __init__(self, norma, paginas):
        self._norma = norma
        self._query = FakeQueryPaginas(paginas)

    def table(self, nombre):
        if nombre == "digemid_normas":
            return MagicMock(
                select=lambda *_a: MagicMock(
                    eq=lambda *_a: MagicMock(
                        limit=lambda *_a: MagicMock(execute=lambda: MagicMock(data=[self._norma])),
                    ),
                ),
            )
        return self._query


class TestGetCandidatePagesConPageNumber(unittest.TestCase):
    def test_page_number_filtra_por_pagina_y_evita_el_filtro_de_pendientes(self):
        norma = {"id": "norma-1", "document_key": "RM-431-2019"}
        supabase = FakeSupabasePaginas(norma, paginas=[{"page_number": 74, "norma_id": "norma-1"}])
        args = Namespace(
            document_key="RM-431-2019", page_number=74, all_pages=False,
            filtro="tablas-pendientes", quality_below=0.85, limit=10,
        )

        paginas = get_candidate_pages(supabase, args)

        self.assertEqual(len(paginas), 1)
        self.assertEqual(paginas[0]["norma"], norma)
        self.assertEqual(supabase._query.page_number_filtrado, 74)
        self.assertFalse(supabase._query.uso_filtro_pendientes)


class TestPostConReintentos(unittest.TestCase):
    def test_reintenta_en_503_y_sigue_con_el_siguiente_intento(self):
        """Confirmado en produccion: Gemini devolvio 503 Service Unavailable
        varias veces seguidas para la misma pagina real, y un reintento con
        espera si funciono despues -- era sobrecarga puntual del proveedor,
        no un problema de la imagen o el prompt."""
        llamadas = []

        def fake_post(url, **kwargs):
            llamadas.append(url)
            resp = MagicMock()
            resp.status_code = 503 if len(llamadas) < 2 else 200
            resp.reason = "Service Unavailable"
            return resp

        with patch("scripts.ocr_normativa_openai_pages.requests.post", side_effect=fake_post), \
             patch("scripts.ocr_normativa_openai_pages.time.sleep", return_value=None):
            respuesta = _post_con_reintentos("http://fake", intentos=3, espera_base=0.01)

        self.assertEqual(respuesta.status_code, 200)
        self.assertEqual(len(llamadas), 2)

    def test_agota_intentos_y_relanza_el_ultimo_error(self):
        with patch(
            "scripts.ocr_normativa_openai_pages.requests.post",
            side_effect=lambda *a, **k: MagicMock(status_code=503, reason="Service Unavailable"),
        ), patch("scripts.ocr_normativa_openai_pages.time.sleep", return_value=None):
            with self.assertRaises(Exception):
                _post_con_reintentos("http://fake", intentos=2, espera_base=0.01)

    def test_no_reintenta_errores_no_transitorios_como_403(self):
        llamadas = []

        def fake_post(url, **kwargs):
            llamadas.append(url)
            return MagicMock(status_code=403, reason="Forbidden")

        with patch("scripts.ocr_normativa_openai_pages.requests.post", side_effect=fake_post):
            respuesta = _post_con_reintentos("http://fake", intentos=3, espera_base=0.01)

        self.assertEqual(respuesta.status_code, 403)
        self.assertEqual(len(llamadas), 1)


class TestTranscribePageWithProvider(unittest.TestCase):
    def test_gemini_se_despacha_al_transcriptor_correcto(self):
        with patch("scripts.ocr_normativa_openai_pages.transcribe_page_gemini") as mock_gemini:
            mock_gemini.return_value = {"transcripcion": "texto"}
            resultado = transcribe_page_with_provider(
                api_key="clave", provider="gemini", model="gemini-flash-latest", detail="original",
                document_key="RM-1-2020", title="Titulo", page_number=1, image_base64="abc",
            )
        self.assertEqual(resultado, {"transcripcion": "texto"})
        mock_gemini.assert_called_once_with(
            api_key="clave", model="gemini-flash-latest", document_key="RM-1-2020",
            title="Titulo", page_number=1, image_base64="abc",
        )

    def test_proveedor_no_soportado_lanza_error(self):
        with self.assertRaises(ValueError):
            transcribe_page_with_provider(
                api_key="clave", provider="inexistente", model="x", detail="x",
                document_key="RM-1-2020", title=None, page_number=1, image_base64="abc",
            )


if __name__ == "__main__":
    unittest.main()
