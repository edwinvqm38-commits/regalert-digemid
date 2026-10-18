"""aplicar_filtro_pendientes decide que paginas entran como candidatas a la
verificacion por vision (scripts/ocr_normativa_openai_pages.py). El filtro
"graficos-pendientes" existe porque una pagina con posible_grafico=true puede
tener quality_score alto y sin tablas -- sin ese filtro nunca entraba como
candidata a pesar de que el heuristico existe justo para marcarla.
"""

from contextlib import ExitStack
from copy import deepcopy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from argparse import Namespace
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from unittest.mock import MagicMock, Mock, patch  # noqa: E402

from scripts import ocr_normativa_openai_pages as ocr  # noqa: E402

from scripts.ocr_normativa_openai_pages import (  # noqa: E402
    _debe_reemplazar_texto,
    _post_con_reintentos,
    aplicar_filtro_pendientes,
    contar_avance_catalogo,
    contar_avance_norma,
    elegir_norma_prioritaria,
    get_candidate_pages,
    transcribe_page_with_provider,
    update_page,
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


class TestDebeReemplazarTexto(unittest.TestCase):
    def test_replace_text_manual_permanece_bloqueado(self):
        args = Namespace(replace_text=True, auto_solo_alta_confianza=False, min_confianza=0.85)
        with self.assertRaisesRegex(ValueError, "no puede reemplazar"):
            _debe_reemplazar_texto({"confianza_estimada": 0.1, "advertencias": ["x"]}, args)

    def test_sin_ninguna_bandera_no_reemplaza(self):
        args = Namespace(replace_text=False, auto_solo_alta_confianza=False, min_confianza=0.85)
        self.assertFalse(_debe_reemplazar_texto({"confianza_estimada": 0.99}, args))

    def test_auto_alta_confianza_no_autoriza_reemplazo(self):
        args = Namespace(replace_text=False, auto_solo_alta_confianza=True, min_confianza=0.85)
        self.assertFalse(_debe_reemplazar_texto({"confianza_estimada": 0.9, "advertencias": []}, args))
        self.assertFalse(_debe_reemplazar_texto({"confianza_estimada": 0.5, "advertencias": []}, args))
        self.assertFalse(_debe_reemplazar_texto({"confianza_estimada": 0.9, "advertencias": ["ilegible"]}, args))
        self.assertFalse(_debe_reemplazar_texto({"confianza_estimada": None, "advertencias": []}, args))
        self.assertFalse(_debe_reemplazar_texto({"confianza_estimada": "alta", "advertencias": []}, args))


class FakeQueryUpdate:
    def __init__(self):
        self.payload = None

    def update(self, payload):
        self.payload = payload
        return self

    def eq(self, *_a):
        return self

    def execute(self):
        return MagicMock()


class FakeSupabaseUpdate:
    def __init__(self):
        self.query = FakeQueryUpdate()

    def table(self, _nombre):
        return self.query


class TestUpdatePage(unittest.TestCase):
    def test_auto_alta_confianza_solo_propone_sin_marcar_revision(self):
        supabase = FakeSupabaseUpdate()
        page = {
            "id": "pagina-1", "metadata": {}, "text_raw": "viejo", "text_normalized": "viejo",
            "extraction_method": "pymupdf", "quality_score": 0.4,
        }
        ai_result = {"transcripcion": "nuevo", "confianza_estimada": 0.95, "advertencias": []}
        args = Namespace(
            replace_text=False, auto_solo_alta_confianza=True, min_confianza=0.85,
            provider="gemini", model="gemini-flash-latest", detail="original", dpi=150,
        )

        reemplazado = update_page(supabase, page, ai_result, args)

        self.assertFalse(reemplazado)
        payload = supabase.query.payload
        self.assertEqual(set(payload), {"metadata", "updated_at"})
        self.assertNotIn("tabla_verificada", payload)
        self.assertNotIn("revisado_manual", payload)
        self.assertNotIn("previous_extraction_before_openai_vision", payload["metadata"])
        propuesta = payload["metadata"]["openai_vision_ocr"]
        self.assertEqual(propuesta["evidence_status"], "proposal_requires_human_review")
        self.assertFalse(propuesta["verified"])

    def test_auto_baja_confianza_no_reemplaza_solo_guarda_metadata(self):
        supabase = FakeSupabaseUpdate()
        page = {"id": "pagina-1", "metadata": {}, "text_raw": "viejo", "text_normalized": "viejo"}
        ai_result = {"transcripcion": "nuevo", "confianza_estimada": 0.5, "advertencias": []}
        args = Namespace(
            replace_text=False, auto_solo_alta_confianza=True, min_confianza=0.85,
            provider="gemini", model="gemini-flash-latest", detail="original", dpi=150,
        )

        reemplazado = update_page(supabase, page, ai_result, args)

        self.assertFalse(reemplazado)
        payload = supabase.query.payload
        self.assertNotIn("text_normalized", payload)
        self.assertIn("openai_vision_ocr", payload["metadata"])


class FakeCountResult:
    def __init__(self, count):
        self.count = count


class FakeQueryConteo:
    def __init__(self, resultado):
        self._resultado = resultado

    def select(self, *_a, **_k):
        return self

    def gte(self, *_a):
        return self

    def execute(self):
        return self._resultado


class TestContarAvanceCatalogo(unittest.TestCase):
    def test_calcula_altas_y_bajas_a_partir_del_total(self):
        respuestas = [FakeCountResult(100), FakeCountResult(80)]
        supabase = MagicMock()
        supabase.table.return_value.select.return_value.execute.side_effect = [respuestas[0]]
        supabase.table.return_value.select.return_value.gte.return_value.execute.side_effect = [respuestas[1]]

        resultado = contar_avance_catalogo(supabase, 0.85)

        self.assertEqual(resultado, {"total": 100, "altas": 80, "bajas": 20})


class FakePaginasQueryElegir:
    """Distingue 3 consultas por su forma, no por orden de llamada: la
    muestra de pendientes (sin count), el total por norma (count sin gte) y
    las altas por norma (count con gte)."""

    def __init__(self, muestra, conteos):
        self._muestra = muestra
        self._conteos = conteos
        self._modo = "muestra"
        self._norma_id = None
        self._con_gte = False

    def select(self, *_a, **kwargs):
        if kwargs.get("count") == "exact":
            self._modo = "conteo"
        return self

    def lt(self, *_a):
        return self

    def gte(self, *_a):
        self._con_gte = True
        return self

    def order(self, *_a):
        return self

    def eq(self, campo, valor):
        if campo == "norma_id":
            self._norma_id = valor
        return self

    def limit(self, _n):
        return self

    def execute(self):
        if self._modo == "muestra":
            return MagicMock(data=self._muestra)
        conteo = self._conteos.get(self._norma_id, {"total": 0, "altas": 0})
        valor = conteo["altas"] if self._con_gte else conteo["total"]
        return MagicMock(count=valor)


class FakeNormasQueryElegir:
    def __init__(self, normas):
        self._normas = normas
        self._id = None

    def select(self, *_a):
        return self

    def eq(self, campo, valor):
        if campo == "id":
            self._id = valor
        return self

    def limit(self, _n):
        return self

    def execute(self):
        norma = self._normas.get(self._id)
        return MagicMock(data=[norma] if norma else [])


def _fake_supabase_elegir(muestra_norma_ids, conteos, normas):
    def table(nombre):
        if nombre == "digemid_normas":
            return FakeNormasQueryElegir(normas)
        return FakePaginasQueryElegir([{"norma_id": nid} for nid in muestra_norma_ids], conteos)

    supabase = MagicMock()
    supabase.table.side_effect = table
    return supabase


class TestElegirNormaPrioritaria(unittest.TestCase):
    def test_prioriza_norma_con_avance_y_menos_pendientes(self):
        conteos = {
            "norma-a": {"total": 100, "altas": 90},  # con avance, 10 pendientes
            "norma-b": {"total": 50, "altas": 0},  # sin avance, 50 pendientes
            "norma-c": {"total": 40, "altas": 5},  # con avance, 35 pendientes
        }
        normas = {"norma-a": {"id": "norma-a", "document_key": "RM-A"}}
        supabase = _fake_supabase_elegir(["norma-a", "norma-b", "norma-a", "norma-c"], conteos, normas)

        resultado = elegir_norma_prioritaria(supabase, 0.85)

        self.assertEqual(resultado["norma"]["document_key"], "RM-A")
        self.assertEqual(resultado["bajas"], 10)

    def test_sin_avance_previo_elige_la_con_menos_pendientes_en_total(self):
        conteos = {
            "norma-b": {"total": 50, "altas": 0},
            "norma-d": {"total": 5, "altas": 0},
        }
        normas = {"norma-d": {"id": "norma-d", "document_key": "RM-D"}}
        supabase = _fake_supabase_elegir(["norma-b", "norma-d"], conteos, normas)

        resultado = elegir_norma_prioritaria(supabase, 0.85)

        self.assertEqual(resultado["norma"]["document_key"], "RM-D")

    def test_sin_paginas_pendientes_devuelve_none(self):
        supabase = _fake_supabase_elegir([], {}, {})
        self.assertIsNone(elegir_norma_prioritaria(supabase, 0.85))


class TestContarAvanceNorma(unittest.TestCase):
    def test_calcula_altas_bajas_de_una_sola_norma(self):
        conteos = {"norma-x": {"total": 30, "altas": 12}}
        supabase = _fake_supabase_elegir([], conteos, {})

        resultado = contar_avance_norma(supabase, "norma-x", 0.85)

        self.assertEqual(resultado, {"total": 30, "altas": 12, "bajas": 18})


class VisionProposalSafetyTest(unittest.TestCase):
    def setUp(self):
        self.page = {
            "id": "pagina-1",
            "text_raw": "Texto original",
            "text_normalized": "Texto original",
            "extraction_method": "pymupdf",
            "quality_score": 1.0,
            "metadata": {},
        }
        self.ai_result = {
            "transcripcion": "Texto propuesto por IA",
            "tablas_markdown": "| A | B |\n| --- | --- |\n| 1 | 2 |",
            "advertencias": [],
        }
        self.args = SimpleNamespace(
            replace_text=False,
            provider="openrouter",
            model="modelo-prueba",
            detail="original",
            dpi=150,
        )
        self.supabase = Mock()
        consulta = self.supabase.table.return_value
        consulta.update.return_value.eq.return_value.execute.return_value = Mock()

    def test_apply_guarda_solo_propuesta_en_metadata(self):
        original = deepcopy(self.page)
        reemplazado = update_page(self.supabase, self.page, self.ai_result, self.args)

        payload = self.supabase.table.return_value.update.call_args.args[0]
        self.assertFalse(reemplazado)
        self.assertEqual(set(payload), {"metadata", "updated_at"})
        self.assertEqual(self.page, original)
        propuesta = payload["metadata"]["openai_vision_ocr"]
        self.assertEqual(propuesta["evidence_status"], "proposal_requires_human_review")
        self.assertFalse(propuesta["verified"])
        self.assertEqual(propuesta["tablas_markdown"], self.ai_result["tablas_markdown"])

    def test_replace_text_permanece_bloqueado(self):
        self.args.replace_text = True
        original = deepcopy(self.page)

        with self.assertRaisesRegex(ValueError, "no puede reemplazar"):
            update_page(self.supabase, self.page, self.ai_result, self.args)

        self.supabase.table.assert_not_called()
        self.assertEqual(self.page, original)

    def test_callers_legacy_sin_banderas_opcionales_guardan_solo_propuesta(self):
        del self.args.replace_text
        self.assertFalse(hasattr(self.args, "auto_solo_alta_confianza"))
        self.assertFalse(update_page(self.supabase, self.page, self.ai_result, self.args))
        payload = self.supabase.table.return_value.update.call_args.args[0]
        self.assertEqual(set(payload), {"metadata", "updated_at"})
        self.assertFalse(payload["metadata"]["openai_vision_ocr"]["verified"])

    def test_alta_confianza_sin_advertencias_nunca_promueve_texto(self):
        self.args.auto_solo_alta_confianza = True
        # Tambien debe tolerar callers que no tienen min_confianza.
        for provider in ("gemini", "openai", "openrouter"):
            with self.subTest(provider=provider):
                self.args.provider = provider
                self.ai_result["confianza_estimada"] = 1.0
                self.assertFalse(update_page(self.supabase, self.page, self.ai_result, self.args))
                payload = self.supabase.table.return_value.update.call_args.args[0]
                self.assertEqual(set(payload), {"metadata", "updated_at"})
                propuesta = payload["metadata"]["openai_vision_ocr"]
                self.assertEqual(propuesta["provider"], provider)
                self.assertEqual(propuesta["confianza_estimada"], 1.0)
                self.assertEqual(propuesta["evidence_status"], "proposal_requires_human_review")
                self.assertFalse(propuesta["verified"])

    def test_modelo_no_puede_autodeclarar_verificacion_ni_cambiar_procedencia(self):
        self.ai_result.update({
            "verified": True,
            "evidence_status": "VERIFICADA_HUMANO",
            "provider": "proveedor_inventado",
            "model": "modelo_inventado",
            "generated_at": "fecha_inventada",
        })
        update_page(self.supabase, self.page, self.ai_result, self.args)
        propuesta = self.supabase.table.return_value.update.call_args.args[0]["metadata"]["openai_vision_ocr"]
        self.assertFalse(propuesta["verified"])
        self.assertEqual(propuesta["evidence_status"], "proposal_requires_human_review")
        self.assertEqual(propuesta["provider"], self.args.provider)
        self.assertEqual(propuesta["model"], self.args.model)
        self.assertNotEqual(propuesta["generated_at"], "fecha_inventada")

    def test_propuesta_preserva_revision_previa_y_metadata_ajena(self):
        self.page.update({"revisado_manual": True, "tabla_verificada": True})
        self.page["metadata"] = {
            "source_pdf_sha256": "a" * 64,
            "manual_review": {"text_verified": True},
            "previous_extraction_before_openai_vision": {"text_raw": "Extraccion previa"},
        }
        original = deepcopy(self.page)
        update_page(self.supabase, self.page, self.ai_result, self.args)
        payload = self.supabase.table.return_value.update.call_args.args[0]
        self.assertEqual(set(payload), {"metadata", "updated_at"})
        for key, value in original["metadata"].items():
            self.assertEqual(payload["metadata"][key], value)
        self.assertEqual(self.page, original)
        self.assertFalse(payload["metadata"]["openai_vision_ocr"]["verified"])

    def test_revision_del_texto_anterior_no_autoriza_reemplazar_por_otra_propuesta(self):
        self.page.update({"revisado_manual": True, "tabla_verificada": True})
        self.args.replace_text = True
        self.args.auto_solo_alta_confianza = True
        self.args.min_confianza = 0
        self.ai_result["confianza_estimada"] = 1.0
        with self.assertRaisesRegex(ValueError, "no puede reemplazar"):
            update_page(self.supabase, self.page, self.ai_result, self.args)
        self.supabase.table.assert_not_called()

    def test_cli_rechaza_reemplazo_antes_de_credenciales_o_red(self):
        for options in (
            ["--replace-text"],
            ["--apply", "--replace-text"],
            ["--apply", "--replace-text", "--auto-solo-alta-confianza"],
            ["--replace-text", "--report-html", "no_debe_crearse.html"],
        ):
            with self.subTest(options=options), \
                    patch.object(sys, "argv", ["ocr", *options]), \
                    patch.object(ocr, "load_env") as load_env, \
                    patch.object(ocr, "get_supabase") as get_supabase, \
                    patch.object(ocr, "transcribe_page_with_provider") as transcribe:
                with self.assertRaisesRegex(ValueError, "no puede reemplazar"):
                    ocr.main()
                load_env.assert_not_called()
                get_supabase.assert_not_called()
                transcribe.assert_not_called()

    def test_metadata_ausente_o_invalida_admite_propuesta_sin_mutar_pagina(self):
        for metadata in (None, [], "legacy"):
            with self.subTest(metadata=metadata):
                self.page["metadata"] = metadata
                original = deepcopy(self.page)
                update_page(self.supabase, self.page, self.ai_result, self.args)
                payload = self.supabase.table.return_value.update.call_args.args[0]
                self.assertEqual(set(payload["metadata"]), {"openai_vision_ocr"})
                self.assertEqual(self.page, original)

    def test_flujo_cron_guarda_propuesta_y_report_y_dry_run_no_escriben(self):
        # Ejecuta main con los argumentos reales del cron, sin servicios externos.
        norma = {"id": "norma-1", "document_key": "DS-1-2026", "titulo": "Norma"}
        page = {**self.page, "norma": norma, "page_number": 1}
        ai_result = {**self.ai_result, "confianza_estimada": 1.0}
        avance = {"altas": 0, "total": 1, "bajas": 1}
        for mode in ("apply", "report", "dry-run"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                db = Mock()
                report = Path(directory) / "reporte.html"
                argv = [
                    "ocr", "--provider", "gemini", "--filtro", "calidad-baja",
                    "--limit", "2", "--auto-solo-alta-confianza", "--min-confianza", "0.85",
                    "--completar-un-reglamento", "--telegram-digest",
                ]
                if mode != "dry-run":
                    argv.append("--apply")
                if mode == "report":
                    argv.extend(["--report-html", str(report)])
                stack.enter_context(patch.object(sys, "argv", argv))
                stack.enter_context(patch.dict(ocr.os.environ, {"GEMINI_API_KEY": "dummy-test-key"}, clear=True))
                stack.enter_context(patch.object(ocr, "load_env"))
                stack.enter_context(patch.object(ocr, "get_supabase", return_value=db))
                stack.enter_context(patch.object(ocr, "get_candidate_pages", return_value=[page]))
                stack.enter_context(patch.object(ocr, "elegir_norma_prioritaria", return_value={"norma": norma, **avance}))
                stack.enter_context(patch.object(ocr, "contar_avance_norma", return_value=avance))
                stack.enter_context(patch.object(ocr, "contar_avance_catalogo", return_value=avance))
                download = stack.enter_context(patch.object(ocr, "download_pdf_bytes", return_value=b"%PDF-test"))
                stack.enter_context(patch.object(ocr, "render_page_png_base64", return_value="imagen-simulada"))
                transcribe = stack.enter_context(patch.object(ocr, "transcribe_page_with_provider", return_value=ai_result))
                digest = stack.enter_context(patch.object(ocr, "enviar_mensaje_telegram"))
                network = stack.enter_context(patch.object(ocr.requests.sessions.Session, "request", side_effect=AssertionError("Red prohibida")))
                ocr.main()
                network.assert_not_called()
                if mode == "apply":
                    payload = db.table.return_value.update.call_args.args[0]
                    self.assertEqual(set(payload), {"metadata", "updated_at"})
                    self.assertFalse(payload["metadata"]["openai_vision_ocr"]["verified"])
                    self.assertEqual(payload["metadata"]["openai_vision_ocr"]["confianza_estimada"], 1.0)
                    digest.assert_called_once()
                    self.assertIn("texto conservado", digest.call_args.args[0])
                    self.assertNotIn("ya las usa /consulta", digest.call_args.args[0])
                else:
                    db.table.assert_not_called()
                    digest.assert_not_called()
                if mode == "dry-run":
                    download.assert_not_called()
                    transcribe.assert_not_called()
                else:
                    transcribe.assert_called_once()
                self.assertEqual(report.exists(), mode == "report")


if __name__ == "__main__":
    unittest.main()
