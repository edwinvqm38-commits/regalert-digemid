from contextlib import ExitStack
from copy import deepcopy
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from scripts.ocr_normativa_openai_pages import update_page  # noqa: E402
from scripts import ocr_normativa_openai_pages as ocr  # noqa: E402


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
