import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from scripts.ocr_normativa_openai_pages import update_page  # noqa: E402


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
        update_page(self.supabase, self.page, self.ai_result, self.args)

        payload = self.supabase.table.return_value.update.call_args.args[0]
        self.assertNotIn("text_raw", payload)
        self.assertNotIn("text_normalized", payload)
        propuesta = payload["metadata"]["openai_vision_ocr"]
        self.assertEqual(propuesta["evidence_status"], "proposal_requires_human_review")
        self.assertFalse(propuesta["verified"])
        self.assertEqual(propuesta["tablas_markdown"], self.ai_result["tablas_markdown"])

    def test_replace_text_permanece_bloqueado(self):
        self.args.replace_text = True

        with self.assertRaisesRegex(ValueError, "no puede reemplazar"):
            update_page(self.supabase, self.page, self.ai_result, self.args)

        self.supabase.table.assert_not_called()


if __name__ == "__main__":
    unittest.main()
