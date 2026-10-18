"""F-04B2: materializa páginas físicas sin texto como pendientes de OCR."""

from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import fitz

from scripts import extract_normativa_pdf_text_to_supabase as extractor


ASSET = {
    "id": 77,
    "norma_id": "norma-1",
    "asset_tipo": "pdf_original",
    "asset_subtipo": "cuerpo_principal",
    "drive_file_id": "drive-pdf-1",
    "file_name": "LEY-29459__2009-11-26__original.pdf",
    "mime_type": "application/pdf",
}
NORMA = {
    "id": "norma-1",
    "document_key": "LEY-29459",
    "titulo": "Ley de los productos farmacéuticos",
    "raw": {},
    "process_status": "pdf_downloaded",
}


def make_pdf(page_texts):
    document = fitz.open()
    for text in page_texts:
        page = document.new_page()
        if text:
            page.insert_text((72, 72), text)
    content = document.tobytes()
    document.close()
    # validate_pdf_bytes conserva el umbral documental de 10 KiB. Los bytes
    # posteriores a %%EOF son ignorados por PyMuPDF y no alteran las páginas.
    return content + b"\n" + (b" " * (extractor.MIN_PDF_BYTES + 1))


class FakeQuery:
    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name
        self.action = "select"
        self.payload = None
        self.filters = {}

    def select(self, *_args):
        self.action = "select"
        return self

    def insert(self, payload):
        self.action = "insert"
        self.payload = deepcopy(payload)
        return self

    def update(self, payload):
        self.action = "update"
        self.payload = deepcopy(payload)
        return self

    def eq(self, key, value):
        self.filters[key] = value
        return self

    def execute(self):
        if self.table_name == extractor.PAGE_TABLE_NAME:
            if self.action == "select":
                rows = [
                    deepcopy(row)
                    for row in self.database.pages
                    if all(row.get(key) == value for key, value in self.filters.items())
                ]
                return SimpleNamespace(data=rows)
            if self.action == "insert":
                row = {"id": f"page-{len(self.database.pages) + 1}", **self.payload}
                self.database.pages.append(row)
                self.database.writes.append(("insert_page", deepcopy(self.payload)))
                return SimpleNamespace(data=[deepcopy(row)])
            if self.action == "update":
                matched = []
                for row in self.database.pages:
                    if all(row.get(key) == value for key, value in self.filters.items()):
                        row.update(deepcopy(self.payload))
                        matched.append(deepcopy(row))
                self.database.writes.append(("update_page", deepcopy(self.payload)))
                return SimpleNamespace(data=matched)

        if self.table_name == extractor.NORMA_TABLE_NAME and self.action == "update":
            self.database.norma_updates.append(deepcopy(self.payload))
            self.database.writes.append(("update_norma", deepcopy(self.payload)))
            return SimpleNamespace(data=[deepcopy(self.payload)])

        raise AssertionError(
            f"Consulta falsa no soportada: {self.table_name} {self.action}"
        )


class FakeSupabase:
    def __init__(self, pages=None):
        self.pages = deepcopy(pages or [])
        self.writes = []
        self.norma_updates = []

    def table(self, table_name):
        return FakeQuery(self, table_name)


def process(pdf_bytes, database, apply_changes, force=False):
    with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
         patch.object(extractor, "download_drive_file_bytes", return_value=pdf_bytes):
        return extractor.process_norma(
            service=object(),
            supabase=database,
            norma_row=deepcopy(NORMA),
            apply_changes=apply_changes,
            force=force,
            update_existing_normalized=False,
            ocr_config=None,
        )


class TestPendingOcrPhysicalPages(unittest.TestCase):
    def setUp(self):
        self.blank_pdf = make_pdf(["", "", ""])

    def assert_pending_payload(self, payload, page_number):
        self.assertEqual(payload["page_number"], page_number)
        self.assertEqual(payload["norma_id"], NORMA["id"])
        self.assertEqual(payload["source_asset_id"], ASSET["id"])
        self.assertEqual(payload["asset_subtipo"], ASSET["asset_subtipo"])
        self.assertEqual(payload["document_part"], ASSET["asset_subtipo"])
        self.assertEqual(payload["text_raw"], "")
        self.assertEqual(payload["text_normalized"], "")
        self.assertEqual(payload["extraction_method"], "pending_ocr")
        self.assertFalse(payload["ocr_used"])
        self.assertFalse(payload["has_tables"])
        self.assertEqual(payload["quality_score"], 0.0)
        metadata = payload["metadata"]
        self.assertTrue(metadata["physical_page_detected_from_official_pdf"])
        self.assertTrue(metadata["pending_ocr"])
        self.assertEqual(metadata["reason"], "no_embedded_text")
        self.assertEqual(metadata["source_asset_id"], ASSET["id"])
        self.assertEqual(metadata["source_asset_file_name"], ASSET["file_name"])
        self.assertEqual(metadata["extraction_version"], extractor.EXTRACTION_VERSION)
        self.assertNotIn("revisado_manual", payload)
        self.assertNotIn("fidelidad_verificada", payload)

    def test_three_blank_pages_dry_run_plans_three_inserts_without_writes(self):
        database = FakeSupabase()
        captured_payloads = []
        original_write = extractor.write_page_record

        def capture(*args, **kwargs):
            captured_payloads.append(deepcopy(kwargs["payload"]))
            return original_write(*args, **kwargs)

        with patch.object(extractor, "write_page_record", side_effect=capture):
            result = process(self.blank_pdf, database, apply_changes=False)

        self.assertEqual(database.writes, [])
        self.assertEqual(result["planned_process_status"], "ocr_required")
        self.assertTrue(result["summary"]["requires_ocr"])
        self.assertEqual(result["summary"]["pages_planned_insert"], 3)
        self.assertEqual(result["summary"]["pages_inserted"], 0)
        self.assertEqual(result["assets"][0]["status"], "ocr_required")
        self.assertEqual(
            [op["page_number"] for op in result["operations"] if op["action"] == "insert_page"],
            [1, 2, 3],
        )
        self.assertEqual(len(captured_payloads), 3)
        for page_number, payload in enumerate(captured_payloads, start=1):
            self.assert_pending_payload(payload, page_number)

    def test_three_blank_pages_apply_inserts_placeholders_and_keeps_ocr_required(self):
        database = FakeSupabase()
        result = process(self.blank_pdf, database, apply_changes=True)

        self.assertEqual(len(database.pages), 3)
        self.assertEqual(result["summary"]["pages_inserted"], 3)
        self.assertEqual(result["planned_process_status"], "ocr_required")
        self.assertEqual(database.norma_updates[-1]["process_status"], "ocr_required")
        self.assertTrue(database.norma_updates[-1]["ocr_required"])
        for page_number, payload in enumerate(database.pages, start=1):
            self.assert_pending_payload(payload, page_number)

    def test_apply_rerun_without_force_reuses_and_never_duplicates(self):
        database = FakeSupabase()
        process(self.blank_pdf, database, apply_changes=True)
        writes_before = len(database.writes)

        result = process(self.blank_pdf, database, apply_changes=True, force=False)

        self.assertEqual(len(database.pages), 3)
        self.assertEqual(result["summary"]["pages_reused"], 3)
        self.assertEqual(result["summary"]["pages_inserted"], 0)
        new_writes = database.writes[writes_before:]
        self.assertEqual([kind for kind, _ in new_writes], ["update_norma"])

    def test_force_updates_existing_pages_without_creating_duplicates(self):
        database = FakeSupabase()
        process(self.blank_pdf, database, apply_changes=True)

        result = process(self.blank_pdf, database, apply_changes=True, force=True)

        self.assertEqual(len(database.pages), 3)
        self.assertEqual(result["summary"]["pages_updated"], 3)
        self.assertEqual(result["summary"]["pages_inserted"], 0)
        self.assertEqual(result["planned_process_status"], "ocr_required")
        for page_number, payload in enumerate(database.pages, start=1):
            self.assert_pending_payload(payload, page_number)

    def test_pdf_with_embedded_text_preserves_previous_behavior(self):
        database = FakeSupabase()
        text_pdf = make_pdf(["Artículo 1. Objeto de la norma."])

        result = process(text_pdf, database, apply_changes=True)

        self.assertEqual(result["planned_process_status"], "text_extracted")
        self.assertFalse(result["summary"]["requires_ocr"])
        self.assertEqual(len(database.pages), 1)
        payload = database.pages[0]
        self.assertEqual(payload["extraction_method"], "pymupdf")
        self.assertFalse(payload["ocr_used"])
        self.assertIn("Artículo 1", payload["text_normalized"])
        self.assertNotIn("quality_score", payload)
        self.assertNotIn("pending_ocr", payload["metadata"])
        self.assertEqual(database.norma_updates[-1]["process_status"], "text_extracted")


if __name__ == "__main__":
    unittest.main()
