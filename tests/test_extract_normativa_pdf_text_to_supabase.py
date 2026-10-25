"""F-04B2: materializa páginas físicas sin texto como pendientes de OCR."""

from copy import deepcopy
from pathlib import Path
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

        if self.table_name == extractor.ASSET_TABLE_NAME and self.action == "update":
            self.database.asset_updates.append(deepcopy(self.payload))
            self.database.writes.append(("update_asset", deepcopy(self.payload)))
            return SimpleNamespace(data=[deepcopy(self.payload)])

        raise AssertionError(
            f"Consulta falsa no soportada: {self.table_name} {self.action}"
        )


class FakeSupabase:
    def __init__(self, pages=None):
        self.pages = deepcopy(pages or [])
        self.writes = []
        self.norma_updates = []
        self.asset_updates = []

    def table(self, table_name):
        return FakeQuery(self, table_name)


def process(pdf_bytes, database, apply_changes, force=False):
    with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
         patch.object(extractor, "download_drive_file_bytes", return_value=pdf_bytes), \
         patch.object(extractor, "detect_verified_blank", return_value=(False, {})):
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
        self.assertEqual([kind for kind, _ in new_writes], ["update_asset", "update_norma"])

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

    def test_ocr_result_upgrades_pending_page_without_force(self):
        pending = {
            "id": "page-pending",
            "norma_id": NORMA["id"],
            "source_asset_id": ASSET["id"],
            "page_number": 1,
            "text_raw": "",
            "text_normalized": "",
            "quality_score": 0.0,
            "revisado_manual": False,
            "extraction_method": "pending_ocr",
        }
        database = FakeSupabase([pending])
        ocr_config = {
            "enabled": True,
            "dependencies_ok": True,
            "engine": extractor.OCR_ENGINE_NAME,
            "lang": "spa",
            "dpi": 300,
            "start_page": 1,
            "max_pages": 1,
            "runtime": {
                "pages_attempted": 0,
                "pages_success": 0,
                "pages_failed": 0,
                "ocr_text_chars": 0,
            },
        }

        def fake_ocr(**kwargs):
            runtime = ocr_config["runtime"]
            runtime["pages_attempted"] += 1
            runtime["pages_success"] += 1
            text = "Artículo 5. Texto recuperado mediante OCR."
            runtime["ocr_text_chars"] += len(text)
            return {
                "ok": True,
                "text_raw": text,
                "text_normalized": text,
                "layout_result": None,
                "error": None,
            }

        with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
             patch.object(extractor, "download_drive_file_bytes", return_value=make_pdf([""])), \
             patch.object(extractor, "run_ocr_on_page", side_effect=fake_ocr):
            result = extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=deepcopy(NORMA),
                apply_changes=True,
                force=False,
                update_existing_normalized=False,
                ocr_config=ocr_config,
            )

        self.assertEqual(result["planned_process_status"], "text_extracted_ocr")
        self.assertEqual(len(database.pages), 1)
        self.assertEqual(database.pages[0]["extraction_method"], extractor.OCR_ENGINE_NAME)
        self.assertTrue(database.pages[0]["ocr_used"])
        self.assertIn("Artículo 5", database.pages[0]["text_normalized"])

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
        self.assertGreater(payload["quality_score"], 0)
        self.assertNotIn("pending_ocr", payload["metadata"])
        self.assertEqual(len(payload["metadata"]["source_pdf_sha256"]), 64)
        self.assertEqual(payload["metadata"]["pdf_physical_page_count"], 1)
        self.assertEqual(database.norma_updates[-1]["process_status"], "text_extracted")

    def test_mixed_pdf_registers_every_physical_page(self):
        database = FakeSupabase()
        mixed_pdf = make_pdf([
            "Artículo 1. Objeto de la norma.",
            "",
            "Artículo 2. Ámbito de aplicación.",
        ])

        result = process(mixed_pdf, database, apply_changes=True)

        self.assertEqual([row["page_number"] for row in database.pages], [1, 2, 3])
        self.assertEqual(database.pages[1]["extraction_method"], "pending_ocr")
        self.assertEqual(database.pages[1]["metadata"]["reason"], "no_embedded_text")
        self.assertEqual(result["planned_process_status"], "text_extraction_partial")
        self.assertTrue(result["summary"]["requires_ocr"])
        self.assertEqual(result["summary"]["pages_without_text"], 1)

    def test_sparse_embedded_capture_is_preserved_raw_but_not_consultable(self):
        database = FakeSupabase()

        result = process(make_pdf(["SELLO"]), database, apply_changes=True)

        self.assertEqual(result["planned_process_status"], "ocr_required")
        self.assertEqual(database.pages[0]["text_raw"], "SELLO")
        self.assertEqual(database.pages[0]["text_normalized"], "")
        self.assertEqual(database.pages[0]["extraction_method"], "pending_ocr")
        self.assertEqual(
            database.pages[0]["metadata"]["reason"],
            "insufficient_embedded_text",
        )

    def test_reliable_blank_detection_persists_verified_blank_metadata(self):
        database = FakeSupabase()
        pdf = make_pdf(["", ""])

        def blank_detector(_page, page_number, _total):
            verified = page_number == 1
            return verified, {
                "verified_blank": verified,
                "confidence": "alta" if verified else "baja",
                "reason": "prueba",
            }

        with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
             patch.object(extractor, "download_drive_file_bytes", return_value=pdf), \
             patch.object(extractor, "detect_verified_blank", side_effect=blank_detector):
            result = extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=deepcopy(NORMA),
                apply_changes=True,
                force=False,
                update_existing_normalized=False,
                ocr_config=None,
            )

        self.assertEqual(database.pages[0]["extraction_method"], "verified_blank")
        self.assertTrue(database.pages[0]["metadata"]["verified_blank"])
        self.assertEqual(database.pages[1]["extraction_method"], "pending_ocr")
        self.assertEqual(result["summary"]["verified_blank_pages"], 1)

    def test_quarantined_norma_never_downloads_or_runs_ocr(self):
        database = FakeSupabase()
        quarantined = {
            **deepcopy(NORMA),
            "raw": {"source_validation_status": "needs_source_validation"},
        }

        with patch.object(extractor, "get_pdf_assets") as get_assets:
            result = extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=quarantined,
                apply_changes=False,
                force=False,
                update_existing_normalized=False,
                ocr_config=None,
            )

        get_assets.assert_not_called()
        self.assertEqual(result["planned_process_status"], "needs_source_validation")
        self.assertEqual(database.writes, [])

    def test_force_never_replaces_manually_reviewed_page(self):
        existing = {
            "id": "page-reviewed",
            "norma_id": NORMA["id"],
            "source_asset_id": ASSET["id"],
            "page_number": 1,
            "text_raw": "Texto verificado por una persona.",
            "text_normalized": "Texto verificado por una persona.",
            "quality_score": 1.0,
            "revisado_manual": True,
            "extraction_method": "revision_manual",
        }
        database = FakeSupabase([existing])

        result = process(make_pdf([""]), database, apply_changes=True, force=True)

        self.assertEqual(database.pages[0]["text_normalized"], existing["text_normalized"])
        self.assertEqual(database.pages[0]["extraction_method"], "revision_manual")
        self.assertEqual(result["summary"]["pages_reused"], 1)
        self.assertTrue(any(
            op.get("action") == "preserve_existing_page"
            and op.get("reason") == "manual_review"
            for op in result["operations"]
        ))

    def test_update_existing_normalized_never_replaces_manually_reviewed_page(self):
        existing = {
            "id": "page-reviewed",
            "norma_id": NORMA["id"],
            "source_asset_id": ASSET["id"],
            "page_number": 1,
            "text_raw": "Texto verificado por una persona.",
            "text_normalized": "Texto verificado por una persona.",
            "quality_score": 1.0,
            "revisado_manual": True,
            "extraction_method": "revision_manual",
            "metadata": {},
        }
        database = FakeSupabase([existing])
        pdf = make_pdf(["Texto distinto producido automáticamente."])

        with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
             patch.object(extractor, "download_drive_file_bytes", return_value=pdf), \
             patch.object(extractor, "detect_verified_blank", return_value=(False, {})):
            result = extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=deepcopy(NORMA),
                apply_changes=True,
                force=False,
                update_existing_normalized=True,
                ocr_config=None,
            )

        self.assertEqual(database.pages[0]["text_normalized"], existing["text_normalized"])
        self.assertEqual(database.pages[0]["extraction_method"], "revision_manual")
        self.assertTrue(any(
            op.get("reason") == "manual_review" for op in result["operations"]
        ))

    def test_old_page_sha_never_completes_current_asset(self):
        existing = {
            "id": "page-old-version",
            "norma_id": NORMA["id"],
            "source_asset_id": ASSET["id"],
            "page_number": 1,
            "text_raw": "Texto de una versión anterior mucho más completo.",
            "text_normalized": "Texto de una versión anterior mucho más completo.",
            "quality_score": 1.0,
            "revisado_manual": False,
            "extraction_method": "pymupdf",
            "metadata": {"source_pdf_sha256": "a" * 64},
        }
        database = FakeSupabase([existing])

        result = process(
            make_pdf(["Texto actual breve pero suficiente."]),
            database,
            apply_changes=True,
        )

        self.assertEqual(result["planned_process_status"], "text_extraction_partial")
        self.assertEqual(
            result["assets"][0]["source_hash_mismatch_pages_after_write"],
            [1],
        )

    def test_progressive_ocr_skips_current_text_and_continues_with_next_pending(self):
        pdf = make_pdf(["", "", ""])
        current_sha = extractor.hashlib.sha256(pdf).hexdigest()
        existing = {
            "id": "page-current",
            "norma_id": NORMA["id"],
            "source_asset_id": ASSET["id"],
            "page_number": 1,
            "text_raw": "Artículo 1. Texto OCR ya persistido.",
            "text_normalized": "Artículo 1. Texto OCR ya persistido.",
            "quality_score": 0.8,
            "revisado_manual": False,
            "extraction_method": extractor.OCR_ENGINE_NAME,
            "ocr_used": True,
            "metadata": {"source_pdf_sha256": current_sha},
        }
        database = FakeSupabase([existing])
        ocr_config = {
            "enabled": True,
            "dependencies_ok": True,
            "engine": extractor.OCR_ENGINE_NAME,
            "lang": "spa",
            "dpi": 300,
            "start_page": 1,
            "max_pages": 1,
            "runtime": {
                "pages_attempted": 0,
                "pages_success": 0,
                "pages_failed": 0,
                "ocr_text_chars": 0,
            },
        }
        attempted = []

        def fake_ocr(**kwargs):
            attempted.append(kwargs["page_number"])
            runtime = ocr_config["runtime"]
            runtime["pages_attempted"] += 1
            runtime["pages_success"] += 1
            text = "Artículo 2. Texto recuperado en el siguiente lote."
            runtime["ocr_text_chars"] += len(text)
            return {
                "ok": True,
                "text_raw": text,
                "text_normalized": text,
                "layout_result": None,
                "error": None,
            }

        with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
             patch.object(extractor, "download_drive_file_bytes", return_value=pdf), \
             patch.object(extractor, "detect_verified_blank", return_value=(False, {})), \
             patch.object(extractor, "run_ocr_on_page", side_effect=fake_ocr):
            result = extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=deepcopy(NORMA),
                apply_changes=True,
                force=False,
                update_existing_normalized=False,
                ocr_config=ocr_config,
            )

        self.assertEqual(attempted, [2])
        self.assertEqual(database.pages[1]["page_number"], 2)
        self.assertTrue(database.pages[1]["ocr_used"])
        self.assertEqual(result["summary"]["pages_ocr_attempted"], 1)

    def test_pending_ocr_with_raw_noise_can_upgrade(self):
        pdf = make_pdf([""])
        current_sha = extractor.hashlib.sha256(pdf).hexdigest()
        pending = {
            "id": "page-pending-noise",
            "norma_id": NORMA["id"],
            "source_asset_id": ASSET["id"],
            "page_number": 1,
            "text_raw": "SELLO",
            "text_normalized": "",
            "quality_score": 0.0,
            "revisado_manual": False,
            "extraction_method": "pending_ocr",
            "metadata": {"source_pdf_sha256": current_sha},
        }
        database = FakeSupabase([pending])
        ocr_config = {
            "enabled": True,
            "dependencies_ok": True,
            "engine": extractor.OCR_ENGINE_NAME,
            "lang": "spa",
            "dpi": 300,
            "start_page": 1,
            "max_pages": 1,
            "runtime": {
                "pages_attempted": 0,
                "pages_success": 0,
                "pages_failed": 0,
                "ocr_text_chars": 0,
            },
        }

        def fake_ocr(**_kwargs):
            text = "Artículo 1. Texto útil recuperado por OCR."
            ocr_config["runtime"]["pages_attempted"] += 1
            ocr_config["runtime"]["pages_success"] += 1
            ocr_config["runtime"]["ocr_text_chars"] += len(text)
            return {
                "ok": True,
                "text_raw": text,
                "text_normalized": text,
                "layout_result": None,
                "error": None,
            }

        with patch.object(extractor, "get_pdf_assets", return_value=[deepcopy(ASSET)]), \
             patch.object(extractor, "download_drive_file_bytes", return_value=pdf), \
             patch.object(extractor, "detect_verified_blank", return_value=(False, {})), \
             patch.object(extractor, "run_ocr_on_page", side_effect=fake_ocr):
            extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=deepcopy(NORMA),
                apply_changes=True,
                force=False,
                update_existing_normalized=False,
                ocr_config=ocr_config,
            )

        self.assertEqual(database.pages[0]["extraction_method"], extractor.OCR_ENGINE_NAME)
        self.assertIn("Texto útil", database.pages[0]["text_normalized"])

    def test_asset_sha256_mismatch_aborts_before_page_writes(self):
        database = FakeSupabase()
        bad_asset = {**ASSET, "metadata": {"sha256": "0" * 64}}
        with patch.object(extractor, "get_pdf_assets", return_value=[bad_asset]), \
             patch.object(extractor, "download_drive_file_bytes", return_value=self.blank_pdf):
            result = extractor.process_norma(
                service=object(),
                supabase=database,
                norma_row=deepcopy(NORMA),
                apply_changes=True,
                force=False,
                update_existing_normalized=False,
                ocr_config=None,
            )

        self.assertEqual(database.pages, [])
        self.assertEqual(result["planned_process_status"], "text_extraction_partial")
        self.assertEqual(result["summary"]["errors_count"], 1)

    def test_partial_write_is_detected_without_deleting_successful_pages(self):
        database = FakeSupabase()
        original_write = extractor.write_page_record

        def fail_second_page(*args, **kwargs):
            if kwargs["payload"]["page_number"] == 2:
                raise RuntimeError("fallo simulado")
            return original_write(*args, **kwargs)

        with patch.object(extractor, "write_page_record", side_effect=fail_second_page):
            result = process(self.blank_pdf, database, apply_changes=True)

        self.assertEqual([row["page_number"] for row in database.pages], [1, 3])
        self.assertEqual(result["planned_process_status"], "text_extraction_partial")
        self.assertEqual(result["assets"][0]["missing_pages_after_write"], [2])
        self.assertGreaterEqual(result["summary"]["errors_count"], 1)

    def test_workflow_has_bounded_ocr_and_document_concurrency(self):
        workflow = Path(
            ".github/workflows/digemid-normativa-text-extraction.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("required: true\n        default: \"50\"", workflow)
        self.assertIn("OCR_MAX_PAGES < 1 || OCR_MAX_PAGES > 100", workflow)
        self.assertIn("group: normativa-text-extraction-${{ inputs.document_key }}", workflow)
        self.assertNotIn("vacío = sin límite", workflow)


if __name__ == "__main__":
    unittest.main()
