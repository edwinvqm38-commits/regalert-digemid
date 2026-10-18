"""F-04B1: candidato registrado, validación real y custodia simulada sin red."""

import hashlib
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import download_normativa_pdfs_to_drive as downloader


PDF_BYTES = b"%PDF-1.7\n" + b"x" * downloader.MIN_PDF_BYTES
PDF_SHA256 = hashlib.sha256(PDF_BYTES).hexdigest()


def norma(**overrides):
    row = {
        "id": "norma-1",
        "document_key": "LEY-29459",
        "titulo": "Ley de los productos farmacéuticos",
        "pdf_url": "https://example.invalid/registrado.pdf",
        "source_url": "https://example.invalid/pagina-normativa",
        "fecha_publicacion": "2009-11-26",
        "fuente_oficial": "DIGEMID",
        "has_file": False,
        "process_status": "drive_structured",
        "raw": {"seed": {"document_key": "LEY-29459"}},
        "drive_structure": {"subfolders": {"00_ORIGINAL": {"id": "original-1"}}},
    }
    row.update(overrides)
    return row


def response(status=200, mime="application/pdf", content=PDF_BYTES):
    return SimpleNamespace(status_code=status, headers={"Content-Type": mime}, content=content)


class TestOfficialFilesSelection(unittest.TestCase):
    def test_official_files_has_absolute_priority(self):
        item = {
            "url": "https://example.invalid/prioritario.pdf",
            "file_name": "prioritario.pdf",
            "status": "pending_download",
            "asset_tipo": "pdf_original",
        }
        row = norma(raw={"official_files": [item]})
        before = deepcopy(row)
        files, source = downloader.resolve_official_files(row)
        self.assertEqual(source, "raw.official_files")
        self.assertEqual(files, [item])
        self.assertNotIn(row["pdf_url"], [file["url"] for file in files])
        self.assertEqual(row, before)

    def test_nonempty_invalid_list_is_not_replaced_by_pdf_url(self):
        files, source = downloader.resolve_official_files(norma(raw={"official_files": [None]}))
        self.assertEqual(source, "raw.official_files")
        self.assertEqual(files, [{"raw_value": None}])

    def test_empty_or_missing_official_files_synthesizes_one_original(self):
        for raw in (None, {}, {"official_files": []}):
            with self.subTest(raw=raw):
                row = norma(raw=raw)
                before = deepcopy(row)
                files, source = downloader.resolve_official_files(row)
                self.assertEqual(source, "digemid_normas.pdf_url")
                self.assertEqual(files, [{
                    "url": row["pdf_url"],
                    "title": row["titulo"],
                    "source": row["fuente_oficial"],
                    "source_page": row["source_url"],
                    "status": "pending_download",
                    "asset_tipo": "pdf_original",
                    "file_name": "LEY-29459__2009-11-26__original.pdf",
                }])
                self.assertEqual(row, before)

    def test_fallback_name_without_date(self):
        files, _ = downloader.resolve_official_files(norma(fecha_publicacion=None, fuente_oficial=None))
        self.assertEqual(files[0]["file_name"], "LEY-29459__original.pdf")
        self.assertIsNone(files[0]["source"])

    def test_source_page_never_becomes_pdf_fallback(self):
        for pdf_url in (None, "", "   "):
            with self.subTest(pdf_url=pdf_url):
                files, source = downloader.resolve_official_files(norma(pdf_url=pdf_url))
                self.assertEqual(files, [])
                self.assertEqual(source, "none")

    def test_select_includes_only_the_three_additional_fields(self):
        self.assertEqual(
            {field.strip() for field in downloader.SUPABASE_SELECT_FIELDS.split(",")},
            {"id", "document_key", "titulo", "source_url", "has_file", "drive_folder_id",
             "drive_structure", "raw", "process_status", "updated_at",
             "pdf_url", "fecha_publicacion", "fuente_oficial"},
        )


class TestDryRunValidation(unittest.TestCase):
    def run_dry(self, row, http_response, force=False):
        before = deepcopy(row)
        # Ningún método de custodia disponible: cualquier acceso falla el test.
        service = Mock(spec=[])
        supabase = Mock(spec=[])
        with patch.object(downloader.requests, "get", return_value=http_response) as get, \
             patch.object(downloader, "download_and_validate_pdf", wraps=downloader.download_and_validate_pdf) as validate:
            result = downloader.process_norma(service, supabase, None, row, False, force)
        self.assertEqual(row, before)
        self.assertEqual(service.mock_calls, [])
        self.assertEqual(supabase.mock_calls, [])
        self.assertEqual(result["summary"]["drive_written"], 0)
        self.assertEqual(result["summary"]["assets_ok"], 0)
        self.assertFalse(any(file["asset_registered"] for file in result["files"]))
        return result, get, validate

    def test_dry_run_get_validates_size_and_hash_without_writes(self):
        row = norma()
        result, get, validate = self.run_dry(row, response())
        validate.assert_called_once_with(row["pdf_url"])
        get.assert_called_once_with(
            row["pdf_url"], timeout=120,
            headers={"User-Agent": "RegAlert-DIGEMID-NormativaPDFDownloader/1.0"},
        )
        self.assertEqual(result["official_files_source"], "digemid_normas.pdf_url")
        self.assertEqual(len(result["files"]), 1)
        item = result["files"][0]
        self.assertEqual(item["status_out"], "would_download")
        self.assertEqual(item["file_size_bytes"], len(PDF_BYTES))
        self.assertEqual(item["sha256"], PDF_SHA256)
        self.assertIsNone(item["error"])
        self.assertEqual(result["summary"]["errors"], 0)
        synthesis = [op for op in result["operations"] if op["action"] == "synthesize_official_file_from_pdf_url"]
        self.assertEqual(len(synthesis), 1)
        self.assertEqual(synthesis[0]["url"], row["pdf_url"])

    def test_dry_run_validates_even_without_drive_folder(self):
        result, get, validate = self.run_dry(norma(drive_structure=None), response())
        get.assert_called_once()
        validate.assert_called_once()
        self.assertIsNone(result["original_folder_id"])
        self.assertEqual(result["files"][0]["sha256"], PDF_SHA256)

    def test_dry_run_reports_http_mime_size_and_magic_errors(self):
        cases = [
            (response(status=403), "HTTP inesperado: 403"),
            (response(status=404), "HTTP inesperado: 404"),
            (response(mime="text/html"), "Content-Type no compatible"),
            (response(mime=""), "Content-Type no compatible"),
            (response(content=b"%PDF"), "PDF demasiado pequeno"),
            (response(content=b"%PDF" + b"x" * (downloader.MIN_PDF_BYTES - 4)), "PDF demasiado pequeno"),
            (response(content=b"<html>" + b"x" * downloader.MIN_PDF_BYTES), "Magic bytes invalidos"),
        ]
        for http_response, expected in cases:
            with self.subTest(error=expected):
                result, get, validate = self.run_dry(norma(), http_response)
                get.assert_called_once()
                validate.assert_called_once()
                self.assertEqual(result["files"][0]["status_out"], "download_error")
                self.assertIn(expected, result["files"][0]["error"])
                self.assertIsNone(result["files"][0]["sha256"])
                self.assertEqual(result["summary"]["errors"], 1)

    def test_allowed_binary_mime_still_requires_pdf_magic(self):
        for mime in ("application/pdf; charset=binary", "application/x-pdf", "application/octet-stream", "binary/octet-stream"):
            with self.subTest(mime=mime):
                result, _, _ = self.run_dry(norma(), response(mime=mime))
                self.assertEqual(result["files"][0]["sha256"], PDF_SHA256)
        result, _, _ = self.run_dry(norma(), response(mime="application/octet-stream", content=b"<html>" + PDF_BYTES))
        self.assertEqual(result["files"][0]["status_out"], "download_error")
        self.assertIn("Magic bytes", result["files"][0]["error"])

    def test_official_list_is_the_only_download_source(self):
        item = {"url": "https://example.invalid/oficial.pdf", "status": "pending_download"}
        result, get, validate = self.run_dry(norma(raw={"official_files": [item]}), response())
        validate.assert_called_once_with(item["url"])
        self.assertEqual(get.call_args.args, (item["url"],))
        self.assertEqual(result["official_files_source"], "raw.official_files")
        self.assertEqual(len(result["files"]), 1)
        self.assertFalse(any(op["action"] == "synthesize_official_file_from_pdf_url" for op in result["operations"]))

    def test_already_downloaded_official_file_keeps_selection_policy(self):
        row = norma(raw={"official_files": [{"url": "https://example.invalid/oficial.pdf", "status": "downloaded"}]})
        result, get, validate = self.run_dry(row, response())
        get.assert_not_called()
        validate.assert_not_called()
        self.assertFalse(result["files"][0]["selected"])
        forced, _, validate = self.run_dry(row, response(), force=True)
        validate.assert_called_once()
        self.assertEqual(forced["files"][0]["status_out"], "would_force_download")

    def test_without_candidate_reports_none_and_does_not_download(self):
        result, get, validate = self.run_dry(norma(pdf_url=None, drive_structure=None), response())
        get.assert_not_called()
        validate.assert_not_called()
        self.assertEqual(result["files"], [])
        self.assertEqual(result["official_files_source"], "none")

    def test_main_dry_run_validates_without_drive_credentials(self):
        args = SimpleNamespace(mode="dry-run", document_key="LEY-29459", force=False)
        supabase = Mock(spec=[])
        with patch.object(downloader, "parse_args", return_value=args), \
             patch.object(downloader, "load_env"), \
             patch.object(downloader.os, "getenv", return_value=None), \
             patch.object(downloader, "get_supabase", return_value=supabase), \
             patch.object(downloader, "get_norma_by_document_key", return_value=norma()) as read, \
             patch.object(downloader, "get_drive_service") as drive, \
             patch.object(downloader.requests, "get", return_value=response()) as get, \
             patch.object(downloader, "write_report_files") as report:
            downloader.main()
        read.assert_called_once_with(supabase, args.document_key)
        drive.assert_not_called()
        get.assert_called_once()
        self.assertEqual(supabase.mock_calls, [])
        mode, payload = report.call_args.args
        self.assertEqual(mode, "dry-run")
        self.assertEqual(payload["files"][0]["sha256"], PDF_SHA256)
        self.assertEqual(payload["official_files_source"], "digemid_normas.pdf_url")


class TestApplyCustody(unittest.TestCase):
    def test_apply_uploads_validated_pdf_and_registers_original_with_sha(self):
        row = norma()
        before = deepcopy(row)
        service = Mock()
        drive_files = service.files.return_value
        drive_files.list.return_value.execute.return_value = {"files": []}
        drive_files.create.return_value.execute.return_value = {"id": "pdf-1"}
        supabase = Mock()
        assets, normas = Mock(), Mock()
        supabase.table.side_effect = {"digemid_norma_assets": assets, "digemid_normas": normas}.__getitem__
        assets.select.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=[])
        assets.insert.return_value.execute.return_value = SimpleNamespace(data=[{"id": "asset-1"}])

        with patch.object(downloader.requests, "get", return_value=response()) as get:
            result = downloader.process_norma(service, supabase, None, row, True, False)

        get.assert_called_once()
        self.assertEqual(row, before)
        drive_files.create.assert_called_once()
        upload = drive_files.create.call_args.kwargs
        self.assertEqual(upload["body"]["parents"], ["original-1"])
        self.assertEqual(upload["body"]["name"], "LEY-29459__2009-11-26__original.pdf")
        self.assertEqual(upload["media_body"].getbytes(0, len(PDF_BYTES)), PDF_BYTES)
        assets.insert.assert_called_once()
        payload = assets.insert.call_args.args[0]
        self.assertEqual(payload["asset_tipo"], "pdf_original")
        self.assertEqual(payload["drive_file_id"], "pdf-1")
        self.assertEqual(payload["source_url"], row["pdf_url"])
        self.assertEqual(payload["metadata"]["source_page"], row["source_url"])
        self.assertEqual(payload["metadata"]["source"], row["fuente_oficial"])
        self.assertEqual(payload["metadata"]["sha256"], PDF_SHA256)
        self.assertEqual(payload["file_size_bytes"], len(PDF_BYTES))
        normas.update.assert_called_once()
        update = normas.update.call_args.args[0]
        self.assertTrue(update["has_file"])
        self.assertEqual(update["process_status"], "pdf_downloaded")
        self.assertNotIn("raw", update)
        self.assertEqual(result["files"][0]["status_out"], "downloaded")
        self.assertTrue(result["files"][0]["asset_registered"])


if __name__ == "__main__":
    unittest.main()
