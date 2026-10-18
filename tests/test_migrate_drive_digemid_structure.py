"""A-01.1: custodia directa de alertas DIGEMID desde file_url."""

from copy import deepcopy
import hashlib
import json
import re
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import migrate_drive_digemid_structure as migration


VALID_PDF = b"%PDF-1.7\n" + (b"A" * (migration.MIN_PDF_SIZE_BYTES + 32))


def make_row(**overrides):
    row = {
        "id": "doc-1",
        "source_type": "alerta",
        "source_section": "alertas-digemid",
        "document_key": "72-2026",
        "title": "Alerta DIGEMID 72-2026",
        "published_date": "2026-10-17",
        "file_url": "https://www.digemid.minsa.gob.pe/alerta-72.pdf",
        "file_name": "alerta-72.pdf",
        "drive_file_id": None,
        "drive_file_url": None,
        "drive_download_url": None,
        "drive_folder_id": None,
        "uploaded_to_drive_at": None,
        "mime_type": "application/pdf",
        "file_size_bytes": None,
        "file_hash": None,
        "has_file": True,
        "raw": {},
    }
    row.update(overrides)
    return row


class FakeRequest:
    def __init__(self, callback):
        self.callback = callback

    def execute(self):
        return self.callback()


class FakeDriveFiles:
    def __init__(self, drive):
        self.drive = drive

    def get(self, fileId, **_kwargs):
        return FakeRequest(lambda: deepcopy(self.drive.items[fileId]))

    def get_media(self, fileId, **_kwargs):
        return FakeRequest(lambda: self.drive.items[fileId]["content"])

    def list(self, q, **_kwargs):
        parent_match = re.search(r"'([^']+)' in parents", q)
        name_match = re.search(r"name = '([^']+)'", q)
        mime_match = re.search(r"mimeType = '([^']+)'", q)
        parent_id = parent_match.group(1) if parent_match else None
        name = name_match.group(1) if name_match else None
        mime_type = mime_match.group(1) if mime_match else None

        def execute():
            matches = []
            for item in self.drive.items.values():
                if parent_id not in item.get("parents", []):
                    continue
                if item.get("name") != name:
                    continue
                if mime_type and item.get("mimeType") != mime_type:
                    continue
                matches.append(deepcopy(item))
            return {"files": matches[:10]}

        return FakeRequest(execute)

    def create(self, body, media_body=None, **_kwargs):
        def execute():
            self.drive.create_calls += 1
            file_id = f"drive-{self.drive.next_id}"
            self.drive.next_id += 1
            mime_type = body.get("mimeType") or getattr(
                media_body, "mimetype", lambda: "application/octet-stream"
            )()
            content = b""
            if media_body is not None:
                size = media_body.size()
                content = media_body.getbytes(0, size)
            item = {
                "id": file_id,
                "name": body["name"],
                "mimeType": mime_type,
                "parents": list(body.get("parents", [])),
                "size": str(len(content)) if media_body is not None else None,
                "webViewLink": migration.build_drive_file_url(file_id),
                "webContentLink": migration.build_drive_download_url(file_id),
                "content": content,
            }
            self.drive.items[file_id] = item
            self.drive.mutations.append(("create", file_id, item["name"]))
            return deepcopy(item)

        return FakeRequest(execute)

    def copy(self, fileId, body, **_kwargs):
        def execute():
            self.drive.copy_calls += 1
            source = self.drive.items[fileId]
            file_id = f"drive-{self.drive.next_id}"
            self.drive.next_id += 1
            item = {
                **deepcopy(source),
                "id": file_id,
                "name": body["name"],
                "parents": list(body["parents"]),
                "webViewLink": migration.build_drive_file_url(file_id),
                "webContentLink": migration.build_drive_download_url(file_id),
            }
            self.drive.items[file_id] = item
            self.drive.mutations.append(("copy", file_id, item["name"]))
            return deepcopy(item)

        return FakeRequest(execute)

    def update(self, fileId, media_body=None, **_kwargs):
        def execute():
            item = self.drive.items[fileId]
            if media_body is not None:
                size = media_body.size()
                item["content"] = media_body.getbytes(0, size)
                item["size"] = str(size)
            self.drive.mutations.append(("update", fileId, item["name"]))
            return deepcopy(item)

        return FakeRequest(execute)


class FakeDrive:
    def __init__(self, items=None):
        self.items = {item["id"]: deepcopy(item) for item in (items or [])}
        self.next_id = 1
        self.create_calls = 0
        self.copy_calls = 0
        self.mutations = []
        self._files = FakeDriveFiles(self)

    def files(self):
        return self._files


class FakeQuery:
    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name
        self.action = "select"
        self.payload = None
        self.filters = {}
        self.limit_value = None

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

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def execute(self):
        if self.table_name == "digemid_documentos":
            if self.action == "select":
                rows = [
                    deepcopy(row)
                    for row in self.database.documents
                    if all(row.get(key) == value for key, value in self.filters.items())
                ]
                return SimpleNamespace(data=rows[: self.limit_value])
            if self.action == "update":
                updated = []
                for row in self.database.documents:
                    if all(row.get(key) == value for key, value in self.filters.items()):
                        row.update(deepcopy(self.payload))
                        updated.append(deepcopy(row))
                self.database.writes.append(("update_document", deepcopy(self.payload)))
                return SimpleNamespace(data=updated)

        if self.table_name == "digemid_documento_assets":
            if self.action == "select":
                rows = [
                    deepcopy(row)
                    for row in self.database.assets
                    if all(row.get(key) == value for key, value in self.filters.items())
                ]
                return SimpleNamespace(data=rows)
            if self.action == "insert":
                inserted = {
                    "id": f"asset-{len(self.database.assets) + 1}",
                    **deepcopy(self.payload),
                }
                self.database.assets.append(inserted)
                self.database.writes.append(("insert_asset", deepcopy(self.payload)))
                return SimpleNamespace(data=[deepcopy(inserted)])

        raise AssertionError(f"Consulta no soportada: {self.table_name} {self.action}")


class FakeSupabase:
    def __init__(self, documents=None, assets=None):
        self.documents = deepcopy(documents or [])
        self.assets = deepcopy(assets or [])
        self.writes = []

    def table(self, table_name):
        return FakeQuery(self, table_name)


def successful_response(content=VALID_PDF):
    return SimpleNamespace(
        status_code=200,
        content=content,
        headers={"Content-Type": "application/pdf"},
    )


class MigrateDriveDigemidStructureTests(unittest.TestCase):
    def test_legacy_drive_file_path_keeps_copy_behavior(self):
        row = make_row(drive_file_id="legacy-source")
        drive = FakeDrive(
            [
                {
                    "id": "legacy-source",
                    "name": "alerta.pdf",
                    "mimeType": "application/pdf",
                    "parents": ["legacy-root"],
                    "size": "25000",
                    "webViewLink": "https://drive.google.com/legacy-source",
                    "webContentLink": "https://drive.google.com/download/legacy-source",
                    "content": VALID_PDF,
                }
            ]
        )
        database = FakeSupabase([row])

        with patch.object(migration.requests, "get", side_effect=AssertionError):
            result = migration.process_document(drive, database, "root", row, True)

        actions = [operation["action"] for operation in result["operations"]]
        self.assertIn("copy_file", actions)
        self.assertNotIn("direct_upload", actions)
        self.assertEqual(drive.copy_calls, 1)
        update = next(value for action, value in database.writes if action == "update_document")
        self.assertNotIn("drive_file_id", update)
        pdf_asset = next(asset for asset in database.assets if asset["asset_tipo"] == "pdf_original")
        self.assertEqual(pdf_asset["asset_subtipo"], "copied_to_document_folder")

    def test_file_url_without_drive_id_is_selected_as_candidate(self):
        legacy = make_row(id="legacy", drive_file_id="drive-existing")
        direct = make_row(id="direct", document_key="73-2026")
        unusable = make_row(id="none", document_key="74-2026", file_url=None)
        excluded = make_row(id="no-file", has_file=False)
        database = FakeSupabase([legacy, direct, unusable, excluded])

        rows = migration.get_documents(database, "alerta", None, None, False)

        self.assertEqual([row["id"] for row in rows], ["legacy", "direct"])

    def test_dry_run_validates_and_plans_direct_upload_without_writes(self):
        row = make_row()
        drive = FakeDrive()
        database = FakeSupabase([row])

        with patch.object(migration.requests, "get", return_value=successful_response()):
            result = migration.process_document(drive, database, "root", row, False)

        direct_op = next(
            operation
            for operation in result["operations"]
            if operation["action"] == "planned_direct_upload"
        )
        expected_sha = hashlib.sha256(VALID_PDF).hexdigest()
        self.assertEqual(direct_op["sha256"], expected_sha)
        self.assertEqual(direct_op["size_bytes"], len(VALID_PDF))
        self.assertEqual(result["source_sha256"], expected_sha)
        self.assertEqual(drive.mutations, [])
        self.assertEqual(database.writes, [])

    def test_apply_uploads_once_and_updates_document_assets_and_manifest(self):
        row = make_row()
        drive = FakeDrive()
        database = FakeSupabase([row])

        with patch.object(migration.requests, "get", return_value=successful_response()):
            result = migration.process_document(drive, database, "root", row, True)

        expected_sha = hashlib.sha256(VALID_PDF).hexdigest()
        uploaded = next(
            item
            for item in drive.items.values()
            if item["name"] == "ALERTA-72-2026__2026-10-17__original.pdf"
        )
        original_folder = next(
            item for item in drive.items.values() if item["name"] == "00_ORIGINAL"
        )
        self.assertEqual(uploaded["parents"], [original_folder["id"]])
        self.assertEqual(uploaded["content"], VALID_PDF)
        self.assertIn("direct_upload", [op["action"] for op in result["operations"]])

        document = database.documents[0]
        self.assertEqual(document["drive_file_id"], uploaded["id"])
        self.assertEqual(document["file_hash"], expected_sha)
        self.assertEqual(document["file_size_bytes"], len(VALID_PDF))
        self.assertTrue(document["drive_file_url"])
        self.assertTrue(document["drive_download_url"])
        self.assertEqual(len(database.assets), 2)
        pdf_asset = next(asset for asset in database.assets if asset["asset_tipo"] == "pdf_original")
        self.assertEqual(pdf_asset["drive_file_id"], uploaded["id"])
        self.assertEqual(pdf_asset["sha256"], expected_sha)
        self.assertEqual(pdf_asset["asset_subtipo"], "direct_from_file_url")

        manifest_item = next(
            item for item in drive.items.values() if item["name"] == "manifest.json"
        )
        manifest = json.loads(manifest_item["content"].decode("utf-8"))
        self.assertIsNone(manifest["original_drive_file_id"])
        self.assertEqual(manifest["custody_source"], "digemid_documentos.file_url")
        self.assertEqual(manifest["source_sha256"], expected_sha)

    def test_apply_rerun_reuses_folders_file_and_assets(self):
        row = make_row()
        drive = FakeDrive()
        database = FakeSupabase([row])

        with patch.object(migration.requests, "get", return_value=successful_response()):
            migration.process_document(drive, database, "root", row, True)
            folder_ids = {
                item["id"]
                for item in drive.items.values()
                if item["mimeType"] == migration.DRIVE_FOLDER_MIME
            }
            first_pdf_ids = {
                item["id"]
                for item in drive.items.values()
                if item["name"].endswith("__original.pdf")
            }
            second = migration.process_document(
                drive, database, "root", deepcopy(database.documents[0]), True
            )

        self.assertEqual(
            folder_ids,
            {
                item["id"]
                for item in drive.items.values()
                if item["mimeType"] == migration.DRIVE_FOLDER_MIME
            },
        )
        self.assertEqual(
            first_pdf_ids,
            {
                item["id"]
                for item in drive.items.values()
                if item["name"].endswith("__original.pdf")
            },
        )
        self.assertEqual(len(database.assets), 2)
        actions = [operation["action"] for operation in second["operations"]]
        self.assertIn("reuse_direct_upload", actions)
        self.assertEqual(actions.count("reuse_asset"), 2)

    def test_existing_same_name_same_content_is_integrity_verified_and_reused(self):
        target_name = "ALERTA-72-2026__2026-10-17__original.pdf"
        existing = {
            "id": "existing-pdf",
            "name": target_name,
            "mimeType": "application/pdf",
            "parents": ["original-folder"],
            "size": str(len(VALID_PDF)),
            "webViewLink": migration.build_drive_file_url("existing-pdf"),
            "webContentLink": migration.build_drive_download_url("existing-pdf"),
            "content": VALID_PDF,
        }
        drive = FakeDrive([existing])
        operations = []
        downloaded_pdf = {
            "content": VALID_PDF,
            "size_bytes": len(VALID_PDF),
            "sha256": hashlib.sha256(VALID_PDF).hexdigest(),
            "source_url": "https://www.digemid.minsa.gob.pe/alerta-72.pdf",
        }

        result = migration.drive_upload_pdf_direct(
            drive,
            "original-folder",
            target_name,
            downloaded_pdf,
            True,
            operations,
        )

        self.assertEqual(result["id"], "existing-pdf")
        self.assertEqual(len(drive.items), 1)
        self.assertEqual(drive.mutations, [])
        self.assertEqual(operations[0]["action"], "reuse_direct_upload")
        self.assertTrue(operations[0]["integrity_verified"])
        self.assertEqual(operations[0]["size_bytes"], len(VALID_PDF))
        self.assertEqual(
            operations[0]["sha256"], hashlib.sha256(VALID_PDF).hexdigest()
        )

    def test_existing_same_name_different_content_stops_before_supabase_writes(self):
        row = make_row()
        drive = FakeDrive()
        database = FakeSupabase([row])

        with patch.object(migration.requests, "get", return_value=successful_response()):
            migration.process_document(drive, database, "root", row, True)

        stored_pdf = next(
            item
            for item in drive.items.values()
            if item["name"].endswith("__original.pdf")
        )
        original_pdf_id = stored_pdf["id"]
        stored_pdf["content"] = b"%PDF-1.7\n" + (
            b"B" * (len(VALID_PDF) - len(b"%PDF-1.7\n"))
        )
        stored_pdf["size"] = str(len(stored_pdf["content"]))
        assets_before_conflict = deepcopy(database.assets)
        drive.mutations.clear()
        database.writes.clear()

        with patch.object(
            migration.requests, "get", return_value=successful_response()
        ), self.assertRaisesRegex(ValueError, "Conflicto de integridad"):
            migration.process_document(
                drive,
                database,
                "root",
                deepcopy(database.documents[0]),
                True,
            )

        pdfs = [
            item
            for item in drive.items.values()
            if item["name"].endswith("__original.pdf")
        ]
        self.assertEqual([item["id"] for item in pdfs], [original_pdf_id])
        self.assertEqual(len(pdfs[0]["content"]), len(VALID_PDF))
        self.assertEqual(drive.mutations, [])
        self.assertEqual(database.writes, [])
        self.assertEqual(database.assets, assets_before_conflict)

    def test_invalid_pdf_is_reported_without_drive_or_supabase_writes(self):
        row = make_row()
        drive = FakeDrive()
        database = FakeSupabase([row])
        captured = {}
        args = SimpleNamespace(
            mode="apply",
            source_type="alerta",
            document_key=None,
            pending_only=False,
            limit=None,
        )

        with patch.object(migration, "parse_args", return_value=args), patch.object(
            migration, "load_env"
        ), patch.object(migration.os, "getenv", return_value="root"), patch.object(
            migration, "get_supabase", return_value=database
        ), patch.object(
            migration, "get_drive_service", return_value=(drive, [])
        ), patch.object(
            migration, "get_documents", return_value=[row]
        ), patch.object(
            migration.requests,
            "get",
            return_value=successful_response(b"not a pdf"),
        ), patch.object(
            migration,
            "write_report_files",
            side_effect=lambda mode, payload: captured.update(
                {"mode": mode, "payload": payload}
            ),
        ):
            migration.main()

        reported = captured["payload"]["documents"][0]
        self.assertEqual(reported["status"], "error")
        self.assertIn("firma PDF", reported["error"])
        self.assertEqual(drive.mutations, [])
        self.assertEqual(database.writes, [])

    def test_direct_update_deep_merges_existing_raw(self):
        row = make_row(
            raw={
                "collector": {"request_id": "keep-me"},
                "drive_structure": {
                    "custom": "keep-too",
                    "subfolders": {"legacy": {"id": "legacy-folder"}},
                },
            }
        )
        drive = FakeDrive()
        database = FakeSupabase([row])

        with patch.object(migration.requests, "get", return_value=successful_response()):
            migration.process_document(drive, database, "root", row, True)

        raw = database.documents[0]["raw"]
        self.assertEqual(raw["collector"]["request_id"], "keep-me")
        self.assertEqual(raw["drive_structure"]["custom"], "keep-too")
        self.assertEqual(
            raw["drive_structure"]["subfolders"]["legacy"]["id"],
            "legacy-folder",
        )
        self.assertEqual(
            raw["drive_structure"]["custody_mode"], migration.DIRECT_CUSTODY_MODE
        )


if __name__ == "__main__":
    unittest.main()
