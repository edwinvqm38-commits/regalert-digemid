from copy import deepcopy
from types import SimpleNamespace

import pytest

from scripts import extract_normativa_text_simple as simple


def extraction(number, text, quality=0.9, *, method="pymupdf", ocr_used=False):
    return SimpleNamespace(
        page_number=number,
        text=text,
        method=method,
        ocr_used=ocr_used,
        ocr_confidence=None,
        quality=quality,
        has_tables=False,
        posible_formula=False,
        posible_grafico=False,
        tables=None,
    )


class Query:
    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.action = "select"
        self.payload = None
        self.filters = {}

    def select(self, *_args):
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

    def is_(self, *_args):
        return self

    def execute(self):
        if self.action == "select":
            if self.table_name == simple.ASSET_TABLE:
                return SimpleNamespace(data=deepcopy(self.db.assets))
            return SimpleNamespace(data=deepcopy(self.db.pages))
        if self.action == "insert":
            if self.db.fail_insert_page == self.payload.get("page_number"):
                raise RuntimeError("fallo de insert simulado")
            self.db.pages.append({"id": f"page-{len(self.db.pages) + 1}", **self.payload})
            self.db.actions.append("insert")
            return SimpleNamespace(data=[deepcopy(self.db.pages[-1])])
        if self.action == "update":
            if self.db.noop_updates:
                self.db.actions.append("update_noop")
                return SimpleNamespace(data=[])
            matched = []
            for row in self.db.pages:
                if row.get("id") == self.filters.get("id"):
                    row.update(self.payload)
                    matched.append(deepcopy(row))
            self.db.actions.append("update")
            return SimpleNamespace(data=matched)
        raise AssertionError(self.action)


class Database:
    def __init__(self, pages=None, *, assets=None, fail_insert_page=None, noop_updates=False):
        self.pages = deepcopy(pages or [])
        self.assets = deepcopy(assets or [])
        self.actions = []
        self.fail_insert_page = fail_insert_page
        self.noop_updates = noop_updates

    def table(self, name):
        return Query(self, name)


def test_rerun_updates_in_place_without_delete_or_duplicates():
    db = Database()
    pages = [extraction(1, "Artículo 1. Texto."), extraction(2, "Artículo 2. Texto.")]

    first = simple.write_pages(db, "norma-1", pages)
    second = simple.write_pages(db, "norma-1", pages)

    assert first["insertadas"] == 2
    assert second["preservadas"] == 2
    assert len(db.pages) == 2
    assert "delete" not in db.actions
    assert second["persistence_verified"] is True


def test_lower_quality_candidate_does_not_replace_valid_page():
    existing = {
        "id": "page-1",
        "page_number": 1,
        "text_raw": "Artículo 1. Texto confiable.",
        "text_normalized": "Artículo 1. Texto confiable.",
        "quality_score": 0.95,
        "revisado_manual": False,
        "extraction_method": "pymupdf",
    }
    db = Database([existing])

    stats = simple.write_pages(db, "norma-1", [extraction(1, "ruido", quality=0.1)])

    assert stats["preservadas"] == 1
    assert db.pages[0]["text_normalized"] == existing["text_normalized"]
    assert db.actions == []


def test_non_contiguous_preparation_fails_before_writes():
    db = Database()

    try:
        simple.write_pages(db, "norma-1", [extraction(2, "Artículo 2.")])
    except ValueError as error:
        assert "incompleta" in str(error)
    else:
        raise AssertionError("Debió rechazar páginas no contiguas")

    assert db.actions == []


def test_partial_persistence_is_not_reported_as_complete():
    db = Database(fail_insert_page=2)
    pages = [extraction(1, "Artículo 1."), extraction(2, "Artículo 2.")]

    with pytest.raises(RuntimeError, match="fallo de insert"):
        simple.write_pages(db, "norma-1", pages)

    assert [row["page_number"] for row in db.pages] == [1]


def test_unexpected_stale_page_fails_reconciliation():
    stale = {
        "id": "page-3",
        "page_number": 3,
        "text_raw": "Página obsoleta",
        "text_normalized": "Página obsoleta",
        "quality_score": 0.8,
        "revisado_manual": False,
        "extraction_method": "pymupdf",
        "source_asset_id": None,
        "document_part": None,
    }
    db = Database([stale])

    with pytest.raises(RuntimeError, match=r"sobrantes=\[3\]"):
        simple.write_pages(db, "norma-1", [extraction(1, "Artículo 1.")])


def test_null_quality_good_text_is_preserved_conservatively():
    existing = {
        "id": "page-1",
        "page_number": 1,
        "text_raw": "Artículo 1. Texto normativo completo y coherente para evaluación.",
        "text_normalized": "Artículo 1. Texto normativo completo y coherente para evaluación.",
        "quality_score": None,
        "revisado_manual": False,
        "extraction_method": "pymupdf",
        "source_asset_id": None,
        "document_part": None,
    }
    db = Database([existing])

    stats = simple.write_pages(db, "norma-1", [extraction(1, "ruido", quality=0.1)])

    assert stats["preservadas"] == 1
    assert db.pages[0]["text_normalized"] == existing["text_normalized"]


def test_update_that_affects_no_rows_is_detected():
    existing = {
        "id": "page-1",
        "page_number": 1,
        "text_raw": "texto pobre",
        "text_normalized": "texto pobre",
        "quality_score": 0.1,
        "revisado_manual": False,
        "extraction_method": "pymupdf",
        "source_asset_id": None,
        "document_part": None,
    }
    db = Database([existing], noop_updates=True)

    with pytest.raises(RuntimeError, match="sin filas afectadas"):
        simple.write_pages(
            db,
            "norma-1",
            [extraction(1, "Artículo 1. Texto de mejor calidad.", quality=0.9)],
        )


def test_asset_backed_document_must_use_canonical_writer():
    db = Database(assets=[{"id": 77, "asset_tipo": "pdf_original"}])

    with pytest.raises(ValueError, match="escritor canónico"):
        simple.write_pages(db, "norma-1", [extraction(1, "Artículo 1.")])


def test_ocr_required_means_pending_not_ocr_was_used():
    completed = Database()
    completed_stats = simple.write_pages(
        completed,
        "norma-1",
        [extraction(1, "Artículo 1. Texto OCR útil.", method="ocr_tesseract", ocr_used=True)],
    )
    pending = Database()
    pending_stats = simple.write_pages(
        pending,
        "norma-2",
        [extraction(1, "", quality=0.0, method="pending_ocr")],
    )

    assert completed_stats["ocr_usado"] is True
    assert completed_stats["ocr_required"] is False
    assert pending_stats["ocr_usado"] is False
    assert pending_stats["ocr_required"] is True
