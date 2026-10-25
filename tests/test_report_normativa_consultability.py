from scripts.report_normativa_consultability import classify_norma, render_markdown


NORMA = {
    "document_key": "LEY-29459",
    "process_status": "text_extracted_ocr",
    "pdf_url": "https://example.invalid/ley.pdf",
}
SHA = "a" * 64


def asset(asset_id=10, *, total=3, sha=SHA, subtype="cuerpo"):
    return {
        "id": asset_id,
        "asset_tipo": "pdf_original",
        "asset_subtipo": subtype,
        "file_name": f"asset-{asset_id}.pdf",
        "metadata": {
            "sha256": sha,
            "pdf_physical_page_count": total,
        },
    }


def page(number, text="texto", *, ocr=False, reviewed=False, total=3, asset_id=10, sha=SHA):
    return {
        "source_asset_id": asset_id,
        "page_number": number,
        "text_normalized": text,
        "text_raw": text,
        "ocr_used": ocr,
        "revisado_manual": reviewed,
        "metadata": {
            "pdf_physical_page_count": total,
            "source_pdf_sha256": sha,
        },
    }


def test_complete_ocr_document_is_consultable_but_fidelity_pending():
    row = classify_norma(NORMA, [asset()], [
        page(1, ocr=True),
        page(2, ocr=True),
        page(3, ocr=True),
    ])

    assert row["consultable"] is True
    assert row["pages_ocr"] == 3
    assert row["fidelity_pending"] is True
    assert row["pages_pending"] == 0


def test_empty_or_missing_physical_page_prevents_consultability():
    row = classify_norma(NORMA, [asset()], [page(1), page(3, text="")])

    assert row["text_available"] is True
    assert row["consultable"] is False
    assert row["pages_pending"] == 1
    assert row["missing_pages"] == [2]


def test_pending_ocr_raw_capture_is_not_counted_as_consultable_text():
    pending = page(1, text="sello aislado", total=1)
    pending["text_normalized"] = ""
    pending["extraction_method"] = "pending_ocr"

    row = classify_norma(NORMA, [asset(total=1)], [pending])

    assert row["pages_with_text"] == 0
    assert row["pages_pending"] == 1
    assert row["consultable"] is False


def test_registered_without_pdf_or_pages_is_not_consultable():
    row = classify_norma(
        {"document_key": "DS-014-2011-SA", "process_status": "drive_structured"},
        [],
        [],
    )

    assert row["registered"] is True
    assert row["pdf_available"] is False
    assert row["pages_registered"] is False
    assert row["consultable"] is False


def test_markdown_reports_required_mvp_fields():
    markdown = render_markdown([
        classify_norma(NORMA, [asset(total=1)], [page(1, total=1)])
    ])
    assert "document_key" in markdown
    assert "con texto" in markdown
    assert "pendientes" in markdown
    assert "consultable" in markdown


def test_multiasset_is_evaluated_independently():
    assets = [
        asset(10, total=2, sha="a" * 64, subtype="resolucion"),
        asset(11, total=3, sha="b" * 64, subtype="anexo"),
    ]
    pages = [
        page(1, asset_id=10, total=2, sha="a" * 64),
        page(2, asset_id=10, total=2, sha="a" * 64),
        page(1, asset_id=11, total=3, sha="b" * 64),
        page(2, asset_id=11, total=3, sha="b" * 64),
        page(3, asset_id=11, total=3, sha="b" * 64),
    ]

    row = classify_norma(NORMA, assets, pages)

    assert row["consultable"] is True
    assert row["expected_physical_pages"] == 5
    assert [item["persisted_pages"] for item in row["assets"]] == [2, 3]


def test_verified_blank_counts_for_completeness_but_not_as_text():
    blank = page(2, text="", total=2)
    blank["extraction_method"] = "verified_blank"
    blank["metadata"]["verified_blank"] = True

    row = classify_norma(NORMA, [asset(total=2)], [page(1, total=2), blank])

    assert row["consultable"] is True
    assert row["pages_with_text"] == 1
    assert row["verified_blank_pages"] == 1
    assert row["pages_pending"] == 0


def test_empty_page_without_verified_blank_remains_pending():
    blank = page(2, text="", total=2)
    blank["extraction_method"] = "pagina_en_blanco"

    row = classify_norma(NORMA, [asset(total=2)], [page(1, total=2), blank])

    assert row["consultable"] is False
    assert row["verified_blank_pages"] == 0
    assert row["pages_pending"] == 1


def test_expected_58_with_only_4_persisted_is_not_consultable():
    pages = [page(number, total=58) for number in range(1, 5)]

    row = classify_norma(NORMA, [asset(total=58)], pages)

    assert row["consultable"] is False
    assert row["expected_physical_pages"] == 58
    assert row["missing_pages"] == list(range(5, 59))


def test_old_sha_pages_do_not_complete_asset():
    old = page(1, total=1, sha="b" * 64)

    row = classify_norma(NORMA, [asset(total=1, sha="a" * 64)], [old])

    assert row["consultable"] is False
    assert row["assets"][0]["source_mismatch_pages"] == [1]


def test_legacy_orphan_pages_cannot_complete_asset():
    legacy = page(1, total=1)
    legacy["source_asset_id"] = None

    row = classify_norma(NORMA, [asset(total=1)], [legacy])

    assert row["consultable"] is False
    assert row["orphan_pages"] == 1


def test_quarantined_source_is_never_consultable():
    norma = {**NORMA, "process_status": "needs_source_validation"}

    row = classify_norma(norma, [asset(total=1)], [page(1, total=1)])

    assert row["consultable"] is False
    assert row["needs_source_validation"] is True
