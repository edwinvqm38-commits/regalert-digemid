from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_guided_review_prefers_archived_storage_pdf():
    text = INDEX.read_text(encoding="utf-8")
    assert "async function descargarPdfRevisionNorma" in text
    assert ".from(NORMATIVA_STORAGE_BUCKET)" in text
    assert ".download(norma.file_storage_path)" in text
    assert "return await descargarPdfRevision(norma.pdf_url)" in text
    assert "const pdfOriginal = await descargarPdfRevisionNorma(norma);" in text


def test_guided_review_loads_storage_path_from_norma():
    text = INDEX.read_text(encoding="utf-8")
    assert 'id, document_key, titulo, pdf_url, file_storage_path, drive_file_url' in text


def test_guided_review_does_not_offer_full_document_button():
    text = INDEX.read_text(encoding="utf-8")
    assert 'text: "📚 Revisar documento completo"' not in text
    assert "Toca el tipo de observación y luego una página." in text
