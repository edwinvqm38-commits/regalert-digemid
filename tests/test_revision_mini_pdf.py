from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"
PDF_HELPER = ROOT / "supabase/functions/telegram-bot/revision_pdf.ts"


def test_guided_review_sends_selected_page_pdf_not_html():
    text = INDEX.read_text(encoding="utf-8")
    guided = text.split("async function enviarRevisionPorMotivo(", 1)[1].split("/** Parte una fila Markdown", 1)[0]
    assert "enviarMiniPdfRevision" in guided
    assert "construirReporteHtmlRevision" not in guided
    assert "revision_\${reason}_\${norma.document_key}.html" not in guided
    assert "PDF reducido" in guided


def test_mini_pdf_keeps_original_page_numbers_in_caption_and_filename():
    text = INDEX.read_text(encoding="utf-8")
    assert "página original" in text
    assert "páginas originales" in text
    assert "nombrePaginasRevision" in text


def test_pdf_extraction_is_physical_page_based():
    text = PDF_HELPER.read_text(encoding="utf-8")
    assert "validPages.map((n) => n - 1)" in text
    assert "copyPages" in text
    assert "getPageCount" in text


def test_table_review_still_uses_table_review_type():
    text = INDEX.read_text(encoding="utf-8")
    assert 'reason === "tabla" ? "tabla" : "contenido"' in text
