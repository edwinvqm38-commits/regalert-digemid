from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_guided_review_sends_original_pdf_not_reduced_pdf_or_html():
    text = INDEX.read_text(encoding="utf-8")
    guided = text.split("async function enviarRevisionPorMotivo(", 1)[1].split("/** Parte una fila Markdown", 1)[0]
    assert "descargarPdfRevision" in guided
    assert "enviarDocumentoBytes" in guided
    assert "__oficial.pdf" in guided
    assert "PDF oficial completo" in guided
    assert "enviarMiniPdfRevision" not in guided
    assert "construirReporteHtmlRevision" not in guided


def test_guided_review_keeps_pages_to_review_explicit():
    text = INDEX.read_text(encoding="utf-8")
    assert "Revisar página(s):" in text
    assert "revise únicamente la(s) página(s)" in text


def test_guided_review_uses_server_storage_download():
    text = INDEX.read_text(encoding="utf-8")
    assert "parseSupabaseStorageObjectUrl" in text
    assert ".download(storageRef.path)" in text


def test_table_review_still_uses_table_review_type():
    text = INDEX.read_text(encoding="utf-8")
    assert 'reason === "tabla" ? "tabla" : "contenido"' in text
