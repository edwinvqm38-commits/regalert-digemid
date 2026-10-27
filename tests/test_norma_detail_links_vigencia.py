from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_norma_detail_prefers_drive_or_public_official_pdf():
    text = INDEX.read_text(encoding="utf-8")
    assert "norma.drive_file_url || extraerPdfPublicoNorma(norma.raw)" in text
    assert "parseSupabaseStorageObjectUrl(candidato, SUPABASE_URL)" in text
    assert 'text: "📄 Ver PDF"' in text


def test_norma_detail_shows_confirmed_derogation():
    text = INDEX.read_text(encoding="utf-8")
    assert '.eq("tipo_relacion", "deroga")' in text
    assert '.eq("estado", "confirmada")' in text
    assert "Derogada por" in text
    assert "norma_origen_document_key" in text


def test_norma_detail_remains_compact():
    text = INDEX.read_text(encoding="utf-8")
    assert '📋 <b>Documento</b>' in text
    assert '• Páginas:' in text
    assert '• Revisadas:' in text
    assert '• Pendientes:' in text
