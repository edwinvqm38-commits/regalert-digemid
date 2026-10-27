from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_norma_detail_hides_historical_source_when_drive_exists():
    text = INDEX.read_text(encoding="utf-8")
    assert 'if (!norma.drive_file_url && norma.source_url)' in text
    assert 'text: "📄 Ver PDF"' in text
    assert 'text: "🔗 Fuente oficial"' in text
