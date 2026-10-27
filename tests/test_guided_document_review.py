from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_guided_review_is_visible_admin_entrypoint():
    text = INDEX.read_text(encoding="utf-8")
    assert '{ command: "revision", description: "Bandeja guiada de revisión documental" }' in text
    visible_block = text.split("const COMANDOS_ADMIN", 1)[1].split("];", 1)[0]
    assert '{ command: "normarevisar"' not in visible_block
    assert '{ command: "tablarevisar"' not in visible_block
    assert '{ command: "revisarnorma"' not in visible_block


def test_legacy_review_commands_remain_compatible():
    text = INDEX.read_text(encoding="utf-8")
    assert 'if (trimmed === "/normasrevisar")' in text
    assert 'if (trimmed.startsWith("/normarevisar"))' in text
    assert 'if (trimmed.startsWith("/revisarnorma"))' in text
    assert 'if (trimmed.startsWith("/tablarevisar"))' in text


def test_guided_review_callbacks_and_filters_exist():
    text = INDEX.read_text(encoding="utf-8")
    assert 'if (data === "rev:home")' in text
    assert 'if (data.startsWith("rev:n:"))' in text
    assert 'if (data.startsWith("rev:t:"))' in text
    assert 'if (data.startsWith("rev:all:"))' in text
    assert "filterRevisionPages" in text
    assert "summarizeRevisionPages" in text
    assert "hasPendingRevision" in text


def test_guided_review_uses_existing_auditable_txt_flow():
    text = INDEX.read_text(encoding="utf-8")
    assert 'construirPlantillaTxtRevision(' in text
    assert '"contenido",' in text
    assert "El bot conservará el OCR original" in text
