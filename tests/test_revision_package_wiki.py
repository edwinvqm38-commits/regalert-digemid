from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"
WIKI = ROOT / "docs/GUIA_COMANDOS_TELEGRAM.md"


def test_guided_review_works_one_page_at_a_time():
    text = INDEX.read_text(encoding="utf-8")
    assert "async function enviarPaqueteRevisionPagina" in text
    assert "Selecciona una página. Cada revisión se trabaja por separado" in text
    assert 'callback_data: `rev:p:${reason}:${normaId}:${p.page_number}`' in text


def test_revision_template_has_stable_revision_id():
    text = INDEX.read_text(encoding="utf-8")
    assert "# REVISION_ID:" in text
    assert "buildRevisionId" in text
    assert "parseRevisionId" in text
    assert "REVISION_ID no coincide con norma/página/tipo esperado" in text


def test_review_acceptance_offers_next_observation():
    text = INDEX.read_text(encoding="utf-8")
    assert "➡️ Siguiente observación" in text
    assert "nextRevision" in text
    assert 'callback_data: `rev:p:${resultado.nextRevision.reason}:' in text


def test_admin_has_guide_command_and_wiki():
    text = INDEX.read_text(encoding="utf-8")
    assert '{ command: "guia", description: "Guía rápida de comandos y revisión" }' in text
    assert 'if (trimmed === "/guia")' in text
    assert "GUIA_COMANDOS_TELEGRAM.md" in text

    wiki = WIKI.read_text(encoding="utf-8")
    assert "Flujo recomendado de revisión" in wiki
    assert "REVISION_ID" in wiki
    assert "Tabla no estructurada" in wiki
    assert "Fórmula o notación técnica" in wiki
    assert "Gráfico / diagrama / imagen de referencia" in wiki
    assert "PDFs y Google Drive" in wiki
