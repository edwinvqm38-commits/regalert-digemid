from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_guided_template_carries_review_reason():
    text = INDEX.read_text(encoding="utf-8")
    assert "# MOTIVO_REVISION:" in text
    assert "motivoRevision?: RevisionReason" in text
    assert "seleccionadas,\n    tipoRevision,\n    reason," in text


def test_returned_txt_is_identified_by_content_not_filename():
    text = INDEX.read_text(encoding="utf-8")
    upload = text.split("if (update.message?.document)", 1)[1]
    assert "aplicarRevisionManualNorma(contenido, chatId)" in upload
    assert "documentoRecibido.file_name" not in upload.split("try {", 1)[1].split("return new Response", 1)[0]


def test_bot_reports_accepted_or_still_observed():
    text = INDEX.read_text(encoding="utf-8")
    assert "Revisión aceptada por el sistema" in text
    assert "Sigue observado" in text
    assert "sin observaciones automáticas pendientes" in text
    assert "fidelidad final sigue referida al PDF oficial" in text


def test_formula_review_has_structural_validation():
    text = INDEX.read_text(encoding="utf-8")
    assert "validateReviewedText" in text
    assert "motivo_revision" in text
