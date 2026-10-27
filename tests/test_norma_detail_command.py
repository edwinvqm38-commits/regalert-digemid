from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"


def test_norma_command_is_registered_for_users():
    text = INDEX.read_text(encoding="utf-8")
    assert '{ command: "norma", description: "Ver ficha y estado de una norma" }' in text


def test_norma_command_reads_normative_tables_not_alerts():
    text = INDEX.read_text(encoding="utf-8")
    assert 'async function getNormaDetail(documentKey: string)' in text
    assert '.from("digemid_normas")' in text
    assert '.from("digemid_norma_paginas")' in text
    assert 'if (trimmed === "/norma" || trimmed.startsWith("/norma "))' in text


def test_detalle_remains_alert_only():
    text = INDEX.read_text(encoding="utf-8")
    detalle = text.split('if (trimmed.startsWith("/detalle"))', 1)[1].split('if (trimmed.startsWith("/buscar"))', 1)[0]
    assert "getAlertDetail" in detalle
    assert "getNormaDetail" not in detalle


def test_norma_detail_exposes_document_review_status():
    text = INDEX.read_text(encoding="utf-8")
    assert "Revisadas manualmente" in text
    assert "Con revisión pendiente" in text
    assert "Sin observaciones automáticas pendientes" in text
    assert 'callback_data: `rev:n:${norma.id}`' in text
