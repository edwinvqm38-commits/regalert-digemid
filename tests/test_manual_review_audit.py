from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "supabase/functions/telegram-bot/index.ts"
MIGRATION = ROOT / "supabase/sql/2026_10_26_create_digemid_norma_revisiones.sql"


def test_manual_review_preserves_raw_and_records_audit():
    text = INDEX.read_text(encoding="utf-8")
    assert 'from("digemid_norma_revisiones")' in text
    assert "buildManualReviewPageUpdate" in text
    assert "parseManualReviewKind" in text
    assert "parseSourceAssetId" in text
    assert 'text_raw: textoCorregido' not in text
    assert 'aplicarRevisionManualNorma(contenido, chatId)' in text


def test_template_encodes_review_kind_and_asset_identity():
    text = INDEX.read_text(encoding="utf-8")
    assert "# TIPO_REVISION:" in text
    assert "source_asset_id:" in text
    assert 'construirPlantillaTxtRevision(documentKey, paginas, "contenido")' in text
    assert 'construirPlantillaTxtRevision(documentKey, paginas, "tabla")' in text
    assert 'construirPlantillaTxtRevision(documentKey, paginas, "contenido_completo")' in text


def test_review_history_migration_is_append_only_shape():
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "create table if not exists public.digemid_norma_revisiones" in sql
    assert "texto_raw_original" in sql
    assert "texto_normalizado_anterior" in sql
    assert "texto_revisado" in sql
    assert "tipo_revision" in sql
    assert "revisor_id" in sql
    assert "source_asset_id" in sql
