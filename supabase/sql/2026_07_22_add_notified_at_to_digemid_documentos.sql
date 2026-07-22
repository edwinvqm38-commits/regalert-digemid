-- Permite reintentar automaticamente la notificacion por Telegram de un
-- documento ya guardado en Supabase cuando el envio anterior fallo (por
-- ejemplo, TELEGRAM_CHAT_ID invalido), en lugar de darlo por perdido para
-- siempre porque ya no aparece como "nuevo" frente a Supabase.
alter table digemid_documentos
  add column if not exists notified_at timestamptz;

-- Backfill: las alertas historicas ya notificadas antes de esta columna
-- existir no deben reenviarse en masa; se marcan como notificadas con su
-- propia fecha de registro.
update digemid_documentos
  set notified_at = coalesce(notified_at, created_at)
  where source_type = 'alerta' and notified_at is null;

create index if not exists idx_digemid_documentos_notify_pending
  on digemid_documentos (source_type, published_date)
  where notified_at is null;
