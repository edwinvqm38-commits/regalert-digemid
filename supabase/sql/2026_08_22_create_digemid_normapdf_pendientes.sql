-- Permite que un admin escriba "/normapdf DOCUMENT_KEY" como mensaje de texto
-- y luego mande el PDF solo (sin caption), util en Telegram movil donde no se
-- puede adjuntar un archivo y escribir el caption a la vez. El registro
-- expira a los 30 minutos para no dejar "atado" un PDF futuro por error.
create table if not exists digemid_normapdf_pendientes (
  chat_id text primary key,
  document_key text not null,
  expira_en timestamptz not null,
  created_at timestamptz not null default now()
);
