alter table digemid_documentos
  add column if not exists archived_at timestamptz;

create index if not exists idx_digemid_documentos_archived_pending
  on digemid_documentos (source_type, published_date)
  where archived_at is null;
