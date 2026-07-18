alter table digemid_documentos
  add column if not exists storage_backup_path text;

create index if not exists idx_digemid_documentos_storage_backup_pending
  on digemid_documentos (source_type, has_file)
  where storage_backup_path is null;
