-- La columna file_storage_path ya existe en digemid_documentos (agregada
-- previamente fuera de control de versiones). Este script solo documenta
-- su uso como respaldo en Supabase Storage y agrega el índice de soporte.

create index if not exists idx_digemid_documentos_storage_backup_pending
  on digemid_documentos (source_type, has_file)
  where file_storage_path is null;
