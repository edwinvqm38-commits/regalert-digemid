alter table digemid_bot_invitaciones
  add column if not exists recordatorios_enviados integer not null default 0,
  add column if not exists ultimo_recordatorio_at timestamptz;
