alter table digemid_bot_usuarios
  add column if not exists telefono text;

create table if not exists digemid_bot_invitaciones (
  id uuid primary key default gen_random_uuid(),
  codigo text not null unique,
  telefono text,
  nombre text,
  creado_por text,
  estado text not null default 'pendiente' check (estado in ('pendiente', 'usado', 'expirado')),
  telegram_chat_id text,
  created_at timestamptz not null default now(),
  used_at timestamptz
);

create index if not exists digemid_bot_invitaciones_codigo_idx
  on digemid_bot_invitaciones (codigo);
