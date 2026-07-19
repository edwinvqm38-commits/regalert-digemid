create table if not exists digemid_suscripciones (
  id uuid primary key default gen_random_uuid(),
  telegram_chat_id text not null,
  telegram_username text,
  nivel text not null default 'gratis' check (nivel in ('gratis', 'basico', 'consultoria', 'empresarial')),
  estado text not null default 'activo' check (estado in ('activo', 'vencido', 'pendiente_pago', 'cancelado')),
  fecha_inicio date not null default current_date,
  fecha_fin date,
  metodo_pago text,
  notas text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_digemid_suscripciones_chat_id
  on digemid_suscripciones (telegram_chat_id);

create index if not exists idx_digemid_suscripciones_estado
  on digemid_suscripciones (estado)
  where estado = 'activo';
