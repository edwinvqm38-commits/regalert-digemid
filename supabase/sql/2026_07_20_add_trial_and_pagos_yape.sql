-- Prueba gratuita con vencimiento (solo usuarios que llegan por el landing page)
-- y registro de pagos por Yape con codigo de operacion unico (evita duplicidad).

alter table digemid_bot_usuarios
  add column if not exists origen text,
  add column if not exists plan_interes text,
  add column if not exists prueba_estado text check (prueba_estado in ('activa', 'finalizada')),
  add column if not exists prueba_inicio timestamptz,
  add column if not exists prueba_alertas_enviadas integer not null default 0,
  add column if not exists pago_pendiente_nivel text;

create index if not exists digemid_bot_usuarios_prueba_activa_idx
  on digemid_bot_usuarios (prueba_inicio)
  where prueba_estado = 'activa';

create table if not exists digemid_pagos_yape (
  id uuid primary key default gen_random_uuid(),
  chat_id text not null,
  nivel text not null,
  monto_esperado numeric not null,
  codigo_operacion text not null unique,
  estado text not null default 'pendiente' check (estado in ('pendiente', 'confirmado', 'rechazado')),
  creado_at timestamptz not null default now(),
  confirmado_at timestamptz,
  confirmado_por text
);

create index if not exists digemid_pagos_yape_chat_idx
  on digemid_pagos_yape (chat_id);

create index if not exists digemid_pagos_yape_pendiente_idx
  on digemid_pagos_yape (estado)
  where estado = 'pendiente';
