-- Marca paginas de normativa que un administrador corrigio manualmente por
-- Telegram (flujo /normarevisar), para distinguirlas de una extraccion
-- automatica y no volver a ofrecerlas para revision.
alter table digemid_norma_paginas
  add column if not exists revisado_manual boolean not null default false,
  add column if not exists revisado_en timestamptz;

create index if not exists digemid_norma_paginas_revisado_manual_idx
  on digemid_norma_paginas (revisado_manual)
  where revisado_manual = false;
