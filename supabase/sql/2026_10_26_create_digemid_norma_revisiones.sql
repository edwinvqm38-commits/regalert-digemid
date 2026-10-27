-- Historial append-only de revisiones humanas de páginas normativas.
-- Conserva el OCR original en digemid_norma_paginas.text_raw y registra
-- cada corrección aplicada a text_normalized con trazabilidad suficiente.
create table if not exists public.digemid_norma_revisiones (
  id uuid primary key default gen_random_uuid(),
  norma_id uuid not null references public.digemid_normas(id) on delete cascade,
  pagina_id uuid not null references public.digemid_norma_paginas(id) on delete cascade,
  page_number integer not null check (page_number > 0),
  source_asset_id bigint null references public.digemid_norma_assets(id) on delete set null,
  tipo_revision text not null check (tipo_revision in ('contenido','contenido_completo','tabla')),
  texto_raw_original text null,
  texto_normalizado_anterior text null,
  texto_revisado text not null,
  revisor_tipo text not null default 'telegram_admin',
  revisor_id text null,
  notas text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_digemid_norma_revisiones_norma_pagina
  on public.digemid_norma_revisiones (norma_id, page_number, created_at desc);

create index if not exists idx_digemid_norma_revisiones_pagina
  on public.digemid_norma_revisiones (pagina_id, created_at desc);

comment on table public.digemid_norma_revisiones is
  'Historial append-only de revisiones humanas; nunca reemplaza el OCR original de digemid_norma_paginas.text_raw.';
