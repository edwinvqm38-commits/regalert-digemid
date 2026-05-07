alter table if exists public.digemid_norma_paginas
  add column if not exists source_asset_id bigint null;

alter table if exists public.digemid_norma_paginas
  add column if not exists asset_subtipo text null;

alter table if exists public.digemid_norma_paginas
  add column if not exists document_part text null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'digemid_norma_paginas_source_asset_id_fkey'
      and conrelid = 'public.digemid_norma_paginas'::regclass
  ) then
    alter table public.digemid_norma_paginas
      add constraint digemid_norma_paginas_source_asset_id_fkey
      foreign key (source_asset_id)
      references public.digemid_norma_assets(id)
      on delete set null;
  end if;
end $$;

alter table if exists public.digemid_norma_paginas
  drop constraint if exists digemid_norma_paginas_norma_page_uidx;

drop index if exists public.digemid_norma_paginas_norma_page_idx;

create unique index if not exists digemid_norma_paginas_norma_asset_page_uidx
  on public.digemid_norma_paginas (norma_id, source_asset_id, page_number)
  where source_asset_id is not null;

create unique index if not exists digemid_norma_paginas_norma_part_page_uidx
  on public.digemid_norma_paginas (norma_id, coalesce(document_part, ''), page_number)
  where source_asset_id is null;

create index if not exists digemid_norma_paginas_source_asset_id_idx
  on public.digemid_norma_paginas (source_asset_id);

create index if not exists digemid_norma_paginas_asset_subtipo_idx
  on public.digemid_norma_paginas (asset_subtipo);

create index if not exists digemid_norma_paginas_document_part_idx
  on public.digemid_norma_paginas (document_part);

create index if not exists digemid_norma_paginas_norma_page_idx
  on public.digemid_norma_paginas (norma_id, page_number);

comment on column public.digemid_norma_paginas.source_asset_id is
'Referencia opcional al asset PDF origen en digemid_norma_assets. Permite que una misma norma tenga multiples fuentes PDF con numeracion de paginas independiente.';

comment on column public.digemid_norma_paginas.asset_subtipo is
'Subtipo del asset PDF fuente (por ejemplo resolucion_ministerial, documento_tecnico_anexo) para trazabilidad de paginas por parte documental.';

comment on column public.digemid_norma_paginas.document_part is
'Identificador textual de parte documental dentro de la norma cuando no exista source_asset_id. Permite unicidad por pagina en normas con multiples PDFs oficiales.';
