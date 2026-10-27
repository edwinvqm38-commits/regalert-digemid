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

-- Impide editar o borrar el historial una vez creado.
create or replace function public.block_digemid_norma_revisiones_mutation()
returns trigger
language plpgsql
as $$
begin
  raise exception 'digemid_norma_revisiones es append-only';
end;
$$;

drop trigger if exists trg_block_digemid_norma_revisiones_mutation
  on public.digemid_norma_revisiones;

create trigger trg_block_digemid_norma_revisiones_mutation
before update or delete on public.digemid_norma_revisiones
for each row execute function public.block_digemid_norma_revisiones_mutation();

-- Aplica la revisión de forma atómica: guarda historial y actualiza únicamente
-- el texto normalizado/estado de revisión. text_raw nunca se modifica.
create or replace function public.apply_digemid_norma_revision(
  p_pagina_id uuid,
  p_texto_revisado text,
  p_tipo_revision text,
  p_revisor_id text default null,
  p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_page public.digemid_norma_paginas%rowtype;
  v_revision_id uuid;
  v_now timestamptz := now();
begin
  if p_tipo_revision not in ('contenido','contenido_completo','tabla') then
    raise exception 'tipo_revision inválido: %', p_tipo_revision;
  end if;

  if nullif(btrim(p_texto_revisado), '') is null then
    raise exception 'texto revisado vacío';
  end if;

  select *
  into v_page
  from public.digemid_norma_paginas
  where id = p_pagina_id
  for update;

  if not found then
    raise exception 'página normativa no encontrada: %', p_pagina_id;
  end if;

  insert into public.digemid_norma_revisiones (
    norma_id,
    pagina_id,
    page_number,
    source_asset_id,
    tipo_revision,
    texto_raw_original,
    texto_normalizado_anterior,
    texto_revisado,
    revisor_tipo,
    revisor_id,
    metadata,
    created_at
  )
  values (
    v_page.norma_id,
    v_page.id,
    v_page.page_number,
    v_page.source_asset_id,
    p_tipo_revision,
    v_page.text_raw,
    v_page.text_normalized,
    p_texto_revisado,
    'telegram_admin',
    p_revisor_id,
    coalesce(p_metadata, '{}'::jsonb),
    v_now
  )
  returning id into v_revision_id;

  update public.digemid_norma_paginas
  set
    text_normalized = p_texto_revisado,
    quality_score = 1,
    extraction_method = 'revision_manual',
    revisado_manual = true,
    revisado_en = v_now,
    tabla_verificada = case
      when p_tipo_revision = 'tabla' then true
      else tabla_verificada
    end,
    tabla_verificada_en = case
      when p_tipo_revision = 'tabla' then v_now
      else tabla_verificada_en
    end,
    updated_at = v_now
  where id = v_page.id;

  return v_revision_id;
end;
$$;

revoke all on function public.apply_digemid_norma_revision(uuid,text,text,text,jsonb)
  from public, anon, authenticated;
grant execute on function public.apply_digemid_norma_revision(uuid,text,text,text,jsonb)
  to service_role;
