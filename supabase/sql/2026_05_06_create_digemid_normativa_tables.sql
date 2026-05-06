create extension if not exists pgcrypto;

create table if not exists public.digemid_normas (
  id uuid primary key default gen_random_uuid(),
  document_key text not null unique,
  source_type text not null default 'norma',
  source_section text,
  tipo_norma text,
  numero text,
  anio integer,
  titulo text not null,
  fecha_publicacion date,
  fecha_promulgacion date,
  entidad_emisora text,
  fuente_oficial text,
  source_url text,
  pdf_url text,
  file_name text,
  mime_type text not null default 'application/pdf',
  has_file boolean not null default false,
  drive_file_id text,
  drive_file_url text,
  drive_folder_id text,
  drive_structure jsonb not null default '{}'::jsonb,
  raw jsonb not null default '{}'::jsonb,
  process_status text not null default 'registered',
  ocr_required boolean not null default false,
  has_tables boolean not null default false,
  botica_relevance jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.digemid_norma_paginas (
  id uuid primary key default gen_random_uuid(),
  norma_id uuid not null references public.digemid_normas(id) on delete cascade,
  page_number integer not null,
  text_raw text,
  text_normalized text,
  extraction_method text,
  ocr_used boolean not null default false,
  has_tables boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint digemid_norma_paginas_norma_page_uidx unique (norma_id, page_number)
);

create table if not exists public.digemid_norma_chunks (
  id uuid primary key default gen_random_uuid(),
  norma_id uuid not null references public.digemid_normas(id) on delete cascade,
  page_start integer,
  page_end integer,
  chunk_index integer not null,
  chunk_text text not null,
  chunk_type text not null default 'text',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint digemid_norma_chunks_norma_chunk_uidx unique (norma_id, chunk_index)
);

create index if not exists digemid_normas_document_key_idx
  on public.digemid_normas (document_key);

create index if not exists digemid_normas_tipo_norma_idx
  on public.digemid_normas (tipo_norma);

create index if not exists digemid_normas_anio_idx
  on public.digemid_normas (anio);

create index if not exists digemid_normas_process_status_idx
  on public.digemid_normas (process_status);

create index if not exists digemid_norma_paginas_norma_page_idx
  on public.digemid_norma_paginas (norma_id, page_number);

create index if not exists digemid_norma_chunks_norma_chunk_idx
  on public.digemid_norma_chunks (norma_id, chunk_index);

grant usage on schema public to service_role;
grant all privileges on table public.digemid_normas to service_role;
grant all privileges on table public.digemid_norma_paginas to service_role;
grant all privileges on table public.digemid_norma_chunks to service_role;

comment on table public.digemid_normas is
'Modulo normativo DIGEMID independiente del modulo de alertas.';

comment on table public.digemid_norma_paginas is
'Paginas extraidas de normas DIGEMID, independientes del modulo de alertas.';

comment on table public.digemid_norma_chunks is
'Chunks consultables de normas DIGEMID, independientes del modulo de alertas.';
