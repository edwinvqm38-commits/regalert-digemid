-- Permite marcar una norma como derogada/modificada por otra y que /consulta
-- avise de ello en vez de citarla como vigente sin mas. La deteccion de estas
-- relaciones corre automaticamente en el pipeline de extraccion (analiza el
-- texto ya extraido con IA, scripts/detectar_derogaciones_normativa.py) y
-- queda "pendiente" hasta que un admin la confirme con los botones que manda
-- el bot -- no se aplica sola porque un error de la IA aqui significa citar
-- mal una norma legal.

alter table digemid_normas
  add column if not exists estado_vigencia text not null default 'vigente'
    check (estado_vigencia in ('vigente', 'derogada', 'derogada_parcialmente', 'modificada')),
  add column if not exists derogacion_analizada boolean not null default false;

create table if not exists digemid_norma_relaciones (
  id uuid primary key default gen_random_uuid(),
  norma_origen_id uuid not null references digemid_normas(id) on delete cascade,
  norma_origen_document_key text not null,
  tipo_relacion text not null check (tipo_relacion in ('deroga', 'modifica', 'deja_sin_efecto')),
  norma_afectada_id uuid references digemid_normas(id),
  tipo_norma_afectada text,
  numero_afectada text,
  anio_afectada integer,
  descripcion_afectada text not null,
  fragmento_fuente text,
  estado text not null default 'pendiente' check (estado in ('pendiente', 'confirmada', 'rechazada')),
  resuelto_por text,
  resuelto_en timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_digemid_norma_relaciones_estado
  on digemid_norma_relaciones(estado);
