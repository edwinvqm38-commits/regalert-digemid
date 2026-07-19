create extension if not exists pg_trgm;

create or replace function sugerir_alertas_similares(query_texto text, limite int default 3)
returns table (
  document_key text,
  title text,
  published_date date,
  detail_url text,
  similitud real
)
language sql stable
as $$
  select
    d.document_key,
    d.title,
    d.published_date,
    d.detail_url,
    max(word_similarity(query_texto, p.text_content)) as similitud
  from digemid_documento_paginas p
  join digemid_documentos d on d.id = p.document_id
  where d.source_type = 'alerta'
  group by d.document_key, d.title, d.published_date, d.detail_url
  having max(word_similarity(query_texto, p.text_content)) > 0.15
  order by similitud desc
  limit limite;
$$;
