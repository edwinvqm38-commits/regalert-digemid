-- Habilita busqueda de texto en paginas de normativa (antes solo alertas).

alter table digemid_norma_paginas
  add column if not exists text_search_tsv tsvector
  generated always as (
    to_tsvector('spanish', coalesce(text_normalized, text_raw, ''))
  ) stored;

create index if not exists digemid_norma_paginas_tsv_idx
  on digemid_norma_paginas using gin (text_search_tsv);

-- Buscador unificado: alertas + normas, con la MISMA firma de columnas,
-- para que el edge function no cambie. document_key/title ya distinguen
-- si es una alerta (ej. "51-2026") o una norma (ej. "RM-182-2025").
create or replace function buscar_paginas_texto(query_texto text, limite int default 4)
returns table (
  text_content text,
  page_number int,
  document_key text,
  title text,
  published_date date,
  detail_url text
)
language plpgsql stable
as $$
declare
  stopwords text[] := array[
    'que','qué','de','del','la','el','los','las','un','una','unos','unas',
    'y','o','en','con','por','para','se','es','fue','son','esta','está','este','estos','estas',
    'sobre','hay','hubo','ha','han','sido','cual','cuál','cuales','cuáles','quien','quién','quienes','quiénes',
    'como','cómo','donde','dónde','cuando','cuándo','porque','por qué','a','al','su','sus','le','les','lo',
    'me','mi','tu','ya','muy','mas','más','pero','si','sí','no','sin','entre','hasta','desde',
    'sera','será','seria','sería','fueron','paso','pasó'
  ];
  palabras text[];
  filtradas text[];
  tsq_text text;
  tsq tsquery;
begin
  palabras := regexp_split_to_array(lower(query_texto), '[^a-záéíóúñü0-9]+');

  select array_agg(distinct w) into filtradas
  from unnest(palabras) as w
  where length(w) >= 3 and not (w = any(stopwords));

  if filtradas is null or array_length(filtradas, 1) = 0 then
    return;
  end if;

  tsq_text := array_to_string(filtradas, ' | ');
  tsq := to_tsquery('spanish', tsq_text);

  return query
  with resultados as (
    select
      p.text_content,
      p.page_number,
      d.document_key,
      d.title,
      d.published_date,
      d.detail_url,
      ts_rank(p.text_content_tsv, tsq) as rango
    from digemid_documento_paginas p
    join digemid_documentos d on d.id = p.document_id
    where p.text_content_tsv @@ tsq

    union all

    select
      coalesce(np.text_normalized, np.text_raw) as text_content,
      np.page_number,
      n.document_key,
      n.titulo as title,
      n.fecha_publicacion as published_date,
      coalesce(n.source_url, n.pdf_url) as detail_url,
      ts_rank(np.text_search_tsv, tsq) as rango
    from digemid_norma_paginas np
    join digemid_normas n on n.id = np.norma_id
    where np.text_search_tsv @@ tsq
  )
  select
    resultados.text_content,
    resultados.page_number,
    resultados.document_key,
    resultados.title,
    resultados.published_date,
    resultados.detail_url
  from resultados
  order by resultados.rango desc
  limit limite;
end;
$$;
