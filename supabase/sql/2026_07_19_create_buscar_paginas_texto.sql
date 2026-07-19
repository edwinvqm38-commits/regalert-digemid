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
    'sera','será','seria','sería','fueron','paso','pasó','del','las','los'
  ];
  palabras text[];
  filtradas text[];
  tsq_text text;
begin
  palabras := regexp_split_to_array(lower(query_texto), '[^a-záéíóúñü0-9]+');

  select array_agg(distinct w) into filtradas
  from unnest(palabras) as w
  where length(w) >= 3 and not (w = any(stopwords));

  if filtradas is null or array_length(filtradas, 1) = 0 then
    return;
  end if;

  tsq_text := array_to_string(filtradas, ' | ');

  return query
  select
    p.text_content,
    p.page_number,
    d.document_key,
    d.title,
    d.published_date,
    d.detail_url
  from digemid_documento_paginas p
  join digemid_documentos d on d.id = p.document_id
  where p.text_content_tsv @@ to_tsquery('spanish', tsq_text)
  order by ts_rank(p.text_content_tsv, to_tsquery('spanish', tsq_text)) desc
  limit limite;
end;
$$;
