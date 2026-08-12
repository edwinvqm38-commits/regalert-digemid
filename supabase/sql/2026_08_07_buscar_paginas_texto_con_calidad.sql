-- Fase 1 de confiabilidad en /consulta: buscar_paginas_texto ahora tambien
-- devuelve las senales de calidad ya calculadas en la extraccion
-- (quality_score, has_tables, posible_formula, ocr_confidence,
-- revisado_manual), que hasta ahora solo se usaban en el flujo de revision
-- manual de admin y nunca llegaban a la IA que responde /consulta. Sin esto,
-- la IA no tenia forma de saber si estaba citando una pagina OCR de baja
-- confianza o una tabla aplanada a texto corrido.

-- create or replace no permite cambiar el tipo de retorno de una funcion
-- existente (nuevas columnas en RETURNS TABLE), asi que se elimina primero.
drop function if exists buscar_paginas_texto(text, integer);

create function buscar_paginas_texto(query_texto text, limite int default 4)
returns table (
  text_content text,
  page_number int,
  document_key text,
  title text,
  published_date date,
  detail_url text,
  quality_score real,
  has_tables boolean,
  posible_formula boolean,
  ocr_confidence real,
  revisado_manual boolean
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
    -- Alertas: no tienen quality_score/posible_formula/revisado_manual
    -- propios (esquema mas viejo, distinto al de normas). Se devuelve lo que
    -- si existe (tablas, confianza OCR) y null/false para el resto en vez de
    -- inventar un valor.
    select
      p.text_content,
      p.page_number,
      d.document_key,
      d.title,
      d.published_date,
      d.detail_url,
      ts_rank(p.text_content_tsv, tsq) as rango,
      null::real as quality_score,
      coalesce(p.contiene_tablas, p.has_table, false) as has_tables,
      false as posible_formula,
      coalesce(p.ocr_confianza, p.ocr_confidence)::real as ocr_confidence,
      false as revisado_manual
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
      ts_rank(np.text_search_tsv, tsq) as rango,
      np.quality_score,
      coalesce(np.has_tables, false) as has_tables,
      coalesce(np.posible_formula, false) as posible_formula,
      np.ocr_confidence,
      coalesce(np.revisado_manual, false) as revisado_manual
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
    resultados.detail_url,
    resultados.quality_score,
    resultados.has_tables,
    resultados.posible_formula,
    resultados.ocr_confidence,
    resultados.revisado_manual
  from resultados
  order by resultados.rango desc
  limit limite;
end;
$$;
