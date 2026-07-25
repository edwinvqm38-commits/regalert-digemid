-- RPC para traer el texto completo de un documento (alerta o norma) por su
-- document_key exacto, sin pasar por busqueda de texto/relevancia. Se usa
-- para generar un resumen con IA de "la ultima alerta"/"la norma de esta
-- semana" etc.: esas preguntas ya se resuelven con las mismas consultas
-- ordenadas/contadas que /ultimas, /hoy, /semana y /mes (ver
-- ambitoTemporalAlertas en el edge function), pero antes se quedaban solo
-- en el listado crudo porque buscar_paginas_texto no puede ubicar un
-- documento especifico por su clave.
create or replace function paginas_por_document_key(p_document_key text, p_limite int default 6)
returns table (
  text_content text,
  page_number int,
  document_key text,
  title text,
  published_date date,
  detail_url text
)
language sql stable
as $$
  select p.text_content, p.page_number, d.document_key, d.title, d.published_date, d.detail_url
  from digemid_documento_paginas p
  join digemid_documentos d on d.id = p.document_id
  where d.document_key = p_document_key

  union all

  select coalesce(np.text_normalized, np.text_raw), np.page_number, n.document_key, n.titulo,
    n.fecha_publicacion, coalesce(n.source_url, n.pdf_url)
  from digemid_norma_paginas np
  join digemid_normas n on n.id = np.norma_id
  where n.document_key = p_document_key

  order by page_number
  limit p_limite;
$$;
