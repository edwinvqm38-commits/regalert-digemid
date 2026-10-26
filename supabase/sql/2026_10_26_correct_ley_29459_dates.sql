-- Corrección auditable de metadata confirmada en la ficha oficial del
-- Congreso: promulgación 25/11/2009 y publicación 26/11/2009.
-- No modifica identidad, URLs, estado de vigencia ni contenido documental.
with correction as (
  select jsonb_build_object(
    'reason', 'Fechas de promulgación y publicación corregidas desde fuente oficial',
    'source_url', 'https://www.gob.pe/institucion/congreso-de-la-republica/normas-legales/2813441-29459',
    'fecha_promulgacion', '2009-11-25',
    'fecha_publicacion', '2009-11-26'
  ) as audit
)
update public.digemid_normas as norma
set
  fecha_promulgacion = date '2009-11-25',
  fecha_publicacion = date '2009-11-26',
  raw = jsonb_set(
    coalesce(norma.raw, '{}'::jsonb),
    '{metadata_corrections}',
    coalesce(norma.raw -> 'metadata_corrections', '{}'::jsonb) ||
      jsonb_build_object('ley_29459_dates', correction.audit),
    true
  ),
  updated_at = now()
from correction
where norma.document_key = 'LEY-29459'
  and (
    norma.fecha_promulgacion is distinct from date '2009-11-25'
    or norma.fecha_publicacion is distinct from date '2009-11-26'
    or norma.raw #> '{metadata_corrections,ley_29459_dates}'
      is distinct from correction.audit
  );
