-- Señal heuristica de "esta pagina tiene una imagen embebida de tamaño
-- razonable" (posible grafico/diagrama), analoga a posible_formula: ni el
-- texto plano ni el OCR interpretan graficos, asi que se marca para
-- revision humana en vez de fingir que no hay nada ahi. Solo se calcula
-- para paginas procesadas de aqui en adelante (agents/pdf_extract.py); las
-- ya existentes quedan en false hasta que se reprocesen.
alter table digemid_norma_paginas
  add column if not exists posible_grafico boolean not null default false;

create index if not exists digemid_norma_paginas_posible_grafico_idx
  on digemid_norma_paginas (posible_grafico)
  where posible_grafico = true;

-- Resumen por norma para el reporte maestro de Telegram (/reportenormas):
-- una fila por norma con todas las señales de fidelidad agregadas, para no
-- tener que traer las ~3800 paginas al bot y agrupar en JS.
create or replace view digemid_normas_resumen as
select
  n.id,
  n.document_key,
  n.titulo,
  n.anio,
  n.pdf_url,
  n.file_storage_path,
  n.process_status,
  count(np.id) as total_paginas,
  count(*) filter (where np.quality_score >= 0.85) as calidad_alta,
  count(*) filter (where np.quality_score >= 0.5 and np.quality_score < 0.85) as calidad_media,
  count(*) filter (where np.quality_score < 0.5) as calidad_baja,
  count(*) filter (where np.quality_score < 0.5 and np.revisado_manual = false) as calidad_baja_sin_revisar,
  count(*) filter (where np.has_tables) as tablas_total,
  count(*) filter (where np.has_tables and np.tabla_verificada = false) as tablas_sin_verificar,
  count(*) filter (where np.posible_formula) as formulas,
  count(*) filter (where np.posible_grafico) as graficos,
  count(*) filter (where np.revisado_manual) as revisadas_manual,
  count(*) filter (where np.extraction_method ilike '%ocr%') as via_ocr
from digemid_normas n
left join digemid_norma_paginas np on np.norma_id = n.id
group by n.id, n.document_key, n.titulo, n.anio, n.pdf_url, n.file_storage_path, n.process_status;

comment on view digemid_normas_resumen is
'Una fila por norma con las señales de fidelidad ya agregadas (calidad, tablas, formulas, graficos, OCR), usada por /reportenormas en el bot de Telegram para el reporte maestro de todas las normas.';
