-- Complementa quality_score (heuristica de forma del texto) con la
-- confianza real de Tesseract por palabra cuando se usa OCR, y agrega una
-- señal de "posible formula/notacion tecnica" para marcar paginas que
-- requieren revision humana antes de usarse en consultas legales, ya que
-- ni el texto plano ni el OCR reconstruyen formulas de forma confiable.
alter table digemid_norma_paginas
  add column if not exists ocr_confidence real,
  add column if not exists posible_formula boolean not null default false;

create index if not exists digemid_norma_paginas_ocr_confidence_idx
  on digemid_norma_paginas (ocr_confidence)
  where ocr_confidence is not null;

create index if not exists digemid_norma_paginas_revision_idx
  on digemid_norma_paginas (posible_formula, quality_score)
  where posible_formula = true or quality_score < 0.5;
