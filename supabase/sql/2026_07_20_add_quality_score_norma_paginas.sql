alter table digemid_norma_paginas
  add column if not exists quality_score real;

create index if not exists digemid_norma_paginas_quality_idx
  on digemid_norma_paginas (quality_score)
  where quality_score is not null;
