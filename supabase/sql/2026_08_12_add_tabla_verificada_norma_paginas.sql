-- Bandera separada de revisado_manual: una pagina puede tener texto de
-- calidad alta (quality_score >= 0.85) y aun asi tener una tabla cuya
-- correspondencia fila-columna nadie confirmo contra el PDF original. Se
-- marca aparte porque el flujo de verificacion (/tablarevisar) y su umbral
-- de "pendiente" son distintos del de calidad de texto (/normarevisar).
alter table digemid_norma_paginas
  add column if not exists tabla_verificada boolean not null default false,
  add column if not exists tabla_verificada_en timestamptz;

create index if not exists digemid_norma_paginas_tabla_pendiente_idx
  on digemid_norma_paginas (has_tables, tabla_verificada)
  where has_tables = true and tabla_verificada = false;
