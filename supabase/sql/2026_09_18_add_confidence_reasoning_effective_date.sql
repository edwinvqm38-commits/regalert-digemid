-- FASE 1 (inteligencia normativa): confianza y razonamiento de la IA por
-- relacion, y fecha de vigencia -SOLO cuando el texto la declara de forma
-- explicita-. Hasta ahora la unica "confianza" guardada era la de IDENTIDAD
-- (que norma es la afectada, identidad_confianza), nunca la de si la
-- relacion juridica en si es correcta.
--
-- Las 3 columnas son nullable y no tienen valor por defecto: para las 148
-- relaciones ya existentes quedan en NULL (no se reinterpreta retroactivamente
-- sin volver a pasar por el modelo con ANALYZER_VERSION nuevo).
alter table digemid_norma_relaciones
  add column if not exists confidence_score numeric check (confidence_score >= 0 and confidence_score <= 1),
  add column if not exists ai_reasoning text,
  add column if not exists effective_date date;

comment on column digemid_norma_relaciones.confidence_score is
  'Confianza (0-1) de que la RELACION JURIDICA (tipo_relacion) es correcta, distinta de identidad_confianza (que es sobre a que norma exacta se vincula). NULL si el modelo no la reporto: nunca se inventa un numero.';

comment on column digemid_norma_relaciones.ai_reasoning is
  'Razonamiento breve del modelo sobre por que clasifico asi la relacion. Contexto para el humano en /derogacionespendientes, no una fuente de verdad.';

comment on column digemid_norma_relaciones.effective_date is
  'Fecha de entrada en vigencia SOLO si el texto la declara explicitamente (fecha absoluta, no un plazo relativo como "a los 90 dias"). NULL en cualquier otro caso, incluida la duda: nunca se infiere ni se calcula a partir de la fecha de publicacion.';
