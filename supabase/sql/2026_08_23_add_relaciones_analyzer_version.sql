-- Versionado del analizador de relaciones normativas (hallazgo H-04).
--
-- Antes, `derogacion_analizada` era un booleano sin memoria de COMO se analizo:
-- una vez en true, la norma quedaba fuera de la cola para siempre, aunque el
-- motor mejorara. Eso dejo 168 normas (57,5% del corpus) congeladas con el
-- resultado de una version que amputaba la parte dispositiva del documento.
--
-- Con estas columnas, el detector reencola automaticamente toda norma cuya
-- version almacenada sea menor que ANALYZER_VERSION.
alter table digemid_normas
  add column if not exists relaciones_analyzer_version integer,
  add column if not exists relaciones_analizadas_en timestamptz;

-- Las ya analizadas quedan como v1 (la version que truncaba por prefijo), de
-- modo que el detector -hoy en v2- las vuelva a tomar sin borrar su historial
-- ni tocar las relaciones ya registradas.
update digemid_normas
   set relaciones_analyzer_version = 1
 where derogacion_analizada = true
   and relaciones_analyzer_version is null;

create index if not exists idx_digemid_normas_analyzer_version
  on digemid_normas (relaciones_analyzer_version)
  where derogacion_analizada = true;
