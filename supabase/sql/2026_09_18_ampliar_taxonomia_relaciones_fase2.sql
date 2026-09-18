-- FASE 2 (inteligencia normativa): 5 tipos de relacion nuevos, todos
-- deliberadamente SIN efecto automatico sobre estado_vigencia (ver el mismo
-- criterio en scripts/detectar_derogaciones_normativa.py::TIPOS_RELACION_SIN_EFECTO_EN_VIGENCIA
-- y en supabase/functions/telegram-bot/index.ts::ESTADO_VIGENCIA_POR_RELACION):
--
-- - complementa / reglamenta / aclara: relaciones descriptivas por diseño.
--   Reglamentar una ley, o complementarla, NUNCA la deroga ni la modifica
--   automaticamente -son instrumentos jerarquicamente distintos-, y una
--   "aclaracion" que sí cambia una regla debe reportarse como "modifica",
--   no reclasificarse aqui.
--
-- - anula_acto_administrativo / anula_disposicion_normativa: se separan
--   porque "declarar la nulidad" de un acto administrativo puntual (ej. una
--   resolucion que otorgo un registro sanitario a un administrado) no es lo
--   mismo que anular una disposicion de alcance general. NINGUNA de las dos
--   cambia estado_vigencia sola -a diferencia de "deroga"/"deja_sin_efecto"-
--   porque una nulidad normativa suele venir de un fuero distinto (control
--   de constitucionalidad, contencioso-administrativo) y amerita la misma
--   cautela que "pendiente_verificacion".
alter table digemid_norma_relaciones
  drop constraint if exists digemid_norma_relaciones_tipo_relacion_check;

alter table digemid_norma_relaciones
  add constraint digemid_norma_relaciones_tipo_relacion_check
  check (tipo_relacion in (
    'deroga', 'deja_sin_efecto', 'modifica', 'sustituye', 'incorpora',
    'exonera', 'suspende', 'prorroga', 'pendiente_verificacion',
    'complementa', 'reglamenta', 'aclara',
    'anula_acto_administrativo', 'anula_disposicion_normativa'
  ));
