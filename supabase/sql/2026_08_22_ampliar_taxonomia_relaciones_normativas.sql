-- El detector solo distinguia deroga/modifica/deja_sin_efecto, lo que lo
-- forzaba a encajar relaciones que no son ninguna de esas tres (ej. "se
-- exonera de la aplicacion de los articulos 10 y 11 de la Ley 29459" se
-- clasifico como "modifica" cuando en realidad es una exoneracion/excepcion
-- de aplicacion, no una modificacion textual). "Mencion de una norma !=
-- modificacion de una norma": se amplia la taxonomia al verbo juridico
-- exacto, y se agrega un tipo "pendiente_verificacion" para cuando el texto
-- no permite determinar el efecto con certeza (en vez de inventar
-- deroga/modifica por defecto).
alter table digemid_norma_relaciones
  drop constraint if exists digemid_norma_relaciones_tipo_relacion_check;

alter table digemid_norma_relaciones
  add constraint digemid_norma_relaciones_tipo_relacion_check
  check (tipo_relacion in (
    'deroga', 'deja_sin_efecto', 'modifica', 'sustituye', 'incorpora',
    'exonera', 'suspende', 'prorroga', 'pendiente_verificacion'
  ));

-- Articulos/numerales/anexos afectados y si la afectacion es total o parcial
-- de la norma citada (antes no se distinguia).
alter table digemid_norma_relaciones
  add column if not exists articulos_afectados text,
  add column if not exists alcance text check (alcance in ('total', 'parcial'));

-- "suspendida" es un estado real distinto de "derogada"/"modificada": la
-- norma sigue vigente pero temporalmente inaplicable.
alter table digemid_normas
  drop constraint if exists digemid_normas_estado_vigencia_check;

alter table digemid_normas
  add constraint digemid_normas_estado_vigencia_check
  check (estado_vigencia in ('vigente', 'derogada', 'derogada_parcialmente', 'modificada', 'suspendida'));
