-- El "fragmento" que reporta la IA se le pide como cita textual exacta del
-- documento (no un resumen), pero nada garantiza que lo cumpla al 100%. Este
-- campo guarda si scripts/detectar_derogaciones_normativa.py pudo confirmar
-- que ese fragmento realmente aparece en el texto extraido de la norma
-- origen; si no, el bot avisa al admin que la cita no quedo verificada
-- textualmente (aunque la relacion en si pueda seguir siendo correcta).
alter table digemid_norma_relaciones
  add column if not exists fragmento_verificado boolean not null default false;
