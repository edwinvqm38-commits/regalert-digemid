alter table digemid_suscripciones
  add constraint digemid_suscripciones_chat_id_unique unique (telegram_chat_id);

-- Acelera el conteo de consultas diarias por chat y globales.
create index if not exists digemid_bot_consultas_command_created_idx
  on digemid_bot_consultas (command, created_at);

create index if not exists digemid_bot_consultas_chat_command_created_idx
  on digemid_bot_consultas (telegram_chat_id, command, created_at);
