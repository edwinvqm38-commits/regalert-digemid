-- Registra los mensajes que el bot envia a cada chat de Telegram, para poder
-- borrarlos con /limpiar (deleteMessage solo puede referenciar un message_id
-- conocido; Telegram no expone un "borrar todo el historial" via Bot API).
-- Solo se registran los mensajes que MANDA el bot, no los que escribe el
-- usuario: /limpiar limpia el "ruido" de respuestas del bot, no el chat
-- completo.

create table if not exists digemid_chat_mensajes (
  id bigint generated always as identity primary key,
  chat_id text not null,
  message_id bigint not null,
  created_at timestamptz not null default now(),
  unique (chat_id, message_id)
);

create index if not exists digemid_chat_mensajes_chat_id_idx
  on digemid_chat_mensajes (chat_id);
