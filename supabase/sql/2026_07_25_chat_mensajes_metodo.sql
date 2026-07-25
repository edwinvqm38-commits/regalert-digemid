-- Guarda que metodo de Telegram genero cada mensaje registrado (sendMessage,
-- sendAudio, sendDocument, sendPhoto), para poder borrar selectivamente los
-- audios anteriores del bot antes de mandar uno nuevo: Telegram encadena la
-- reproduccion de audio con el audio anterior que quede en el chat, y no hay
-- forma de desactivar eso desde el Bot API.
alter table digemid_chat_mensajes
  add column if not exists metodo text;
