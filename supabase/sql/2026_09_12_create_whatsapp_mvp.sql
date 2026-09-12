-- Canal WhatsApp Business (Cloud API), MVP inbound-only.
--
-- Por que tablas propias y NO reutilizar digemid_bot_usuarios /
-- digemid_suscripciones:
--
-- agents/agent_notify.py (_usuarios_elegibles_dm) y
-- scripts/enviar_recordatorio_planes.py toman TODOS los telegram_chat_id de
-- esas dos tablas y les envian mensajes por la API de Telegram, sin filtrar
-- por canal. Un usuario de WhatsApp registrado ahi recibiria intentos de
-- envio proactivo (que ademas fallarian en Telegram), rompiendo la regla
-- inbound-only del canal y gastando conversaciones de pago en Meta.
--
-- Con tablas separadas el aislamiento es estructural: ningun proceso
-- proactivo existente conoce estas filas, asi que es imposible que dispare
-- un mensaje de WhatsApp aunque alguien lo intente por error.
--
-- El log de consultas SI se comparte (digemid_bot_consultas, con
-- telegram_chat_id = 'wa:<wa_id>'): esa tabla es solo de registro y conteo
-- de limites, nadie envia mensajes a partir de ella.

create table if not exists digemid_whatsapp_usuarios (
  id uuid primary key default gen_random_uuid(),
  -- wa_id: identificador del usuario en WhatsApp (su numero en formato
  -- internacional sin "+"), tal como lo entrega Meta en el webhook.
  wa_id text not null unique,
  telefono text,
  nombre_perfil text,
  nivel text not null default 'gratis'
    check (nivel in ('gratis', 'basico', 'consultoria', 'empresarial')),
  estado text not null default 'activo'
    check (estado in ('activo', 'bloqueado')),
  mensajes_recibidos integer not null default 0,
  created_at timestamptz not null default now(),
  last_seen_at timestamptz
);

create index if not exists idx_digemid_whatsapp_usuarios_wa_id
  on digemid_whatsapp_usuarios (wa_id);

-- Idempotencia: Meta reintenta la entrega de un webhook si no recibe 200 a
-- tiempo, y el mismo message_id puede llegar varias veces. El unique sobre
-- message_id convierte "ya procesado" en un error de insercion detectable,
-- sin necesidad de una lectura previa (que tendria condicion de carrera).
create table if not exists digemid_whatsapp_mensajes_procesados (
  id uuid primary key default gen_random_uuid(),
  message_id text not null unique,
  wa_id text not null,
  received_at timestamptz not null default now(),
  processed_at timestamptz,
  status text not null default 'recibido'
    check (status in ('recibido', 'procesado', 'error', 'ignorado')),
  comando text,
  error_detalle text
);

create index if not exists idx_digemid_whatsapp_mensajes_wa_id
  on digemid_whatsapp_mensajes_procesados (wa_id, received_at desc);
