alter table digemid_bot_usuarios
  add column if not exists plan_gratis_legado boolean not null default false,
  add column if not exists recordatorio_prueba_enviados integer not null default 0,
  add column if not exists ultimo_recordatorio_prueba_at timestamptz;

-- Todo usuario que ya existia antes de este cambio queda exento (gratis
-- para siempre), para no cortarle el servicio de golpe a alguien que ya
-- lo estaba usando bajo las reglas anteriores.
update digemid_bot_usuarios set plan_gratis_legado = true;
