alter table digemid_bot_usuarios
  add column if not exists nombre_confirmado boolean not null default false;
