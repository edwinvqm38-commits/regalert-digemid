-- Historial de saldo de DeepSeek para poder proyectar cuantos dias de
-- credito quedan (no solo alertar cuando ya se acabo) y avisar con
-- anticipacion antes de dejar a los usuarios sin consultas de IA.
create table if not exists public.deepseek_balance_historial (
  id uuid primary key default gen_random_uuid(),
  checked_at timestamptz not null default now(),
  balance_usd numeric,
  is_available boolean,
  raw jsonb not null default '{}'::jsonb
);

create index if not exists deepseek_balance_historial_checked_at_idx
  on public.deepseek_balance_historial (checked_at desc);
