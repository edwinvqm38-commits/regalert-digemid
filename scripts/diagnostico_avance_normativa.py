"""Diagnostico temporal: por que 'procesadas en esta corrida' a veces sale 0
o el contador de 'con texto extraido' no sube aunque se reporten normas
procesadas. Revisa cuantas normas quedaron en process_status de error, y si
alguna se esta reprocesando en cada corrida sin nunca completar."""

import os
from collections import Counter

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

normas = supabase.table("digemid_normas").select(
    "id, document_key, pdf_url, process_status, updated_at"
).not_.is_("pdf_url", "null").neq("pdf_url", "").execute().data or []

print("Total normas con pdf_url:", len(normas))

estados = Counter(n.get("process_status") for n in normas)
print("Por process_status:", dict(estados))

con_pdf_ids = [n["id"] for n in normas]
paginas = supabase.table("digemid_norma_paginas").select("norma_id").in_("norma_id", con_pdf_ids).execute().data or []
con_paginas = {p["norma_id"] for p in paginas}

sin_paginas = [n for n in normas if n["id"] not in con_paginas]
print("Con pdf_url pero SIN ninguna pagina (candidatas a quedar pendientes):", len(sin_paginas))

for n in sin_paginas[:15]:
    print(" -", n["document_key"], "| status:", n.get("process_status"), "| updated_at:", n.get("updated_at"))
