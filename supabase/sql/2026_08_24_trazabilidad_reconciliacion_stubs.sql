-- TRAZABILIDAD DE RECONCILIACION DE STUBS (H-08).
--
-- ⚠️ ESTE ARCHIVO NO ESTA APLICADO EN PRODUCCION. Es el diseño que acompaña al
-- DRY-RUN; se aplicara junto con la primera ejecucion autorizada de
-- `scripts/reconciliar_stubs_normativos.py --apply`.
--
-- Principio: no se borra nada. Un stub reconciliado NO se elimina; queda como
-- alias historico apuntando a la norma real, y cada relacion re-apuntada
-- conserva a que registro apuntaba antes. Debe poder responderse siempre:
-- que apuntaba a que, cuando se corrigio y por que.

-- 1) El stub deja de ser una norma "propia" y pasa a ser alias de la real.
ALTER TABLE digemid_normas
    ADD COLUMN IF NOT EXISTS reconciliado_con_id uuid REFERENCES digemid_normas(id),
    ADD COLUMN IF NOT EXISTS reconciliado_en timestamptz,
    ADD COLUMN IF NOT EXISTS reconciliado_por text,
    ADD COLUMN IF NOT EXISTS reconciliador_version integer,
    ADD COLUMN IF NOT EXISTS motivo_reconciliacion text;

COMMENT ON COLUMN digemid_normas.reconciliado_con_id IS
    'Si no es NULL, este registro es un ALIAS historico de esa norma real. No se borra: se conserva para poder auditar a que apuntaba antes cada relacion.';

-- 2) La relacion conserva su destino anterior.
ALTER TABLE digemid_norma_relaciones
    ADD COLUMN IF NOT EXISTS norma_afectada_id_anterior uuid,
    ADD COLUMN IF NOT EXISTS reconciliado_en timestamptz,
    ADD COLUMN IF NOT EXISTS reconciliado_por text,
    ADD COLUMN IF NOT EXISTS reconciliador_version integer,
    ADD COLUMN IF NOT EXISTS motivo_reconciliacion text;

COMMENT ON COLUMN digemid_norma_relaciones.norma_afectada_id_anterior IS
    'Destino que tenia antes de la reconciliacion (normalmente un stub duplicado). Permite revertir con un UPDATE.';

-- 3) Un stub reconciliado no puede volver a ser destino de una relacion nueva.
--    (No se fuerza sobre lo historico: es un indice de consulta, no un CHECK.)
CREATE INDEX IF NOT EXISTS digemid_normas_reconciliado_con_idx
    ON digemid_normas (reconciliado_con_id)
    WHERE reconciliado_con_id IS NOT NULL;

-- ROLLBACK de una reconciliacion concreta (no destructivo):
--
--   UPDATE digemid_norma_relaciones
--      SET norma_afectada_id = norma_afectada_id_anterior,
--          norma_afectada_id_anterior = NULL,
--          reconciliado_en = NULL, reconciliado_por = NULL,
--          reconciliador_version = NULL, motivo_reconciliacion = NULL
--    WHERE id = '<relacion_id>';
--
--   UPDATE digemid_normas
--      SET reconciliado_con_id = NULL, reconciliado_en = NULL,
--          reconciliado_por = NULL, reconciliador_version = NULL,
--          motivo_reconciliacion = NULL
--    WHERE id = '<stub_id>';
--
-- Como el stub nunca se borra y su vigencia nunca se copia a la norma real,
-- revertir devuelve exactamente el estado anterior.
