-- IDENTIDAD NORMATIVA (H-05 / H-06 / H-07).
--
-- Migracion ESTRUCTURAL y ADITIVA: solo agrega columnas nullable y un indice
-- unico PARCIAL. No toca ninguna fila existente, ningun estado_vigencia,
-- ningun stub y ninguna relacion confirmada o rechazada.
--
-- clave_dedupe queda NULL en todo lo historico a proposito: rellenarla seria
-- escribir sobre relaciones ya confirmadas, cosa no autorizada en esta fase.
-- El indice es parcial (WHERE clave_dedupe IS NOT NULL), asi que convive con
-- ese NULL historico y solo impone unicidad sobre lo que escriba el detector
-- desde ahora. La deduplicacion contra lo historico la hace el detector en
-- memoria, recomputando la clave de las filas antiguas sin escribirlas.

ALTER TABLE digemid_norma_relaciones
    ADD COLUMN IF NOT EXISTS clave_dedupe text,
    ADD COLUMN IF NOT EXISTS identidad_nivel text,
    ADD COLUMN IF NOT EXISTS identidad_confianza text,
    ADD COLUMN IF NOT EXISTS identidad_candidatas text;

COMMENT ON COLUMN digemid_norma_relaciones.clave_dedupe IS
    'Clave estable (origen::tipo_relacion::identidad_afectada::articulos). No incluye el fragmento: la evidencia no es identidad.';
COMMENT ON COLUMN digemid_norma_relaciones.identidad_nivel IS
    'Como se resolvio la norma afectada: RESUELTA_EXACTA, RESUELTA_TIPO_NUMERO_ANIO, RESUELTA_TIPO_NUMERO, RESUELTA_NUMERO_ANIO, IDENTIDAD_AMBIGUA, NORMA_NO_ENCONTRADA, DATOS_INSUFICIENTES.';
COMMENT ON COLUMN digemid_norma_relaciones.identidad_candidatas IS
    'Cuando el nivel es IDENTIDAD_AMBIGUA: document_key de todas las candidatas, para que decida un humano. Nunca se elige la primera.';

CREATE UNIQUE INDEX IF NOT EXISTS digemid_norma_relaciones_clave_dedupe_uidx
    ON digemid_norma_relaciones (clave_dedupe)
    WHERE clave_dedupe IS NOT NULL;
