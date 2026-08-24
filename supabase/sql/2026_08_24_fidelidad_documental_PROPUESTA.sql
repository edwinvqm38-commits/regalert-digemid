-- FIDELIDAD Y CUSTODIA DOCUMENTAL (F-01 + F-02) — PROPUESTA.
--
-- ⚠️ NO APLICADA. Se aplica cuando se autorice, junto con la primera
-- reextracción verificada. Es ADITIVA: no modifica ni borra ninguna fila ni
-- ninguna columna existente. text_raw y text_normalized quedan intactos.
--
--     EL PDF OFICIAL ES LA EVIDENCIA.
--     LA TRANSCRIPCIÓN ES UNA REPRESENTACIÓN DE ESA EVIDENCIA.
--
-- Hoy no se puede responder "¿qué PDF exacto produjo esta transcripción?":
-- no se guarda el hash ni el número de páginas del original, y el respaldo en
-- Storage se sube con upsert, así que una descarga distinta pisa la anterior.
-- La auditoría F-02 encontró además tres pares de normas cuya transcripción
-- proviene del MISMO documento -es decir, al menos una de cada par guarda el
-- texto de otra norma-, algo que ninguna heurística de calidad puede ver.

-- ===========================================================================
-- 1) CADENA DE CUSTODIA: versiones documentales INMUTABLES  (F-02 · 2)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS digemid_norma_documentos (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    norma_id uuid NOT NULL REFERENCES digemid_normas(id) ON DELETE CASCADE,

    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    byte_size bigint NOT NULL CHECK (byte_size > 0),
    pdf_page_count integer CHECK (pdf_page_count IS NULL OR pdf_page_count > 0),

    source_url text,
    source_domain text,
    storage_path text,
    -- FUENTE_OFICIAL_VERIFICADA | FUENTE_OFICIAL_DECLARADA_NO_REVALIDADA |
    -- COPIA_LOCAL_SIN_PROCEDENCIA_VERIFICADA | FUENTE_NO_OFICIAL
    source_verified text NOT NULL DEFAULT 'COPIA_LOCAL_SIN_PROCEDENCIA_VERIFICADA',
    revalidado_contra_origen_en timestamptz,

    downloaded_at timestamptz NOT NULL DEFAULT now(),
    extractor_version text,
    is_current boolean NOT NULL DEFAULT true,
    previous_version_id uuid REFERENCES digemid_norma_documentos(id),
    metadata jsonb,

    -- Una descarga con SHA distinto es una VERSIÓN NUEVA, nunca un overwrite.
    UNIQUE (norma_id, sha256)
);

-- Como mucho una versión vigente por norma.
CREATE UNIQUE INDEX IF NOT EXISTS digemid_norma_documentos_una_vigente_idx
    ON digemid_norma_documentos (norma_id) WHERE is_current;

COMMENT ON TABLE digemid_norma_documentos IS
    'Historial inmutable de los PDF de cada norma. Nunca se sobrescribe: una descarga con hash distinto agrega una fila y marca la anterior is_current=false. Permite responder que archivo exacto produjo cada transcripcion.';
COMMENT ON COLUMN digemid_norma_documentos.source_verified IS
    'Tener el PDF en Storage NO demuestra que sea oficial. FUENTE_OFICIAL_VERIFICADA solo cuando el hash se revalido descargando desde la URL oficial.';

-- Detecta el error que la calidad no ve: dos normas del mismo documento.
CREATE OR REPLACE VIEW digemid_documentos_compartidos AS
SELECT d.sha256,
       count(DISTINCT d.norma_id)                       AS normas_distintas,
       array_agg(DISTINCT n.document_key ORDER BY n.document_key) AS normas
FROM digemid_norma_documentos d
JOIN digemid_normas n ON n.id = d.norma_id
WHERE d.is_current
GROUP BY d.sha256
HAVING count(DISTINCT d.norma_id) > 1;

COMMENT ON VIEW digemid_documentos_compartidos IS
    'Dos normas distintas con el mismo PDF vigente: al menos una guarda la transcripcion de otra norma.';

-- ===========================================================================
-- 2) CAPAS DE TRANSCRIPCIÓN: nunca se descarta el candidato  (F-02 · 6)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS digemid_pagina_transcripciones (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pagina_id uuid NOT NULL REFERENCES digemid_norma_paginas(id) ON DELETE CASCADE,
    documento_id uuid REFERENCES digemid_norma_documentos(id),
    page_number integer NOT NULL,

    -- embedded_pymupdf | embedded_pdfplumber | ocr_tesseract | vision | verified
    capa text NOT NULL,
    texto text,

    engine text,
    engine_version text,
    provider text,
    model text,
    model_real text,
    response_id text,
    prompt_version text,
    dpi integer,
    ocr_confidence real,
    pdf_sha256 text,

    creado_en timestamptz NOT NULL DEFAULT now(),
    creado_por text,

    -- Una transcripción sin trazabilidad no puede ascender a 'verified'.
    CONSTRAINT verified_exige_trazabilidad CHECK (
        capa <> 'verified' OR (pdf_sha256 IS NOT NULL AND creado_por IS NOT NULL)
    ),
    -- 'auto' no identifica a nadie: no es auditable.
    CONSTRAINT modelo_auditable CHECK (
        model IS NULL OR model NOT IN ('auto', 'openrouter/auto')
    ),
    UNIQUE (pagina_id, capa, engine, model, dpi)
);

COMMENT ON TABLE digemid_pagina_transcripciones IS
    'Cada motor conserva SU salida. Elegir la mejor no autoriza a borrar las otras: sin los candidatos alternativos es imposible detectar despues que dos motores discrepaban.';
COMMENT ON COLUMN digemid_pagina_transcripciones.capa IS
    'verified es la unica capa citable juridicamente. Ninguna asciende por confianza declarada del motor: requiere evidencia cruzada o revision humana.';

-- ===========================================================================
-- 3) ESTADO DE FIDELIDAD POR PÁGINA  (F-01 · 16)
-- ===========================================================================
ALTER TABLE digemid_norma_paginas
    ADD COLUMN IF NOT EXISTS verification_status text,
    ADD COLUMN IF NOT EXISTS risk_level text,
    ADD COLUMN IF NOT EXISTS nivel_uso text,
    ADD COLUMN IF NOT EXISTS fidelity_checked_at timestamptz,
    ADD COLUMN IF NOT EXISTS fidelity_evidence jsonb,
    ADD COLUMN IF NOT EXISTS documento_id uuid REFERENCES digemid_norma_documentos(id),
    ADD COLUMN IF NOT EXISTS es_dispositiva boolean,
    ADD COLUMN IF NOT EXISTS es_alto_riesgo boolean,
    ADD COLUMN IF NOT EXISTS verificado_por text,
    ADD COLUMN IF NOT EXISTS verificado_en timestamptz;

COMMENT ON COLUMN digemid_norma_paginas.quality_score IS
    'LEGIBILIDAD (heuristica de forma), NO fidelidad. Una pagina puede tener 1.0 y decir "Articulo 13" donde el PDF dice "Articulo 18". Para fidelidad usar verification_status.';
COMMENT ON COLUMN digemid_norma_paginas.verification_status IS
    'NO_EVALUADA | EXTRACCION_DIGITAL_ALTA_CONCORDANCIA | OCR_PENDIENTE_VERIFICACION | DISCREPANCIA_ENTRE_MOTORES | REQUIERE_REVISION_HUMANA | VERIFICADA_AUTOMATICAMENTE | VERIFICADA_HUMANO | ILEGIBLE_PARCIAL | DOCUMENTO_INCOMPLETO | PDF_NO_DISPONIBLE.';
COMMENT ON COLUMN digemid_norma_paginas.nivel_uso IS
    'NIVEL_0_SOLO_INDICE | NIVEL_1_DIGITAL_CONCORDANTE | NIVEL_2_AUTO_VERIFICADA | NIVEL_3_VERIFICADA_HUMANO. Separa buscar (todos) de citar juridicamente (2 y 3).';
COMMENT ON COLUMN digemid_norma_paginas.es_alto_riesgo IS
    'Incluye la primera, la penultima y la ultima pagina aunque su texto no traiga marcadores: si el OCR destruyo "SE RESUELVE", la pagina no deja de ser riesgosa.';

CREATE INDEX IF NOT EXISTS digemid_norma_paginas_verification_idx
    ON digemid_norma_paginas (verification_status) WHERE verification_status IS NOT NULL;
CREATE INDEX IF NOT EXISTS digemid_norma_paginas_alto_riesgo_idx
    ON digemid_norma_paginas (norma_id) WHERE es_alto_riesgo;

-- Una verificación humana no se pisa con una automática.
CREATE OR REPLACE FUNCTION digemid_proteger_verificacion_humana()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.verification_status = 'VERIFICADA_HUMANO'
       AND NEW.verification_status IS DISTINCT FROM 'VERIFICADA_HUMANO'
       AND NEW.verificado_por IS NULL THEN
        RAISE EXCEPTION
            'No se puede degradar una pagina VERIFICADA_HUMANO (%) sin una nueva revision humana',
            OLD.id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS digemid_proteger_verificacion_humana_trg ON digemid_norma_paginas;
CREATE TRIGGER digemid_proteger_verificacion_humana_trg
    BEFORE UPDATE ON digemid_norma_paginas
    FOR EACH ROW EXECUTE FUNCTION digemid_proteger_verificacion_humana();

-- ===========================================================================
-- 4) EXTRACCIÓN EN STAGING: nunca borrar para reextraer  (F-02 · 5)
-- ===========================================================================
-- El pipeline actual hace DELETE de todas las paginas y luego reinserta. Una
-- corrida cortada a la mitad deja la norma incompleta. La corrida nueva se
-- escribe como un LOTE aparte y solo se promueve cuando termino entera.
CREATE TABLE IF NOT EXISTS digemid_extraccion_lotes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    norma_id uuid NOT NULL REFERENCES digemid_normas(id) ON DELETE CASCADE,
    documento_id uuid REFERENCES digemid_norma_documentos(id),
    extractor_version text NOT NULL,
    -- en_progreso | completo | promovido | fallido | descartado
    estado text NOT NULL DEFAULT 'en_progreso',
    paginas_esperadas integer,
    paginas_escritas integer NOT NULL DEFAULT 0,
    iniciado_en timestamptz NOT NULL DEFAULT now(),
    terminado_en timestamptz,
    promovido_en timestamptz,
    error text,
    CONSTRAINT solo_se_promueve_lo_completo CHECK (
        estado <> 'promovido'
        OR (paginas_esperadas IS NOT NULL AND paginas_escritas = paginas_esperadas)
    )
);

COMMENT ON TABLE digemid_extraccion_lotes IS
    'Staging de reextracciones. Una corrida solo se promueve a vigente cuando escribio TODAS las paginas del PDF; si falla, la version anterior sigue intacta.';

-- ===========================================================================
-- 5) COMPLETITUD DEL DOCUMENTO  (F-02 · 4)
-- ===========================================================================
CREATE OR REPLACE VIEW digemid_normas_completitud AS
SELECT
    n.id                                            AS norma_id,
    n.document_key,
    d.sha256                                        AS pdf_sha256,
    d.pdf_page_count,
    count(p.id)                                     AS stored_page_count,
    CASE
        WHEN d.id IS NULL THEN 'PDF_NO_DISPONIBLE'
        WHEN d.pdf_page_count IS NULL THEN 'DESCONOCIDO'
        WHEN d.pdf_page_count = count(p.id) THEN 'COMPLETO'
        ELSE 'INCOMPLETO'
    END                                             AS completitud,
    d.source_verified                               AS procedencia
FROM digemid_normas n
LEFT JOIN digemid_norma_documentos d ON d.norma_id = n.id AND d.is_current
LEFT JOIN digemid_norma_paginas p ON p.norma_id = n.id
GROUP BY n.id, n.document_key, d.id, d.sha256, d.pdf_page_count, d.source_verified;

COMMENT ON VIEW digemid_normas_completitud IS
    'DESCONOCIDO no es COMPLETO. Ni INCOMPLETO ni DESCONOCIDO habilitan confirmar una relacion juridica: una norma de 50 paginas con 49 extraidas no esta verificada.';

-- ===========================================================================
-- ROLLBACK (no destructivo: ninguna fila existente fue modificada)
-- ===========================================================================
--   DROP TRIGGER digemid_proteger_verificacion_humana_trg ON digemid_norma_paginas;
--   DROP FUNCTION digemid_proteger_verificacion_humana();
--   DROP VIEW digemid_normas_completitud, digemid_documentos_compartidos;
--   ALTER TABLE digemid_norma_paginas DROP COLUMN verification_status, ... (las 10 nuevas);
--   DROP TABLE digemid_extraccion_lotes, digemid_pagina_transcripciones, digemid_norma_documentos;
