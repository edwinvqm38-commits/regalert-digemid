-- FIDELIDAD DOCUMENTAL (F-01) — PROPUESTA.
--
-- ⚠️ NO APLICADA. Es el diseño que acompaña a la auditoría; se aplica cuando
-- se autorice, junto con la primera reextracción verificada.
--
-- Principio: el PDF oficial es la fuente de verdad. Hoy no se puede responder
-- "¿exactamente qué PDF produjo esta transcripción?" porque no se guarda ni el
-- hash ni el número de páginas del original, y el respaldo en Storage se sube
-- con upsert (una descarga distinta pisa la anterior sin dejar historial).

-- ---------------------------------------------------------------------------
-- 1) CADENA DE CUSTODIA del documento (F-01 · 6)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS digemid_norma_documentos (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    norma_id uuid NOT NULL REFERENCES digemid_normas(id) ON DELETE CASCADE,
    sha256 text NOT NULL,
    bytes bigint NOT NULL,
    page_count integer NOT NULL,
    source_url text,
    storage_path text,
    fuente text,                       -- digemid | elperuano | carga_manual | drive
    descargado_en timestamptz NOT NULL DEFAULT now(),
    extractor_version text,
    es_vigente boolean NOT NULL DEFAULT true,
    UNIQUE (norma_id, sha256)          -- una descarga nueva NO pisa: agrega fila
);

COMMENT ON TABLE digemid_norma_documentos IS
    'Historial inmutable de los PDF de cada norma. Una descarga con SHA distinto crea una fila nueva y marca la anterior es_vigente=false; nunca sobrescribe. Permite responder que archivo exacto produjo cada transcripcion.';

CREATE INDEX IF NOT EXISTS digemid_norma_documentos_norma_idx
    ON digemid_norma_documentos (norma_id) WHERE es_vigente;

-- ---------------------------------------------------------------------------
-- 2) CAPAS DE TEXTO SEPARADAS (F-01 · 7)
-- ---------------------------------------------------------------------------
-- Hoy text_raw y text_normalized son casi lo mismo y una reextraccion los pisa
-- a los dos. Cada motor debe conservar su propia salida: un OCR nuevo no puede
-- destruir la evidencia anterior.
CREATE TABLE IF NOT EXISTS digemid_pagina_transcripciones (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pagina_id uuid NOT NULL REFERENCES digemid_norma_paginas(id) ON DELETE CASCADE,
    documento_id uuid REFERENCES digemid_norma_documentos(id),
    capa text NOT NULL,                -- source_text_embedded | ocr_tesseract | vision_candidate | verified
    texto text,
    motor text,                        -- pymupdf | pdfplumber | tesseract | openai | openrouter
    -- Trazabilidad exigida para considerar algo verificable (F-01 · 13).
    provider text,
    model text,
    model_version text,
    prompt_version text,
    response_id text,
    dpi integer,
    ocr_confidence real,
    creado_en timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pagina_id, capa, motor, model, dpi)
);

COMMENT ON COLUMN digemid_pagina_transcripciones.capa IS
    'verified es la unica capa citable. Ninguna transcripcion asciende a verified por confianza del motor: requiere evidencia cruzada o revision humana.';

-- ---------------------------------------------------------------------------
-- 3) ESTADO DE FIDELIDAD POR PAGINA (F-01 · 16)
-- ---------------------------------------------------------------------------
-- Aditivo y nullable: no toca ninguna fila existente. quality_score se
-- CONSERVA, pero pasa a llamarse por lo que realmente mide.
ALTER TABLE digemid_norma_paginas
    ADD COLUMN IF NOT EXISTS verification_status text,
    ADD COLUMN IF NOT EXISTS risk_level text,
    ADD COLUMN IF NOT EXISTS fidelity_checked_at timestamptz,
    ADD COLUMN IF NOT EXISTS fidelity_evidence jsonb,
    ADD COLUMN IF NOT EXISTS documento_id uuid REFERENCES digemid_norma_documentos(id),
    ADD COLUMN IF NOT EXISTS es_dispositiva boolean;

COMMENT ON COLUMN digemid_norma_paginas.quality_score IS
    'LEGIBILIDAD (heuristica de forma), NO fidelidad. Una pagina puede tener 1.0 y decir "Articulo 13" donde el PDF dice "Articulo 18". Para fidelidad usar verification_status.';
COMMENT ON COLUMN digemid_norma_paginas.verification_status IS
    'NO_EVALUADA | EXTRACCION_DIGITAL_ALTA_CONCORDANCIA | OCR_PENDIENTE_VERIFICACION | DISCREPANCIA_ENTRE_MOTORES | REQUIERE_REVISION_HUMANA | VERIFICADA_AUTOMATICAMENTE | VERIFICADA_HUMANO | ILEGIBLE_PARCIAL | DOCUMENTO_INCOMPLETO | PDF_NO_DISPONIBLE. Solo los dos VERIFICADA_* autorizan a citar la pagina como fuente legal.';
COMMENT ON COLUMN digemid_norma_paginas.fidelity_evidence IS
    'Que respalda el estado: motores comparados, CER/WER, legal_token_error_rate, tokens en discrepancia. Sin evidencia no hay verificacion.';

CREATE INDEX IF NOT EXISTS digemid_norma_paginas_verification_idx
    ON digemid_norma_paginas (verification_status) WHERE verification_status IS NOT NULL;
CREATE INDEX IF NOT EXISTS digemid_norma_paginas_dispositiva_idx
    ON digemid_norma_paginas (norma_id) WHERE es_dispositiva;

-- ---------------------------------------------------------------------------
-- 4) COMPLETITUD DEL DOCUMENTO (F-01 · 5)
-- ---------------------------------------------------------------------------
-- Regla: PDF 35 paginas + Supabase 34 paginas = DOCUMENTO NO VERIFICADO,
-- aunque las 34 tengan quality_score 1.0.
CREATE OR REPLACE VIEW digemid_normas_completitud AS
SELECT
    n.id                                            AS norma_id,
    n.document_key,
    d.sha256                                        AS pdf_sha256,
    d.page_count                                    AS pdf_page_count,
    count(p.id)                                     AS stored_page_count,
    CASE
        WHEN d.page_count IS NULL THEN 'DESCONOCIDA'
        WHEN d.page_count = count(p.id) THEN 'COMPLETO'
        ELSE 'INCOMPLETO'
    END                                             AS completitud
FROM digemid_normas n
LEFT JOIN digemid_norma_documentos d ON d.norma_id = n.id AND d.es_vigente
LEFT JOIN digemid_norma_paginas p ON p.norma_id = n.id
GROUP BY n.id, n.document_key, d.sha256, d.page_count;

COMMENT ON VIEW digemid_normas_completitud IS
    'DESCONOCIDA no es COMPLETO: mientras no se registre el page_count del PDF oficial, la completitud no esta verificada.';

-- ROLLBACK: DROP VIEW digemid_normas_completitud;
--           ALTER TABLE digemid_norma_paginas DROP COLUMN ... (las 6 nuevas);
--           DROP TABLE digemid_pagina_transcripciones, digemid_norma_documentos;
-- Ninguna fila existente se modifica, asi que revertir no pierde datos.
