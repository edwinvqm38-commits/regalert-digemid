# Arquitectura de Procesamiento Documental DIGEMID

## Principio Rector
Conservar el documento oficial, extraer texto fiel, estructurar lo necesario y consultar con IA con trazabilidad.

## Alcance
Esta arquitectura aplica a:
- alertas DIGEMID
- normas regulatorias DIGEMID
- futuros documentos de botica, POES y derivados documentales

## Separacion De Modulos
Los modulos funcionales se mantienen separados. El procesamiento documental comparte criterios tecnicos, no una tabla unica.

### Alertas
Las alertas usan:
- `digemid_documentos`
- `digemid_documento_paginas`
- `digemid_documento_assets`

### Normas
Las normas usan:
- `digemid_normas`
- `digemid_norma_paginas`
- `digemid_norma_assets`
- `digemid_norma_chunks`

## Contrato Comun De Assets
Los modulos documentales deben converger en un contrato comun de tipos de asset para facilitar trazabilidad y procesamiento transversal:

- `pdf_original`
- `manifest`
- `page_render`
- `extracted_image`
- `detected_table`
- `ocr_artifact`
- `structured_json`
- `ai_analysis`

Cada asset puede incluir, segun el modulo:
- identificador del documento origen
- referencia opcional a pagina
- backend de almacenamiento
- `drive_file_id`
- `file_name`
- `mime_type`
- `bbox`
- `metadata`

## Contrato Comun Por Pagina
Cada modulo documental debe poder representar, como minimo, el siguiente contrato logico por pagina:

- `page_number`
- `text_raw`
- `text_normalized`
- `text_length`
- `ocr_required`
- `ocr_used`
- `has_tables`
- `has_images`
- `image_count`
- `table_count`
- `extraction_method`
- `metadata`

No todos los campos tienen que vivir hoy en una sola tabla fisica. El objetivo es mantener un estandar comun para evolucion futura de pipelines, OCR y analisis.

### Invariantes de evidencia textual y tabular

- `text_raw` contiene exclusivamente el texto originalmente extraido de la pagina.
- `text_normalized` aplica solo normalizacion conservadora a esa misma fuente.
- Una correccion humana se conserva como capa separada y trazable en metadata; no reescribe las capas fuente.
- `has_tables` / `possible_table` solo indican que existen indicios de tabla.
- `tables` y `tables_markdown` contienen candidatos aceptados automaticamente, separados del texto fuente.
- `table_requires_review` es verdadero si al menos una region geometrica detectada no tiene candidato seguro.
- `table_quality_score` mide calidad estructural automatica; no es verificacion legal.
- `revisado_manual` confirma revision del texto y nunca implica `tabla_verificada`.
- `tabla_verificada` requiere una accion humana explicita sobre la estructura,
  Markdown estricto, cobertura de todas las regiones detectadas y el SHA-256
  del PDF fuente. El booleano aislado nunca es evidencia suficiente.
- La autoridad de lectura es `isTrustedTableVerification`: exige metadata de
  método, fecha, hash coincidente y `unresolved_regions = 0` en la cobertura
  manual. Esta revisión es estructural y no equivale a validación jurídica
  profesional.
- La cobertura regional nunca se infiere: si faltan `detected_regions`,
  `accepted_regions` o `unresolved_regions`, se registra
  `coverage_unknown = true` y la tabla no puede ser confiable.
- `table_requires_review = true` es un veto absoluto. Si coexiste con una
  afirmación de tabla verificada, el estado se diagnostica como contradictorio
  y falla cerrado.
- `metadata.manual_review.table_verification.reviewer_id` permanece `null`
  mientras el flujo no disponga de una identidad auditable segura; no se
  infiere ni se inventa a partir del contenido de la plantilla.
- En la representación editable, la identidad técnica de las columnas es
  ordinal (`C1`, `C2`, `C3`...), nunca el texto del encabezado. Esto conserva
  orden, cardinalidad y encabezados duplicados sin colisiones.
- `Columna N` es exclusivamente un placeholder visual para un encabezado
  fuente vacío. El roundtrip conserva el encabezado canónico vacío y por ello
  la tabla no puede elevarse a verificada desde Telegram.
- Telegram es un canal de consulta rápida y revisión estructural limitada: no
  repara encabezados vacíos o ambiguos. Esas correcciones deberán realizarse
  en un futuro panel documental que compare explícitamente PDF original y
  estructura extraída; ese panel no forma parte de esta fase.
- Los encabezados duplicados conservan identidad ordinal y cardinalidad, pero
  producen `duplicate_header_ambiguity`. Una consulta que use solo el texto
  duplicado debe indicar `C2`, `C3` u otra posición inequívoca antes de llegar
  al modelo; los identificadores `C<n>` son internos y no contenido del PDF.
- El formato Markdown escapa de forma reversible backslashes y pipes; la
  matriz ordinal canónica, no el Markdown visible, preserva la identidad.
- Mientras no exista `safe_prose_text` derivado de regiones con trazabilidad,
  una página con tabla pendiente se excluye completa del contexto RAG, incluso
  para preguntas narrativas. Si no queda otra página segura, la respuesta es
  determinista y no se invoca al LLM.
- `revisado_manual` solo acredita revisión textual. En páginas con indicios de
  tabla sin evidencia estructural moderna, el estado es
  `TEXTO_REVISADO_HUMANO_ESTRUCTURA_PENDIENTE` y no autoriza cita legal.
- Una propuesta de OCR/vision IA permanece en metadata con estado de propuesta; no sustituye evidencia buscable automaticamente.

## Estructura Drive Recomendada
Estructura general recomendada para mantener separacion funcional y consistencia documental:

- `01_ALERTAS`
- `02_NORMATIVA`
- `03_POES_BOTICA`
- `04_CONSULTAS_IA`

Cada documento debe conservar:
- carpeta principal por `document_key`
- `00_ORIGINAL` para el PDF oficial
- subcarpetas tecnicas para texto, renders, imagenes, tablas, estructurado, IA y manifest

## Cuando Usar Flujo Estandar
Usar flujo estandar cuando el documento tenga:

- PDF con texto embebido
- tablas simples
- paginas legibles

El flujo estandar prioriza:
- extraccion directa de texto
- deteccion de tablas simples
- registro de assets derivados
- trazabilidad por pagina y por archivo

## Cuando Usar OCR
Usar OCR cuando se detecte alguno de estos casos:

- pagina sin texto
- pagina escaneada
- texto extraido muy corto o incoherente

Senales operativas recomendadas:
- `text_length` anormalmente bajo
- exceso de caracteres rotos o ruido
- paginas con sellos, firmas o imagen raster dominante

## Cuando Usar IA
Usar IA como capa de interpretacion y apoyo, no como reemplazo del documento fuente. Casos recomendados:

- resumen regulatorio
- obligaciones de botica
- impacto en POES
- relacion entre normas y alertas
- analisis de tablas o anexos complejos

La salida de IA debe apuntar siempre a:
- documento fuente
- pagina o rango de paginas
- fecha del documento
- fuente oficial

## Reglas
- no inventar obligaciones
- citar documento, pagina, fecha y fuente
- diferenciar dato confirmado, analisis, recomendacion y pendiente de validacion
- no reemplazar al Director Tecnico ni a la autoridad sanitaria

## Objetivo De La Capa General
Esta capa general de procesamiento documental busca que alertas, normas y futuros documentos compartan una base tecnica comun para:

- conservar evidencia documental
- extraer contenido con trazabilidad
- estructurar texto, tablas e imagenes
- soportar OCR cuando haga falta
- habilitar analisis con IA de manera controlada

La separacion funcional entre modulos se mantiene. Lo comun es el estandar tecnico documental.
