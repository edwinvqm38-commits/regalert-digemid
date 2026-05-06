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
