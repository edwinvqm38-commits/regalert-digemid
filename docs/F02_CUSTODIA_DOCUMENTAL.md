# F-02 · Cadena de custodia, verificación documental y piloto

**Nada de esto modifica producción.** Solo `SELECT` y lectura de metadatos de
Storage. No se aplicó la migración, no se ejecutó OCR, no se reemplazó texto,
no se desplegó nada, no se tocaron relaciones ni vigencias.

> EL PDF OFICIAL ES LA EVIDENCIA. LA TRANSCRIPCIÓN ES UNA REPRESENTACIÓN DE ESA
> EVIDENCIA. EL LLM SOLO INTERPRETA UNA TRANSCRIPCIÓN YA VERIFICADA.

---

## 0. Hallazgo crítico: normas con la transcripción de OTRA norma

Comparando el `eTag` (MD5) de los objetos de Storage y el hash del texto
guardado —sin descargar un solo byte— aparecieron **tres pares de normas cuya
transcripción es idéntica**, y un cuarto par que comparte el PDF de origen:

| normas | evidencia | gravedad |
|---|---|---|
| **`LEY-29698` ↔ `RM-373-2024`** | **33 páginas idénticas**; ambas apuntan a `RM_373-2024-MINSA.pdf` | **CRÍTICO** |
| `DS-9-2015` ↔ `DS-10-2015` | 4 páginas idénticas; mismo MD5 de PDF; ambas apuntan a `DS_009-2015.pdf` | **CRÍTICO** |
| `RM-195-2022` ↔ `RM-98-2024` | 2 páginas idénticas; ambas apuntan a `PERUANO_RM_98-2024-MINSA.pdf` | **CRÍTICO** |
| `DS-24-2018` ↔ `DS-30-2023` | mismo `pdf_url`, texto distinto | ALTO |

**Qué significa:** al menos una norma de cada par guarda el texto de otra. Es
**indetectable por `quality_score`** —el texto se ve perfecto— y solo se ve
comparando la procedencia documental.

`LEY-29698` importa especialmente: es la norma real contra la que H-08 decidió
**no** fusionar el stub `LEY-29698-ART9`. Su título ya era sospechoso
(*"Disponen la publicación del proyecto de Decreto Supremo…"*, que es el título
de una RM, no de una ley). **NO VERIFICADO** cuál de las dos filas tiene el
texto correcto: hace falta el PDF oficial. **Ninguna conclusión de H-08 sobre
`LEY-29698` debe darse por firme hasta resolver esto.**

Además:

- **`DS-10-2017`** tiene `file_storage_path = normas/DS-10-2017.pdf`, que **no
  existe** en el bucket (falta la subcarpeta). Es el origen de la relación
  confirmada que deroga `DS-004-2016`: su PDF no está donde la base dice.
- Existe un objeto huérfano `normas/DS-10-2005/DS-10-2005.pdf` (722 KB) para una
  norma que figura **sin** `file_storage_path` y con 0 páginas: evidencia
  recuperable.
- **11 normas** tienen como `pdf_url` una **URL firmada de nuestro propio
  Storage, ya caducada**. Eso no es una fuente: es una copia nuestra que además
  ya no se puede volver a descargar.

---

## 1. `NO VERIFICADO` ≠ `INCORRECTO`

F-01 dijo «20 páginas verificadas». Eso **no** significa que las otras 4 063
estén mal. Significa que nadie las comparó con el original. Son estados
distintos:

| estado | significado | evidencia que lo respalda |
|---|---|---|
| **INCORRECTA** | se comprobó que difiere del PDF | comparación con discrepancia |
| **NO VERIFICADA** | no se sabe | ninguna |
| **VERIFICADA AUTOMÁTICAMENTE** | motores independientes coinciden, sin errores de token | evidencia cruzada |
| **VERIFICADA POR HUMANO** | una persona la comparó con la imagen | revisión |

El objetivo de F-02 es **maximizar la verificación automática segura** para que
la intervención humana quede reservada al riesgo real.

---

## 2–3. Cadena de custodia

`agents/custodia_documental.py`. Una descarga con SHA distinto es una **versión
nueva**, nunca un overwrite:

```
registrar_version(existentes, nueva) ->
    mismo SHA que la vigente        -> "sin_cambios"
    SHA nunca visto                 -> "nueva_version"    (la anterior queda is_current=false)
    SHA que ya estuvo               -> "reactivada_version_previa"
    nunca se elimina ni se altera una versión previa
```

### Inventario de lo que ya existe en Storage

Sin descargar bytes, desde los metadatos:

| | |
|---|---|
| buckets | `digemid-documentos` (407 objetos) y `digemid-normas` (2) — ambos **públicos** |
| objetos en `digemid-documentos` | 349 PDF + 58 PNG · **423 MB** |
| carpetas | `normas/` 290 · `alertas/` 59 · `paginas-baja-calidad/` 58 |
| normas con `file_storage_path` | 282 |
| …que **existen** en el bucket | **281** |
| …**rotas** | **1** (`DS-10-2017`) |
| normas con páginas pero **sin** PDF guardado | **10** |
| objetos con `eTag` | **407 / 407** |
| `eTag` usable como MD5 simple | **404** (3 son multipart: no comparables) |

**SHA-256 calculable hoy: 0** — el `eTag` es MD5, no SHA-256, y para 3 objetos
ni siquiera es un hash de contenido plano. **`pdf_page_count` verificable hoy:
0** — exige abrir el PDF.

Ambos se obtienen ejecutando el workflow (§ siguiente). Este entorno **no puede
descargar los PDF**: el proxy bloquea `supabase.co`, `digemid.minsa.gob.pe` y El
Peruano (403). **No invento esos valores.**

---

## 4. Completitud

`evaluar_completitud(pdf_page_count, paginas_guardadas)` → `COMPLETO` ·
`INCOMPLETO` · `DESCONOCIDO` · `PDF_NO_DISPONIBLE` · `PDF_CORRUPTO`, detectando
faltantes, duplicadas, extras y numeración fuera de secuencia.

**`DESCONOCIDO` no es `COMPLETO`**, y ni `INCOMPLETO` ni `DESCONOCIDO` habilitan
confirmar una relación jurídica. Cuando falta la última página se dice
expresamente, porque ahí va la disposición derogatoria.

**Completitud real del corpus hoy: `DESCONOCIDA` en las 292 normas con texto.**

---

## 5. Reextracción sin destruir: staging → promote

El pipeline actual hace `DELETE` de todas las páginas y reinserta. La migración
propone `digemid_extraccion_lotes`, con un `CHECK` que impide promover un lote
que no escribió todas las páginas del PDF. Si la corrida se corta, la versión
anterior sigue intacta.

## 6. Capas de transcripción

`digemid_pagina_transcripciones`: una fila por motor
(`embedded_pymupdf`, `embedded_pdfplumber`, `ocr_tesseract`, `vision`,
`verified`) con `engine`, `engine_version`, `provider`, `model`, `model_real`,
`response_id`, `prompt_version`, `dpi`, `ocr_confidence`, `pdf_sha256`.

Dos restricciones en la propia tabla:

- `verified` exige `pdf_sha256` **y** `creado_por`;
- `model` **no puede ser `auto`** ni `openrouter/auto`.

**Elegir el mejor candidato no autoriza a borrar los otros:** sin ellos es
imposible detectar después que dos motores discrepaban.

---

## 7–8. Verificación automática, y por qué dos parsers no bastan

Para una página digital limpia **no** hace falta revisión humana. Pero:

> **PyMuPDF y pdfplumber leen la MISMA capa de texto embebida.** Si esa capa
> está mal, los dos coinciden en el mismo error y «coincidencia» no prueba nada.

Por eso, para páginas dispositivas, el piloto usa un **tercer motor que no lee
la capa embebida sino el render visual**: Tesseract sobre la imagen a 300 DPI.

| tipo de página | motores |
|---|---|
| digital no dispositiva | PyMuPDF + pdfplumber |
| **digital dispositiva** | PyMuPDF + pdfplumber + **OCR sobre render** |
| escaneada | Tesseract 300 DPI + segundo motor independiente |

**Sobre el DPI:** la prueba de F-01 fue **sintética**, así que **no** afirmo que
300 DPI sea un óptimo universal. Se adopta **300 DPI como base provisional para
alto riesgo**, con 400 DPI evaluable en letra muy pequeña. Reescalar por encima
de la resolución original del escaneo no agrega información.

---

## 9. Motor de visión auditable

`es_auditable(EjecucionMotor)` exige `provider`, `model` (**nunca `auto`**),
`model_real`, `response_id`, `prompt_version`, `dpi` y `pdf_sha256`. Sin eso, la
transcripción **no puede considerarse verificada**, por alta que sea la
confianza declarada.

**El piloto no usa ningún modelo de pago:** el tercer motor es Tesseract local.
Así el primer piloto **cuesta 0 USD** y mide el acuerdo antes de gastar.

---

## 10–12. Tokens jurídicos y niveles de uso

`LEGAL_TOKEN_ERROR_RATE` sobre referencias normativas, artículos, numerales,
años, plazos, montos, porcentajes, fechas, medidas (dosis, concentraciones,
temperaturas) y **verbos comparados por clase**. `Artículo 13 ≠ Artículo 18`,
`10 ≠ 100 días`, `DEROGA ≠ MODIFICA` ⇒ **`DISCREPANCIA_CRITICA`**, y **no se
compensa** con un CER global excelente.

Tres puertas, no un booleano:

| nivel | buscar | citar | detector |
|---|:--:|:--:|:--:|
| `NIVEL_0_SOLO_INDICE` | ✅ | ❌ | solo si **no** es dispositiva |
| `NIVEL_1_DIGITAL_CONCORDANTE` | ✅ | ❌ | solo si **no** es dispositiva |
| `NIVEL_2_AUTO_VERIFICADA` | ✅ | ✅ | ✅ |
| `NIVEL_3_VERIFICADA_HUMANO` | ✅ | ✅ | ✅ |

Un documento **incompleto** baja cualquier página a `NIVEL_0` — salvo que una
persona la haya verificado. Y `advertencia_para_consulta(nivel)` devuelve el
aviso explícito que `/consulta` debe mostrar en los niveles bajos.

**Así la app sigue sirviendo mientras se verifica el corpus:** encontrar la
norma se permite siempre; afirmar el contenido de un artículo, no.

---

## 13. Alto riesgo por POSICIÓN, no solo por texto

`es_pagina_alto_riesgo(page_number, total, texto_dispositivo)` marca **siempre**
la primera, la penúltima y la última página, además de cualquiera con
marcadores dispositivos.

**Motivo:** si el OCR destruyó «SE RESUELVE», la página no debe dejar de ser
riesgosa solo porque el propio texto defectuoso ya no contiene el marcador.

## 14. Las 115 páginas «en blanco»

El promedio de píxel **no** prueba nada: una hoja A4 con una línea de texto
sigue promediando ~254/255. `evaluar_pagina_en_blanco()` exige varias señales
coincidentes (texto embebido, bloques de texto, objetos de dibujo, **ratio de
píxeles con tinta** con umbral 0,15 %) y **ante cualquier duda devuelve
`False`**. Una **última página nunca** se declara en blanco por heurística.

Las 115 quedan `NO_EVALUADA` hasta reclasificarlas con el PDF.

## 15. Tablas

Una tabla perfecta en palabras pero con **columnas cruzadas** sigue siendo
jurídicamente incorrecta. El estado `VERIFICADA_AUTOMATICAMENTE` **no se
concede** a una página con tabla, fórmula o gráfico aunque el texto coincida:
pasa a `REQUIERE_REVISION_HUMANA`. Las tablas de sanciones, plazos, requisitos y
dosis exigen verificación estructural (filas/columnas/encabezados), no textual.

---

## 16. Golden dataset sin transcribir 4 000 páginas

30–50 páginas. `tests/fixtures/golden/README.md` tiene la lista completa, que
además de los 9 casos históricos incorpora **los pares detectados en F-02**
(`LEY-29698`/`RM-373-2024`, `DS-9-2015`/`DS-10-2015`, `RM-195-2022`/`RM-98-2024`,
`DS-10-2017`).

`scripts/generar_revision_visual.py` produce un **HTML autocontenido**: imagen
real del PDF a la izquierda, transcripción candidata **editable** a la derecha,
tokens jurídicos **resaltados**, discrepancias entre motores, y tres botones —
*coincide* · *la corregí* · *ilegible*. Al terminar se descarga el JSON. **Nada
se envía a ningún servidor.** La persona **confirma o corrige**, no reescribe.

`ilegible` es un resultado válido y útil: es preferible a una invención.

---

## 17–18. Piloto y costo

`scripts/piloto_verificacion_paginas.py` — **50 páginas de alto riesgo**,
selección **estratificada**: OCR de confianza baja · OCR de confianza alta ·
digitales · con tablas · último folio · documentos largos.

Por página compara **texto guardado vs PDF**, **capa embebida vs render** y
**parser vs parser**, y emite CER, WER, LTER, discrepancias por token, DPI,
tiempo y completitud del documento.

Veredictos posibles: `CONCORDANTE` · `REQUIERE_REVISION_HUMANA` ·
`DISCREPANCIA_CRITICA_ENTRE_PARSERS` ·
`DISCREPANCIA_CRITICA_CAPA_EMBEBIDA_VS_RENDER` ·
`DISCREPANCIA_CRITICA_TEXTO_GUARDADO_VS_PDF`.

**Costo del piloto: 0 USD** (Tesseract local). **NO VERIFICADO** el costo del
motor de visión: no se ejecutó ninguno, y no se puede extrapolar desde
`openrouter/auto`. Se medirá cuando se elija un modelo fijo.

**Resultados del piloto: NO DISPONIBLES.** Este entorno no puede descargar los
PDF (proxy 403 contra `supabase.co`). Por eso el piloto va empaquetado en un
workflow manual.

---

## Workflow — el camino para ejecutar todo esto

`.github/workflows/f02-auditoria-custodia.yml`, **sin `schedule`**, con tres
tareas: `inventario`, `piloto` y `revision_visual`. Publica los resultados como
**artifact** (30 días).

Tiene una **red de seguridad**: antes de ejecutar nada, `grep` busca operaciones
de escritura contra Supabase en los tres scripts y **falla el workflow** si
aparece alguna. (Distingue `sys.path.insert()`, que es legítimo, de
`table(...).insert()`, que no lo es.)

```
tarea: inventario      · limite: 300   → SHA-256 + page_count + completitud
tarea: piloto          · limite: 50    → verificación multimotor de alto riesgo
tarea: revision_visual · document_key: RM-894-2024,DS-12-2023 → HTML para revisar
```

---

## Qué falta antes de aplicar la migración

1. **Ejecutar `inventario`** y obtener SHA-256 + `page_count` reales. Sin eso,
   `digemid_norma_documentos` nacería vacía de lo único que importa.
2. **Resolver los pares de normas con documento compartido** — sobre todo
   `LEY-29698` / `RM-373-2024`, porque afecta a H-08.
3. **Ejecutar el piloto** y ver cuántas páginas de alto riesgo son realmente
   concordantes: eso decide cuánta revisión humana hace falta.
4. **Construir el golden dataset** (30–50 páginas) con la revisión visual.
5. Recién entonces aplicar la migración y poblar la cadena de custodia.

## Riesgos residuales

1. Si **dos motores leen mal igual**, la página pasa como verificada. Por eso el
   golden dataset humano no es opcional.
2. El `eTag` de 3 objetos es multipart: **no** sirve como hash de contenido.
3. Las **10 normas sin PDF** y la ruta rota de `DS-10-2017` pueden ser
   irrecuperables si la URL de origen cayó. Hay un huérfano recuperable
   (`DS-10-2005`).
4. **11 normas** tienen como origen una URL firmada caducada: su procedencia ya
   no es revalidable contra la fuente oficial.
5. La detección de página dispositiva sigue dependiendo del texto auditado; se
   mitiga con la regla por posición, no se elimina.
6. El piloto compara contra el PDF **de Storage**, no contra el oficial: prueba
   que la transcripción representa ese archivo, no que ese archivo sea el
   oficial. Eso exige revalidar el hash contra la URL cuando haya conectividad.
