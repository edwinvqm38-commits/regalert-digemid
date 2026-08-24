# F-01 · Auditoría integral de fidelidad documental

**Nada de esto modifica producción.** Solo `SELECT`. No se reemplazó texto, no se
ejecutó OCR sobre el corpus, no se tocaron relaciones ni vigencias, no se
fusionó ni desplegó nada.

**Principio:** el PDF oficial es la fuente de verdad. Ningún OCR, LLM,
`quality_score` ni transcripción previa se considera verdad por sí solo.

---

## 1. El pipeline reconstruido

```
FUENTE OFICIAL (digemid.minsa.gob.pe / El Peruano)
   │  crawl_normativa_pdf_urls.py descubre pdf_url
   ▼
DESCARGA  download_pdf()                     ── sin SHA-256, sin bytes, sin page_count
   ▼
RESPALDO  respaldar_pdf() → Storage           ── upsert:true  ⇒ PISA la evidencia anterior
   ▼
BORRADO   DELETE de TODAS las páginas         ── sin transacción ⇒ una corrida cortada
   │                                             deja la norma incompleta
   ▼
EXTRACCIÓN  agents/pdf_extract.extract_page()
   ├─ 1. PyMuPDF (texto embebido)
   ├─ 2. pdfplumber   SOLO si quality < 0.75
   ├─ 3. Tesseract 300 DPI  SOLO si quality < 0.5 o len < 25
   └─ best = max(candidatos, key=quality_score)   ⇒ gana la FORMA, y los demás
                                                     candidatos SE DESCARTAN
   ▼
NORMALIZACIÓN  normalize_text() = NFC + strip   ── text_raw ≈ text_normalized
   ▼
ESCRITURA  write_pages() → digemid_norma_paginas
   ▼
(opcional, manual) OCR VISIÓN IA  ocr_normativa_openai_pages.py
   └─ --replace-text ⇒ pisa text_raw/text_normalized y fija quality_score = 0.9 A MANO
   ▼
RAG  buscar_paginas_texto()  ── lee text_normalized; no conoce fidelidad
   ├─► /consulta
   └─► detector de relaciones (texto_de_norma → DeepSeek)
```

### Lo que el pipeline NO tiene

| Falta | Consecuencia |
|---|---|
| SHA-256, bytes y `page_count` del PDF | no se puede responder *"¿qué archivo produjo esta transcripción?"* |
| comparación de `page_count` PDF vs. páginas guardadas | un PDF de 35 páginas del que se guardan 34 queda `text_extracted` con calidad alta |
| historial de descargas | `upsert:true` pisa el PDF anterior sin dejar rastro |
| capas de texto separadas | una reextracción destruye la evidencia previa |
| cualquier noción de fidelidad | `quality_score` mide forma, no correspondencia con el original |

---

## 2. Riesgos

### CRÍTICO

1. **La selección del mejor texto premia la forma, no la fidelidad.**
   `best = max(candidatos, key=quality_score)`. Si pdfplumber devuelve un texto
   más "bonito" pero pierde un número, gana. **Y los demás candidatos se
   descartan**: la comparación multimotor existe durante un instante y se tira.
2. **El cron horario escribe en `apply` sin verificar nada** (ver §3).
3. **755 páginas dispositivas sin ninguna verificación de fidelidad**, en 272 de
   las 292 normas con texto. Son las que producen efectos jurídicos.
4. **`quality_score = 0.9` inventado a mano** cuando el OCR de visión reemplaza
   texto (`ocr_normativa_openai_pages.py:648`). No mide nada: es una constante.
5. **`--provider openrouter --model openrouter/auto`** por defecto: no queda
   registro de qué modelo produjo el texto. Una transcripción cuyo autor se
   desconoce no puede considerarse verificada.
6. **El `DELETE` previo no es transaccional.** Fallo de red a mitad ⇒ norma con
   páginas parciales y `process_status` sin actualizar.

### ALTO

7. **150 DPI en el OCR de visión** mientras Tesseract usa 300 (ver §13): a las
   páginas *más difíciles* se les da *la mitad* de resolución.
8. **`es_pagina_en_blanco()` decide por promedio de píxel > 250 a 72 DPI.** Si se
   equivoca, una página con contenido real se guarda **vacía con
   `quality_score = 1.0`**. Hoy hay **115 páginas** así y ninguna fue confirmada
   contra el PDF.
9. **pdfplumber solo corrió en 5 páginas de 4 083.** No hay comparación
   multimotor en la práctica.
10. **El estado de la norma se decide por calidad PROMEDIO.** Una página pésima
    entre 30 buenas no baja el promedio: la norma queda `text_extracted`.
11. **El historial del OCR de visión es de un solo nivel y se pisa**: una segunda
    corrida con `--replace-text` sobrescribe
    `previous_extraction_before_openai_vision` con el texto de visión y pierde
    la extracción original.

### MEDIO

12. Las tablas se aplanan a Markdown y se concatenan al texto **sin afectar
    `quality_score`**: una tabla mal reconstruida es invisible al puntaje.
13. `text_raw` y `text_normalized` son casi idénticos: no hay capa "verificada".
14. **10 normas tienen páginas pero no `file_storage_path`**: el PDF que produjo
    esa transcripción no está guardado en ninguna parte.

### BAJO

15. `posible_formula` / `posible_grafico` funcionan como marcas de revisión, que
    es lo correcto, pero nadie las revisa: **6 fórmulas y 2 gráficos**, ninguno
    verificado.

---

## 3. Estado del cron automático — y recomendación

`.github/workflows/digemid-normativa-text-simple.yml`:

```yaml
schedule:
  - cron: "0 * * * *"
...
if [[ "${{ github.event_name }}" == "schedule" ]]; then
  MODE="apply"; LIMIT="5"
...
CMD+=(--no-telegram)
```

Cada hora, **en modo apply y en silencio**, procesa 5 normas: descarga, borra
todas sus páginas y escribe texto nuevo.

**Recomendación: PAUSARLO. Sí, confirmo el riesgo.** Es el único proceso
automático que escribe transcripciones; los workflows de `text-extraction` y de
`OCR con visión IA` **no tienen `schedule`** — son solo manuales.

Ya está preparado el PR, **con ese único cambio y sin fusionar**: rama
`claude/f01-pausar-cron-extraccion`. Solo comenta el `schedule`;
`workflow_dispatch` sigue disponible, incluido `dry-run`.

---

## 4. `quality_score`: qué mide y qué no

`agents/pdf_extract.quality_score()` penaliza palabras pegadas, exceso de
caracteres no alfabéticos y textos muy cortos. Es **legibilidad**: *"¿esto se ve
como prosa normal?"*.

No puede detectar —y no pretende hacerlo— que el texto diga `Artículo 13` donde
el PDF dice `Artículo 18`. Ese texto es perfectamente legible: **1.0**.

| señal actual | mide de verdad |
|---|---|
| `quality_score` | legibilidad / forma |
| `ocr_confidence` (Tesseract) | seguridad del reconocedor, por palabra |
| `confianza_estimada` (LLM) | **nada verificable** — es la opinión del modelo sobre sí mismo |
| `has_tables` | presencia, no corrección |
| `posible_formula` / `posible_grafico` | marca de revisión (correcto) |

**Ninguna mide fidelidad.** La fidelidad exige una segunda fuente. Por eso la
arquitectura propuesta separa:

```
quality_visual        ← el quality_score de hoy, renombrado por lo que es
quality_structure     ← ¿la tabla conserva la correspondencia fila-columna?
fidelity_text         ← CER/WER contra otra fuente
fidelity_numbers      ← tasa de error en dígitos
fidelity_legal_refs   ← tasa de error en referencias normativas
fidelity_tables       ← verificación de celdas
verification_status   ← el veredicto, y qué evidencia lo respalda
```

Implementado en `agents/fidelidad_legal.py`.

---

## 5. Confianza del LLM y de Tesseract

Ninguna alcanza para marcar una página como verificada. La prueba está en §13:
a 150 DPI con letra de 6 pt, Tesseract reportó **confianza 0,765** —por encima
de muchos umbrales— y aun así **perdió 7 tokens jurídicos**, entre ellos
`artículo 13`, `artículo 18`, el plazo `10` y el número de norma
`339-2023/MINSA`, que quedó truncado en `339`.

En `evaluar_pagina()`, ninguna confianza por sí sola llega a `VERIFICADA_*`: el
techo con OCR de confianza alta es `OCR_PENDIENTE_VERIFICACION`. Solo ascienden
la **evidencia cruzada entre dos motores independientes** o la **revisión
humana**.

---

## 6. Inventario del corpus (4 083 páginas · 292 normas con texto · 342 normas)

### Por origen de la página

| origen | páginas | % |
|---|---:|---:|
| PyMuPDF (texto embebido) | 2 002 | 49,0 % |
| Tesseract (escaneado) | 1 941 | 47,5 % |
| "Página en blanco" | 115 | 2,8 % |
| Revisión manual | 17 | 0,4 % |
| pdfplumber | 5 | 0,1 % |
| Formulario/anexo | 3 | 0,1 % |

**Casi la mitad del corpus es OCR de escaneo.**

### Calidad y OCR

| métrica | valor |
|---|---|
| `quality_score` = 1.0 | 1 787 (incluye las 115 "en blanco", forzadas a 1.0) |
| 0,75 – 0,99 | 2 194 |
| < 0,75 | 102 |
| páginas con OCR | 1 956 |
| OCR **sin confianza** registrada | 78 |
| OCR con confianza < 0,7 | 77 |
| confianza media de OCR | 0,887 |
| páginas vacías | 115 |
| **revisadas por una persona** | **20 (0,49 %)** |
| tablas / fórmulas / gráficos | 231 / 6 / 2 |
| tablas verificadas | 8 |

### Integridad

| | |
|---|---|
| numeración con huecos | 0 |
| páginas duplicadas | 0 |
| **completitud del documento** | **DESCONOCIDA en el 100 %** — no existe `pdf_page_count` |
| normas con páginas pero sin PDF en Storage | 10 |

La numeración interna es consistente, pero eso solo prueba coherencia interna:
**no hay forma de saber si falta la última página**, que es justamente donde va
la disposición derogatoria en la técnica legislativa peruana.

---

## 7 y 8. Verificabilidad real

Aplicando el motor de estados al corpus completo:

| verification_status | páginas | % |
|---|---:|---:|
| `EXTRACCION_DIGITAL_ALTA_CONCORDANCIA` | 1 998 | 48,9 % |
| `OCR_PENDIENTE_VERIFICACION` | 1 504 | 36,8 % |
| `REQUIERE_REVISION_HUMANA` | 446 | 10,9 % |
| `NO_EVALUADA` (en blanco sin confirmar) | 115 | 2,8 % |
| **`VERIFICADA_HUMANO`** | **20** | **0,49 %** |

- **Páginas citables hoy como fuente legal segura: 0,49 %.**
- **Páginas que requieren OCR nuevo o revisión: 51,1 %** (`OCR_PENDIENTE` +
  `REQUIERE_REVISION` + `NO_EVALUADA`).
- El 48,9 % restante es texto embebido legible, pero **fidelidad no verificada**:
  nadie lo comparó contra el PDF.

---

## 9. Páginas dispositivas no confiables

| | |
|---|---|
| páginas dispositivas | **755** |
| **no aptas para alimentar el detector** | **755 (100 %)** |
| normas afectadas | **272 de 292** |
| dispositivas provenientes de OCR | 214 |
| dispositivas con OCR de confianza < 0,85 o nula | 29 (riesgo **CRÍTICO**) |
| dispositivas con tablas | 45 |
| **dispositivas revisadas por una persona** | **0** |

Es el hallazgo central: **todas las relaciones normativas detectadas hasta hoy
—incluidas las 15 confirmadas— se dedujeron de páginas cuya fidelidad nunca se
verificó.**

---

## 13. DPI: 150 vs 200 vs 300 — medido, no supuesto

**NO VERIFICADO sobre el corpus real:** este entorno no alcanza
`digemid.minsa.gob.pe` ni El Peruano (el proxy devuelve 403) y no hay
credenciales de Supabase, así que no pude bajar un PDF normativo real. Lo que
falta exactamente: ejecutar `scripts/comparar_dpi_ocr.py` sobre páginas
escaneadas reales del corpus.

Lo que **sí** medí, con Tesseract español real sobre una página sintética con
texto legal y cuerpos de 11, 8 y 6 pt — **el mejor caso posible**, porque un
escaneo real trae ruido, inclinación y pérdida previa de resolución:

| cuerpo | DPI | confianza | tokens numéricos correctos | tasa de error |
|---|---:|---:|---:|---:|
| 11 pt | 150 | 0,947 | 24/24 | 0 % |
| 11 pt | 200 | 0,942 | 23/24 | 4,2 % *(perdió `32319`)* |
| 11 pt | 300 | 0,934 | 24/24 | 0 % |
| 8 pt | 150/200/300 | ~0,93 | 24/24 | 0 % |
| **6 pt** | **150** | **0,765** | **20/24** | **16,7 %** |
| **6 pt** | **200** | 0,903 | 22/24 | 8,3 % |
| **6 pt** | **300** | 0,945 | **24/24** | **0 %** |

Tokens perdidos a 6 pt / 150 DPI: `339`, `2023`, `000,00`, `0,5`.
A 200 DPI: `2011`, y `014-2011-SA` **truncado a `014-201`** — peligroso
precisamente porque parece plausible.

**Conclusiones:**

1. **150 DPI no es suficiente** para letra pequeña —notas, tablas, anexos,
   cabeceras `N° ...-DG-DIGEMID`—, que es justo donde viven los números de norma.
2. **300 DPI es el mínimo defendible**; fue el único que acertó el 100 % en todos
   los cuerpos.
3. Hay una **inconsistencia interna**: Tesseract renderiza a 300
   (`agents/pdf_extract.py:290`) pero el OCR de visión usa 150 por defecto
   (`ocr_normativa_openai_pages.py:99`) y el workflow no pasa `--dpi`. A las
   páginas que Tesseract ya no pudo leer se les da **la mitad** de resolución.
4. Un error a 11 pt/200 DPI que desaparece a 300 muestra que el OCR **no es
   monótono**: por eso no basta un solo motor a una sola resolución.

---

## 14. Golden dataset propuesto

`tests/fixtures/golden/` con una carpeta por página:

```
golden/RM-894-2024-p12/
    referencia.txt      ← transcripción HUMANA o texto oficial verificable
    fuente.json         ← {document_key, page_number, pdf_sha256, url_oficial,
                           transcrito_por, fecha, metodo}
```

**Regla: la salida de otro LLM nunca es ground truth.** La referencia sale de una
persona leyendo el PDF, o de la versión textual oficial de El Peruano.

Casos históricos obligatorios (los que ya causaron errores): `RM-894-2024`,
`RM-899-2025`, `RM-419-2025`, `RM-727-2025`, `RM-883-2024`, `DS-008-2025`,
`DS-015-2025`, `DS-12-2023`, `Ley 32319`.

Más, como mínimo, un caso de cada tipo: PDF digital limpio · escaneo malo ·
tabla · anexo · letra pequeña · edición de El Peruano con varias normas ·
página con números parecidos · página con sellos · **disposición derogatoria al
final del documento**.

**Necesito de tu parte** las transcripciones de referencia: no puedo generarlas
sin acceso a los PDF, y generarlas con un modelo sería exactamente el error que
esta fase busca evitar.

---

## 15. Métricas

Implementadas en `agents/fidelidad_legal.py`:

- **CER** y **WER** — distancia de edición sobre caracteres y palabras.
- **LEGAL_TOKEN_ERROR_RATE** — sobre referencias normativas, artículos,
  numerales, años, plazos, montos, porcentajes, fechas, medidas (dosis,
  concentraciones, temperaturas) y **verbos normativos** comparados por clase
  (`deróguese` = `derogar`, pero `derogar` ≠ `modificar`).

Ejemplo real del módulo:

```
referencia: "Artículo 18.- Derógase la RM N° 339-2023/MINSA en 10 días"
obtenido:   "Artículo 13.- Modifícase la RM N° 339-2028/MINSA en 100 días"

CER 0,130   WER 0,364   LEGAL_TOKEN_ERROR_RATE 1,000
   articulo: '18' -> '13'
   referencia_normativa: '339-2023/MINSA' -> '339-2028/MINSA'
   plazo: '10' -> '100'
   anio: '2023' -> '2028'
   verbo_normativo: 'DEROGA' -> 'MODIFICA'
```

Un CER de 13 % y un LTER de 100 %: exactamente la asimetría que hace que WER no
sirva como criterio jurídico.

---

## 16. Estados de verificación

```
              ┌─ revisado por persona ──────────────► VERIFICADA_HUMANO
              │
señales de ───┼─ dos motores coinciden, 0 errores ──► VERIFICADA_AUTOMATICAMENTE
la página     │      └─ pero hay tabla/fórmula ─────► REQUIERE_REVISION_HUMANA
              │
              ├─ dos motores difieren en un token ──► DISCREPANCIA_ENTRE_MOTORES
              ├─ dice [ilegible] ───────────────────► ILEGIBLE_PARCIAL
              ├─ "en blanco" sin confirmar ─────────► NO_EVALUADA
              ├─ OCR conf < 0,85 o nula ────────────► REQUIERE_REVISION_HUMANA
              ├─ OCR conf ≥ 0,85 ───────────────────► OCR_PENDIENTE_VERIFICACION
              ├─ embebido quality < 0,75 ───────────► REQUIERE_REVISION_HUMANA
              └─ embebido limpio ───────────────────► EXTRACCION_DIGITAL_ALTA_CONCORDANCIA

  y por encima de todo:  sin PDF → PDF_NO_DISPONIBLE
                         faltan páginas → DOCUMENTO_INCOMPLETO
```

**`DOCUMENTO_INCOMPLETO` gana a cualquier `quality_score`**: 34 páginas perfectas
de un PDF de 35 siguen siendo un documento no verificado.

---

## 17 y 18. Las dos puertas

**`/consulta`** — `puede_citarse_como_fuente_legal(estado)`: solo
`VERIFICADA_HUMANO` y `VERIFICADA_AUTOMATICAMENTE`. Con el corpus de hoy eso son
**20 páginas**. El resto puede usarse como contexto **con advertencia explícita**,
nunca presentarse como cita legal segura. Para preguntas concretas sobre
artículos, plazos, derogaciones, montos, sanciones o requisitos: **preferible no
responder a inventar desde una transcripción dudosa**.

**Detector de relaciones** — `puede_alimentar_detector(estado, dispositiva)`:

- página **no dispositiva** no verificada → puede leerse (de ahí no salen
  relaciones);
- página **dispositiva** no verificada → **no**: la relación queda
  `PENDIENTE_VERIFICACION_FUENTE`;
- **documento incompleto o sin PDF → nunca**, dispositiva o no: la disposición
  derogatoria peruana va al final, así que justo la página que falta suele ser
  la decisiva.

*(Las puertas están implementadas y testeadas, pero todavía **no conectadas** a
`/consulta` ni al detector: eso cambia comportamiento de producción y espera tu
autorización.)*

---

## 19. Migraciones propuestas — **NO APLICADAS**

`supabase/sql/2026_08_24_fidelidad_documental_PROPUESTA.sql`:

1. **`digemid_norma_documentos`** — cadena de custodia: `sha256`, `bytes`,
   `page_count`, `source_url`, `storage_path`, `fuente`, `descargado_en`,
   `extractor_version`, `es_vigente`, con `UNIQUE (norma_id, sha256)`. Una
   descarga con hash distinto **agrega una fila**, no pisa la anterior.
2. **`digemid_pagina_transcripciones`** — capas separadas
   (`source_text_embedded` / `ocr_tesseract` / `vision_candidate` / `verified`)
   con `provider`, `model`, `model_version`, `prompt_version`, `response_id`,
   `dpi`, `ocr_confidence`. Un OCR nuevo ya no destruye la evidencia anterior.
3. **Columnas en `digemid_norma_paginas`** (aditivas, nullable):
   `verification_status`, `risk_level`, `fidelity_checked_at`,
   `fidelity_evidence`, `documento_id`, `es_dispositiva`. `quality_score` **se
   conserva**, con un `COMMENT` que dice lo que realmente mide.
4. **Vista `digemid_normas_completitud`** — `COMPLETO` / `INCOMPLETO` /
   `DESCONOCIDA`, y `DESCONOCIDA` **no** es `COMPLETO`.

---

## 20. Tests agregados — 29, todos verdes

`tests/test_fidelidad_legal.py` falla si el pipeline vuelve a aceptar:

número de artículo cambiado · número de norma cambiado · año cambiado · plazo
cambiado · monto cambiado · dosis/concentración cambiada · **verbo jurídico
cambiado con WER < 0,2** · OCR vacío sobre página con contenido · confianza alta
de OCR tomada como verificación · `quality_score` 1.0 tomado como verificación ·
discrepancia entre motores tratada como "confianza media" · tabla dada por
verificada por coincidencia textual · documento incompleto tapado por calidad
alta · página "en blanco" dada por buena · página dispositiva no verificada
alimentando al detector · documento incompleto alimentando al detector · página
no verificada citada como fuente legal segura.

Y un test de que **no hay falsos positivos**: dos textos equivalentes sin tokens
jurídicos dan LTER 0.

---

## 22. Plan seguro para reauditar el corpus

1. **Pausar el cron** (PR listo, sin fusionar).
2. Aplicar la migración de fidelidad.
3. **Registrar la cadena de custodia sin reextraer**: para las 292 normas con
   texto, bajar el PDF, calcular SHA-256 y `page_count`, y poblar
   `digemid_norma_documentos`. **No toca el texto.** Aquí aparecen los
   documentos incompletos.
4. **Clasificar** las 4 083 páginas con `auditar_fidelidad_documental.py` y
   escribir `verification_status`. Sigue sin tocar texto.
5. **Verificación cruzada por lotes, empezando por las 755 dispositivas**:
   segundo motor independiente (pdfplumber para digitales; visión IA a **300
   DPI** para escaneadas), guardado como **capa nueva**, nunca reemplazando.
   Comparar por tokens jurídicos.
6. Revisión humana de las discrepancias y de las 45 dispositivas con tablas,
   por el bot.
7. Recién entonces: conectar las puertas de `/consulta` y del detector.
8. Reactivar el cron con la puerta de verificación puesta.

**Nada de esto reemplaza texto masivamente ni marca páginas como verificadas por
heurística.**

## 23. Costo y tiempo estimados

| paso | volumen | estimación |
|---|---|---|
| 3 · cadena de custodia | 292 PDF | ~1–2 h de proceso, sin costo de API |
| 4 · clasificación | 4 083 páginas | minutos, sin costo |
| 5 · segundo motor, digitales | ~2 000 páginas | pdfplumber local, ~1–2 h, sin costo |
| 5 · segundo motor, escaneadas a 300 DPI | ~1 950 páginas | **coste de API real**; a 300 DPI la imagen pesa ~4× que a 150 |
| 6 · revisión humana | ~755 dispositivas, priorizando 29 críticas + 45 con tablas | el cuello de botella real |

**NO VERIFICADO — el costo de API:** depende del proveedor y modelo que elijas, y
`openrouter/auto` no permite estimarlo. Con un modelo fijado puedo calcularlo
antes de correr nada. Sugiero empezar por un lote de 50 páginas dispositivas
para medir costo real y tasa de discrepancia antes de comprometerse al corpus.

---

## 24. Riesgos residuales

1. **La comparación entre motores detecta desacuerdos, no errores comunes.** Si
   los dos leen mal igual, la página pasa como `VERIFICADA_AUTOMATICAMENTE`. Por
   eso el golden dataset humano no es opcional.
2. **La detección de página dispositiva es por regex sobre el propio texto que se
   está auditando.** Si el OCR destruyó "SE RESUELVE", la página no se marca como
   dispositiva y se le exige menos. Mitigación: tratar como dispositiva también
   la **última página** de cada norma.
3. **La extracción de tokens sensibles puede tener falsos negativos** en formatos
   raros; un token no detectado no se compara.
4. **Los 115 "en blanco" siguen sin confirmarse** contra el PDF.
5. **Las 10 normas sin PDF en Storage** pueden ser irrecuperables si la URL de
   origen cayó.
6. **El costo del OCR a 300 DPI** puede empujar a bajar la resolución. La
   evidencia de §13 dice que eso reintroduce el problema.
7. Las puertas están implementadas **pero no conectadas**: hasta que se conecten,
   `/consulta` y el detector siguen consumiendo texto sin conocer su fidelidad.
