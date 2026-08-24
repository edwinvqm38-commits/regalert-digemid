# F-03 · Identidad documental y auditoría del crawler

**Nada de esto modifica producción.** No se tocó `pdf_url`, `source_url`,
`storage_path`, páginas, relaciones ni vigencias. No se ejecutó OCR apply ni
reextracción. Todo fue `SELECT`, lectura de PDF y DRY-RUN.

> IDENTIDAD NORMATIVA CORRECTA → PDF CORRECTO → DOCUMENTO COMPLETO →
> TRANSCRIPCIÓN FIEL → INTERPRETACIÓN JURÍDICA.
>
> **Un PDF encontrado no es un PDF identificado.**

---

## 1. La norma `INCOMPLETO`: RM-734-2025

Segunda corrida del inventario ([run 32758231228](https://github.com/edwinvqm38-commits/regalert-digemid/actions/runs/32758231228), 142,6 s, `success`):

```
[INCOMPLETO] RM-734-2025: PDF 4 pag. / 1 guardadas
    faltan 3 pagina(s): [2, 3, 4] — INCLUYE LA ULTIMA, donde suele ir la disposicion derogatoria
```

Esto demuestra **TRANSCRIPCIÓN INCOMPLETA**, y nada más. Se guardó 1 de 4
páginas, faltando la última, que es donde suele ir la disposición derogatoria.

Lo que **no** demuestra es que el PDF sea el equivocado. Su `pdf_url` es
`.../Normatividad/2025/ANEXO_RM_734-2025.pdf`, y es tentador concluir del
nombre que el documento es el anexo y no la resolución. **Esa conclusión no es
válida**: el nombre del archivo es señal secundaria, y F-03 existe precisamente
para prohibir esa inferencia. Un PDF llamado `ANEXO_…` puede contener la
resolución seguida de su anexo, solo el anexo, resolución + anexo + otra norma,
o cualquier otra combinación. Nadie ha abierto el documento todavía.

Estado hasta que la auditoría lo abra y lea sus encabezados:
**`IDENTIDAD_DOCUMENTAL_PENDIENTE`** + `TRANSCRIPCION_INCOMPLETA_CONFIRMADA`.
Tiene 1 relación saliente (pendiente).

El `DESCONOCIDO` es **DS-10-2017**, con el error literal confirmado:
`{'statusCode': 404, 'error': not_found, 'message': Object not found}`.

---

## 2. Causa raíz del crawler

`scripts/crawl_normativa_pdf_urls.py :: elegir_pdf(html, base_url)`

```python
def elegir_pdf(html: str, base_url: str) -> str | None:
    ...
    candidatos.sort(key=prioridad)      # 0=archivos/normatividad, 1=archivos, 2=resto
    return candidatos[0]
```

**El defecto no es el orden: es la firma.** `elegir_pdf` **no recibe la norma
objetivo**. No tiene forma de comprobar si el documento elegido es el correcto,
así que es *estructuralmente incapaz* de validar identidad. Devuelve el primer
PDF de la ruta más prometedora, y eso es todo.

Agravantes:

1. **`sort` es estable**: entre candidatos de igual prioridad gana el que
   aparece **primero en el HTML**. Si la página de la norma X lista en su
   barra lateral el PDF de la norma Y bajo `Archivos/Normatividad`, y el PDF de
   X está en otra ruta, **gana Y**.
2. **El fallback de PR #31 amplió el riesgo, no lo redujo**: cuando no hay
   ningún `<a href>`, busca por regex *cualquier* URL `.pdf` en el HTML crudo —
   menús, banners, "documentos relacionados".

### Historial

| fecha | commit | qué pasó |
|---|---|---|
| **2026-07-23** | `c87fe89` | nace `elegir_pdf()` con `return candidatos[0]`; se añade el paso del crawler al workflow de extracción |
| **2026-07-23** | `744a4a6` | ese workflow pasa a correr **cada hora** — y el `schedule` fuerza `MODE=apply` |
| **2026-08-07** | `20c19e7` (#31) | "Reparar deteccion de PDF normativo": se añade el fallback por regex |
| **2026-08-24** | #71 | se pausa el cron |

**Periodo activo: ~32 días, cada hora, en modo apply**, 5 normas por corrida.

## 3. Todas las rutas que escriben `pdf_url`

| ruta | ¿valida identidad? |
|---|---|
| `crawl_normativa_pdf_urls.py:188` `update({"pdf_url": pdf_url})` | **no** — la ruta principal del daño |
| `agents/agent_normative_pdf_detector.py:240` | **no** — puntúa por `is_pdf_url`, ruta y texto del enlace (`score_pdf_link`), y toma `candidate_links[0]`. Usa el anchor text, que es mejor, pero **nunca compara con la norma objetivo** |
| `scripts/import_normativa_inventory_to_supabase.py:308` | importa lo que trae el inventario |
| `scripts/crawl_digemid_normativa_inventory.py:451` | `pdf_urls[0] if pdf_urls else None` — **misma lógica de "el primero"** |
| `scripts/seed_digemid_normas_minimas.py:129` | semilla manual |
| `scripts/extract_normativa_text_simple.py:737` | propaga el valor existente |

**Son cuatro rutas que pueden escribir un `pdf_url` sin prueba de identidad**,
no una. Corregir solo el crawler dejaría abiertas las otras tres.

---

## 4–7. La capa nueva

`agents/identidad_documental.py`. Prioridad de evidencia:

```
CONTENIDO DEL PDF  >  contexto del enlace  >  nombre del archivo
```

Lo central es distinguir **encabezado propio** de **cita**:

```
RESOLUCION MINISTERIAL          ← encabezado: el documento ES esta norma
N° 373-2024/MINSA

CONSIDERANDO:
Que, mediante Decreto Supremo    ← cita: el documento MENCIONA esta norma
N° 014-2011-SA se aprobo...
```

Se detecta por forma: el encabezado va en una línea con ≥80 % de mayúsculas y
el tipo escrito completo; la cita va en prosa. Un encabezado pesa 10; una
mención, 1.

Clasificación por norma: `PDF_IDENTIDAD_EXACTA` ·
`PDF_CONTIENE_NORMA_EN_MULTINORMA` · `PDF_IDENTIDAD_CONTRADICTORIA` ·
`PDF_IDENTIDAD_AMBIGUA` · `PDF_NO_DISPONIBLE` · `PDF_CORRUPTO` ·
`FUENTE_NO_VERIFICADA`.

Tipo de documento: `DOCUMENTO_NORMA_UNICA` · `DOCUMENTO_MULTINORMA` ·
`DOCUMENTO_ANEXO` · `DOCUMENTO_PROYECTO` · `DOCUMENTO_INDETERMINADO`.

**Multinorma con rangos:** `rango_de_paginas()` devuelve desde el encabezado de
la norma hasta la página anterior al encabezado siguiente. Un PDF de El Peruano
con 20 páginas y 3 normas **no** puede guardarse entero como si cada norma
fuera todo el documento.

**Comparación de identidad tolerante donde debe serlo:** el sector y el año se
comparan **solo si ambos lados los traen** — que la ficha no registre `/MINSA`
no contradice al PDF que sí lo trae. Lo que nunca se relaja es tipo y número:
`RM 1000` y `RM 1001` son normas distintas.

---

## 8. El resolvedor que sustituye a `candidatos[0]`

```python
resolver_pdf_para_norma(candidatos, identidad_objetivo) -> ResultadoResolucion
    exactamente 1 exacto          -> MATCH_EXACTO        escribe
    exactamente 1 multinorma      -> MATCH_MULTINORMA    escribe (con rango)
    varios coinciden              -> AMBIGUO             NO escribe
    ninguno coincide              -> NO_ENCONTRADO       NO escribe
    el contenido dice otra norma  -> CONTRADICTORIO      NO escribe
```

`ResultadoResolucion.puede_escribirse` es la puerta: el `UPDATE` de `pdf_url`
solo ocurre si es `True`. Todo lo demás se registra con el detalle de cada
candidato evaluado, para revisión.

El crawler nuevo descarga cada candidato (máx. 6, con 1 s de cortesía entre
descargas), lee sus 12 primeras páginas y decide por evidencia.

---

## 9. Casos obligatorios

**NO VERIFICADO — todos los diagnósticos de contenido.** Este entorno no puede
descargar los PDF: el proxy devuelve 403 contra `supabase.co`,
`digemid.minsa.gob.pe` y El Peruano. Lo que falta exactamente es **ejecutar el
workflow con `tarea: identidad_documental`**, que abre cada PDF y lee sus
encabezados. Lo que sí está probado documentalmente:

| caso | evidencia disponible hoy | qué falta |
|---|---|---|
| **A · LEY-29698 ↔ RM-373-2024** | LEY-29698 **no tiene PDF propio** (`PDF_NO_DISPONIBLE`, 33 páginas guardadas); ambas apuntan a `RM_373-2024-MINSA.pdf`; RM-373-2024 sí tiene copia (1,39 MB) y **33 páginas idénticas** | leer los encabezados del PDF: ¿contiene solo la RM, o también la Ley? |
| **B · DS-9-2015 ↔ DS-10-2015** | **mismo SHA-256**, mismos 195 388 bytes, 4 páginas idénticas, ambas → `DS_009-2015.pdf` | ¿el PDF contiene DS 009, DS 010, ambos (multinorma) u otra cosa? El texto empieza con una autorización de viaje, lo que **no encaja con ninguno de los dos** |
| **C · RM-195-2022 ↔ RM-98-2024** | RM-195-2022 sin PDF propio; ambas → `PERUANO_RM_98-2024-MINSA.pdf`; 2 páginas idénticas | encabezados del PDF |
| **D · DS-24-2018 ↔ DS-30-2023** | comparten `pdf_url` (`DS_030-2023-SA.pdf`) pero **texto distinto**; DS-24-2018 está `PDF_NO_DISPONIBLE` | la hipótesis más probable es que el `pdf_url` de DS-24-2018 se sobrescribió *después* de su extracción — la URL es mutable y no hay historial. **No lo afirmo sin el PDF** |
| **E · RM-1000-2016 ↔ RM-1001-2016** | cada una apunta al archivo de la otra (`RM_1001-2016.pdf` / `RM_1000-2016.pdf`); ambas `COMPLETO` con 4 páginas | confirmar con los encabezados. **Ya es test permanente** (`test_6_caso_real_rm_1000_y_rm_1001_intercambiadas`) |
| **F · DS-6-2001 → LEYN27444.pdf** | el nombre apunta a la Ley 27444 (Procedimiento Administrativo General) | el DS 006-2001 podría estar *dentro* o el PDF ser otra norma |
| **G · RD-19-2014 → RM_376-2023-MINSA.pdf** | tipo y año no encajan | encabezados |
| **H · RM-1194-2021 → PROYECTO_DS_CANNABIS.pdf** | el nombre dice **PROYECTO** | `tipo_de_documento()` ya marca `DOCUMENTO_PROYECTO`: un proyecto **no** es una norma aprobada |

### DS-10-2017

`storage_path = normas/DS-10-2017.pdf` → **404 confirmado**. Pero existe
`normas/DS-10-2017/DS-10-2017.pdf` (606 424 bytes, MD5 `ed0d8e32…`): **la copia
está, el campo apunta mal**. Su relación confirmada `dfb28e22` (deroga
`DS-004-2016`, *"Artículo 2.- Derogatoria Deróguese el Decreto Supremo N°
004-2016-SA"*, cita verificada, **evidencia en la página 1**) sigue como
`RELACION_CONFIRMADA_CON_EVIDENCIA_DOCUMENTAL_PENDIENTE`. **No modifiqué el
storage_path.**

### DS-23-2005 — corrección: no hubo pérdida

| | |
|---|---|
| `process_status` | **`pdf_download_error`** |
| `pdf_url` | `.../Normatividad/2006/DS023-2005.pdf` — coherente con la norma |
| `storage_path` | **null** |
| Drive | **null** |
| páginas | **0** |
| objetos parecidos en Storage | **ninguno** |
| relación confirmada saliente | **`c339bb5a`: deroga DS-14-2002** |
| `derogacion_analizada` | **`false`** |
| `relaciones_analizadas_en` | **`null`** |
| `raw.text_extraction_last_error` | **`HTTP 404`** el `2026-08-07T15:49:58Z` |
| páginas huérfanas / assets / objetos en Storage / backups | **0 / 0 / 0 / 0** |

**Una versión anterior de este documento afirmaba que la relación confirmada
probaba que la norma tuvo texto y lo perdió, y que era «la prueba directa» del
`DELETE` sin transacción. Eso era falso**, y era falso por el peor motivo: se
convirtió una inferencia plausible en un hecho sin buscar la evidencia.

Buscada la evidencia, la refuta:

1. El error registrado es **`HTTP 404` en la descarga**, no un fallo de
   extracción. El PDF nunca llegó a bajarse.
2. `derogacion_analizada = false` y `relaciones_analizadas_en = null`: **el
   detector nunca analizó esta norma**. La relación no puede haber salido de
   un análisis de su texto.
3. El `fragmento_fuente` de la relación lo dice literalmente:

   > «**Registrado manualmente**: el DS N° 023-2005-SA (Art. 2) señala
   > expresamente al DS N° 014-2002-SA entre las normas derogadas (fuente:
   > página de Transparencia de MINSA, **aún no verificado contra el texto
   > propio** de DS-023-2005-SA **porque su PDF no se pudo descargar
   > automáticamente** — process_status pdf_download_error).»

   La registró un humano (`resuelto_por = 185595724`, 2026-08-22) sabiendo
   que el PDF no bajaba y dejándolo anotado.
4. No hay páginas huérfanas, ni assets, ni objetos en Storage con ese nombre,
   ni rastro en los backups del 29-04 y 04-05.

Estado correcto: **`PDF_NO_DISPONIBLE_EN_ORIGEN_404`** +
**`RELACION_DE_FUENTE_EXTERNA_NO_VERIFICADA_CONTRA_PDF`**. Ni siquiera
`HIPOTESIS_COMPATIBLE_CON_PERDIDA_POR_REEXTRACCION`: la hipótesis de pérdida
está descartada por evidencia positiva, no meramente sin respaldo.

Esto **no** exonera al `DELETE` sin transacción que F-01 documentó — ese
defecto sigue estando en el código y sigue siendo real. Lo que ya no puede
decirse es que DS-23-2005 sea su víctima demostrada.

No se tocó nada. La acción pendiente es recuperar el PDF de otra fuente (el
404 es del servidor de DIGEMID) para poder verificar la relación contra el
texto propio.

---

## 10. Las 15 relaciones confirmadas

Cruzadas con el estado documental **de custodia** (identidad de contenido
pendiente del workflow):

| clasificación provisional | n | cuáles |
|---|---:|---|
| `CONFIRMADA_PDF_IDENTIDAD_PENDIENTE` | 12 | tienen PDF y está completo, pero **nadie verificó que sea el PDF de esa norma** |
| `CONFIRMADA_SIN_PDF` | 2 | **DS-23-2005**, RM-49-2025 |
| `CONFIRMADA_PDF_IDENTIDAD_PENDIENTE` (ruta rota) | 1 | DS-10-2017 |
| `CONFIRMADA_PDF_IDENTIDAD_VERIFICADA` | **0** | — |

**Ninguna de las 15 puede llamarse documentalmente respaldada todavía**, porque
"hay un PDF" no es "es el PDF correcto".

---

## 11. Tests — 19 nuevos, 144 en total

`tests/test_identidad_documental.py` cubre los 10 casos exigidos:

1. dos PDF en la página, solo uno coincide → gana el que coincide, **no el primero**
2. dos PDF que coinciden → `AMBIGUO`, no escribe
3. ninguno coincide → `NO_ENCONTRADO`
4. nombre correcto, contenido incorrecto → **rechazar** (caso RM-1000/1001)
5. nombre genérico, contenido correcto → **aceptar**
6. RM-1000 ↔ RM-1001 intercambiadas → detectado en ambas direcciones
7. multinorma con rangos → `MATCH_MULTINORMA` con páginas 3-4, no 1-4
8. proyecto anexado → `DOCUMENTO_PROYECTO`
9. página con varios PDF → no elige el primero
10. OCR del render contradice la capa de texto → `DISCREPANCIA_IDENTIDAD_CRITICA`

Más: encabezado vs cita, un PDF que solo menciona la norma no es esa norma, y
que el nombre del archivo por sí solo **nunca** autoriza a escribir.

---

## 12. Qué falta antes del piloto OCR

En orden:

1. **Fusionar #68** — la capa de identidad canónica sigue sin estar en `main`,
   y F-03 depende de ella.
2. **Ejecutar `tarea: identidad_documental`** (≈10-15 min con OCR de encabezado).
   Eso convierte todos los "NO VERIFICADO" de arriba en diagnósticos reales y
   produce `MATRIZ_IDENTIDAD_DOCUMENTAL.csv` + `PLAN_CORRECCION_DOCUMENTAL.csv`.
3. **Revisar el plan** y decidir qué se corrige.
4. Recién entonces el piloto OCR — **medir la fidelidad de una transcripción
   contra el PDF equivocado no sirve de nada**.

## 13. Qué podrá corregirse automáticamente y qué no

**Automatizable con evidencia inequívoca** (una vez ejecutada la auditoría):

- `DS-10-2017`: la ruta correcta existe y el nombre coincide con la carpeta;
  basta corregir `storage_path`. **Requiere confirmar el encabezado del PDF.**
- Normas cuyo PDF resulte `PDF_IDENTIDAD_EXACTA`: no hay nada que corregir.

**Requiere humano siempre:**

- Los 4 grupos de documento compartido: decidir cuál norma se queda con el PDF
  exige leer ambos documentos.
- Las normas `PDF_IDENTIDAD_CONTRADICTORIA`: hay que **buscar** el PDF correcto,
  no solo desasociar el incorrecto.
- Todo lo que sea `DOCUMENTO_PROYECTO` o `DOCUMENTO_ANEXO`.
- `DS-23-2005`: recuperar la prueba primaria (404 en origen) para verificar
  contra el texto propio una relación registrada a mano desde fuente externa.

## 14. Riesgos residuales

1. La detección de encabezado depende de que el PDF tenga capa de texto legible.
   Para escaneos hace falta el OCR de encabezado (`--con-ocr`), y ahí vuelve a
   aplicar todo lo de F-01 sobre fidelidad.
2. Un PDF **sin** encabezado propio queda `PDF_IDENTIDAD_AMBIGUA` aunque sea el
   correcto. Es el lado conservador del error, y es el que queremos.
3. Corregir el crawler **no repara lo ya escrito**: las asociaciones erróneas
   siguen en la base hasta que se corrijan una por una, con revisión.
4. Quedan **tres rutas más** que escriben `pdf_url` sin validar identidad.

---

## 12. Matriz de TODAS las rutas que pueden escribir `pdf_url`

Auditadas de nuevo tras la corrección. Son **cinco** rutas, no cuatro, y una de
ellas —la número 2— **está corriendo hoy, a diario**.

| # | Archivo | Función | Workflow que la ejecuta | Auto / manual | ¿Puede escribir `pdf_url`? | Cómo elige candidato HOY | ¿Usa identidad objetivo? | ¿Puede escribir ambiguo? | Estado recomendado |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `scripts/crawl_normativa_pdf_urls.py` | `elegir_pdf()` → `.update({"pdf_url": …})` (l. 275) sobre `digemid_normas` | `digemid-normativa-text-simple.yml` | **cron PAUSADO** (#71) + `workflow_dispatch` | **Sí, directo** | `candidatos.sort(prioridad)` → `candidatos[0]`; `sort` es estable, así que entre iguales gana el primero del HTML | **No** — `elegir_pdf(html, base_url)` ni siquiera recibe la norma | **Sí** | **F03-B**: sustituir por `resolver_pdf_para_norma()`. Mantener el cron pausado hasta que F03-B esté fusionado y validado |
| 2 | `agents/agent_normative_pdf_detector.py` | `detect_pdf()` → `update_document()` (l. 278) sobre `digemid_documentos` | `digemid-normativa.yml` → `run_normative_pipeline.py --phase all` | **AUTOMÁTICO — cron `0 13 * * *` ACTIVO** | Escribe `file_url`, no `pdf_url` — **otra tabla**, mismo defecto | `score_pdf_link()` → `sort(reverse=True)` → `candidate_links[0]` | **No** — puntúa el enlace, no lo compara con la norma | **Sí** | **F03-B**: aplicar la misma política. Decisión tuya si además se pausa el cron mientras tanto — **no lo he tocado** |
| 3 | `scripts/crawl_digemid_normativa_inventory.py` | construcción del inventario (l. 451) | ninguno | Manual | **No escribe en BD** — produce `digemid_normativa_inventory.json` | `pdf_url = pdf_urls[0]`, y guarda además `pdf_urls` completo | **No** | Sí, en el JSON | **F03-B**: dejar de emitir `pdf_url` y emitir solo `pdf_urls`; que la decisión la tome quien sí conoce la norma objetivo |
| 4 | `scripts/import_normativa_inventory_to_supabase.py` | `build_payload()` → `.insert()` (l. 626) sobre `digemid_normas` | ninguno | Manual (`--input`) | **Sí, en el INSERT inicial** | hereda el `pdf_url` ya elegido por la ruta 3 | **No** | **Sí** | **F03-B**: insertar con `pdf_url = NULL` + `pdf_url_candidatos`, y dejar que la resolución por identidad lo complete |
| 5 | `scripts/seed_digemid_normas_minimas.py` | `.insert()` (l. 136) sobre `digemid_normas` | ninguno | Manual | **Sí, en el INSERT inicial** | lee `data/normativa_seed_minima.json`, escrito a mano | N/A (curado humano) | No en la práctica | **Mantener**. Es la única ruta cuya procedencia es humana y trazable. Documentar que el seed es fuente curada |

Rutas que **no** escriben `pdf_url` pero se confundieron con ellas en revisiones
anteriores: `extract_normativa_text_simple.py` escribe `file_storage_path`
(l. 676) y mete el `pdf_url` fallido dentro de `raw.text_extraction_last_error`
(l. 737) — es metadato de error, no una elección de documento.

**Ninguna ruta se ha retirado ni modificado en F03-A.** La propuesta de retiro
—rutas 3 y 4 en su forma actual— queda registrada aquí y se ejecuta, si lo
apruebas, en F03-B.

### La ruta 2 merece un aviso aparte

El cron de `digemid-normativa.yml` está **activo** y su propio comentario dice
que corre a las 13:00 UTC «para que las normas nuevas del día ya tengan
`pdf_url` a tiempo». Escribe sobre `digemid_documentos`, que alimenta el canal
de alertas, no la base normativa; por eso el pausado de #71 no lo cubrió. **No
lo he pausado**: hacerlo detiene las alertas diarias, y esa es una decisión de
producto, no de auditoría.

---

## 13. Los límites de la auditoría ya no pueden fabricar conclusiones

El riesgo que señalaste es real y estaba en el código: `MAX_PAGINAS_ANALIZADAS
= 40` y `MAX_CANDIDATOS = 6` truncaban en silencio, y después el resolvedor
devolvía `NO_ENCONTRADO` — convirtiendo «no miré» en «no está».

Lo corregido:

- **No hay tope de páginas.** La capa de texto embebida se extrae del documento
  **entero**. El encabezado de una norma en una edición de El Peruano suele
  estar en medio, no al principio.
- **El único corte posible es temporal** (`SEGUNDOS_POR_DOCUMENTO = 90`), y
  cuando ocurre el documento sale clasificado `PDF_AUDITORIA_INCOMPLETA` con
  el detalle de cuántas páginas se leyeron de cuántas.
- **Truncar candidatos se propaga.** `resolver_pdf_para_norma()` recibe
  `candidatos_omitidos`; si es distinto de cero el resultado es
  `AUDITORIA_INCOMPLETA`, nunca `NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA`.
  Si la página lista 20 PDF, se auditan los 20 o se declara incompleta.
- **OCR solo donde hace falta**: primera página sin capa de texto, o texto
  presente pero sin ningún encabezado propio. En cualquier otro caso el render
  sirve para *confirmar*, no para decidir. Si el render haría falta y está
  desactivado, eso también marca la auditoría como incompleta.

### Estados

| Estado | Significa | ¿Autoriza a escribir? |
|---|---|---|
| `PDF_IDENTIDAD_EXACTA` | leído entero; el único encabezado es la norma objetivo | sí |
| `PDF_CONTIENE_NORMA_EN_MULTINORMA` | contiene la objetivo entre otras | solo con rango cerrado y con evidencia |
| `PDF_IDENTIDAD_CONTRADICTORIA` | leído entero; los encabezados son de otras normas | no |
| `PDF_IDENTIDAD_AMBIGUA` | evidencia insuficiente o render que contradice al texto | no |
| `PDF_NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA` | leído entero; no hay ningún encabezado normativo | no |
| `PDF_AUDITORIA_INCOMPLETA` | **no se terminó de mirar**: no concluye nada | no |
| `PDF_NO_DISPONIBLE` | no hay documento que analizar | no |
| `PDF_CORRUPTO` | el PDF no abre | no |

Un hallazgo **positivo** sobrevive a la incompletitud: si se vio el encabezado
de la norma, el documento la contiene, se haya leído el resto o no. Lo que la
incompletitud impide es afirmar **exclusividad** (`EXACTA`) y cualquier
conclusión **negativa**.

### El rango de un multinorma lleva su prueba encima

`SegmentoMultinorma` carga `pdf_sha256`, `identity`, `start_page`, `end_page`,
**`evidencia_inicio`** y **`evidencia_fin`**. Un rango sin evidencia no es
verificable por un humano, y un rango cuyo final es «el final del documento»
solo vale si alguien leyó hasta el final: si no, `rango_completo = False` y no
se escribe.
