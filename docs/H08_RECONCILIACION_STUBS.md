# H-08 · Reconciliación de stubs e identidad de punta a punta — DIAGNÓSTICO + DRY-RUN

**Esta fase NO modifica datos.** Todo lo ejecutado contra producción fue `SELECT`.
`--apply` no se ejecutó y además está **bloqueado en el código**. El cron sigue
pausado. No hubo backfill.

---

## 1. Inventario de stubs

No se confió solo en `process_status = 'stub_derogada'`: se buscó por
`process_status LIKE 'stub%'` **y** por la convención de clave `NORM-%` que usaba
el bot cuando no podía construir un `document_key` con tipo+número+año.

Corpus: **342 normas**. `process_status` presentes:

| process_status | n | ¿es stub? |
|---|---|---|
| `text_extracted` | 290 | no |
| `pdf_download_error` | 41 | no — **norma real sin PDF** |
| `stub_derogada` | **5** | **sí** |
| `drive_structured` | 3 | no — curadas a mano |
| `text_extracted_baja_calidad` | 2 | no |
| `inventory_imported` | 1 | no — norma real sin PDF |

**No hay variantes históricas adicionales**: `stub_derogada` es el único valor
que empieza por `stub`, y solo un registro usa el prefijo `NORM-`.

**Distinción crítica:** "sin páginas" **no** es sinónimo de stub. `DS-014-2011-SA`
y `DS-016-2011-SA` tienen 0 páginas y son `drive_structured`, pero son normas
reales curadas con **14 y 13 relaciones entrantes**. Y las 42 con
`pdf_download_error`/`inventory_imported` son normas reales identificadas a las
que solo les falta el documento — 5 de ellas ya son destino de relaciones
(`RM-13-2009` ×3, `DS-17-2006`, `RD-182-2007`, `RD-760-2001`, `RM-2-2001`).
Tratarlas como stubs sería el error inverso al que persigue H-08. Aparecen en la
matriz como `NO_ES_STUB_REAL`, sin acción.

### Los 5 stubs

| stub | tipo/número/año | vigencia | relación entrante | origen | estado |
|---|---|---|---|---|---|
| `DS-004-2016` | DS·004·2016 | derogada | deroga | `DS-10-2017` | confirmada |
| `LEY-29698-ART9` | LEY·29698·— | derogada | deroga art. 9 (parcial) | `LEY-32319-2025` | confirmada |
| `NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT` | LEY·29459·— | vigente | exonera arts. 10 y 11 (parcial) | `LEY-32319-2025` | confirmada |
| `RD-354-99-DG-DIGEMID-1999` | RD·354·1999 | derogada | deroga | `RD-144-2016` | confirmada |
| `RD-993-99-DG-DIGEMID-1999` | RD·993·1999 | derogada | deroga | `RD-144-2016` | confirmada |

Cada uno tiene **exactamente una** relación entrante, y **las cinco están
confirmadas** por el administrador. No hay ninguna rechazada apuntando a stubs.

Matriz completa: `docs/reportes/MATRIZ_RECONCILIACION_STUBS.csv` y `.json`.

---

## 2. Una sola identidad canónica

`scripts/reconciliar_stubs_normativos.py` **no implementa ningún resolvedor**:
importa `resolver_identidad`, `identidad_de_norma`, `normalizar_articulos` y
`clave_dedupe` de `scripts/identidad_normativa.py`.

Resultado del DRY-RUN:

| stub | clasificación | norma real | acción |
|---|---|---|---|
| `NORM-LEY-29459-…` | **MATCH_EXACTO_UNICO** | `LEY-29459` | proponer re-apuntar |
| `LEY-29698-ART9` | **STUB_QUE_REPRESENTA_UNIDAD_PARCIAL** | `LEY-29698` | **no fusionar** (H-09) |
| `DS-004-2016` | SIN_NORMA_REAL | — | conservar |
| `RD-354-99-DG-DIGEMID-1999` | SIN_NORMA_REAL | — | conservar |
| `RD-993-99-DG-DIGEMID-1999` | SIN_NORMA_REAL | — | conservar |

**1 reconciliable, 4 requieren humano.** Ninguna candidata se eligió
arbitrariamente: un stub jamás se reconcilia contra otro stub, y ante varias
candidatas la clasificación es `IDENTIDAD_AMBIGUA` sin destino.

Dos reglas que hubo que afinar durante el DRY-RUN:

1. **Identidad y alcance son cosas distintas.** Mi primera versión marcaba como
   "unidad parcial" cualquier stub cuya relación fuera parcial — y eso bloqueaba
   `NORM-LEY-29459`, cuya relación es parcial pero cuyo registro representa la
   ley entera. Ser "una parte" es propiedad **del stub** (su clave `-ART9`, su
   título "artículo 9 de la Ley 29698"), no de la relación.
2. **Para una Ley, tipo+número ya es la identidad completa.** La numeración de
   las leyes es única a nivel nacional y no se reinicia cada año, a diferencia de
   DS/RM/RD. Está declarado en `config/identidad_normativa.spec.json`
   (`tipos_numero_unico_nacional: ["LEY", "DL"]`), no incrustado en el código.

---

## 3. Caso obligatorio LEY 29459

| dato | valor |
|---|---|
| id del stub | `c80418ab-c4e7-49f7-bf4a-ccf6053684f3` |
| clave del stub | `NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT` |
| id de la ley real | `ea3c9b32-8388-4cad-bd8d-201d3bda2e47` |
| clave de la ley real | `LEY-29459` |
| relaciones que apuntan al stub | **1** |
| relación confirmada | `c04771be-2416-4073-9a2d-bafeff10c06a` |
| — origen | `LEY-32319-2025` |
| — tipo | `exonera` · alcance **parcial** · artículos **10 y 11** |
| — cita verificada | sí |
| `norma_afectada_id` correcto | **`ea3c9b32-8388-4cad-bd8d-201d3bda2e47`** (`LEY-29459`) |
| `estado_vigencia` del stub | `vigente` |
| `estado_vigencia` de la ley real | `vigente` |
| relaciones que apuntan hoy a `LEY-29459` | **0** |

**Cómo quedaría tras reconciliar:** la relación pasa a apuntar a `LEY-29459`;
el stub queda como **alias** (`reconciliado_con_id = ea3c9b32…`), no se borra;
**ninguna vigencia cambia** — `exonera` no altera la vigencia y además el
alcance es parcial.

### Qué se está perdiendo hoy en `/consulta` — y qué NO arregla esto

Lo que **sí** se pierde y sí se recupera: la exoneración está colgada de un
registro fantasma cuya clave nadie escribiría nunca. Cualquier consulta que
resuelva "Ley 29459" llega a `LEY-29459`, que hoy tiene **cero** relaciones. Y
mientras coexistan las dos filas, la capa de identidad declara `IDENTIDAD_AMBIGUA`
y **bloquea la resolución automática de citas futuras** — ya está pasando:
`LEY-32033 → "Ley 29459"` (`68b24cd5`, pendiente) sigue sin vincular por eso.

**NO VERIFICADO / lo que la reconciliación NO arregla:** revisé la RPC
`buscar_paginas_texto` y **no consulta `digemid_norma_relaciones` en absoluto**.
Solo anota el `estado_vigencia` de la norma dueña de las páginas recuperadas.
Además `LEY-29459` tiene **0 páginas y 0 chunks**: su texto no está en la base.
Por lo tanto, reconciliar **no** hará que `/consulta` avise de la exoneración de
los artículos 10 y 11: eso exige (a) cargar el texto de la Ley 29459 y (b)
enriquecer `/consulta` con las relaciones — es H-10, no H-08.

---

## 4. Caso LEY-29698-ART9

**Veredicto: (B) es el artículo 9 modelado erróneamente como norma independiente.**

Evidencia:

- `document_key = LEY-29698-ART9`; `tipo_norma = LEY`, `numero = 29698`, `anio = NULL`.
- `titulo = "artículo 9 de la Ley 29698 incorporado en la Ley 31738"` — describe una
  parte, no una norma.
- Existe por separado `LEY-29698` (`2f41afd7-…`, `text_extracted`, **33 páginas**,
  `estado_vigencia = vigente`).
- La relación `26c8b6aa-…` (confirmada, `deroga`, **alcance parcial**,
  `articulos_afectados = "9"`, cita verificada: *"Deróguese el artículo 9 de la
  Ley 29698 incorporado en la Ley 31738"*).

El stub está marcado **`derogada`** y la ley real **`vigente`** — que es lo
correcto hoy. Por eso el script clasifica `STUB_QUE_REPRESENTA_UNIDAD_PARCIAL` y
**no propone fusión**: re-apuntar la relación a `LEY-29698` sin modelar el
alcance dejaría una derogación *total* colgando de una ley que sigue vigente en
todo lo demás. **No se borró, no se fusionó, no se tocó.** Queda preparado para
H-09.

**NO VERIFICADO:** si el artículo 9 de la Ley 29698 fue *incorporado* por la Ley
31738 y luego derogado por la Ley 32319, la cadena completa requiere las tres
normas en la base. La Ley 31738 **no está en el corpus**. No completo esa cadena
por inferencia.

---

## 5. Diagnóstico del creador de stubs de Telegram

`resolverRelacionDerogacion()` en `supabase/functions/telegram-bot/index.ts`.
Cinco defectos, todos reproducibles:

1. **Buscaba por `document_key` armado a mano, no por identidad.**
   `construirDocumentKeyStub(tipo, numero, anio)` exigía los tres campos; al citar
   una ley (*"Ley 29459"*, sin año) caía a un **slug de la descripción del LLM** →
   `NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT`. Como esa clave no existía,
   **creaba el stub aunque `LEY-29459` ya estuviera en la base**. Ese es el origen
   exacto del stub actual.
2. **La clave dependía de la redacción del modelo** → dos redacciones distintas
   de la misma norma producían dos stubs distintos. No idempotente.
3. **`tipoNorma.toUpperCase()` y `numero` sin normalizar** → `"Decreto Supremo"`
   generaba `DECRETO SUPREMO-014-2011` y `"DS"` generaba `DS-014-2011`; `"014"` y
   `"14"` daban claves distintas para la misma norma.
4. **Sin guarda de ambigüedad**: nunca comprobaba si había varias candidatas.
5. **Escribía `estado_vigencia` global sin mirar el alcance**: `deroga` ponía
   `derogada` aunque la afectación fuera de un solo artículo. Así nació
   `LEY-29698-ART9` marcado `derogada`.

### Corregido (código, **sin desplegar**)

La regla se extrajo a `supabase/functions/telegram-bot/decision_stub.ts`
(`decidirVinculoNorma`), que es pura y por eso testeable sin base de datos:

| situación | acción |
|---|---|
| existe norma real única | **vincular**, no crear stub |
| varias candidatas | **abortar**, explicar y listar candidatas |
| sin número / sin tipo canónico | **abortar** |
| cita no verificada textualmente | **abortar**, no dar de alta una norma sin evidencia |
| no existe la norma + cita verificada | **crear stub** con clave canónica |
| relación ya vinculada | **no hacer nada** (idempotente) |
| afectación parcial | **nunca** escribir `estado_vigencia` global |

Los stubs viejos se excluyen del catálogo al resolver, para que un stub no
vuelva ambigua una cita que sí tiene norma real.

⚠️ **La Edge Function NO se ha desplegado.** El cambio de vigencia parcial altera
el comportamiento del bot y requiere tu autorización.

---

## 6. Arquitectura de identidad compartida Python ↔ TypeScript

Evalué tres opciones:

| opción | ventaja | por qué no |
|---|---|---|
| **RPC/función SQL canónica** | un solo motor real | la lógica en plpgsql es mucho más difícil de testear y versionar; Python perdería la resolución offline (hoy carga el catálogo una vez y resuelve en memoria) y pasaría a un round-trip por relación. Sigue siendo el destino natural si el corpus crece un orden de magnitud. |
| **Duplicar a mano** | ninguna | dos fuentes de verdad divergentes: exactamente el problema. |
| **✅ Spec declarativa + motores delgados + fixtures de paridad** | los datos que cambian (tipos de norma, pivote de año, tipos de numeración única) viven en **un solo archivo**; el algoritmo es pequeño y queda **congelado por tests** | requiere disciplina de regenerar el `.ts`, que la suite verifica |

Implementación elegida:

```
config/identidad_normativa.spec.json          ← ÚNICA fuente de los datos
   ├─ scripts/identidad_normativa.py           lo lee en tiempo de ejecución
   └─ scripts/generar_spec_identidad_ts.py  →  supabase/functions/telegram-bot/identidad_spec.generated.ts
                                                 (generado y versionado; --check falla si se desincroniza)

tests/fixtures/identidad_casos.json            ← MISMOS casos para los dos motores
   ├─ Python  → tests/test_paridad_identidad.py
   └─ TS      → tests/paridad_identidad_ts.ts
                          ↓
             salida comparada campo por campo
```

`tests/test_paridad_identidad.py` ejecuta 12 casos de tipo, 6 de identidad, 15 de
resolución y 10 de deduplicación contra **ambos** motores y falla si difieren en
uno solo. Agregar un tipo de norma nuevo se hace en el JSON; si alguien edita el
`.ts` a mano, la suite lo detecta.

**Paridad verificada en este entorno con Node 22.** Si no hay runtime de
JavaScript, el test **no pasa en silencio**: se salta con el mensaje
`PARIDAD NO VERIFICADA`.

---

## 7. Reglas de reconciliación aplicadas

- **La vigencia del stub NUNCA se traslada.** Se evalúa de cero:
  parcial → `no_tocar`; `exonera`/`prorroga`/`pendiente_verificacion` → `no_tocar`;
  total sobre una norma con vigencia distinta → `requiere_humano` (nunca automático).
- Identidad, tipo de relación, alcance y unidades afectadas se evalúan por separado.
- Ante duda: `REQUIERE_REVISION_HUMANA`.

En el DRY-RUN actual, **las 5 filas dan `accion_sobre_vigencia = no_tocar`**.
Ninguna reconciliación propuesta cambia el estado de ninguna norma.

---

## 8. Relaciones que apuntan a stubs

Las 5 son **confirmadas** (0 pendientes, 0 rechazadas — el historial de
rechazadas no se ve afectado por esta fase).

| relación | ¿el stub duplica una norma real? | norma real | ¿la relación jurídica sigue siendo correcta? | ¿solo cambia `norma_afectada_id`? | ¿cambia alcance/objeto? | ¿afecta vigencia? |
|---|---|---|---|---|---|---|
| `c04771be` (exonera, Ley 32319) | **sí** | `LEY-29459` | sí — cita verificada | **sí** | no | **no** |
| `26c8b6aa` (deroga art. 9, Ley 32319) | parcialmente: el stub es *el artículo*, no la ley | `LEY-29698` | sí — cita verificada | **no**: exige modelar el alcance primero | sí (H-09) | **no** |
| `dfb28e22` (deroga, DS-10-2017) | no | — | sí | no aplica | no | no |
| `9eea3d74` (deroga, RD-144-2016) | no | — | sí | no aplica | no | no |
| `251371f5` (deroga, RD-144-2016) | no | — | sí | no aplica | no | no |

---

## 9. Script

`scripts/reconciliar_stubs_normativos.py` — **DRY-RUN por defecto**.

```
python scripts/reconciliar_stubs_normativos.py                    # DRY-RUN (producción, solo SELECT)
python scripts/reconciliar_stubs_normativos.py --desde-json dir/  # DRY-RUN offline reproducible
python scripts/reconciliar_stubs_normativos.py --apply            # BLOQUEADO a propósito
```

Sin `--apply` no ejecuta una sola escritura. Con `--apply` **aborta con un
mensaje explícito**: habilitarlo requiere tu autorización y aplicar antes la
migración de trazabilidad. Imprime ANTES / PROPUESTA con todos los campos
pedidos y escribe la matriz en CSV y JSON.

**Cómo se ejecutó aquí:** este entorno no tiene credenciales de Supabase, así
que el DRY-RUN corrió con `--desde-json` sobre un snapshot de solo lectura
extraído por `SELECT` (66 normas: los 5 stubs, todos los destinos de relaciones
y todas las normas que comparten número con un stub; 16 relaciones: las 5 de los
stubs, las de sus mismos orígenes y los 6 candidatos a colisión). Ese
subconjunto es **suficiente** para el análisis de stubs: una candidata solo puede
serlo si comparte el número, y todas están incluidas.

---

## 10. No borrar historial

`supabase/sql/2026_08_24_trazabilidad_reconciliacion_stubs.sql` — **escrito, NO
aplicado**. Añade `reconciliado_con_id`, `reconciliado_en`, `reconciliado_por`,
`reconciliador_version`, `motivo_reconciliacion` en `digemid_normas`, y
`norma_afectada_id_anterior` + los mismos campos de auditoría en
`digemid_norma_relaciones`. El stub sobrevive como **alias**; el destino anterior
de cada relación queda registrado. El archivo incluye el SQL de rollback.

---

## 11. Colisiones de `clave_dedupe`

`clave_dedupe` sigue **NULL en las 148 relaciones históricas**. No se rellenó nada.

Para no perder ninguna colisión, se usó un prefiltro **superconjunto estricto** en
SQL — agrupar por `origen + tipo_relacion + dígitos del número citado`, que la
clave canónica siempre incluye — y la decisión final la tomó el motor canónico.
De las 148 relaciones salieron **3 grupos candidatos**:

| grupo | veredicto |
|---|---|
| `DS-12-2016 deroga` nº 16 | **no colisiona**: una afecta a `DS-016-2013-SA` y la otra a `DS-016-2011-SA` — años distintos, identidades distintas |
| `DS-15-2025 modifica` nº 14 | **colisionaba** — ver abajo |
| `RM-680-2021 modifica` nº 1053 | **colisionaba** — ver abajo |

**Los dos casos reales son "parecen iguales pero afectan partes distintas", no duplicados:**

- `DS-15-2025` → `DS-014-2011-SA`, **dos relaciones confirmadas**: una modifica el
  **artículo 43** del Reglamento, la otra la **infracción Nº 30 del Anexo 01**.
  Ambas con `articulos_afectados = NULL`.
- `RM-680-2021` → `RM-1053-2020`, **dos pendientes**: una modifica el **artículo 2**
  de la RM, la otra los **numerales 6.2.5.13 y 6.2.6.3** del Documento Técnico que
  esa RM aprobó. Ambas con `articulos_afectados = NULL`.

Con la clave tal como quedó en H-07, ambas parejas colapsaban en **una sola
clave** — es decir, rellenar `clave_dedupe` hoy habría **borrado silenciosamente
una relación jurídica confirmada** (o el índice único habría rechazado la
segunda). **Este es el hallazgo más importante de esta fase.**

**Corregido:** cuando no hay unidades explícitas, la clave incorpora un
discriminador del **objeto** afectado (`obj:` + descripción normalizada). Se
acepta a cambio el riesgo contrario —que una redacción distinta genere un
duplicado **visible**— porque un duplicado se revisa y se descarta, mientras que
una relación perdida no se ve nunca. Los cuatro casos reales están como test de
regresión en ambos motores.

Tras la corrección: **0 colisiones** en el DRY-RUN.

**Causa de fondo (no corregida aquí):** el detector no extrajo `articulos_afectados`
aunque el fragmento dice "artículo 43" y "numerales 6.2.5.13 y 6.2.6.3". Eso es
H-09.

---

## 12–13. Casos de prueba y suite

**84 tests · OK** (56 al empezar esta fase → 67 con paridad → 84 con las reglas
del bot y de reconciliación). Ningún test eliminado ni relajado.

| caso exigido | dónde |
|---|---|
| A · Ley 32319 → Ley 29459 resuelve a la real | `test_paridad_identidad`, `test_reconciliacion_stubs` |
| B · Ley 32033 → Ley 29459 usa la misma identidad | `test_paridad_identidad` |
| C · derogación parcial no marca la ley entera | `test_decision_stub`, `test_reconciliacion_stubs` |
| D · `DS 02-92-SA` sigue sin resolver | `test_paridad_identidad` (`NORMA_NO_ENCONTRADA`) |
| E · `RM 097-2000-SA/DM` → `RM-97-2000` | `test_paridad_identidad` |
| F · stub + norma real única → usar la real | `test_decision_stub`, `test_reconciliacion_stubs` |
| G · stub + dos candidatas → `IDENTIDAD_AMBIGUA` | `test_decision_stub`, `test_reconciliacion_stubs` |
| H · datos insuficientes → no crear stub | `test_decision_stub` |
| I · confirmación/reconciliación repetida → idempotente | `test_decision_stub`, `test_reconciliacion_stubs` |
| preservación de confirmadas y rechazadas | `test_reconciliacion_stubs` |
| paridad Python/TypeScript | `test_paridad_identidad` |

**Corrección respecto a lo que te reporté en la fase anterior:** dije que
`DS-13-2002 → "DS N° 02-92-SA"` era `IDENTIDAD_AMBIGUA`. Ese dato venía de mi
réplica en SQL, no del motor canónico. El motor real lo clasifica
**`NORMA_NO_ENCONTRADA`**: `"02-92-SA"` se interpreta como DS·2·**1992**·SA y en
el corpus no hay ningún DS de 1992. El efecto práctico es el mismo —no se
resuelve automáticamente— pero el motivo es distinto y ahora está fijado por test.

---

## 14. Plan exacto de cambios en producción (pendiente de tu autorización)

Nada de esto se ha ejecutado.

1. **Desplegar la Edge Function** con la nueva decisión de stubs. Efecto: el bot
   deja de crear duplicados y deja de marcar vigencias globales por afectaciones
   parciales. *No cambia ningún dato existente.*
2. **Aplicar** `2026_08_24_trazabilidad_reconciliacion_stubs.sql` (aditiva, nullable).
3. **Ejecutar 1 sola reconciliación**, la única con confianza suficiente:
   ```
   UPDATE digemid_norma_relaciones
      SET norma_afectada_id = 'ea3c9b32-8388-4cad-bd8d-201d3bda2e47',   -- LEY-29459
          norma_afectada_id_anterior = 'c80418ab-c4e7-49f7-bf4a-ccf6053684f3'
    WHERE id = 'c04771be-2416-4073-9a2d-bafeff10c06a';

   UPDATE digemid_normas
      SET reconciliado_con_id = 'ea3c9b32-8388-4cad-bd8d-201d3bda2e47'
    WHERE id = 'c80418ab-c4e7-49f7-bf4a-ccf6053684f3';
   ```
   **Sin tocar ninguna vigencia** y **sin borrar el stub**.
4. Verificar que `LEY-29459` pasa de 0 a 1 relación entrante y que
   `LEY-32033 → "Ley 29459"` deja de ser ambigua.
5. Los otros 4 stubs: **no se tocan**.

**Rollback:** dos `UPDATE` que devuelven los campos a su valor anterior (SQL
completo en el archivo de migración). Como el stub nunca se borra y ninguna
vigencia cambia, la reversión es exacta.

---

## 15. Riesgos residuales

1. **`clave_dedupe` sigue NULL en lo histórico.** Rellenarla exige antes extraer
   `articulos_afectados` de los fragmentos; si no, el discriminador de objeto es
   lo único que separa dos afectaciones distintas, y depende de la redacción del
   modelo.
2. **El discriminador de objeto puede generar duplicados visibles** si el modelo
   redacta la misma afectación de dos formas muy distintas y no extrae artículos.
   Es el riesgo que se eligió a cambio de no perder relaciones.
3. **La Edge Function no está desplegada**: hasta que lo esté, el bot sigue
   creando stubs con la lógica vieja. **El cron no debe reactivarse antes.**
4. **`LEY-29698-ART9` sigue sin modelar** (H-09). Mientras tanto, una consulta
   sobre la Ley 29698 no verá que su artículo 9 fue derogado, porque esa
   información vive en un registro aparte.
5. **`/consulta` no lee relaciones** y `LEY-29459` no tiene texto cargado: la
   reconciliación no arregla eso (H-10).
6. **El DRY-RUN corrió sobre un snapshot**, no sobre las 342 normas en vivo. El
   subconjunto es suficiente para los stubs (toda candidata comparte número), y
   el análisis de colisiones sí barrió las 148 relaciones vía prefiltro
   superconjunto. Ejecutar el script con credenciales reproduce ambos resultados.
7. **`fragmento_verificado = false` en varias relaciones pendientes**
   (`LEY-32033 → Ley 29459`, `RM-132-2015 → RM 097-2000`): su identidad se resuelve,
   pero la **evidencia** no está verificada. Identidad resuelta ≠ relación jurídica
   comprobada.
