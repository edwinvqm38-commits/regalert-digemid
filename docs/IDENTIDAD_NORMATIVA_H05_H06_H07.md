# Capa de IDENTIDAD NORMATIVA — cierre de H-05, H-06 y H-07

Fase ejecutada bajo restricciones estrictas: cron del detector **PAUSADO**,
**sin backfill masivo**, **sin tocar** relaciones confirmadas/rechazadas ni
`estado_vigencia`. Lo único escrito en producción fue una migración
**estructural aditiva** (columnas nullable + índice único parcial), justificada
más abajo.

## 1. Diagnóstico

| Hallazgo | Causa raíz real |
|---|---|
| H-05 | `digemid_normas.tipo_norma` guarda a la vez `"DS"` y `"Decreto Supremo"`, `"RM"` y `"Resolución Ministerial"`, `"LEY"` y `"Ley"`. El detector comparaba con la abreviatura que devuelve la IA, así que el filtro por tipo **nunca coincidía**; se optó por eliminarlo (PR #55) y esa supresión se convirtió en la causa de H-06. |
| H-06 | Sin tipo, la identidad se resolvía por **número + año**: `RM 150-2025` y `RD 150-2025` son la misma clave. Además, con año nulo (frecuente en leyes: *"Ley 29459"*) la búsqueda fallaba siempre → se creaba un stub. Y ante varias filas se devolvía **la primera**. |
| H-07 | La deduplicación comparaba la **descripción libre** del LLM. La misma relación jurídica redactada distinto entre corridas insertaba un duplicado, lo que hacía inseguro `--force` y el rebackfill. |

Los tres son **el mismo problema**: no existía una definición única de "qué norma
es ésta". Por eso se resolvieron como una sola capa.

## 2. Arquitectura implementada

`scripts/identidad_normativa.py` — módulo único, sin red, sin dependencias del
detector. Todo lo que decide identidad vive ahí.

```
cita del LLM (tipo, numero, anio)
      │
      ├─ normalizar_tipo_norma()  →  abreviatura canónica o None (nunca inventa)
      ├─ construir_identidad()    →  NormaIdentity(tipo, numero, anio, sector)
      │                              extrae año y sector embebidos: "014-2011-SA" → DS·14·2011·SA
      │                                                             "354-99-DG-DIGEMID" → RD·354·1999·DG-DIGEMID
      └─ resolver_identidad(cita, catálogo)
             N1  tipo + número + año + sector        confianza alta
             N2  tipo + número + año                 confianza alta
             N4  tipo + número (SIN año)             confianza media   ← caso "Ley 29459"
             N3  número + año (SIN tipo)             confianza media
             ─────────────────────────────────────────────────────────
             >1 candidata en cualquier nivel  →  IDENTIDAD_AMBIGUA (norma = None)
             0 candidatas                     →  NORMA_NO_ENCONTRADA
             sin número                       →  DATOS_INSUFICIENTES
```

Decisiones que importan jurídicamente:

- **Nunca se elige "la primera" candidata.** Si hay dos, la relación se guarda
  sin `norma_afectada_id` y con la lista de candidatas para que la resuelva un
  humano. Ante la duda, no vincular.
- **N4 se evalúa ANTES que N3.** Con el tipo disponible, `tipo+número` es más
  seguro que `número+año` sin tipo. Y se aplica **solo si la cita no trae año**:
  sin esa guarda, una cita de `RM 339-2023` podía enlazarse a `RM-339-2024`.
  (Este error existió en mi primera réplica SQL del DRY-RUN; el módulo Python
  nunca lo tuvo. Es exactamente el argumento para tener **una sola**
  implementación.)
- **`document_key` no es identidad.** Los históricos no son homogéneos
  (`DS-14-2002` y `DS-008-2025-SA` conviven), así que la identidad se recompone
  siempre desde los campos `tipo_norma / numero / anio`.
- **Tipos jurídicamente distintos jamás se fusionan** (RM ≠ RD, DS ≠ DL), aunque
  compartan número y año.

### Deduplicación (H-07)

```
clave = origen_id :: tipo_relacion :: identidad_afectada :: artículos_normalizados
```

- El **fragmento no participa**: es evidencia, no identidad. Si participara, un
  cambio de redacción del modelo crearía un duplicado — que es el bug H-07.
- Los artículos se normalizan a conjunto ordenado: `"artículos 10 y 11"`,
  `"arts. 10 y 11"` y `"10, 11"` dan la misma clave; `art. 10` y `art. 12` no.
  Se conservan numerales compuestos (`5.1.4`).
- La identidad usada es la de la **norma real** cuando se resolvió, y la de la
  cita cuando no. Así `"Ley 29459"` y `"Ley N° 29459 (2009)"` convergen.

## 3. Archivos

| Archivo | Cambio |
|---|---|
| `scripts/identidad_normativa.py` | **nuevo** · capa canónica completa |
| `scripts/detectar_derogaciones_normativa.py` | `buscar_norma_afectada()`, `construir_document_key_candidato()`, `normalizar_numero()` y `relacion_ya_registrada()` **eliminados** y sustituidos por la capa; el catálogo se carga una vez por corrida (sin verlo entero era imposible detectar ambigüedad) |
| `scripts/dryrun_identidad_relaciones.py` | **nuevo** · DRY-RUN solo lectura sobre TODAS las relaciones |
| `tests/test_identidad_normativa.py` | **nuevo** · 26 tests |
| `tests/test_detector_relaciones.py` | +6 tests (idempotencia real de `procesar_norma`, ambigüedad, caso D) |
| `supabase/sql/2026_08_24_add_identidad_normativa_relaciones.sql` | migración estructural |

## 4. Migración (única escritura en producción)

Aditiva y reversible: `clave_dedupe`, `identidad_nivel`, `identidad_confianza`,
`identidad_candidatas` (todas nullable) + índice **único parcial**
`WHERE clave_dedupe IS NOT NULL`.

`clave_dedupe` queda **NULL en todo lo histórico a propósito**: rellenarla sería
escribir sobre relaciones ya confirmadas, no autorizado en esta fase. El índice
parcial convive con esos NULL y sólo impone unicidad sobre lo que escriba el
detector desde ahora; la deduplicación contra lo histórico la hace el detector
**en memoria**, recomputando la clave de las filas antiguas sin escribirlas.

## 5. Estado del corpus (DRY-RUN, solo lectura)

**42 relaciones sin `norma_afectada_id`:**

| Resultado | N |
|---|---|
| `NORMA_NO_ENCONTRADA` (la norma no está en la base) | 38 |
| `IDENTIDAD_AMBIGUA` | 2 |
| `DATOS_INSUFICIENTES` | 1 |
| **resolubles automáticamente con seguridad** | **1** (`RM-132-2015` → `RM-97-2000`) |

**106 relaciones ya vinculadas, reauditadas con el resolvedor nuevo:**

| Resultado | N |
|---|---|
| `RESUELTA_TIPO_NUMERO_ANIO` | 101 |
| `RESUELTA_NUMERO_ANIO` | 3 |
| `IDENTIDAD_AMBIGUA` | 2 |
| **`DISCREPANCIA_IDENTIDAD`** (apunta a una norma distinta de la que elegiría el resolvedor) | **0** |

Cero discrepancias: la capa nueva **no contradice ninguna** relación ya
confirmada por el humano. No se modificó ninguna.

### Casos que requieren decisión humana (3)

1. `DS-13-2002` cita *"DS N° 02-92-SA"* → varias candidatas.
2. `LEY-32033` cita *"Ley 29459"* → `LEY-29459` **y** el stub `NORM-LEY-29459-…`.
3. `LEY-32319-2025` (×2, ya vinculadas) → apuntan a los stubs `LEY-29698-ART9` y
   `NORM-LEY-29459-…`.

Los tres son el **mismo problema**: los stubs duplican la identidad de normas
reales. Eso es H-08 y **no se toca aquí**.

### Evidencia del caso D ("Ley 29459")

```
candidatas tipo=LEY num=29459 (catálogo completo)
    LEY-29459                                        [drive_structured]
    NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT    [stub_derogada]

candidatas EXCLUYENDO stubs → LEY-29459 · RESUELTA_TIPO_NUMERO
```

Con el código anterior, *"Ley 29459"* (sin año) **fallaba siempre** y generaba un
stub nuevo. Ahora resuelve a la ley real; y mientras el stub siga existiendo, la
resolución se declara AMBIGUA en vez de adivinar. **NO VERIFICADO**: cuál de las
dos filas debe sobrevivir a la reconciliación — es una decisión de datos que
corresponde a H-08 y necesita validación humana.

## 6. Idempotencia (`--force`)

`tests/test_detector_relaciones.py::TestIdempotenciaConForce` ejecuta
`procesar_norma()` **tres veces** sobre la misma norma con **tres redacciones
distintas del modelo** para la misma relación jurídica, contra un doble de
Supabase que además **emula el índice único** de la migración.

```
relaciones insertadas por corrida: [1, 0, 0]
filas finales en la tabla:          1
```

Y `test_relaciones_distintas_del_mismo_origen_no_se_fusionan` comprueba lo
contrario: `art. 10` y `art. 12` siguen siendo dos relaciones.

## 7. Resultado de la suite

```
56 tests · OK   (26 identidad + 30 detector)
```

Antes de esta fase eran 50. **No se eliminó ninguna cobertura**: el único test
que dependía de una función borrada (`construir_document_key_candidato`) fue
reescrito sobre la API nueva, no suprimido.

## 8. Riesgos residuales

1. **Los stubs siguen creando ambigüedad** (3 casos). Es H-08.
2. **El bot sigue creando stubs** al confirmar una relación cuya norma afectada
   no está en la base; todavía no usa esta capa. Debería hacerlo antes de
   reactivar el cron.
3. **`clave_dedupe` está NULL en las 148 relaciones históricas**, así que el
   índice único no las protege; la protección es en memoria. Rellenarlas exige
   autorización explícita (toca filas confirmadas).
4. **N3 (número+año sin tipo)** sigue admitido cuando hay una sola candidata en
   todo el corpus. Es correcto hoy, pero se vuelve más frágil a medida que crece
   la base; conviene revisarlo cuando el catálogo supere el orden de miles.
5. **38 `NORMA_NO_ENCONTRADA`** no son un error de identidad: esos documentos
   simplemente no están cargados. Vincularlos exige incorporarlos primero.

El cron **sigue pausado**. No se ejecutó backfill.
