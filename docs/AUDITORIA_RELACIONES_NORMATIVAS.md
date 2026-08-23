# Auditoría — Detector de relaciones normativas (RegAlert / Alertas DIGEMID)

**Alcance:** `main` @ `51ec0ba` · datos de producción (proyecto Supabase `wnhqyccqxifhusltigmq`)
**Modo:** solo lectura. **No se modificó ningún dato de producción en esta fase.**
**Método:** lectura del código real en `main` (no de las descripciones de PR), ejecución de las
funciones puras contra fragmentos reales, y consultas `SELECT` sobre producción.

> Criterio aplicado: los PR #52–#65 **no** se dieron por buenos. Se verificó su implementación
> efectiva en `main`. Una de las correcciones previas resultó ser una **regresión** (H-02).

---

## 0. Inventario verificado

| Métrica | Valor |
|---|---|
| Relaciones totales | **143** (123 pendientes · 15 confirmadas · 5 rechazadas) |
| Relaciones con `fragmento_verificado = false` | **73 (51,0 %)** |
| Relaciones **confirmadas** con cita no verificada | **5** |
| Relaciones sin `norma_afectada_id` (identidad no resuelta) | **42 (29,4 %)** |
| Normas con texto extraído | 292 |
| Normas que **exceden la ventana de 15 000 chars** | **168 (57,5 %)** — todas ya marcadas como analizadas |
| Tamaño medio / máximo de norma | 44 474 / 763 506 chars |
| Normas con verbo dispositivo **fuera** de la ventana | **44** |
| Stubs (`stub_derogada`) | 5 |
| Duplicados (origen+tipo+afectada) | 2 |
| Identidades ambiguas (nº+año con varios tipos) | 3 |
| Parciales que dejaron la norma `derogada` | **1** |
| Anomalías cronológicas | 0 |
| `estado_vigencia ≠ vigente` sin relación confirmada que lo sustente | 0 ✅ |

Variantes reales de `tipo_norma` en producción (**normalización canónica inexistente**):
`RM` (181) · `DS` (95) · `RD` (43) · `LEY` (12) · `DU` (3) · **`Decreto Supremo` (3)** ·
**`Resolución Ministerial` (2)** · **`Ley` (2)** · `RS` (1).

---

## CRÍTICO

### H-01 · La ventana de 15 000 chars amputa la parte dispositiva del 57,5 % del corpus
`scripts/detectar_derogaciones_normativa.py:40,364` — `MAX_CHARS_TEXTO = 15000`;
`texto_de_norma()` concatena páginas en orden y corta.

En técnica legislativa peruana las **disposiciones complementarias derogatorias/modificatorias
van al final**. Cortar por el principio elimina sistemáticamente la evidencia jurídica.

**Evidencia dura — RM-894-2024** (el caso que el usuario reportó como fallo de interpretación):

| Página | chars | acumulado | ¿dentro de ventana? |
|---|---|---|---|
| 1 | 7 177 | 7 177 | sí |
| 2 | 8 559 | 15 736 | **NO** |
| 3 | 7 306 | 23 042 | **NO** — aquí está `Artículo 3.- Derogar la RM N° 339-2023/MINSA` (offset ≈ 17 900) |

El detector **nunca vio** el artículo derogatorio. Por eso citó un *considerando*: era lo único
que tenía. El fix del PR #65 (prompt "cita la parte dispositiva") **no puede funcionar** si la
parte dispositiva no está en el prompt. **La causa raíz seguía viva.**

- **168 normas** ya fueron "analizadas" con texto amputado y están marcadas
  `derogacion_analizada = true` → **nunca se reanalizarán** (ver H-03).
- **44 normas** tienen verbos dispositivos fuera de ventana.
- **Falso negativo probado:** `DS-12-2023` → *"Derogar las siguientes disposiciones: - Decreto
  Supremo N° 012-2016…"* fuera de ventana, **0 relaciones registradas**.

**Riesgo jurídico:** normas realmente derogadas se sirven como vigentes en `/consulta`.
**Corrección:** segmentación estructural (encabezado propio + parte dispositiva + disposiciones
finales), priorizando el final del documento; nunca truncado ciego por prefijo.
**Test:** `RM-894-2024` debe producir `deroga → RM-339-2023`; `DS-12-2023` → `deroga → DS-012-2016`.

---

### H-02 · REGRESIÓN del PR #64: el filtro de linaje descarta relaciones correctas y confirmadas
`detectar_derogaciones_normativa.py:276` — `es_cita_de_linaje()`.

El patrón cubre `aprobado/a por` **y** `modificado/a por`. Pero en la fórmula estándar peruana
*"Modificar el artículo N del **Reglamento …, aprobado por** DS X"* el objeto modificado **es** ese
DS: el reglamento vive dentro del instrumento que lo aprobó. Solo `modificado por` denota una
enmienda previa (linaje real).

**Ejecución del código real de `main`** — las tres se descartan:

```
FILTRADA <- DS-15-2025    -> DS-014-2011-SA  (CONFIRMADA y correcta)
FILTRADA <- DS-008-2025   -> DS-014-2011-SA  (CONFIRMADA y correcta)
FILTRADA <- RM-899-2025   -> RM-615-2024     (linaje real — correcto descartarla)
```

`DS-008-2025-SA → DS-014-2011-SA` fue el **primer caso de éxito validado** de todo el proyecto y
hoy sería silenciosamente eliminado. También rompería `RM-899-2025 → incorpora → RM-737-2010`
(confirmada, "aprobada por").

**Regla refinada validada** (solo `modificado/a por` es linaje) — 4/4 casos correctos:

```
OK filtrada=False <- DS-15-2025  -> DS-014-2011-SA   (pasa)
OK filtrada=False <- DS-008-2025 -> DS-014-2011-SA   (pasa)
OK filtrada=False <- RM-899      -> RM-737-2010      (pasa, "aprobada por")
OK filtrada=True  <- RM-899      -> RM-615-2024      (filtrada, "modificado por")
```

**Riesgo:** falsos negativos masivos en modificaciones de reglamentos (el patrón más frecuente
del corpus DIGEMID). **Test:** los 4 casos anteriores como regresión permanente.

---

### H-03 · Un error del modelo se registra como "cero relaciones" y congela la norma
`detectar_derogaciones_normativa.py:320-347` — `call_deepseek()`.

`JSONDecodeError` → `return {"relaciones": []}` → `procesar_norma()` continúa → línea 560 marca
`derogacion_analizada = True`. **Indistinguible de "no hay relaciones".** La norma queda cerrada
para siempre sin haber sido analizada.

Agravante: `max_tokens = 1024`. Una norma con **muchas** relaciones produce JSON **truncado** →
`JSONDecodeError` → descartada. Es decir: **cuanto más rica jurídicamente es la norma, mayor la
probabilidad de perderla entera.** No hay reintentos.

(Los fallos HTTP/timeout sí propagan la excepción y **no** marcan la norma — ese camino es seguro.)

**Corrección:** estados explícitos `OK | ERROR_API | ERROR_JSON | TIMEOUT | TEXTO_INSUFICIENTE |
RESPUESTA_INCOMPLETA`; **solo `OK`** puede marcar analizado; subir `max_tokens`; reintento acotado.
**Test:** JSON inválido y timeout simulados no deben marcar `derogacion_analizada`.

---

### H-04 · Sin `--force` ni versionado del analizador: el corpus queda congelado
`main()` (línea 564) expone solo `--limit` y `--document-key`, y `normas_pendientes()` filtra
`derogacion_analizada = False`. **`--document-key X` no reanaliza X si ya fue analizada.**

Consecuencia directa: tras cada mejora del motor, las 168 normas afectadas por H-01 quedan con el
resultado viejo. Durante esta sesión hubo que hacer `UPDATE` manuales en producción para reanalizar
— eso es la prueba operativa del defecto.

**Corrección:** `relaciones_analyzer_version` + `relaciones_analizadas_en`; reanálisis cuando
`version_guardada < VERSION_ACTUAL`; flag `--force`; reanálisis **idempotente** (H-07 lo condiciona).

---

## ALTO

### H-05 · `tipo_norma` sin normalización canónica (confirmado en datos)
Conviven `Decreto Supremo`/`DS`, `Resolución Ministerial`/`RM`, `Ley`/`LEY`. El PR #55 "resolvió"
esto **eliminando** el filtro por tipo en `buscar_norma_afectada()` (línea 406) en vez de
normalizar → hoy la identidad se decide por **número + año**, sin tipo (ver H-06).
**Corrección:** una única `normalizar_tipo_norma()` compartida; sin duplicar lógica.

### H-06 · Identidad por número+año: `RM 150-2025` y `RD 150-2025` son indistinguibles
`buscar_norma_afectada()` recorre candidatas del mismo año y devuelve **la primera** cuyo número
coincide — sin comparar tipo ni sufijo de sector. Hay **3** combinaciones nº+año con varios tipos.
Además, con `anio = NULL` (frecuente en leyes: *"Ley 29459"*) la resolución falla siempre y se crea
un **stub** (→ H-08). **42 relaciones (29,4 %) no tienen identidad resuelta.**
**Corrección:** algoritmo jerárquico N1 canónico exacto → N2 tipo+nº+año → N3 nº+año **solo si hay
candidata única**; si hay varias → `IDENTIDAD_AMBIGUA` + registro de candidatas. **Nunca la primera.**

### H-07 · Deduplicación frágil ante identidad no resuelta
`relacion_ya_registrada()` compara nº+año normalizados, y cae a **texto exacto** de la descripción
si falta ese dato. Con las descripciones libres de la IA, el reanálisis puede duplicar.
Hay **2 duplicados** ya en producción. Sin esto, H-04 (`--force`) es peligroso.
**Corrección:** clave de dedupe estable (origen + tipo_relación + identidad canónica de la afectada
+ hash normalizado del fragmento) y constraint único parcial.

### H-08 · Stubs que duplican normas reales y desconectan relaciones confirmadas
| Stub | Norma real existente | Problema |
|---|---|---|
| `NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT` (`anio` NULL) | **`LEY-29459`** (`drive_structured`, 2009) | **Duplicado de la ley farmacéutica marco.** La exoneración **confirmada** de la Ley 32319 apunta al *stub* → `/consulta` sobre la ley real **nunca la verá**. |
| `LEY-29698-ART9` (`estado_vigencia = derogada`) | `LEY-29698` (vigente) | Un **artículo** modelado como norma; el stub queda "derogado" (ver H-09). |

`RD-354-99` y `RD-993-99`: **verificados correctos** — el Art. 2 de `RD-144-2016` deroga ambas
(texto literal comprobado). No son duplicados.
**Corrección:** reconciliación stub↔norma real y repunte de `norma_afectada_id`; **sin borrar**.

### H-09 · Afectación parcial modelada como norma-artefacto en vez de como alcance
`Deróguese el artículo 9 de la Ley 29698` generó el pseudo-registro `LEY-29698-ART9` marcado
`derogada`. La `LEY-29698` real sigue `vigente` (correcto), pero el modelo mezcla dos planos.
Hoy hay **1** caso; el diseño permite que escale.
**Corrección:** separar **estado global** de la norma de la **afectación por artículo**
(tabla de afectaciones puntuales); estados `derogada_parcialmente` / `suspendida_parcialmente`.
**Test obligatorio:** derogar un artículo **no** puede dejar la norma `derogada`.

### H-10 · `/consulta` no recibe las relaciones confirmadas
`supabase/functions/telegram-bot/index.ts` — `advertenciasDelBloque()` / `buildConsultaContext()`
solo inyectan `estado_vigencia`. El LLM **no** sabe qué norma afectó, qué artículos, en qué fecha,
ni con qué nivel de verificación.

Consecuencia concreta: la **exoneración confirmada** de los arts. 10-11 de la Ley 29459
(`tipo_relacion = exonera` → no cambia `estado_vigencia`, por diseño correcto de H-11) es
**invisible** para `/consulta`. Doble invisibilidad, porque además apunta al stub (H-08).
**Corrección:** capa "contexto de relaciones normativas" adjunta a los fragmentos recuperados
(afectación, artículos, norma origen, fecha, estado de verificación).

---

## MEDIO

### H-11 · Etiquetado de vigencia incompleto en el bot
`estado_vigencia === "modificada" ? "MODIFICADA" : "DEROGADA / SIN EFECTO"`. El esquema admite
`derogada_parcialmente` y `suspendida`; ambas caerían en **"DEROGADA / SIN EFECTO"** →
una norma parcialmente derogada se presentaría como derogada por completo. Hoy no hay filas con
esos valores (por eso es MEDIO y no CRÍTICO), pero la migración ya los habilita.

### H-12 · 5 relaciones confirmadas con `fragmento_verificado = false`
Confirmadas sin cita verificable, p. ej. `RM-267-2025 → deroga → RM-159-2022`,
`RM-339-2024 → modifica → RM-116-2018`, `RM-545-2019 → modifica → RM-1361-2018`,
`RM-899-2025 → incorpora → RM-737-2010`, `DS-23-2005 → deroga → DS-14-2002` (registro manual con
fuente externa declarada). Varias son consecuencia directa de H-01: la cita real estaba truncada.
**Acción:** reverificar contra texto tras corregir H-01; no revertir a ciegas.

### H-13 · `recortar_decreto_anexado()` corta ante cualquier `DECRETA:`
Regla estructural razonable, pero un `DECRETA:` **citado** dentro de un considerando o de un anexo
que reproduce otra norma truncaría la parte dispositiva legítima. No se halló un caso real todavía.
**Corrección:** exigir contexto de inicio de instrumento (encabezado/marca de proyecto próximos),
no la sola aparición del literal.

### H-14 · Título contaminado en `LEY-29698`
`titulo = "Descripción: Disponen la publicación del proyecto de Decreto Supremo q…"` — metadato del
crawler dentro del título. Afecta la identificación por nombre. Revisar el extractor de origen.

---

## BAJO

- **H-15** — El workflow exporta `TELEGRAM_BOT_TOKEN`/`TELEGRAM_ADMIN_CHAT_ID` ya sin uso
  (PR #60 eliminó el envío). Superficie de secreto innecesaria.
- **H-16** — Sin control de concurrencia a nivel de fila: el `concurrency` del workflow evita
  solapamiento, pero una ejecución manual simultánea podría duplicar (mitigado por H-07).
- **H-17** — No hay tests automatizados de ningún tipo en el repositorio para este flujo.

---

## Respuesta a FASE 22 (A–I)

| | Concepto | Valor |
|---|---|---|
| A | Pendientes / confirmadas / rechazadas | 123 / 15 / 5 |
| B | Auditadas | 143 (100 % por reglas y agregados); 15 confirmadas + 5 rechazadas revisadas caso a caso |
| C | Correctas verificadas / incorrectas / ambiguas / sin evidencia | 10 / 1 (H-02 en riesgo) / 3 / **73** con cita no verificable |
| D | Stubs | 5 (2 problemáticos: H-08) |
| E | Discrepancias `estado_vigencia` sin respaldo | **0** ✅ |
| F | Falsos negativos | 44 normas con verbo fuera de ventana; **≥1 probado** (`DS-12-2023`) |
| G | Duplicados | 2 |
| H | Identidades ambiguas | 3 (+42 relaciones sin identidad resuelta) |
| I | Parciales tratadas como totales | 1 (`LEY-29698-ART9`) |

---

## Arquitectura recomendada del detector

```
PDF → extracción por página
   → SEGMENTACIÓN ESTRUCTURAL           (H-01: encabezado propio · dispositiva · disp. finales)
   → recortes de contaminación          (proyecto anexado · decreto anexado · norma ajena)
   → DeepSeek con estado explícito      (H-03: solo OK marca analizado)
   → filtros determinísticos            (cláusula genérica · linaje SOLO "modificado por", H-02)
   → verificación de cita contra texto
   → resolución de identidad jerárquica (H-06: N1/N2/N3 · ambigua ⇒ pendiente_verificacion)
   → dedupe por clave estable           (H-07)
   → digemid_norma_relaciones (pendiente)
   → confirmación humana (Telegram)     ← único camino que altera vigencia
   → estado global + afectaciones por artículo (H-09)
   → contexto de relaciones para /consulta (H-10)
```

**Escalado a segundo modelo** (no reemplazar DeepSeek; usarlo como primera pasada): escalar cuando
`fragmento_verificado = false`, identidad ambigua, `pendiente_verificacion`, sospecha de
proyecto/anexo, varias candidatas, conflicto regex↔modelo, afectación parcial compleja, cronología
inconsistente o fuente de baja calidad. **Ningún modelo altera vigencia sin confirmación humana.**

---

## Estado de los criterios de aceptación (FASE 25)

| Criterio | Estado |
|---|---|
| Error de modelo ≠ "0 relaciones" | ❌ H-03 |
| Identidad ambigua no se asigna a la primera | ❌ H-06 |
| `tipo_norma` normalizado | ❌ H-05 |
| Derogación parcial no marca norma completa | ⚠️ H-09 (1 caso) |
| Proyectos anexados no crean relaciones vigentes | ✅ verificado (RM-419/RM-727) |
| Considerandos no generan relaciones dispositivas | ⚠️ regla en prompt, pero H-01 la hace inoperante |
| Linaje ≠ objeto afectado | ❌ **H-02 (regresión)** |
| Fragmentos verificables contra fuente | ⚠️ 51 % sin verificar |
| Confirmadas reauditadas | ✅ esta auditoría |
| Rechazadas revisadas | ✅ 5/5 — sin falsos rechazos |
| Tests de bugs históricos | ❌ H-17 |
| Reanálisis por versión | ❌ H-04 |
| Idempotencia | ⚠️ H-07 |
| `/consulta` recibe afectaciones | ❌ H-10 |
| Trazabilidad de auditoría | ⚠️ parcial |
| Correcciones no destruyen historial | ✅ |

---

## Orden de corrección propuesto

1. **H-03** (fallo seguro) — barato y desbloquea confiar en el resto.
2. **H-02** (regresión) — está descartando relaciones correctas hoy mismo.
3. **H-01** (segmentación) — causa raíz de la mayoría de errores de interpretación.
4. **H-04 + H-07** (versionado + idempotencia) — habilitan el rebackfill seguro.
5. **H-05 + H-06** (identidad canónica) — reduce stubs y ambigüedad.
6. **H-08 + H-09** (reconciliación y afectación parcial) — con DRY-RUN previo.
7. **H-10** (`/consulta`) — cierra el ciclo hacia el usuario final.

**Nada de lo anterior se ha aplicado.** Producción quedó intacta en esta fase.
