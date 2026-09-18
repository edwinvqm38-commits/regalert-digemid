# Cálculo de `estado_vigencia` (FASE 3 — inteligencia normativa)

Este documento explica la regla que decide qué `estado_vigencia` se asigna a
la norma **afectada** cuando un humano confirma una relación jurídica
(`/derogacionespendientes` en Telegram). La lógica vive en
`calcularEstadoVigencia()`, duplicada a propósito en:

- `supabase/functions/telegram-bot/index.ts`
- (la etiqueta de despliegue, `advertenciasDelBloque()`, se duplica también
  en `supabase/functions/_shared/consulta-ia.ts`, usado por `whatsapp-bot`)

No hay forma de compartir un módulo entre ambos bots hoy sin refactorizar la
arquitectura de edge functions, así que cualquier cambio a esta regla debe
aplicarse en los dos archivos. `tests/test_detector_relaciones.py` (Python)
declara `TIPOS_RELACION_SIN_EFECTO_EN_VIGENCIA` como espejo de intención,
para que un cambio en un lado sin el otro se note en revisión de código.

## Por qué existe esta regla

Antes de FASE 3, `ESTADO_VIGENCIA_POR_RELACION` era una tabla fija
`tipo_relacion -> estado`, sin mirar el alcance. Una relación real y
verificada —`LEY-32319-2025 → deroga (parcial, artículo 9) → LEY-29698`—
habría marcado **toda** la Ley 29698 como `derogada`, cuando el texto solo
deroga un artículo puntual. "Derogar un artículo no es lo mismo que derogar
la norma completa", el mismo principio que ya regía para "mención ≠
modificación" en el detector de relaciones.

## Criterio de `calcularEstadoVigencia()`

```ts
function calcularEstadoVigencia(
  tipoRelacion: string,
  alcance: string | null | undefined,
  articulosAfectados: string | null | undefined,
): string | undefined {
  const esParcial = alcance === "parcial" || Boolean(articulosAfectados && articulosAfectados.trim());

  if (tipoRelacion === "deroga" || tipoRelacion === "deja_sin_efecto") {
    return esParcial ? "derogada_parcialmente" : "derogada";
  }
  if (tipoRelacion === "modifica" || tipoRelacion === "sustituye" || tipoRelacion === "incorpora") {
    return "modificada";
  }
  if (tipoRelacion === "suspende") {
    return "suspendida";
  }
  return undefined;
}
```

Regla de decisión de `esParcial`: si el modelo marcó `alcance = "parcial"`
**o** reportó `articulos_afectados` con algún valor, se trata como parcial.
La ausencia de ambas señales (`alcance` no es `"parcial"` y no hay
`articulos_afectados`) es la única condición para tratar el efecto como
total. Nunca se asume total por defecto cuando hay evidencia de que es
parcial; nunca se asume parcial cuando no hay ninguna señal de que lo sea
(ver la sección de revisión humana más abajo para el caso ambiguo real que
sí exige esta distinción).

## Tabla de decisión

| `tipo_relacion` | `alcance` | `articulos_afectados` | `estado_vigencia` resultante |
|---|---|---|---|
| `deroga` / `deja_sin_efecto` | `total` o `null` | `null`/vacío | `derogada` |
| `deroga` / `deja_sin_efecto` | `parcial` | (cualquiera) | `derogada_parcialmente` |
| `deroga` / `deja_sin_efecto` | `total` o `null` | tiene valor | `derogada_parcialmente` (la mención de artículos pesa más que un `alcance` no marcado) |
| `modifica` / `sustituye` / `incorpora` | (cualquiera) | (cualquiera) | `modificada` (no distingue alcance: modificar ya implica una afectación parcial por definición) |
| `suspende` | (cualquiera) | (cualquiera) | `suspendida` (sin cambios en FASE 3; no estaba en el alcance de esta corrección) |
| `exonera`, `prorroga`, `pendiente_verificacion`, `complementa`, `reglamenta`, `aclara`, `anula_acto_administrativo`, `anula_disposicion_normativa` | (cualquiera) | (cualquiera) | `undefined` — no se toca `estado_vigencia` (relación queda registrada solo para trazabilidad) |

`undefined` significa literalmente que el código no ejecuta ningún
`UPDATE` sobre `digemid_normas.estado_vigencia`: la norma conserva el
estado que ya tenía.

## Ejemplos reales evaluados (no hipotéticos)

Ejecutados contra la función real extraída de `index.ts`, y contra datos de
producción confirmados con `execute_sql`:

| Fuente | Cita textual (evidencia) | `tipo_relacion` | `alcance` | `articulos_afectados` | Resultado |
|---|---|---|---|---|---|
| `LEY-32319-2025` → `LEY-29698` | *"Deróguese el artículo 9 de la Ley 29698 incorporado en la Ley 31738."* | `deroga` | `parcial` | `"9"` | `derogada_parcialmente` |
| `RM-63-2026` → `RD-59-2022` | *"Artículo 2.- Derogar la Resolución Directoral N° 059-2022-DIGEMID-DG-MINSA..."* | `deroga` | `total` | `null` | `derogada` |
| `DS-18-2020` → `RM-255-2020` | *"Artículo 2.- Dejar sin efecto la Resolución Ministerial N° 255-2020-MINSA."* | `deja_sin_efecto` | `total` | `null` | `derogada` |
| `RM-894-2024` → `RM-792-2024` | *"Artículo 1.- Modificar el rubro Autorización de Uso del Anexo de la RM N° 792-2024-MINSA..."* | `modifica` | `parcial` | `"Anexo, rubro Autorización de Uso"` | `modificada` |

## Visualización (H-11, corregida en FASE 3)

`advertenciasDelBloque()` (contexto que recibe la IA en `/consulta`, tanto
Telegram como WhatsApp) etiqueta cada estado:

| `estado_vigencia` | Etiqueta mostrada | Instrucción a la IA |
|---|---|---|
| `derogada` | `DEROGADA / SIN EFECTO` | "No la presentes como norma vigente sin esa aclaración" |
| `derogada_parcialmente` | `VIGENTE, CON UN ARTÍCULO DEROGADO` | "Sigue vigente en todo lo demás... no digas que la norma completa está derogada" |
| `modificada` | `MODIFICADA` | (misma instrucción genérica de no vigencia sin aclarar) |
| `suspendida` | `SUSPENDIDA` | (misma instrucción genérica) |

Antes de esta fase, `derogada_parcialmente` no tenía fila propia en el
`switch`/ternario y caía en `DEROGADA / SIN EFECTO` — el bug nunca se había
manifestado porque ninguna fila usaba ese valor todavía (el `check
constraint` lo permite desde 2026-08-22, pero el código nunca lo escribía).

## Casos que requieren revisión humana (no los decide `calcularEstadoVigencia()`)

Esta función nunca es la única línea de defensa: toda relación pasa primero
por `/derogacionespendientes` y un humano confirma o rechaza. Casos donde
la ambigüedad debe resolverla una persona, no el código:

1. **`alcance` y `articulos_afectados` ambos ausentes, pero el texto en
   realidad sí acota un artículo** (el modelo no llenó los campos, no que la
   derogación sea total). Mitigado por el prompt del detector, que exige
   `alcance` cuando el texto lo permite saber — pero si el modelo omite el
   campo, `calcularEstadoVigencia()` no tiene forma de detectarlo y tratará
   el caso como total. El fragmento (`fragmento_fuente`) queda visible en
   `/derogacionespendientes` precisamente para que el humano lo note antes
   de confirmar.
2. **`tipo_relacion = pendiente_verificacion`**: por diseño no cambia
   `estado_vigencia` nunca — el efecto jurídico exacto no se determinó con
   certeza, así que ni "total" ni "parcial" son una respuesta válida.
3. **`tipo_relacion = anula_acto_administrativo` / `anula_disposicion_normativa`**
   (FASE 2): tampoco cambian `estado_vigencia` automáticamente, aunque
   conceptualmente una nulidad de disposición normativa se parezca a una
   derogación total. Una nulidad normativa suele originarse en un fuero
   distinto (control de constitucionalidad, contencioso-administrativo) y
   amerita la misma cautela que `pendiente_verificacion`: el humano decide
   si corresponde además marcar la norma como `derogada`/`derogada_parcialmente`
   manualmente, esta función no lo hace por él.
4. **Reconciliación de stubs** (H-08, `scripts/reconciliar_stubs_normativa.py`):
   al repuntar una relación desde un stub hacia la norma real, el script
   **nunca** llama a `calcularEstadoVigencia()` ni toca `estado_vigencia` de
   la norma real — es una decisión deliberada documentada en el propio
   script (ver el caso `LEY-29698-ART9 → LEY-29698`, que sigue "vigente"
   tras la reconciliación).
