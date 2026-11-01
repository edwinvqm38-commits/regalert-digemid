# F04C.1 — Auditoría de pertenencia documental

## Objetivo y alcance

F04C.1 identifica páginas mixtas o regiones potencialmente ajenas a la norma
registrada. Es una auditoría local y conservadora: no corrige texto, no cambia
atribuciones y no escribe en Supabase. La fuente oficial permanece intacta.

El problema no es de `quality_score`. Ese valor mide legibilidad y forma del
texto; una tabla perfectamente legible puede pertenecer a otra disposición.

## Diagnóstico del pipeline actual

- `agents/pdf_extract.py` elige por página entre PyMuPDF, pdfplumber y OCR. Las
  tablas de pdfplumber se añaden como Markdown al texto de toda la página.
- `extract_normativa_pdf_text_to_supabase.py` conserva página física,
  `source_asset_id`, hash de fuente y metadata; protege páginas revisadas y
  valida completitud. No delimita disposiciones dentro de una página mixta.
- `extract_normativa_text_simple.py` usa el extractor común para corpus legacy
  monoasset y protege texto revisado o de mejor calidad. Tampoco determina
  pertenencia normativa.
- Telegram descarga primero el PDF archivado, genera un TXT de una página con
  `REVISION_ID`, `source_asset_id` y motivo, y valida esos identificadores al
  recibirlo. `revision_safety.ts` protege estructura tabular y cobertura, pero
  no decide si una región pertenece a otra norma.

Por ello F04C.1 se implementa como diagnóstico separado y no duplica esos
controles.

## Señales utilizadas

La auditoría combina, sin tratar ninguna señal aislada como verdad:

- identidad esperada y encabezados normativos encontrados;
- encabezados de otras normas, distinguidos de citas dentro de prosa;
- cambios de ministerio o sector en bloques con apariencia de encabezado;
- marcadores de inicio y cierre de disposiciones;
- posición normalizada de bloques y tablas;
- inicio tardío de la norma dentro de una página;
- layout de una o dos columnas;
- continuidad estructural entre páginas;
- anexos ligados explícitamente a la norma;
- tablas anteriores al inicio de la norma o con otra identidad;
- indicios de fragmentación entre celdas.

Estados:

- `scope_verified`: existe evidencia positiva de la identidad esperada y no
  hay encabezado competidor.
- `mixed_pending_review`: existe señal fuerte de otra disposición o ministerio.
- `scope_unknown`: no hay evidencia positiva suficiente.

La ausencia de una señal extranjera nunca basta para `scope_verified`.
Las firmas `Ministro/Ministra de ...` no se tratan como cambios de sector: un
decreto puede estar refrendado legítimamente por varias carteras. La señal de
sector exige un encabezado institucional o se combina con límites de
disposición.

### Orden documental y encabezados divididos

En páginas de El Peruano con dos columnas, el flujo se evalúa por columna
(izquierda de arriba hacia abajo y luego derecha), no por coordenada `y`
global. Esto permite distinguir una tabla de la norma objetivo situada en la
columna derecha de una tabla anterior situada arriba en otra columna.

PyMuPDF puede separar un encabezado en dos bloques, por ejemplo
`RESOLUCIÓN MINISTERIAL` y `N° 260-2019/MINSA`. El auditor une únicamente
bloques próximos, alineados en la misma columna y con la forma
tipo-normativo + identificador. La misma composición se aplica cuando ambas
partes están en líneas consecutivas de un único bloque PyMuPDF. No se unen
líneas alejadas ni bloques de columnas distintas. También tolera el carácter
sustituto `�` que aparece en algunos PDF en lugar de `Ó`, `°` o `º`. La
evidencia y las coordenadas originales no se reescriben.

Una referencia que sigue inmediatamente a conectores como `aprobado por`,
`modificado por` o `regulado por` se conserva como cita jurídica. No inicia
otra disposición por el solo hecho de aparecer centrada o en una línea
separada.

Para cada tabla se determina la última disposición activa antes de su posición
en el flujo de lectura. Un encabezado ajeno anterior no contamina una tabla si
un encabezado posterior de la norma objetivo ya tomó el flujo. Las tablas que
preceden el inicio objetivo siguen marcadas y una tabla que cruza columnas se
mantiene pendiente por ambigüedad espacial.

La palabra `ANEXO` no verifica por sí sola la pertenencia. La vinculación
explícita exige que el bloque tenga forma de encabezado de anexo y que allí se
identifique la norma esperada. Frases como “De conformidad con el Anexo del
Decreto Supremo...” permanecen como citas; sin encabezado propio ni continuidad
física demostrada, la página queda `scope_unknown`.

### Continuidad física

La identidad activa solo se hereda desde la página PDF inmediatamente
anterior. Una selección con saltos, por ejemplo páginas 1 y 12, no transfiere
continuidad entre ellas. Si la página adyacente mantiene la norma activa,
presenta estructura normativa compatible (`Artículo`, `Anexo`, etc.) y no
contiene un encabezado competidor, esa continuidad constituye evidencia
positiva. Un nuevo encabezado posterior cierra el flujo esperado al final de
la página.

## Ejecución dry-run

```powershell
python scripts/audit_normativa_document_scope.py `
  --pdf "C:\ruta\DS-5-2019__oficial.pdf" `
  --document-key DS-5-2019-SA `
  --source-asset-id 77 `
  --pages 1-13 `
  --printed-page-offset 34 `
  --json-output reports/F04C1_DS-5-2019.json `
  --summary-output reports/F04C1_DS-5-2019.txt
```

El comando abre un PDF local, calcula su SHA-256 y escribe únicamente archivos
de reporte locales. No contiene cliente Supabase, flags de aplicación ni rutas
de actualización. Para evaluar continuidad deben incluirse páginas físicas
contiguas; una selección con saltos se audita de forma independiente y
conservadora.

## Validación con evidencia oficial real

La versión `f04c1_scope_v2` se ejecutó en modo dry-run contra dos PDF oficiales
locales y se revisaron visualmente las cinco páginas relevantes. Los PDF no se
modificaron.

| PDF / página física (impresa) | Antes | Resultado v2 | Evidencia principal |
| --- | --- | --- | --- |
| DS-5-2019 / 1 (35) | `scope_unknown` | `mixed_pending_review` | Dos cierres y sector Relaciones Exteriores antes del DS esperado; RM 0120/RE-2019; tres tablas anteriores al inicio del DS. |
| DS-5-2019 / 12 (46) | `mixed_pending_review` | `scope_verified` | Continuidad física del decreto y anexos; D.S. 023-2001-SA aparece después de “aprobado por” y se trata como referencia. |
| DS-5-2019 / 13 (47) | `scope_unknown` | `mixed_pending_review` | Termina el anexo y comienza la RM 174-2019/MINSA en la columna derecha. |
| RM-250-2019 / 1 (21) | `mixed_pending_review` | `mixed_pending_review` | Cierre y firma de Relaciones Exteriores antes del encabezado esperado; la tabla TUPA posterior permanece asociada a la RM objetivo. |
| RM-250-2019 / 2 (22) | `scope_unknown` | `mixed_pending_review` | Termina la RM 250 y comienzan las RM 260 y 261-2019/MINSA. |

SHA-256 verificados:

- `DS-5-2019__oficial.pdf`:
  `324fccec9c9a5f10d2c6fb3966facf9d2af4ef135bfa79647c31c0afa1ad0d50`.
- `RM-250-2019__oficial.pdf`:
  `48bbb22ad60aabd03acab0f53a906bbded7b3c04412b79ef9b1d7222e015a0d1`.

Los tests `RealPdfLayoutRegressionTest` usan transcripciones de bloques y
coordenadas obtenidas de esos PDF. No incorporan ni alteran los binarios. Los
tests sintéticos usan identificadores distintos para cubrir generalización,
saltos de página, citas y transiciones dentro del mismo sector.

## Falsos positivos y negativos

- Una cita legal en una oración no se considera encabezado nuevo, pero un PDF
  con tipografía o extracción muy degradada puede ocultar esa diferencia.
- Los conectores de cita cubren formulaciones frecuentes, no todas las
  variantes lingüísticas posibles. Una fórmula desconocida puede quedar en
  `mixed_pending_review`; no se transforma en atribución automática.
- Un cambio de ministerio es señal de revisión, no prueba definitiva: podría
  aparecer en un convenio o anexo legítimo.
- El orden lineal de PyMuPDF puede mezclar columnas; por eso se reportan bandas
  y coordenadas y no se usa el orden textual como única evidencia.
- Una tabla sin identidad propia se marca sospechosa por posición solo cuando
  precede al inicio explícito de la norma.
- La fragmentación de celdas es un problema de integridad tabular distinto de
  la pertenencia documental.
- Una página intermedia seleccionada sin su antecesora queda `scope_unknown`
  aunque pertenezca a la norma. Es un falso negativo deliberado: para evaluar
  continuidad debe auditarse el rango físico completo.
- La lectura por columnas supone columnas verticales estables. Diseños con
  recuadros flotantes, tres columnas o texto rotado pueden requerir revisión
  visual y permanecer `scope_unknown`.
- `scope_verified` acredita pertenencia documental con la evidencia disponible;
  no certifica exactitud OCR, integridad de celdas ni corrección jurídica.

## Propuesta F04C.2

Después de validar reglas y coordenadas contra PDFs reales:

1. almacenar resultados como assets JSON derivados o historial de auditoría,
   sin reescribir `text_raw`/`text_normalized`;
2. mostrar en `/revision` una advertencia de alcance y las regiones sospechosas;
3. impedir que confirmar una tabla marque como propia otra región de la página;
4. permitir al revisor aceptar/rechazar regiones con identidad, hash, bbox,
   `source_asset_id`, página y `REVISION_ID`;
5. aplicar cambios de atribución solo mediante una operación posterior,
   explícita, auditable y separada de la corrección de texto.
