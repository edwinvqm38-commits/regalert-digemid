# F-04-B — Instructivo de revisión humana (golden)

> **Actualización F-04-B.1:** ya existe un artifact único y autocontenido,
> `F04B_GOLDEN_REVISION_PACKAGE`, que trae la herramienta, los datos **y las 50
> imágenes de página ya renderizadas** en una sola descarga — con eso, la revisión
> ya no requiere abrir el PDF por separado ni buscar la página a mano (el enlace al
> PDF queda solo como respaldo). Si tienes acceso a ese paquete, descárgalo y sigue
> el `README.md` que trae adentro; el resto de este documento sigue siendo válido
> para las secciones 6 en adelante (qué mirar, las 5 decisiones, qué es crítico,
> cómo guardar y entregar) y como referencia si solo cuentas con el artifact anterior
> (`f04-fidelidad-piloto-4`, sin imágenes).

Guía paso a paso, para una persona **sin conocimientos técnicos**, para revisar las 50
páginas del Manifest V2 y producir la transcripción validada (`golden_text`) de cada una.

Esta fase es **100% offline y de solo lectura**: no se conecta a internet salvo para
descargar el PDF que quieras consultar, no escribe nada en Supabase, y no envía nada a
ningún servidor. Todo el trabajo queda en tu computadora hasta que tú decides exportarlo.

---

## 1) Descargar los datos desde GitHub Actions

1. Abre en tu navegador:
   `https://github.com/edwinvqm38-commits/regalert-digemid/actions/runs/32890148016`
2. Baja hasta el final de la página, a la sección **Artifacts**.
3. Haz clic en `f04-fidelidad-piloto-4` para descargar el archivo ZIP (se guardará en tu
   carpeta de Descargas, normalmente como `f04-fidelidad-piloto-4.zip`).

## 2) Descomprimir el ZIP

En Windows:

1. Ve a tu carpeta de Descargas.
2. Haz clic derecho sobre `f04-fidelidad-piloto-4.zip`.
3. Elige **"Extraer todo..."**.
4. Elige una carpeta destino fácil de encontrar, por ejemplo
   `Escritorio\F04-revision`, y confirma.

Dentro de esa carpeta vas a encontrar, entre otros, estos archivos (los que importan
para la revisión):

- `F04_MANIFEST_PILOTO_V2.json` — la lista de las 50 páginas seleccionadas, con el
  enlace a cada PDF.
- `F04_COMPARACION_MOTORES.json` — el texto que leyó cada motor en cada página, y el
  diagnóstico automático (dónde no coinciden).

(También hay versiones `.csv` de ambos — no las necesitas para la herramienta, son solo
para quien prefiera mirar los datos en Excel.)

## 3) Descargar la herramienta de revisión

1. Abre: `https://github.com/edwinvqm38-commits/regalert-digemid/blob/main/herramientas/f04b_revision_humana.html`
2. Arriba a la derecha del contenido del archivo hay un ícono de descarga (una flecha
   hacia abajo, "Download raw file"). Haz clic ahí.
3. Guarda el archivo `f04b_revision_humana.html` **en la misma carpeta** donde
   descomprimiste el ZIP del paso 2 (por ejemplo `Escritorio\F04-revision`). No es
   obligatorio que estén en la misma carpeta para que funcione, pero así es más fácil
   de encontrar todo junto.

## 4) Abrir la herramienta

**Opción A — la más simple (probar primero):**

Haz doble clic sobre `f04b_revision_humana.html`. Se abrirá en tu navegador por
defecto (Chrome, Edge o Firefox). Prueba a cargar los archivos (ver sección 5) y a
exportar un archivo de prueba (botón "Exportar decisiones (JSON)"): si la descarga
funciona, puedes trabajar así sin nada más.

**Opción B — si algo no funciona (por ejemplo, la exportación no descarga nada):**
un servidor local muy simple resuelve cualquier restricción del navegador al abrir el
archivo directamente. En Windows:

1. Abre **PowerShell** (búscalo en el menú Inicio como "PowerShell").
2. Ve a la carpeta donde guardaste el archivo, por ejemplo:

   ```powershell
   cd "$HOME\Desktop\F04-revision"
   ```

3. Ejecuta:

   ```powershell
   python -m http.server 8000
   ```

   (Si Windows dice que no reconoce `python`, prueba con `py -m http.server 8000`.)
4. Deja esa ventana de PowerShell abierta mientras trabajas.
5. En tu navegador, abre esta dirección:

   ```
   http://localhost:8000/f04b_revision_humana.html
   ```

6. Para cerrar el servidor cuando termines, vuelve a la ventana de PowerShell y
   presiona `Ctrl+C`.

## 5) Cargar los datos dentro de la herramienta

Arriba de todo en la herramienta hay tres casillas para elegir archivos:

1. **"Comparación de motores (obligatorio)"** → elige `F04_COMPARACION_MOTORES.json`.
2. **"Manifest V2 (opcional, da el enlace al PDF)"** → elige
   `F04_MANIFEST_PILOTO_V2.json`. Este paso es opcional pero muy recomendado: sin él no
   tendrás el enlace directo a cada PDF.
3. **"Continuar progreso guardado (opcional)"** → solo úsalo si ya habías exportado un
   avance anterior (ver sección 13).

En cuanto cargues el archivo 1, aparecerá la lista de páginas a la izquierda.

---

## 6) Qué mirar en cada página

Por cada página verás:

- Un enlace **"Abrir PDF"** (si cargaste el manifest) — ábrelo y ubica el número de
  página indicado.
- Cuatro cuadros de texto: el texto **ya guardado**, y lo que leyeron **PyMuPDF**,
  **pdfplumber** y **Tesseract (OCR)**, de forma independiente.
- Una caja de **"Diagnóstico automático"**: explica en qué NO coincidieron los motores
  entre sí. Es una ayuda para saber dónde mirar con más cuidado — **no es una
  verificación**, y no debes confiar en ella como si ya estuviera comprobada.
- Una tabla con las comparaciones entre pares de fuentes (LTER, CER, WER, si hay error
  jurídico, y los primeros errores detectados).

Compara los cuatro textos entre sí y contra el PDF real, y decide cuál (si alguno) es
exactamente correcto.

## 7) Las 5 decisiones posibles

| Decisión | Cuándo marcarla |
|---|---|
| `CORRECTO_ALMACENADO` | El texto que **ya estaba guardado** en el sistema es correcto tal cual está, comparado contra el PDF. |
| `CORRECTO_PYMUPDF` | El texto de **PyMuPDF** es correcto tal cual está. |
| `CORRECTO_PDFPLUMBER` | El texto de **pdfplumber** es correcto tal cual está. |
| `CORRECTO_TESSERACT` | El texto de **Tesseract (OCR)** es correcto tal cual está. |
| `NINGUNO_TRANSCRIBIR_MANUAL` | Ninguno de los 4 es correcto tal cual — tuviste que escribir o corregir el texto tú misma/o leyendo el PDF. |

## 8) Cómo editar `golden_text`

`golden_text` es el cuadro grande de texto editable, debajo de las decisiones. Tres
formas de completarlo:

- Si una de las 4 fuentes es exactamente correcta, haz clic en su botón
  **"Copiar a golden_text"** (esto solo copia el texto, tú decides aparte cuál decisión
  marcar).
- Si necesitas corregir pequeños detalles sobre una fuente casi correcta, cópiala y
  edítala directamente en el cuadro.
- Si ninguna sirve como base, bórralo y escribe tú el texto completo leyendo el PDF.

El objetivo final es que `golden_text` sea exactamente lo que dice el PDF en esa
página, ni más ni menos.

## 9-10) Qué ignorar y qué es jurídicamente crítico

La misma tabla está siempre visible dentro de la herramienta (panel "Guía rápida"), para
consultarla mientras revisas:

| Diferencia | ¿Qué hacer? |
|---|---|
| Tildes/acentos | Ignorar si la palabra se reconoce igual. No es crítico. |
| Espacios de más o de menos | Ignorar. No es crítico. |
| Saltos de línea en un lugar distinto | Ignorar dónde corta la línea; importa el orden de las palabras, no el layout. |
| Encabezados / pies de página | Inclúyelos tal cual si tienes dudas. **Es crítico** solo si contienen un número de norma o una fecha oficial que no aparece en ningún otro lugar de la página. |
| Números de artículo, numeral, inciso, literal | **CRÍTICO.** Un dígito distinto apunta a una disposición legal distinta. |
| Fechas | **CRÍTICO.** Un día, mes o año distinto cambia la vigencia de la norma. |
| Porcentajes | **CRÍTICO.** |
| Números de norma (Ley, D.S., D.L., D.U., R.M., R.D., R.S. + número/año) | **CRÍTICO.** Un dígito distinto es una norma completamente distinta. |
| Montos en soles (S/.) y plazos en días | **CRÍTICO.** |
| Dosis, concentraciones, medidas (mg, mL, %, °C) | **CRÍTICO** — puede ser un riesgo sanitario directo, no solo documental. |
| Verbos como DEROGA / MODIFICA / SUSTITUYE / INCORPORA / SUSPENDE, y la palabra "NO" justo antes de ellos | **EXTREMADAMENTE CRÍTICO.** "Deróguese" y "NO deróguese" son efectos legales opuestos, aunque ningún número haya cambiado. |
| Tablas (filas y columnas) | **CRÍTICO.** Ningún motor automático verifica si una fila o columna quedó bien ubicada — siempre léela contra el PDF real, celda por celda. |
| Firmas y sellos | No se pueden transcribir con certeza. Anota en "observaciones" algo como "firma/sello no verificable" — no lo marques como error. |

## 11) Cómo guardar tu trabajo

Haz clic en **"Guardar esta página y continuar"** después de elegir una decisión (y
completar `golden_text`). La herramienta pasa automáticamente a la siguiente página sin
revisar.

Este guardado es solo **dentro de la herramienta abierta en tu navegador** — para
guardarlo de verdad en un archivo, sigue el punto 12.

## 12) Dónde queda el archivo con tus decisiones

Este archivo HTML puede o no recordar tu avance si cierras el navegador — depende de
cómo lo hayas abierto y de tu navegador. **La única forma garantizada de no perder tu
trabajo** es hacer clic en el botón **"Exportar decisiones (JSON)"**, arriba de la
página. Esto descarga un archivo llamado `F04B_DECISIONES_HUMANAS.json` a tu carpeta de
Descargas (o donde tu navegador guarde las descargas). Muévelo a tu carpeta de trabajo
si quieres tenerlo ordenado junto a los demás archivos.

Guarda ese archivo cada cierto tiempo mientras avanzas, no solo al final — así, si algo
sale mal (se cierra el navegador, se apaga la computadora), no pierdes todo tu trabajo.
La herramienta también te avisará con un mensaje de confirmación si intentas cerrar la
pestaña con cambios sin exportar.

## 13) Cómo reanudar otro día sin perder el progreso

1. Abre la herramienta igual que la primera vez.
2. Vuelve a cargar los mismos archivos 1 y 2 (`F04_COMPARACION_MOTORES.json` y
   `F04_MANIFEST_PILOTO_V2.json`).
3. En la casilla **3) "Continuar progreso guardado"**, elige el último
   `F04B_DECISIONES_HUMANAS.json` que exportaste.
4. Tu avance reaparece tal como lo dejaste (las páginas que ya tenían decisión se
   marcan con un punto verde en la lista).

Si abriste la herramienta en el mismo navegador y computadora, es posible que además te
pregunte automáticamente si quieres recuperar un progreso guardado localmente — puedes
aceptar esa opción también, pero **no dependas solo de eso**: usa siempre el archivo
exportado como respaldo real.

## 14) Cómo entregar el archivo revisado

Cuando termines todas las páginas (o cuando quieras entregar un avance parcial),
exporta el JSON (punto 12) y **adjunta ese archivo `F04B_DECISIONES_HUMANAS.json`** en
tu próxima conversación con Claude (o por el medio que hayan acordado). Con ese archivo
se pueden calcular las métricas de error (CER/WER/LTER) de cada motor contra tu
revisión humana. No hace falta enviar el CSV: ese es solo para revisarlo tú misma/o en
Excel, la herramienta no lo puede volver a leer.

---

## Qué NO hace esta fase (F-04-B)

- No escribe nada en Supabase ni en ninguna base de datos.
- No usa ningún modelo de pago ni servicio en la nube.
- No se conecta a ningún servidor propio: la herramienta funciona enteramente en tu
  computadora, con archivos locales.
- No toca el bot de Telegram (`supabase/functions/telegram-bot`), sus comandos ni su
  despliegue — eso queda fuera de esta fase por decisión explícita.

**Regla general de todo el proyecto F-04: CONCORDANCIA != VERDAD. Ante duda: NO
VERIFICADO.** Si una página te genera dudas razonables, es preferible dejarla marcada
`NINGUNO_TRANSCRIBIR_MANUAL` con una observación explicando la duda, que forzar una
decisión de "correcto" sin estar segura/o.
