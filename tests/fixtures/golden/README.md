# Golden dataset — transcripciones de referencia

**La verdad es el PDF oficial leído por una persona.** La salida de cualquier
modelo —incluido el que produjo el candidato— **nunca** es ground truth.

## Cómo se construye

1. Ejecutar el workflow `F-02 - Auditoria de custodia y piloto de verificacion`
   con `tarea: revision_visual` y las `document_key` deseadas.
2. Descargar el artifact y abrir `REVISION_VISUAL.html` en el navegador.
3. Por cada página: mirar la **imagen del PDF** (izquierda) y **confirmar o
   corregir** la transcripción candidata (derecha). No hay que reescribirla.
   Prestar atención a los tokens resaltados: número de norma, año, artículos,
   plazos, montos y los verbos `derogar` / `modificar`.
4. Pulsar «Descargar golden dataset (JSON)» y guardar el archivo aquí.

Nada se envía a ningún servidor: la revisión ocurre entera en el navegador.

## Estructura por página

```
golden/<DOCUMENT_KEY>-p<N>/
    referencia.txt    ← la transcripción confirmada por la persona
    fuente.json       ← { document_key, page_number, pdf_sha256, url_oficial,
                          transcrito_por, fecha, metodo, estado }
```

`estado` ∈ `confirmada` · `corregida` · `ilegible`.

`ilegible` es un resultado **válido y útil**: significa que esa página no puede
sostener una afirmación jurídica, y es preferible a una invención silenciosa.

## Cobertura mínima (30–50 páginas)

**Casos históricos que ya causaron errores** — prioridad máxima:

| norma | por qué |
|---|---|
| `RM-894-2024` | derogación fuera de la ventana de texto analizada (H-01) |
| `RM-899-2025` | cita de linaje confundida con objeto de la relación |
| `RM-419-2025` | proyecto anexado leído como parte dispositiva |
| `RM-727-2025` | decreto anexado dentro de una resolución |
| `RM-883-2024` | contaminación entre normas de la misma edición de El Peruano |
| `DS-008-2025` | modificación de reglamento aprobado por otro DS |
| `DS-015-2025` | dos afectaciones distintas a la misma norma (art. 43 vs Anexo 01) |
| `DS-12-2023` | 5 derogaciones que solo aparecieron tras corregir el truncado |
| `LEY-32319-2025` | exoneración confundida con modificación |

**Y estos casos del corpus, detectados en F-02:**

| norma | por qué |
|---|---|
| `LEY-29698` **y** `RM-373-2024` | 33 páginas **idénticas**: una guarda el texto de la otra |
| `DS-9-2015` **y** `DS-10-2015` | 4 páginas idénticas, mismo PDF de origen |
| `RM-195-2022` **y** `RM-98-2024` | 2 páginas idénticas |
| `DS-10-2017` | `file_storage_path` roto; es el origen de una relación confirmada |

**Tipos de página que deben estar representados:** digital limpio · escaneo malo ·
letra pequeña · tabla · anexo · **última página** · disposición derogatoria ·
edición de El Peruano con varias normas · página con sellos o firmas.
