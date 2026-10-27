# Guía de comandos y revisión documental — RegAlert DIGEMID

> Documento operativo del MVP.  
> Principio: **primero conservar el documento oficial, luego extraer texto fiel, después estructurar lo necesario y finalmente consultar con IA con trazabilidad.**

## 1. Comandos de uso frecuente

### Alertas DIGEMID

| Comando | Uso |
|---|---|
| `/ultimas` | Últimas alertas registradas. |
| `/hoy` | Alertas con publicación oficial de hoy. |
| `/semana` | Alertas publicadas en la semana actual. |
| `/recientes` | Alertas registradas recientemente en RegAlert. |
| `/mes` | Alertas publicadas durante el mes actual. |
| `/buscar texto` | Busca alertas por producto, laboratorio, lote o palabra clave. |
| `/detalle 50-2026` | Ficha de una alerta. |

### Normativa

| Comando | Uso |
|---|---|
| `/normas` | Últimas normas registradas. |
| `/norma DOCUMENT_KEY` | Ficha documental de una norma. Ej.: `/norma RM-1753-2002`. |
| `/consulta pregunta` | Consulta con IA y evidencia normativa/alerta. |
| `/normaestado [DOCUMENT_KEY]` | Reporte técnico de fidelidad documental. |
| `/revision` | Bandeja guiada de revisión documental. |
| `/guia` | Resumen rápido de esta guía y acceso a la wiki. |

## 2. Flujo recomendado de revisión

Usar siempre `/revision`. Los comandos técnicos antiguos se conservan para compatibilidad, pero no son el flujo normal.

```text
/revision
   ↓
Elegir norma
   ↓
Elegir tipo de observación
   ↓
Elegir UNA página
   ↓
PDF oficial completo + TXT de esa página + REVISION_ID
   ↓
Subir PDF + TXT a ChatGPT
   ↓
Corregir únicamente esa página
   ↓
Reenviar TXT corregido a Telegram
   ↓
✅ aceptado / ⚠️ sigue observado
   ↓
➡️ Siguiente observación
```

### ¿Por qué una página por vez?

- reduce errores de página;
- evita revisar PDFs completos de 30, 60 o más páginas;
- facilita comparar visualmente contra la fuente;
- permite que cada corrección tenga trazabilidad propia;
- simplifica tablas, fórmulas y gráficos complejos.

## 3. REVISION_ID

Cada paquete de revisión por página incorpora un identificador estable:

```text
# REVISION_ID: REV-RM-1753-2002-P48-FORMULA
# NORMA: RM-1753-2002
# TIPO_REVISION: contenido
# MOTIVO_REVISION: formula
```

El nombre del archivo puede cambiar al descargarlo o compartirlo. RegAlert identifica la revisión por el contenido del TXT, no por el nombre del archivo.

**No borrar ni modificar:**
- `# REVISION_ID`
- `# NORMA`
- `# TIPO_REVISION`
- `# MOTIVO_REVISION`
- `### PAGINA ...`

## 4. Qué enviar a ChatGPT

Para cada revisión subir:

1. PDF oficial completo entregado por Telegram.
2. TXT de revisión de la página.
3. La instrucción que muestra RegAlert.

Después pedir a ChatGPT que devuelva **solo el TXT corregido**, sin resumir ni interpretar.

### Texto / OCR

Objetivo: corregir transcripción.

Instrucción sugerida:

> Compara únicamente la página indicada con el PDF oficial. Corrige errores de transcripción. No resumas, no interpretes y no agregues contenido.

### Tabla no estructurada

Objetivo: conservar la relación visual entre filas, columnas, encabezados y notas.

Instrucción sugerida:

> Reconstruye fielmente encabezados, filas, columnas, celdas combinadas, unidades y notas visibles. No completes valores ausentes ni deduzcas datos.

Una tabla se considera verificada únicamente mediante revisión de tipo `tabla`. Una revisión narrativa no debe marcar `tabla_verificada=true`.

### Fórmula o notación técnica

Objetivo: preservar literalmente la notación.

Instrucción sugerida:

> Conserva exactamente símbolos, operadores, agrupaciones, fracciones, subíndices y superíndices. No simplifiques ni interpretes la fórmula.

### Gráfico / diagrama / imagen de referencia

Objetivo: registrar fielmente lo visible.

Instrucción sugerida:

> Transcribe títulos, ejes, rótulos, leyendas, valores y relaciones visibles. No inventes ni deduzcas información que no aparezca en el gráfico.

Si el gráfico no puede representarse fielmente como texto, mantener una descripción visual conservadora y dejar pendiente la estructuración avanzada.

### Anexo / formulario

Objetivo: conservar títulos, campos, casillas, instrucciones, firmas y estructura.

Instrucción sugerida:

> Conserva los campos, casillas, títulos, instrucciones, fechas, espacios de firma y orden visual del anexo. No completar campos vacíos.

## 5. Qué ocurre cuando se devuelve el TXT

RegAlert:

1. lee `REVISION_ID`, norma, motivo y página;
2. valida que correspondan entre sí;
3. comprueba estructura básica;
4. conserva `text_raw` original;
5. guarda la corrección en la capa revisada;
6. registra el historial en `digemid_norma_revisiones`;
7. vuelve a evaluar la página;
8. responde:
   - `✅ Revisión aceptada por el sistema`, o
   - `⚠️ Sigue observado`;
9. si quedan pendientes, muestra `➡️ Siguiente observación`.

**“Aceptada por el sistema” no significa dictamen jurídico.** Significa que la revisión pasó controles estructurales y quedó registrada. La fuente de verdad sigue siendo el PDF archivado.

## 6. PDFs y Google Drive

Para normativa antigua, los enlaces históricos de DIGEMID/MINSA pueden desaparecer.

Regla operativa:

```text
Fuente oficial histórica
        ↓
conservar URL como trazabilidad
        ↓
archivar PDF en Google Drive
        ↓
Supabase guarda metadata + drive_file_id + drive_file_url
        ↓
/norma → 📄 Ver PDF desde Drive
```

Estructura objetivo:

```text
DIGEMID/
├── 01_ALERTAS/
├── 02_NORMATIVA/
│   ├── LEY-29459/
│   ├── DS-014-2011-SA/
│   └── RM-1753-2002/
├── 03_POES_BOTICA/
└── 04_CONSULTAS_IA/
```

Google Drive conserva el documento. Supabase funciona como índice regulatorio, metadata, páginas, chunks, relaciones e historial.

## 7. Comandos técnicos de administrador

Estos comandos siguen funcionando, pero normalmente se debe preferir `/revision`.

| Comando | Uso |
|---|---|
| `/normasrevisar` | Lista páginas de baja confiabilidad. |
| `/normarevisar DOCUMENT_KEY` | Revisión técnica de páginas de baja calidad. |
| `/revisarnorma DOCUMENT_KEY` | Revisión completa de una norma. Usar solo excepcionalmente. |
| `/tablasrevisar` | Lista tablas sin verificar. |
| `/tablarevisar DOCUMENT_KEY` | Revisión específica de tablas. |
| `/normassinpdf` | Normas sin PDF confirmado. |
| `/normapdf DOCUMENT_KEY` | Vincula/sube manualmente un PDF. |
| `/derogacionespendientes` | Relaciones normativas pendientes de validación. |
| `/reportenormas` | Reporte maestro documental. |

## 8. Reglas de seguridad documental

- No inventar texto faltante.
- No corregir jurídicamente una norma durante la transcripción.
- No sustituir el PDF oficial por un resumen.
- No sobrescribir `text_raw`.
- No marcar una tabla como verificada si no fue revisada como tabla.
- No ocultar advertencias de OCR sin revisión.
- Mantener documento, página, fecha y fuente trazables.
- El Director Técnico y la autoridad sanitaria conservan la decisión regulatoria final.

## 9. Si algo sale mal

**El TXT no es aceptado:** volver a descargar la plantilla desde `/revision` y no modificar encabezados.

**El PDF histórico da 404:** archivar una copia confiable en Drive y registrar su origen en Supabase.

**Una página tiene tabla + fórmula:** puede requerir dos revisiones diferentes. Terminar una no debe borrar la otra.

**El documento tiene muchas páginas:** revisar solo las páginas listadas en `/revision`; evitar `/revisarnorma` salvo auditoría excepcional.

---

Última actualización: 2026-10-27.
