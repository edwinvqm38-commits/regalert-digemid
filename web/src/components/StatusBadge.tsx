type StatusBadgeProps = {
  value: string | null
  kind: 'vigencia' | 'process'
}

const vigenciaLabels: Record<string, string> = {
  vigente: 'Vigente',
  modificada: 'Modificada',
  derogada: 'Derogada',
  derogada_parcialmente: 'Derogada parcialmente',
}

const processLabels: Record<string, string> = {
  registered: 'Registrada',
  inventory_imported: 'Inventario importado',
  pdf_detected: 'PDF detectado',
  drive_structured: 'Respaldo estructurado',
  text_extracted: 'Texto extraído',
  text_extracted_ocr: 'Texto extraído (OCR)',
  text_extracted_baja_calidad: 'Texto de baja calidad',
  text_extraction_partial: 'Extracción parcial',
  ocr_required: 'OCR requerido',
  pdf_download_error: 'Error de descarga',
  text_extraction_error: 'Error de extracción',
  pdf_detection_error: 'Error de detección',
  ocr_dependency_error: 'Error de OCR',
  pdf_drive_error: 'Error de respaldo',
  metadata_error: 'Error de metadatos',
}

const processSuccess = new Set(['text_extracted', 'text_extracted_ocr'])
const processWarning = new Set(['text_extracted_baja_calidad', 'text_extraction_partial', 'ocr_required'])
const processDanger = new Set([
  'pdf_download_error',
  'text_extraction_error',
  'pdf_detection_error',
  'ocr_dependency_error',
  'pdf_drive_error',
  'metadata_error',
])
const processInfo = new Set(['registered', 'inventory_imported', 'pdf_detected', 'drive_structured'])

function normalize(value: string | null) {
  return value?.trim().toLowerCase() ?? ''
}

function humanize(value: string) {
  const text = value.replaceAll('_', ' ')
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : 'Sin registro'
}

export function StatusBadge({ value, kind }: StatusBadgeProps) {
  const normalized = normalize(value)
  let tone = 'neutral'
  let label = value ? humanize(value) : 'Sin registro'

  if (kind === 'vigencia') {
    label = vigenciaLabels[normalized] ?? label
    if (normalized === 'vigente') tone = 'success'
    if (normalized === 'modificada') tone = 'info'
    if (normalized === 'derogada') tone = 'danger'
    if (normalized === 'derogada_parcialmente') tone = 'warning'
  } else {
    label = processLabels[normalized] ?? label
    if (processSuccess.has(normalized)) tone = 'success'
    if (processWarning.has(normalized)) tone = 'warning'
    if (processDanger.has(normalized)) tone = 'danger'
    if (processInfo.has(normalized)) tone = 'info'
  }

  return <span className={`status-badge status-badge--${tone}`}>{label}</span>
}
