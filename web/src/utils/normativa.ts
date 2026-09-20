const completeProcessStatuses = new Set(['text_extracted', 'text_extracted_ocr'])

export function normalizeValue(value: string | null) {
  return value?.trim().toLowerCase() ?? ''
}

export function normalizeSearch(value: string) {
  return value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLocaleLowerCase('es-PE')
    .trim()
}

export function isProcessingPending(value: string | null) {
  return !completeProcessStatuses.has(normalizeValue(value))
}

export function formatDate(value: string | null) {
  if (!value) return 'Sin fecha'

  const [year, month, day] = value.slice(0, 10).split('-').map(Number)
  if (!year || !month || !day) return value

  return new Intl.DateTimeFormat('es-PE', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    timeZone: 'UTC',
  }).format(new Date(Date.UTC(year, month - 1, day)))
}
