export type Norma = {
  id: string
  document_key: string
  source_section: string | null
  tipo_norma: string | null
  numero: string | null
  anio: number | null
  titulo: string | null
  fecha_publicacion: string | null
  entidad_emisora: string | null
  fuente_oficial: string | null
  source_url: string | null
  pdf_url: string | null
  process_status: string | null
  estado_vigencia: string | null
  derogacion_analizada: boolean | null
  ocr_required: boolean | null
  has_tables: boolean | null
  botica_relevance: string | null
  created_at: string
  updated_at: string
}

export type AppPage = 'dashboard' | 'biblioteca'
