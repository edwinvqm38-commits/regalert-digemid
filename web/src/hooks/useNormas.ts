import { useCallback, useEffect, useState } from 'react'
import { supabase } from '../lib/supabase'
import type { Norma } from '../types/normativa'

const PAGE_SIZE = 1000
const NORMA_FIELDS = `
  id,
  document_key,
  source_section,
  tipo_norma,
  numero,
  anio,
  titulo,
  fecha_publicacion,
  entidad_emisora,
  fuente_oficial,
  source_url,
  pdf_url,
  process_status,
  estado_vigencia,
  derogacion_analizada,
  ocr_required,
  has_tables,
  botica_relevance,
  created_at,
  updated_at
`

async function fetchNormas() {
  const normas: Norma[] = []

  for (let from = 0; ; from += PAGE_SIZE) {
    const { data, error } = await supabase
      .from('digemid_normas')
      .select(NORMA_FIELDS)
      .order('fecha_publicacion', { ascending: false, nullsFirst: false })
      .order('document_key', { ascending: true })
      .range(from, from + PAGE_SIZE - 1)

    if (error) throw error

    const page = (data ?? []) as Norma[]
    normas.push(...page)

    if (page.length < PAGE_SIZE) return normas
  }
}

export function useNormas() {
  const [normas, setNormas] = useState<Norma[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [requestVersion, setRequestVersion] = useState(0)

  const reload = useCallback(() => {
    setRequestVersion((version) => version + 1)
  }, [])

  useEffect(() => {
    let active = true

    async function load() {
      setLoading(true)
      setError(null)

      try {
        const data = await fetchNormas()
        if (active) setNormas(data)
      } catch (loadError) {
        if (active) {
          setError(
            loadError instanceof Error
              ? loadError.message
              : 'No se pudo cargar la biblioteca normativa.',
          )
        }
      } finally {
        if (active) setLoading(false)
      }
    }

    void load()
    return () => {
      active = false
    }
  }, [requestVersion])

  return { normas, loading, error, reload }
}
