import { useEffect, useState } from 'react'
import { supabase } from './lib/supabase'

type Norma = {
  id: string
  document_key: string
  tipo_norma: string | null
  numero: string | null
  anio: number | null
  titulo: string | null
  fecha_publicacion: string | null
  estado_vigencia: string | null
  process_status: string | null
}

function App() {
  const [normas, setNormas] = useState<Norma[]>([])
  const [cargando, setCargando] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function cargarNormas() {
      const { data, error } = await supabase
        .from('digemid_normas')
        .select(`
          id,
          document_key,
          tipo_norma,
          numero,
          anio,
          titulo,
          fecha_publicacion,
          estado_vigencia,
          process_status
        `)
        .order('fecha_publicacion', { ascending: false })
        .limit(10)

      if (error) {
        setError(error.message)
      } else {
        setNormas(data ?? [])
      }

      setCargando(false)
    }

    cargarNormas()
  }, [])

  if (cargando) {
    return (
      <main>
        <h1>DIGEMID RegAlert</h1>
        <p>Cargando normas...</p>
      </main>
    )
  }

  if (error) {
    return (
      <main>
        <h1>DIGEMID RegAlert</h1>
        <p>Error de conexión: {error}</p>
      </main>
    )
  }

  return (
    <main>
      <h1>DIGEMID RegAlert</h1>

      <p>Biblioteca normativa conectada a Supabase.</p>

      <h2>Últimas normas</h2>

      {normas.length === 0 ? (
        <p>La conexión funciona, pero no se recibieron normas.</p>
      ) : (
        <ul>
          {normas.map((norma) => (
            <li key={norma.id}>
              <strong>{norma.document_key}</strong>
              {' — '}
              {norma.titulo ?? 'Sin título'}
              {' — '}
              {norma.estado_vigencia ?? 'Vigencia no determinada'}
            </li>
          ))}
        </ul>
      )}
    </main>
  )
}

export default App