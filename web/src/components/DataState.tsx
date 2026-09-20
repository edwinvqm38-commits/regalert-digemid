import { Icon } from './Icon'

type DataStateProps = {
  loading: boolean
  error: string | null
  onRetry: () => void
}

export function DataState({ loading, error, onRetry }: DataStateProps) {
  if (loading) {
    return (
      <div className="data-state" role="status">
        <span className="spinner" />
        <div>
          <strong>Cargando registros normativos</strong>
          <p>Consultando la biblioteca documental…</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="data-state data-state--error" role="alert">
        <Icon name="alert" />
        <div>
          <strong>No se pudo cargar la información</strong>
          <p>{error}</p>
          <button type="button" className="button button--secondary" onClick={onRetry}>Reintentar</button>
        </div>
      </div>
    )
  }

  return null
}
