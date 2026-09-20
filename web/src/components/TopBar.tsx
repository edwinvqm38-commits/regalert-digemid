import type { AppPage } from '../types/normativa'
import { Icon } from './Icon'

type TopBarProps = {
  activePage: AppPage
  loading: boolean
  error: string | null
}

const pageContext: Record<AppPage, { section: string; label: string }> = {
  dashboard: { section: 'Workspace regulatorio', label: 'Resumen' },
  biblioteca: { section: 'Gestión documental', label: 'Biblioteca normativa' },
}

export function TopBar({ activePage, loading, error }: TopBarProps) {
  const context = pageContext[activePage]
  const sourceState = error ? 'Fuente no disponible' : loading ? 'Sincronizando fuente' : 'Fuente documental conectada'

  return (
    <header className="topbar">
      <div className="topbar__context">
        <span>{context.section}</span>
        <strong>{context.label}</strong>
      </div>

      <div className="topbar__tools">
        <div className="global-search-placeholder" aria-disabled="true">
          <Icon name="search" />
          <span>Buscar en RegAlert</span>
          <small>Próximamente</small>
        </div>
        <span className={`source-state${error ? ' source-state--error' : ''}${loading ? ' source-state--loading' : ''}`}>
          <i aria-hidden="true" />
          {sourceState}
        </span>
      </div>
    </header>
  )
}
