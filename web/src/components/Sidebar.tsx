import { useState } from 'react'
import type { AppPage } from '../types/normativa'
import { Icon } from './Icon'

type SidebarProps = {
  activePage: AppPage
  onNavigate: (page: AppPage) => void
}

export function Sidebar({ activePage, onNavigate }: SidebarProps) {
  const [mobileOpen, setMobileOpen] = useState(false)

  function navigate(page: AppPage) {
    onNavigate(page)
    setMobileOpen(false)
  }

  return (
    <aside className={`sidebar${mobileOpen ? ' sidebar--open' : ''}`}>
      <div className="sidebar__brand-row">
        <div className="brand-mark" aria-hidden="true">
          <span>R</span>
        </div>
        <div className="brand-copy">
          <strong>REGALERT</strong>
          <span>DIGEMID · Inteligencia regulatoria</span>
        </div>
        <button
          className="sidebar__toggle"
          type="button"
          aria-label={mobileOpen ? 'Cerrar menú' : 'Abrir menú'}
          aria-expanded={mobileOpen}
          onClick={() => setMobileOpen((open) => !open)}
        >
          <Icon name={mobileOpen ? 'x' : 'menu'} />
        </button>
      </div>

      <nav className="sidebar__nav" aria-label="Navegación principal">
        <div className="nav-group">
          <span className="sidebar__section-label">Principal</span>
          <button
            className={`nav-item${activePage === 'dashboard' ? ' nav-item--active' : ''}`}
            type="button"
            aria-current={activePage === 'dashboard' ? 'page' : undefined}
            onClick={() => navigate('dashboard')}
          >
            <Icon name="dashboard" />
            <span>Resumen</span>
          </button>
          <button
            className={`nav-item${activePage === 'biblioteca' ? ' nav-item--active' : ''}`}
            type="button"
            aria-current={activePage === 'biblioteca' ? 'page' : undefined}
            onClick={() => navigate('biblioteca')}
          >
            <Icon name="library" />
            <span>Biblioteca normativa</span>
          </button>
        </div>

        <div className="nav-group">
          <span className="sidebar__section-label">Inteligencia</span>
          <button className="nav-item nav-item--disabled" type="button" disabled>
            <Icon name="bell" />
            <span>Alertas DIGEMID</span>
            <span className="nav-item__soon">Próx.</span>
          </button>
          <button className="nav-item nav-item--disabled" type="button" disabled>
            <Icon name="sparkles" />
            <span>Consulta IA</span>
            <span className="nav-item__soon">Próx.</span>
          </button>
        </div>

        <div className="nav-group">
          <span className="sidebar__section-label">Cumplimiento</span>
          <button className="nav-item nav-item--disabled" type="button" disabled>
            <Icon name="clipboard" />
            <span>POES y cumplimiento</span>
            <span className="nav-item__soon">Próx.</span>
          </button>
        </div>
      </nav>

      <div className="sidebar__footer">
        <span className="sidebar__status-dot" aria-hidden="true" />
        <div>
          <strong>Fuente documental</strong>
          <span>Conectada</span>
        </div>
      </div>
    </aside>
  )
}
