import { useState } from 'react'
import './App.css'
import { Sidebar } from './components/Sidebar'
import { TopBar } from './components/TopBar'
import { useNormas } from './hooks/useNormas'
import { BibliotecaNormativa } from './pages/BibliotecaNormativa'
import { Dashboard } from './pages/Dashboard'
import type { AppPage } from './types/normativa'

function App() {
  const [activePage, setActivePage] = useState<AppPage>('dashboard')
  const { normas, loading, error, reload } = useNormas()

  return (
    <div className="app-shell">
      <Sidebar activePage={activePage} onNavigate={setActivePage} />

      <div className="workspace">
        <TopBar activePage={activePage} loading={loading} error={error} />
        <main className="app-main" id="contenido-principal">
          {activePage === 'dashboard' ? (
            <Dashboard
              normas={normas}
              loading={loading}
              error={error}
              onRetry={reload}
            />
          ) : (
            <BibliotecaNormativa
              normas={normas}
              loading={loading}
              error={error}
              onRetry={reload}
            />
          )}
        </main>
      </div>
    </div>
  )
}

export default App
