import { DataState } from '../components/DataState'
import { NormaListItem } from '../components/NormaListItem'
import { RegulatoryNote } from '../components/RegulatoryNote'
import { StatusBadge } from '../components/StatusBadge'
import type { Norma } from '../types/normativa'
import { isProcessingPending, normalizeValue } from '../utils/normativa'

type DashboardProps = {
  normas: Norma[]
  loading: boolean
  error: string | null
  onRetry: () => void
}

export function Dashboard({ normas, loading, error, onRetry }: DashboardProps) {
  const recientes = normas.filter((norma) => norma.fecha_publicacion).slice(0, 8)
  const countByVigencia = (status: string) =>
    normas.filter((norma) => normalizeValue(norma.estado_vigencia) === status).length
  const completeCount = normas.filter((norma) => !isProcessingPending(norma.process_status)).length
  const pendingCount = normas.length - completeCount
  const completePercentage = normas.length ? Math.round((completeCount / normas.length) * 100) : 0

  const processCounts = Object.entries(
    normas.reduce<Record<string, number>>((counts, norma) => {
      const status = norma.process_status ?? 'Sin registro'
      counts[status] = (counts[status] ?? 0) + 1
      return counts
    }, {}),
  ).sort(([, countA], [, countB]) => countB - countA)

  return (
    <div className="page page--dashboard">
      <header className="page-header">
        <p className="eyebrow">Inteligencia regulatoria</p>
        <h1>Panorama regulatorio</h1>
        <p className="page-header__description">
          Estado del acervo normativo y del procesamiento documental registrado en RegAlert.
        </p>
      </header>

      <DataState loading={loading} error={error} onRetry={onRetry} />

      {!loading && !error && (
        <>
          <section className="regulatory-overview" aria-label="Panorama regulatorio">
            <div className="overview-primary">
              <span className="section-kicker">Total documentos</span>
              <div className="document-total">
                <strong>{normas.length.toLocaleString('es-PE')}</strong>
                <span>documentos regulatorios</span>
              </div>
              <p>Registros recuperados de la biblioteca documental de RegAlert.</p>

              <dl className="secondary-metrics">
                <div>
                  <dt>Vigentes</dt>
                  <dd>{countByVigencia('vigente').toLocaleString('es-PE')}</dd>
                </div>
                <div>
                  <dt>Modificadas</dt>
                  <dd>{countByVigencia('modificada').toLocaleString('es-PE')}</dd>
                </div>
                <div>
                  <dt>Derogadas / parcialmente</dt>
                  <dd>{(countByVigencia('derogada') + countByVigencia('derogada_parcialmente')).toLocaleString('es-PE')}</dd>
                </div>
                <div>
                  <dt>Procesamiento pendiente</dt>
                  <dd>{pendingCount.toLocaleString('es-PE')}</dd>
                </div>
              </dl>
            </div>

            <aside className="document-status" aria-labelledby="document-status-title">
              <div className="section-heading section-heading--compact">
                <div>
                  <span className="section-kicker">Cobertura</span>
                  <h2 id="document-status-title">Estado documental</h2>
                </div>
                <strong className="coverage-value">{completePercentage}%</strong>
              </div>

              <div className="progress-summary">
                <div className="progress-label">
                  <span>Documentos con extracción completa</span>
                  <strong>{completeCount.toLocaleString('es-PE')} / {normas.length.toLocaleString('es-PE')}</strong>
                </div>
                <div
                  className="progress-track"
                  role="progressbar"
                  aria-label="Documentos con extracción completa"
                  aria-valuemin={0}
                  aria-valuemax={normas.length}
                  aria-valuenow={completeCount}
                >
                  <span style={{ width: `${completePercentage}%` }} />
                </div>
              </div>

              <div className="document-status__totals">
                <div><i className="status-key status-key--complete" /><span>Extracción completa</span><strong>{completeCount}</strong></div>
                <div><i className="status-key status-key--pending" /><span>Procesamiento pendiente</span><strong>{pendingCount}</strong></div>
              </div>

              <div className="process-breakdown">
                <span className="process-breakdown__label">Estados registrados</span>
                {processCounts.slice(0, 4).map(([status, count]) => (
                  <div className="process-breakdown__row" key={status}>
                    <StatusBadge kind="process" value={status === 'Sin registro' ? null : status} />
                    <strong>{count}</strong>
                  </div>
                ))}
              </div>
            </aside>
          </section>

          <section className="recent-section" aria-labelledby="recent-title">
            <div className="section-heading">
              <div>
                <span className="section-kicker">Actividad reciente</span>
                <h2 id="recent-title">Normas recientes</h2>
                <p>Últimos registros por fecha de publicación.</p>
              </div>
              <span className="section-count">{recientes.length} documentos</span>
            </div>

            {recientes.length === 0 ? (
              <div className="empty-state">No hay normas con fecha de publicación registrada.</div>
            ) : (
              <div className="norma-list norma-list--recent">
                {recientes.map((norma) => <NormaListItem norma={norma} variant="recent" key={norma.id} />)}
              </div>
            )}
          </section>

          <RegulatoryNote />
        </>
      )}
    </div>
  )
}
