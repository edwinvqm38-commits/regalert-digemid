import { useMemo, useState } from 'react'
import { DataState } from '../components/DataState'
import { Icon } from '../components/Icon'
import { NormaListItem } from '../components/NormaListItem'
import { Pagination } from '../components/Pagination'
import { RegulatoryNote } from '../components/RegulatoryNote'
import type { Norma } from '../types/normativa'
import { normalizeSearch, normalizeValue } from '../utils/normativa'

type BibliotecaNormativaProps = {
  normas: Norma[]
  loading: boolean
  error: string | null
  onRetry: () => void
}

const PAGE_SIZE = 25
const collator = new Intl.Collator('es-PE', { numeric: true, sensitivity: 'base' })

export function BibliotecaNormativa({ normas, loading, error, onRetry }: BibliotecaNormativaProps) {
  const [search, setSearch] = useState('')
  const [tipo, setTipo] = useState('')
  const [anio, setAnio] = useState('')
  const [vigencia, setVigencia] = useState('')
  const [currentPage, setCurrentPage] = useState(1)

  const tipos = useMemo(
    () => [...new Set(normas.map((norma) => norma.tipo_norma).filter((value): value is string => Boolean(value)))].sort(collator.compare),
    [normas],
  )
  const anios = useMemo(
    () => [...new Set(normas.map((norma) => norma.anio).filter((value): value is number => value !== null))].sort((a, b) => b - a),
    [normas],
  )
  const vigencias = useMemo(
    () => [...new Set(normas.map((norma) => norma.estado_vigencia).filter((value): value is string => Boolean(value)))].sort(collator.compare),
    [normas],
  )
  const quickTypes = useMemo(() => {
    const counts = normas.reduce<Record<string, number>>((accumulator, norma) => {
      if (norma.tipo_norma) accumulator[norma.tipo_norma] = (accumulator[norma.tipo_norma] ?? 0) + 1
      return accumulator
    }, {})

    return Object.entries(counts)
      .filter(([, count]) => count >= 10)
      .sort(([, countA], [, countB]) => countB - countA)
      .slice(0, 5)
  }, [normas])

  const filteredNormas = useMemo(() => {
    const query = normalizeSearch(search)

    return normas.filter((norma) => {
      const searchable = normalizeSearch(
        [norma.document_key, norma.titulo, norma.numero].filter(Boolean).join(' '),
      )

      return (
        (!query || searchable.includes(query)) &&
        (!tipo || norma.tipo_norma === tipo) &&
        (!anio || norma.anio === Number(anio)) &&
        (!vigencia || norma.estado_vigencia === vigencia)
      )
    })
  }, [anio, normas, search, tipo, vigencia])

  const totalPages = Math.max(1, Math.ceil(filteredNormas.length / PAGE_SIZE))
  const safePage = Math.min(currentPage, totalPages)
  const firstResultIndex = (safePage - 1) * PAGE_SIZE
  const paginatedNormas = filteredNormas.slice(firstResultIndex, firstResultIndex + PAGE_SIZE)
  const rangeStart = filteredNormas.length ? firstResultIndex + 1 : 0
  const rangeEnd = Math.min(firstResultIndex + PAGE_SIZE, filteredNormas.length)
  const hasFilters = Boolean(search || tipo || anio || vigencia)

  function clearFilters() {
    setSearch('')
    setTipo('')
    setAnio('')
    setVigencia('')
    setCurrentPage(1)
  }

  function chooseType(value: string) {
    setTipo(value)
    setCurrentPage(1)
  }

  return (
    <div className="page page--library">
      <header className="page-header page-header--library">
        <div>
          <p className="eyebrow">Explorador documental</p>
          <h1>Biblioteca normativa</h1>
          <p className="page-header__description">
            Explore, filtre y revise el acervo regulatorio registrado en RegAlert.
          </p>
        </div>
        {!loading && !error && (
          <div className="library-total">
            <strong>{normas.length.toLocaleString('es-PE')}</strong>
            <span>documentos</span>
          </div>
        )}
      </header>

      <DataState loading={loading} error={error} onRetry={onRetry} />

      {!loading && !error && (
        <>
          <section className="library-tools" aria-label="Búsqueda y filtros de la biblioteca">
            <label className="search-field">
              <span className="sr-only">Buscar por clave, título o número</span>
              <Icon name="search" />
              <input
                type="search"
                value={search}
                placeholder="Buscar por clave, título o número..."
                onChange={(event) => {
                  setSearch(event.target.value)
                  setCurrentPage(1)
                }}
              />
              <kbd>Clave · título · número</kbd>
            </label>

            {quickTypes.length > 0 && (
              <div className="quick-types" aria-label="Exploración rápida por tipo de norma">
                <span>Exploración rápida</span>
                <button type="button" className={!tipo ? 'quick-type quick-type--active' : 'quick-type'} onClick={() => chooseType('')}>
                  Todos <strong>{normas.length}</strong>
                </button>
                {quickTypes.map(([value, count]) => (
                  <button
                    type="button"
                    className={tipo === value ? 'quick-type quick-type--active' : 'quick-type'}
                    onClick={() => chooseType(value)}
                    key={value}
                  >
                    {value} <strong>{count}</strong>
                  </button>
                ))}
              </div>
            )}

            <div className="filters-row">
              <label className="select-field">
                <span>Tipo de norma</span>
                <select
                  value={tipo}
                  onChange={(event) => {
                    setTipo(event.target.value)
                    setCurrentPage(1)
                  }}
                >
                  <option value="">Todos los tipos</option>
                  {tipos.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
              </label>
              <label className="select-field">
                <span>Año</span>
                <select
                  value={anio}
                  onChange={(event) => {
                    setAnio(event.target.value)
                    setCurrentPage(1)
                  }}
                >
                  <option value="">Todos los años</option>
                  {anios.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
              </label>
              <label className="select-field">
                <span>Estado de vigencia</span>
                <select
                  value={vigencia}
                  onChange={(event) => {
                    setVigencia(event.target.value)
                    setCurrentPage(1)
                  }}
                >
                  <option value="">Todos los estados</option>
                  {vigencias.map((value) => (
                    <option key={value} value={value}>{normalizeValue(value).replaceAll('_', ' ')}</option>
                  ))}
                </select>
              </label>
              <button className="button button--clear" type="button" onClick={clearFilters} disabled={!hasFilters}>
                <Icon name="x" />
                Limpiar filtros
              </button>
            </div>

            {hasFilters && (
              <div className="active-filter-summary" aria-live="polite">
                <span>Filtros activos</span>
                {search && <i>Búsqueda: “{search}”</i>}
                {tipo && <i>Tipo: {tipo}</i>}
                {anio && <i>Año: {anio}</i>}
                {vigencia && <i>Vigencia: {normalizeValue(vigencia).replaceAll('_', ' ')}</i>}
              </div>
            )}
          </section>

          <section className="library-results" aria-labelledby="results-title">
            <div className="results-toolbar">
              <div>
                <span className="section-kicker">Resultados</span>
                <h2 id="results-title">{filteredNormas.length.toLocaleString('es-PE')} documentos encontrados</h2>
              </div>
              <span>{rangeStart}–{rangeEnd} de {filteredNormas.length.toLocaleString('es-PE')}</span>
            </div>

            {paginatedNormas.length === 0 ? (
              <div className="empty-state">
                <Icon name="search" />
                <strong>No se encontraron documentos</strong>
                <p>Pruebe con otros términos o limpie los filtros aplicados.</p>
              </div>
            ) : (
              <div className="norma-list norma-list--library">
                {paginatedNormas.map((norma) => <NormaListItem norma={norma} key={norma.id} />)}
              </div>
            )}

            <div className="results-footer">
              <span>Mostrando {rangeStart}–{rangeEnd} de {filteredNormas.length.toLocaleString('es-PE')}</span>
              <Pagination currentPage={safePage} totalPages={totalPages} onPageChange={setCurrentPage} />
            </div>
          </section>

          <RegulatoryNote />
        </>
      )}
    </div>
  )
}
