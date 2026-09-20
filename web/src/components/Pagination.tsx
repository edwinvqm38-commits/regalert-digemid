type PaginationProps = {
  currentPage: number
  totalPages: number
  onPageChange: (page: number) => void
}

function getVisiblePages(currentPage: number, totalPages: number) {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, index) => index + 1)
  }

  const candidates = [1, totalPages, currentPage - 1, currentPage, currentPage + 1]
  const pages = [...new Set(candidates.filter((page) => page >= 1 && page <= totalPages))].sort(
    (a, b) => a - b,
  )
  const items: Array<number | string> = []

  pages.forEach((page, index) => {
    const previous = pages[index - 1]
    if (previous && page - previous > 1) items.push(`ellipsis-${previous}`)
    items.push(page)
  })

  return items
}

export function Pagination({ currentPage, totalPages, onPageChange }: PaginationProps) {
  if (totalPages <= 1) return null

  return (
    <nav className="pagination" aria-label="Paginación de documentos">
      <button
        type="button"
        className="pagination__direction"
        disabled={currentPage === 1}
        onClick={() => onPageChange(currentPage - 1)}
      >
        Anterior
      </button>

      <div className="pagination__pages">
        {getVisiblePages(currentPage, totalPages).map((item) =>
          typeof item === 'number' ? (
            <button
              type="button"
              className={item === currentPage ? 'pagination__page pagination__page--active' : 'pagination__page'}
              aria-label={`Página ${item}`}
              aria-current={item === currentPage ? 'page' : undefined}
              onClick={() => onPageChange(item)}
              key={item}
            >
              {item}
            </button>
          ) : (
            <span className="pagination__ellipsis" aria-hidden="true" key={item}>…</span>
          ),
        )}
      </div>

      <button
        type="button"
        className="pagination__direction"
        disabled={currentPage === totalPages}
        onClick={() => onPageChange(currentPage + 1)}
      >
        Siguiente
      </button>
    </nav>
  )
}
