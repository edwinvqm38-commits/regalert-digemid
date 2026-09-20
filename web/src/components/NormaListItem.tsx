import type { Norma } from '../types/normativa'
import { formatDate } from '../utils/normativa'
import { StatusBadge } from './StatusBadge'

type NormaListItemProps = {
  norma: Norma
  variant?: 'recent' | 'library'
}

export function NormaListItem({ norma, variant = 'library' }: NormaListItemProps) {
  return (
    <article className={`norma-item norma-item--${variant}`}>
      <div className="norma-item__identity">
        <code>{norma.document_key}</code>
        <span>{norma.tipo_norma ?? 'Tipo no registrado'}</span>
      </div>

      <div className="norma-item__body">
        <h3>{norma.titulo ?? 'Sin título registrado'}</h3>
        <p>
          {norma.anio ?? 'Año no registrado'}
          <span aria-hidden="true">·</span>
          {formatDate(norma.fecha_publicacion)}
        </p>
      </div>

      <div className="norma-item__states">
        <StatusBadge kind="vigencia" value={norma.estado_vigencia} />
        <StatusBadge kind="process" value={norma.process_status} />
      </div>
    </article>
  )
}
