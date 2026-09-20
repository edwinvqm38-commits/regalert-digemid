import type { ComponentProps } from 'react'
import { Icon } from './Icon'

type StatCardProps = {
  label: string
  value: number
  helper: string
  icon: ComponentProps<typeof Icon>['name']
  tone?: 'blue' | 'green' | 'amber' | 'red' | 'slate'
}

export function StatCard({ label, value, helper, icon, tone = 'blue' }: StatCardProps) {
  return (
    <article className={`stat-card stat-card--${tone}`}>
      <div className="stat-card__header">
        <span className="stat-card__label">{label}</span>
        <span className="stat-card__icon"><Icon name={icon} /></span>
      </div>
      <strong className="stat-card__value">{value.toLocaleString('es-PE')}</strong>
      <span className="stat-card__helper">{helper}</span>
    </article>
  )
}
