import type { SVGProps } from 'react'

type IconName =
  | 'alert'
  | 'bell'
  | 'check'
  | 'clipboard'
  | 'clock'
  | 'dashboard'
  | 'file'
  | 'library'
  | 'menu'
  | 'search'
  | 'sparkles'
  | 'x'

type IconProps = SVGProps<SVGSVGElement> & { name: IconName }

export function Icon({ name, ...props }: IconProps) {
  const commonProps = {
    'aria-hidden': true,
    fill: 'none',
    stroke: 'currentColor',
    strokeLinecap: 'round' as const,
    strokeLinejoin: 'round' as const,
    strokeWidth: 1.8,
    viewBox: '0 0 24 24',
    ...props,
  }

  if (name === 'dashboard') {
    return <svg {...commonProps}><rect x="3" y="3" width="7" height="7" rx="1" /><rect x="14" y="3" width="7" height="4" rx="1" /><rect x="14" y="11" width="7" height="10" rx="1" /><rect x="3" y="14" width="7" height="7" rx="1" /></svg>
  }
  if (name === 'library') {
    return <svg {...commonProps}><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" /><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2Z" /><path d="M8 7h8" /><path d="M8 11h6" /></svg>
  }
  if (name === 'bell') {
    return <svg {...commonProps}><path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9" /><path d="M10 21h4" /></svg>
  }
  if (name === 'sparkles') {
    return <svg {...commonProps}><path d="m12 3-1.2 3.2L7.5 7.5l3.3 1.3L12 12l1.2-3.2 3.3-1.3-3.3-1.3L12 3Z" /><path d="m18.5 13-.8 2.2-2.2.8 2.2.8.8 2.2.8-2.2 2.2-.8-2.2-.8-.8-2.2Z" /><path d="m5.5 13-.7 1.8-1.8.7 1.8.7.7 1.8.7-1.8 1.8-.7-1.8-.7-.7-1.8Z" /></svg>
  }
  if (name === 'clipboard') {
    return <svg {...commonProps}><rect x="5" y="4" width="14" height="17" rx="2" /><path d="M9 4a3 3 0 0 1 6 0v2H9V4Z" /><path d="m9 14 2 2 4-4" /></svg>
  }
  if (name === 'menu') {
    return <svg {...commonProps}><path d="M4 6h16M4 12h16M4 18h16" /></svg>
  }
  if (name === 'search') {
    return <svg {...commonProps}><circle cx="11" cy="11" r="7" /><path d="m20 20-4-4" /></svg>
  }
  if (name === 'x') {
    return <svg {...commonProps}><path d="m6 6 12 12M18 6 6 18" /></svg>
  }
  if (name === 'file') {
    return <svg {...commonProps}><path d="M6 2h8l4 4v16H6z" /><path d="M14 2v5h5M9 12h6M9 16h6" /></svg>
  }
  if (name === 'check') {
    return <svg {...commonProps}><circle cx="12" cy="12" r="9" /><path d="m8 12 2.5 2.5L16 9" /></svg>
  }
  if (name === 'alert') {
    return <svg {...commonProps}><path d="M12 3 2.8 20h18.4L12 3Z" /><path d="M12 9v5M12 17.5h.01" /></svg>
  }

  return <svg {...commonProps}><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></svg>
}
