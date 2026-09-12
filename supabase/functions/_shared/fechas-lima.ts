/** Fechas en huso horario de Lima (America/Lima, UTC-5 todo el año).
 *
 * Independiente del canal (Telegram/WhatsApp): solo calcula fechas. La
 * semantica es identica a la que ya usa telegram-bot para /hoy, /semana y
 * /mes, para que ambos canales respondan lo mismo ante la misma pregunta.
 */

export function getLimaDateParts(): { isoDate: string; isoWeekday: number } {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Lima",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
  });

  const parts = formatter.formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value ?? "1970";
  const month = parts.find((part) => part.type === "month")?.value ?? "01";
  const day = parts.find((part) => part.type === "day")?.value ?? "01";
  const weekdayLabel = parts.find((part) => part.type === "weekday")?.value ?? "Mon";

  const weekdayMap: Record<string, number> = {
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
    Sun: 7,
  };

  return {
    isoDate: `${year}-${month}-${day}`,
    isoWeekday: weekdayMap[weekdayLabel] ?? 1,
  };
}

export function shiftIsoDate(isoDate: string, days: number): string {
  const [year, month, day] = isoDate.split("-").map(Number);
  const utcDate = new Date(Date.UTC(year, month - 1, day));
  utcDate.setUTCDate(utcDate.getUTCDate() + days);
  return utcDate.toISOString().slice(0, 10);
}

export function getCurrentWeekBounds(): { weekStart: string; weekEnd: string } {
  const { isoDate, isoWeekday } = getLimaDateParts();
  const weekStart = shiftIsoDate(isoDate, -(isoWeekday - 1));
  const weekEnd = shiftIsoDate(weekStart, 6);

  return { weekStart, weekEnd };
}

export function getLimaStartOfDayIso(): string {
  const { isoDate } = getLimaDateParts();
  return `${isoDate}T05:00:00.000Z`;
}

/** created_at (cuando RegAlert registro el documento) en formato legible de
 * Lima. NO es la fecha oficial de publicacion: esa es published_date y se
 * muestra aparte, sin mezclarlas. */
export function formatCreatedAtSimple(value: string | null | undefined): string {
  if (!value) return "Sin fecha";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);

  return new Intl.DateTimeFormat("es-PE", {
    timeZone: "America/Lima",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}
