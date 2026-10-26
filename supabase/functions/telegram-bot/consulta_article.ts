export const MAX_ARTICLE_EVIDENCE_PAGES = 8;

export type ArticleMatchMethod = "exact" | "ocr_contextual_recovery";

export type ArticlePage = {
  page_number: number;
  text_normalized?: string | null;
  text_raw?: string | null;
  source_asset_id?: string | number | null;
  article_match_method?: ArticleMatchMethod;
  [key: string]: unknown;
};

export type ArticleSelection = {
  status: "found" | "not_found" | "limit_exceeded";
  pages: ArticlePage[];
  match_method?: ArticleMatchMethod;
};

const ARTICLE_WORD = String.raw`art[íi1]cu[l1i]o`;
const HEADING_PATTERN = new RegExp(
  String
    .raw`^[\t ]*(?::[\t ]*)?(?:[-•]\s*)?${ARTICLE_WORD}\s+(?:n[°ºo]\.?\s*)?([-–—]?(?:\d{1,4}(?:(?:[-–—]\s*)?[a-z])?|[ivxlcdm]{1,8}|[sS]))([°º*]?)(?:[\t ]*(?:\.?[\t ]*[-–—]))?`,
  "gim",
);

type ArticleHeading = {
  pageIndex: number;
  offset: number;
  rawIdentifier: string;
  suffix: string;
};

function structuralPageText(page: ArticlePage): string {
  if (typeof page.text_raw === "string" && page.text_raw.trim()) {
    return page.text_raw;
  }
  return typeof page.text_normalized === "string" ? page.text_normalized : "";
}

function scanHeadings(pages: ArticlePage[]): ArticleHeading[] {
  const headings: ArticleHeading[] = [];
  for (let pageIndex = 0; pageIndex < pages.length; pageIndex++) {
    const text = structuralPageText(pages[pageIndex]);
    const pattern = new RegExp(HEADING_PATTERN.source, HEADING_PATTERN.flags);
    for (
      let match = pattern.exec(text);
      match;
      match = pattern.exec(text)
    ) {
      headings.push({
        pageIndex,
        offset: match.index,
        rawIdentifier: match[1],
        suffix: match[2] ?? "",
      });
    }
  }
  return headings;
}

function cleanDigits(heading: ArticleHeading): string | null {
  const value = heading.rawIdentifier.replace(/^[-–—]/, "");
  return /^\d+$/.test(value) ? value : null;
}

function exactHeadingValue(heading: ArticleHeading): number | null {
  if (/^[-–—]/.test(heading.rawIdentifier)) return null;
  const digits = cleanDigits(heading);
  return digits === null ? null : Number(digits);
}

function normalizedIdentifier(value: string): string {
  const compact = value.toLowerCase().replace(/\s+/g, "");
  return /^\d+[-–—][a-z]$/.test(compact)
    ? compact.replace(/[-–—]/, "")
    : compact;
}

function isExactRequested(
  heading: ArticleHeading,
  requested: string,
): boolean {
  if (/^[-–—]/.test(heading.rawIdentifier)) return false;
  return normalizedIdentifier(heading.rawIdentifier) ===
    normalizedIdentifier(requested);
}

function malformedCandidateFor(
  heading: ArticleHeading,
  requested: number,
): boolean {
  const raw = heading.rawIdentifier.toLowerCase();
  if (requested === 5 && raw === "s") return true;
  if (!/^[-–—]/.test(raw)) return false;
  const digits = raw.slice(1);
  const expected = String(requested);
  // Admite ruido pegado después del número solo dentro de una secuencia N-1/N+1.
  return digits === expected ||
    (digits.startsWith(expected) && digits.length <= expected.length + 2);
}

function canRepresentNeighbor(
  heading: ArticleHeading,
  expected: number,
): boolean {
  const exact = exactHeadingValue(heading);
  if (exact === expected) return true;
  const digits = cleanDigits(heading);
  // Tesseract confunde a veces el ordinal ° con cero: "Artículo 4°" -> "40".
  // Solo se acepta como vecino de una terna completa, nunca como match aislado.
  return digits === `${expected}0` && !heading.suffix;
}

function locateStart(
  headings: ArticleHeading[],
  requestedIdentifier: string,
): { headingIndex: number; method: ArticleMatchMethod } | null {
  const exactIndex = headings.findIndex((heading) =>
    isExactRequested(heading, requestedIdentifier)
  );
  if (exactIndex >= 0) return { headingIndex: exactIndex, method: "exact" };
  const requested = Number(requestedIdentifier);
  if (!Number.isInteger(requested) || requested < 1) return null;
  if (requested <= 1) return null;

  for (let index = 1; index < headings.length - 1; index++) {
    if (!malformedCandidateFor(headings[index], requested)) continue;
    if (!canRepresentNeighbor(headings[index - 1], requested - 1)) continue;
    if (!canRepresentNeighbor(headings[index + 1], requested + 1)) continue;
    return { headingIndex: index, method: "ocr_contextual_recovery" };
  }
  return null;
}

function isRepeatedRequestedHeading(
  heading: ArticleHeading,
  start: ArticleHeading,
  requested: string,
): boolean {
  return heading.rawIdentifier.toLowerCase() ===
      start.rawIdentifier.toLowerCase() || isExactRequested(heading, requested);
}

function withSelectedText(
  page: ArticlePage,
  text: string,
  method: ArticleMatchMethod,
): ArticlePage {
  return {
    ...page,
    // Copia literal de un intervalo existente; nunca completa ni reconstruye.
    text_normalized: text,
    text_raw: text,
    article_match_method: method,
  };
}

/** Identificadores usados solo para traer una ventana candidata del backend. */
export function articleSearchIdentifiers(article: string): string[] {
  const requested = Number(article);
  if (!Number.isInteger(requested) || requested < 1) {
    const alphanumeric = article.match(/^(\d+)([a-z])$/i);
    return alphanumeric
      ? [article, `${alphanumeric[1]}-${alphanumeric[2]}`]
      : [article];
  }
  return [
    ...new Set([
      String(requested),
      requested > 1 ? String(requested - 1) : null,
      String(requested + 1),
    ].filter((value): value is string => value !== null)),
  ];
}

/**
 * Selecciona únicamente el intervalo literal del artículo. Si el encabezado
 * exacto falta, solo recupera un candidato OCR deformado encerrado por los
 * encabezados inequívocos N-1 y N+1. Nunca corrige el texto retornado.
 */
export function selectArticleEvidence(
  pages: ArticlePage[],
  article: string,
  maxPages = MAX_ARTICLE_EVIDENCE_PAGES,
  hasMorePages = false,
): ArticleSelection {
  if (!article.trim() || maxPages < 1) {
    return { status: "not_found", pages: [] };
  }
  const ordered = [...pages].sort((a, b) => a.page_number - b.page_number);
  const headings = scanHeadings(ordered);
  const located = locateStart(headings, article);
  if (!located) return { status: "not_found", pages: [] };

  const start = headings[located.headingIndex];
  const boundary = headings.slice(located.headingIndex + 1).find((heading) =>
    !isRepeatedRequestedHeading(heading, start, article)
  );
  const selected: ArticlePage[] = [];

  for (let index = start.pageIndex; index < ordered.length; index++) {
    const relativePage = index - start.pageIndex;
    if (relativePage >= maxPages) {
      if (boundary?.pageIndex === index && boundary.offset === 0) {
        return {
          status: "found",
          pages: selected,
          match_method: located.method,
        };
      }
      return { status: "limit_exceeded", pages: [] };
    }

    const text = structuralPageText(ordered[index]);
    const from = index === start.pageIndex ? start.offset : 0;
    const to = boundary?.pageIndex === index ? boundary.offset : text.length;
    const literal = text.slice(from, to).trimEnd();
    if (literal.trim()) {
      selected.push(withSelectedText(ordered[index], literal, located.method));
    }
    if (boundary?.pageIndex === index) {
      return {
        status: "found",
        pages: selected,
        match_method: located.method,
      };
    }
  }

  if (ordered.length - start.pageIndex > maxPages) {
    return { status: "limit_exceeded", pages: [] };
  }
  // Una ventana backend truncada no demuestra fin documental.
  if (hasMorePages) return { status: "limit_exceeded", pages: [] };
  return selected.length
    ? { status: "found", pages: selected, match_method: located.method }
    : { status: "not_found", pages: [] };
}
