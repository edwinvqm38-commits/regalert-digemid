export const MAX_ARTICLE_EVIDENCE_PAGES = 8;

export type ArticlePage = {
  page_number: number;
  text_normalized?: string | null;
  text_raw?: string | null;
  source_asset_id?: string | number | null;
  [key: string]: unknown;
};

export type ArticleSelection = {
  status: "found" | "not_found" | "limit_exceeded";
  pages: ArticlePage[];
};

const ARTICLE_WORD = String.raw`art[íi1]cu[l1i]o`;
const ARTICLE_IDENTIFIER = String
  .raw`(?:\d{1,4}(?:\s*[-–—]\s*[a-z])?|[ivxlcdm]{1,8})`;

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizedIdentifier(value: string): string {
  return value.toLowerCase().replace(/\s+/g, "").replace(/[–—]/g, "-");
}

function headingPattern(identifier: string): RegExp {
  const escaped = escapeRegExp(identifier.trim()).replace(/[-–—]/g, "[-–—]?");
  return new RegExp(
    String
      .raw`^[\t ]*(?:[-•]\s*)?${ARTICLE_WORD}\s+(?:n[°ºo]\.?\s*)?(${escaped})(?![\da-z])(?:[\t ]*(?:\.?[\t ]*[-–—]|[°º]))?`,
    "gim",
  );
}

function anyHeadingPattern(): RegExp {
  return new RegExp(
    String
      .raw`^[\t ]*(?:[-•]\s*)?${ARTICLE_WORD}\s+(?:n[°ºo]\.?\s*)?(${ARTICLE_IDENTIFIER})(?![\da-z])(?:[\t ]*(?:\.?[\t ]*[-–—]|[°º]))?`,
    "gim",
  );
}

function pageText(page: ArticlePage): string {
  return typeof page.text_normalized === "string"
    ? page.text_normalized
    : typeof page.text_raw === "string"
    ? page.text_raw
    : "";
}

function withSelectedText(page: ArticlePage, text: string): ArticlePage {
  return {
    ...page,
    // Copia literal de un intervalo existente; nunca completa ni reconstruye.
    text_normalized: text,
    text_raw: text,
  };
}

function nextDifferentHeading(
  text: string,
  from: number,
  requestedIdentifier: string,
): RegExpExecArray | null {
  const pattern = anyHeadingPattern();
  pattern.lastIndex = from;
  for (let match = pattern.exec(text); match; match = pattern.exec(text)) {
    if (
      normalizedIdentifier(match[1]) !==
        normalizedIdentifier(requestedIdentifier)
    ) {
      return match;
    }
  }
  return null;
}

/**
 * Selecciona únicamente el intervalo literal del artículo. `pages` debe
 * contener páginas consecutivas desde una candidata y, cuando exista, una
 * página centinela adicional al límite defensivo para detectar truncamiento.
 */
export function selectArticleEvidence(
  pages: ArticlePage[],
  article: string,
  maxPages = MAX_ARTICLE_EVIDENCE_PAGES,
): ArticleSelection {
  if (!article.trim() || maxPages < 1) {
    return { status: "not_found", pages: [] };
  }
  const ordered = [...pages].sort((a, b) => a.page_number - b.page_number);
  let startPageIndex = -1;
  let startOffset = -1;

  for (let index = 0; index < ordered.length; index++) {
    const match = headingPattern(article).exec(pageText(ordered[index]));
    if (match) {
      startPageIndex = index;
      startOffset = match.index;
      break;
    }
  }
  if (startPageIndex < 0) return { status: "not_found", pages: [] };

  const selected: ArticlePage[] = [];
  for (let index = startPageIndex; index < ordered.length; index++) {
    const relativePage = index - startPageIndex;
    const text = pageText(ordered[index]);
    const from = relativePage === 0 ? startOffset : 0;
    const boundary = nextDifferentHeading(text, from, article);

    if (relativePage >= maxPages) {
      // Una página centinela que empieza con el artículo siguiente confirma
      // que el solicitado terminó exactamente dentro del límite permitido.
      if (boundary?.index === 0) return { status: "found", pages: selected };
      return { status: "limit_exceeded", pages: [] };
    }

    const literal = text.slice(from, boundary?.index ?? text.length).trimEnd();
    if (literal.trim()) {
      selected.push(withSelectedText(ordered[index], literal));
    }
    if (boundary) return { status: "found", pages: selected };
  }

  // Si se consumieron más páginas que el límite sin hallar frontera, el
  // llamador no puede afirmar que llegó al fin real del documento.
  if (ordered.length - startPageIndex > maxPages) {
    return { status: "limit_exceeded", pages: [] };
  }
  return selected.length
    ? { status: "found", pages: selected }
    : { status: "not_found", pages: [] };
}
