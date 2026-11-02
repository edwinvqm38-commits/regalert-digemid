function plain(value: string): string {
  return value.toLowerCase().normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/**
 * Distingue un listado cronológico genérico de una pregunta temática.
 * Ante cualquier término sustantivo desconocido falla hacia RAG: es preferible
 * recuperar evidencia del tema a devolver un listado global no relacionado.
 */
export function esConsultaDeUltimaNormativa(pregunta: string): boolean {
  const text = plain(pregunta);
  const mentionsNormative =
    /\b(norma|normas|normativa|normativas|ley|leyes|decreto|decretos|resolucion|resoluciones|reglamento|reglamentos)\b/
      .test(text);
  const asksRecency =
    /\b(ultima|ultimas|ultimo|ultimos|reciente|recientes|nueva|nuevas|nuevo|nuevos)\b/
      .test(text);

  if (!mentionsNormative || !asksRecency) return false;

  const remainingTopic = text
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(
      /\b(cual|cuales|que|es|son|hay|existe|existen|dame|muestra|muestrame|lista|listame|listar|quiero|quisiera|saber|ver)\b/g,
      " ",
    )
    .replace(
      /\b(salio|sale|publico|publicada|publicadas|publicado|publicados|emitio|emitida|emitidas|emitido|emitidos|registro|registrada|registradas|registrado|registrados|subio|subida|subidas|subido|subidos)\b/g,
      " ",
    )
    .replace(
      /\b(norma|normas|normativa|normativas|ley|leyes|decreto|decretos|resolucion|resoluciones|reglamento|reglamentos|ministerial|ministeriales)\b/g,
      " ",
    )
    .replace(
      /\b(ultima|ultimas|ultimo|ultimos|reciente|recientes|nueva|nuevas|nuevo|nuevos|actual|actuales|vigente|vigentes|disponible|disponibles)\b/g,
      " ",
    )
    .replace(
      /\b(digemid|minsa|ministerio|salud|peru|sanitaria|sanitarias)\b/g,
      " ",
    )
    .replace(/\b(?:[1-9]|10)\b/g, " ")
    .replace(
      /\b(la|las|el|los|un|una|unos|unas|de|del|para|por|favor|y|o|a|en)\b/g,
      " ",
    )
    .replace(/\s+/g, " ")
    .trim();

  return remainingTopic.length === 0;
}
