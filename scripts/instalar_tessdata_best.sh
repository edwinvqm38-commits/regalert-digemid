#!/usr/bin/env bash
# Descarga el modelo de español de mayor precisión de Tesseract (tessdata_best)
# y exporta TESSDATA_PREFIX para los pasos siguientes del workflow.
# Si la descarga falla, NO rompe: Tesseract sigue con el modelo rápido del sistema.
set -u
destino="${RUNNER_TEMP:-/tmp}/tessdata_best"
mkdir -p "$destino"
url="https://raw.githubusercontent.com/tesseract-ocr/tessdata_best/main/spa.traineddata"
if curl -fsSL --retry 3 --max-time 120 -o "$destino/spa.traineddata" "$url" \
   && [ "$(stat -c %s "$destino/spa.traineddata")" -gt 10000000 ]; then
  cp /usr/share/tesseract-ocr/*/tessdata/osd.traineddata "$destino/" 2>/dev/null || true
  echo "TESSDATA_PREFIX=$destino" >> "${GITHUB_ENV:-/dev/null}"
  echo "tessdata_best listo en $destino"
else
  rm -f "$destino/spa.traineddata"
  echo "::warning::No se pudo descargar tessdata_best; se usa el modelo rápido del sistema"
fi
