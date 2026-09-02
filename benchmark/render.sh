#!/usr/bin/env bash
# Render an E*/results.org report to PDF (org export + pgfplots) and to
# per-page PNGs for quick visual inspection. Invoked from an E* directory
# (the per-case run.sh scripts symlink or call it with cd set).

set -eu
org="${1:-results.org}"
base="${org%.org}"

emacs --batch "$org" \
    --eval "(setq org-confirm-babel-evaluate nil org-latex-pdf-process '(\"pdflatex -interaction nonstopmode -output-directory %o %f\" \"pdflatex -interaction nonstopmode -output-directory %o %f\"))" \
    -f org-latex-export-to-pdf >/dev/null 2>&1 || {
    echo "org export failed; see ${base}.log" >&2
    exit 1
}
pdftoppm -png -r 110 "${base}.pdf" "${base}"
echo "rendered: ${base}.pdf + ${base}-*.png"
