# Memgraph LaTeX Beamer Template

This folder contains all of the needed files for creating a presentation with
Memgraph styling. You should use this style for any public presentations.

Feel free to improve it according to style guidelines and raise issues if you
find any.

## Usage

Copy the contents of this folder (excluding this README file) to where you
want to write your own presentation. After copying, you can start editing the
`template.tex` with your content.

To compile the presentation to a PDF, run `latexmk -pdf -xelatex`. Some
directives require XeLaTeX, so you need to pass `-xelatex` as the final option
of `latexmk`. You may also need to install some packages if the compilation
complains about missing packages.

To clean up the generated files, use `latexmk -C`. This will also delete the
generated PDF. If you wish to remove generated files except the PDF, use
`latexmk -c`.
