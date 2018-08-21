# lcp-mode

Emacs major mode for editing LCP files. It relies on `mmm-mode` for
integrating multiple major modes in a single buffer.

`lcp-mode` is derived from `lisp-mode`, so all your custom bindings for that
mode should work out of the box. Since LCP supports embedding C++ code blocks,
the `lcp-mode` shifts from `lisp-mode` to `c-mode` in such blocks. Therefore,
your `c-mode` bindings should also work. You may wonder why isn't `c++-mode`
used. The reason is that, unfortunately, `c++-mode` has some issues when used
with `mmm-mode`.

Future improvements to this mode may include fixing issues with `c++-mode`,
adding additional syntax highlighting as well as new editing commands.

## Installation

Just put the following somewhere in your Emacs init file. It will
automatically install `mmm-mode` from the ELPA repository if needed.

    (require 'lcp-mode "<path-to-memgraph>/tools/emacs-lcp/lcp-mode.el")
