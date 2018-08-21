# vim-lcp

Adds syntax highlighting of LCP files to vim. In the future the plugin may be
extended with additional features, such as formatting, new editing commands etc.

## Installation

I'm assuming that you are using some kind of plugin manager in vim. If not,
check out [Vundle](https://github.com/VundleVim/Vundle.vim).

  ln -s <path-to-memgraph>/tools/vim-lcp ~/.vim/bundle/vim-lcp

After symlinking, edit your `vimrc` by adding a `Plugin 'vim-lcp'` directive
inside the `call vundle#begin() ... call vundle#end()` block.
