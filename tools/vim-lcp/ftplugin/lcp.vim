if exists("b:did_ftplugin")
  finish
endif

let b:did_ftplugin = 1

setlocal lisp
setlocal lispwords+=lcp:define-class,lcp:define-struct,lcp:define-enum,lcp:define-rpc

" Veeeery basic indentation.
" TODO: This should be in the `indent` folder.
setlocal autoindent
setlocal nosmartindent
