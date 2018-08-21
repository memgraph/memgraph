" Quit when a syntax file was already loaded
if exists("b:current_syntax")
  finish
endif

" Base LCP syntax on Lisp syntax
runtime! syntax/lisp.vim
unlet b:current_syntax

" Include C++ syntax for inlined C++ code in LCP
syntax include @CPP syntax/cpp.vim
unlet b:current_syntax

let b:current_syntax = "lcp"

" Set the inline C++ region
syntax region lcpCPPVar matchgroup=CPPVar start="\${" excludenl end="}" contained containedin=@CPP
highlight link CPPVar SpecialComment
syntax region lcpCPP containedin=lispList,lispBQList,lispAtomList matchgroup=CPPBlock start="#>cpp" end="cpp<#" contains=@CPP
highlight link CPPBlock SpecialComment

" Additional LCP keywords
syntax keyword lcpKeyword contained containedin=lispAtomList,lispBQList,lispList lcp:define-class lcp:define-enum lcp:define-struct
syntax keyword lcpKeyword contained containedin=lispAtomList,lispBQList,lispList lcp:namespace lcp:pop-namespace
syntax keyword lcpKeyword contained containedin=lispAtomList,lispBQList,lispList lcp:capnp-namespace lcp:capnp-import lcp:capnp-type-conversion

highlight link lcpKeyword lispFunc
