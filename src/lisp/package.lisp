(defpackage #:lcp
  (:use #:cl)
  (:export #:define-class
           #:define-struct
           #:define-enum
           #:define-rpc
           #:cpp-list
           #:in-impl
           #:namespace
           #:pop-namespace
           #:process-lcp-file
           #:lcp-syntax))

(defpackage #:lcp.slk
  (:use #:cl)
  (:export #:slk-error
           #:save-function-declaration-for-class
           #:save-function-definition-for-class
           #:construct-and-load-function-declaration-for-class
           #:construct-and-load-function-definition-for-class
           #:load-function-declaration-for-class
           #:load-function-definition-for-class
           #:save-function-declaration-for-enum
           #:save-function-definition-for-enum
           #:load-function-declaration-for-enum
           #:load-function-definition-for-enum))

(defpackage #:lcp.clone
  (:use #:cl)
  (:export #:clone-error
           #:clone-function-definition-for-class))
