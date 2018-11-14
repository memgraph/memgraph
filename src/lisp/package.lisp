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
           #:capnp-namespace
           #:capnp-import
           #:capnp-type-conversion
           #:capnp-save-optional
           #:capnp-load-optional
           #:capnp-save-vector
           #:capnp-load-vector
           #:capnp-save-enum
           #:capnp-load-enum
           #:process-file))

(defpackage #:lcp.slk
  (:use #:cl)
  (:export #:save-function-declaration-for-class
           #:save-function-definition-for-class
           #:slk-error))
