(defsystem "lcp"
  :description "LCP: The Lisp C++ Preprocessor"
  :version "0.0.1"
  :author "Teon Banek <teon.banek@memgraph.io>"
  :depends-on ("cl-ppcre")
  :serial t
  :components ((:file "package")
               (:file "lcp"))
  :in-order-to ((test-op (test-op "lcp/test"))))

(defsystem "lcp/test"
  :depends-on ("lcp" "prove")
  :components ((:file "lcp-test"))
  :perform (test-op :after (op s) (symbol-call :prove :run-test-package :lcp-test)))
