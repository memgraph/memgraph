(defsystem "lcp"
  :description "LCP: The Lisp C++ Preprocessor"
  :version "0.0.1"
  :author "Teon Banek <teon.banek@memgraph.io>"
  :depends-on ("alexandria" "cl-ppcre" "named-readtables" "swank")
  :serial t
  :components ((:file "package")
               (:file "util")
               (:file "reader")
               (:file "names")
               (:file "types")
               (:file "code-gen")
               (:file "slk")
               (:file "clone")
               (:file "lcp")
               (:file "debug"))
  :in-order-to ((test-op (test-op "lcp/test"))))

(defsystem "lcp/test"
  :depends-on ("lcp" "prove")
  :components ((:file "test"))
  :perform (test-op :after (op s)
                    (let ((*package* (find-package :lcp.test))
                          (sym (find-symbol (string :*default-reporter*)
                                            :prove)))
                      (progv (list sym) '(:fiveam)
                        (symbol-call :prove :plan nil)
                        (symbol-call :prove :run-test-package :lcp.test)))))
