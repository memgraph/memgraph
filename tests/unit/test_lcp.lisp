(require 'asdf)

(let ((home (or (uiop:getenvp "QUICKLISP_HOME")
                (concatenate 'string (uiop:getenvp "HOME") "/quicklisp"))))
  (load (concatenate 'string home "/setup.lisp")))

(ql:quickload "lcp/test")
(setf uiop:*image-entry-point*
      (lambda ()
        (let ((prove:*default-reporter* :fiveam))
          (prove:plan nil)
          (unless (prove:run-test-package :lcp-test)
            (uiop:quit 1)))))
(uiop:dump-image "test_lcp" :executable t)
