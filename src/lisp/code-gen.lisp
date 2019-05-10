;;;; This file contains common code for generating C++ code.

(in-package #:lcp)

(defun call-with-cpp-block-output (out fun &key semicolonp name)
  "Surround the invocation of FUN by emitting '{' and '}' to OUT.  If
SEMICOLONP is set, the closing '}' is suffixed with ';'.  NAME is used to
prepend the starting block with a name, for example \"class MyClass\"."
  (if name
    (format out "~A {~%" name)
    (write-line "{" out))
  (funcall fun)
  (if semicolonp (write-line "};" out) (write-line "}" out)))

(defmacro with-cpp-block-output ((out &rest rest &key semicolonp name) &body body)
  "Surround BODY with emitting '{' and '}' to OUT.  For additional arguments,
see `CALL-WITH-CPP-BLOCK-OUTPUT' documentation."
  (declare (ignorable semicolonp name))
  `(call-with-cpp-block-output ,out (lambda () ,@body) ,@rest))

(defun call-with-namespaced-output (out fun)
  "Invoke FUN with a function for opening C++ namespaces.  The function takes
care to write namespaces to OUT without redundantly opening already open
namespaces."
  (check-type out stream)
  (check-type fun function)
  (let (open-namespaces)
    (funcall fun (lambda (namespaces)
                   ;; No namespaces is global namespace
                   (unless namespaces
                     (dolist (to-close open-namespaces)
                       (declare (ignore to-close))
                       (format out "~%}")))
                   ;; Check if we need to open or close namespaces
                   (loop :for namespace :in namespaces
                         :with unmatched := open-namespaces :do
                           (if (string= namespace (car unmatched))
                               (setf unmatched (cdr unmatched))
                               (progn
                                 (dolist (to-close unmatched)
                                   (declare (ignore to-close))
                                   (format out "~%}"))
                                 (format out "namespace ~A {~2%" namespace))))
                   (setf open-namespaces namespaces)))
    ;; Close remaining namespaces
    (dolist (to-close open-namespaces)
      (declare (ignore to-close))
      (format out "~%}"))))

(defmacro with-namespaced-output ((out open-namespace-fun) &body body)
  "Use `CALL-WITH-NAMESPACED-OUTPUT' more conveniently by executing BODY in a
context which binds OPEN-NAMESPACE-FUN function for opening namespaces."
  (let ((open-namespace (gensym)))
    `(call-with-namespaced-output
      ,out
      (lambda (,open-namespace)
        (flet ((,open-namespace-fun (namespaces)
                 (funcall ,open-namespace namespaces)))
          ,@body)))))

(defun cpp-documentation (documentation)
  "Convert DOCUMENTATION to Doxygen style string."
  (check-type documentation string)
  (format nil "/// ~A"
          (cl-ppcre:regex-replace-all
           (string #\Newline) documentation (format nil "~%/// "))))

(defvar *variable-idx* 0 "Used to generate unique variable names")

(defmacro with-vars (vars &body body)
  "Generates unique variable names for use in generated code by
appending an index to desired variable names. Useful when generating
loops which might reuse counter names.

Usage example:
  (with-vars ((loop-counter \"i\"))
    (format nil \"for (auto ~A = 0; ~A < v.size(); ++~A) {
                    // do something
                  }\"
            loop-counter loop-counter loop-counter))"
  `(let* ((*variable-idx* (1+ *variable-idx*))
          ,@(loop :for var :in vars :collecting
                  `(,(first var)
                    (format nil "~A~A" ,(second var) *variable-idx*))))
     ,@body))

(defun cpp-member-reader-name (cpp-member)
  (check-type cpp-member cpp-member)
  (string-right-trim '(#\_) (cpp-member-name cpp-member)))
