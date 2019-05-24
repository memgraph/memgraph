(in-package #:lcp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Name operations on strings

(defun uppercase-property-resolver (name)
  (when (string= name "Uppercase")
    #'upper-case-p))

(defun string-upcase-first (string)
  "Upcase the first letter of the string STRING, if any."
  (check-type string string)
  (if (string= string "") "" (string-upcase string :end 1)))

(defun string-downcase-first (string)
  "Downcase the first letter of the string STRING, if any."
  (check-type string string)
  (if (string= string "") "" (string-downcase string :end 1)))

(defun split-camel-case-string (string)
  "Split the camelCase string STRING into a list of parts. The parts are
delimited by uppercase letters."
  (check-type string string)
  ;; NOTE: We use a custom property resolver which handles the Uppercase
  ;; property by forwarding to UPPER-CASE-P. This is so that we avoid pulling
  ;; CL-PPCRE-UNICODE & co.
  (let ((cl-ppcre:*property-resolver* #'uppercase-property-resolver))
    ;; NOTE: We use an explicit CREATE-SCANNER call in order to avoid issues
    ;; with CL-PPCRE's compiler macros which use LOAD-TIME-VALUE which evaluates
    ;; its forms within a null lexical environment (so our
    ;; CL-PPCRE:*PROPERTY-RESOLVER* binding would not be seen). Edi actually
    ;; hints at the problem within the documentation with the sentence "quiz
    ;; question - why do we need CREATE-SCANNER here?". :-)
    ;;
    ;; NOTE: This regex is a zero-width positive lookahead regex. It'll match
    ;; any zero-width sequence that is followed by an uppercase letter.
    (cl-ppcre:split (cl-ppcre:create-scanner "(?=\\p{Uppercase})") string)))

(defun split-pascal-case-string (string)
  "Split the PascalCase string STRING into a list of parts. The parts are
delimited by uppercase letters."
  (check-type string string)
  (split-camel-case-string string))

(defun split-snake-case-string (string)
  "Split the snake_case string STRING into a list of parts. The parts are
delimited by underscores. The underscores are not preserved. Empty parts are
trimmed on both sides."
  (check-type string string)
  (cl-ppcre:split "_" string))

(defun split-kebab-case-string (string)
  "Split the kebab-case string STRING into a list of parts. The parts are
delimited by dashes. The dashes are not preserved. Empty parts are trimmed on
both sides."
  (check-type string string)
  (cl-ppcre:split "-" string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Name operations on "things"

(defun split-cased-thing (thing &key from-style)
  "Split THING into a list of parts according to its type.

- If THING is a symbol, it is split using SPLIT-KEBAB-CASE-STRING.

- If THING is a string, it is split according to the FROM-STYLE keyword
  argument. FROM-STYLE can be one of :CAMEL, :PASCAL, :SNAKE or :KEBAB and
  denotes splitting using SPLIT-CAMEL-CASE-STRING, SPLIT-PASCAL-CASE-STRING,
  SPLIT-SNAKE-CASE-STRING and SPLIT-KEBAB-CASE-STRING respectively. If
  FROM-STYLE is omitted or NIL, it is treated as if :CAMEL was given."
  (check-type thing (or symbol string))
  (ctypecase thing
    (symbol (split-kebab-case-string (string thing)))
    (string
     (ccase from-style
       ((nil :camel :pascal) (split-camel-case-string thing))
       (:snake (split-snake-case-string thing))
       (:kebab (split-kebab-case-string thing))))))

(defun camel-case-name (thing &key from-style)
  "Return a camelCase string from THING.

The string is formed according to the parts of THING as returned by
SPLIT-CASED-THING. FROM-STYLE is passed to SPLIT-CASED-THING."
  (check-type thing (or symbol string))
  (string-downcase-first
   (format nil "~{~A~}"
           (mapcar (alexandria:compose #'string-upcase-first #'string-downcase)
                   (split-cased-thing thing :from-style from-style)))))

(defun pascal-case-name (thing &key from-style)
  "Return a PascalCase string from THING.

The string is formed according to the parts of THING as returned by
SPLIT-CASED-THING. FROM-STYLE is passed to SPLIT-CASED-THING."
  (check-type thing (or symbol string))
  (string-upcase-first (camel-case-name thing :from-style from-style)))

(defun lower-snake-case-name (thing &key from-style)
  "Return a lower_snake_case string from THING.

The string is formed according to the parts of THING as returned by
SPLIT-CASED-THING. FROM-STYLE is passed to SPLIT-CASED-THING."
  (check-type thing (or symbol string))
  (string-downcase
   (format nil "~{~A~^_~}" (split-cased-thing thing :from-style from-style))))

(defun upper-snake-case-name (thing &key from-style)
  "Return a UPPER_SNAKE_CASE string from THING.

The string is formed according to the parts of THING as returned by
SPLIT-CASED-THING. FROM-STYLE is passed to SPLIT-CASED-THING."
  (check-type thing (or symbol string))
  (string-upcase
   (format nil "~{~A~^_~}" (split-cased-thing thing :from-style from-style))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Namestrings

(defun ensure-namestring-for (thing func)
  "Return the namestring corresponding to the namestring designator THING.

- If THING is a symbol, return the result of calling FUNC on its name.

- If THING is a string, return it."
  (check-type thing (or symbol string))
  (ctypecase thing
    (symbol (funcall func thing))
    (string thing)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; C++ names and namestrings

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +cpp-namestring-docstring+
    "Return the ~A namestring corresponding to the ~A namestring designator
THING.

- If THING is a symbol, return the result of calling ~A on its name.

- If THING is a string, return it."))

(defmacro define-cpp-name (cpp-object name-op)
  "Define a name function and a namestring function for the C++ language element
named by the symbol CPP-OBJECT. Both functions rely on the function named by the
symbol NAME-OP to perform the actual operation.

The name function's name is of the form CPP-<CPP-OBJECT>-NAME.

The namestring function's name is of the form
ENSURE-NAMESTRING-FOR-<CPP-OBJECT>."
  (let ((cpp-name-for (alexandria:symbolicate 'cpp-name-for- cpp-object)))
    `(progn
       (defun ,cpp-name-for (thing &key from-style)
         (check-type thing (or symbol string))
         (,name-op thing :from-style from-style))
       (setf (documentation ',cpp-name-for 'function)
             (documentation ',name-op 'function))
       (defun ,(alexandria:symbolicate 'ensure-namestring-for- cpp-object) (thing)
         ,(format nil +cpp-namestring-docstring+
                  (string-downcase cpp-object)
                  (string-downcase cpp-object)
                  name-op)
         (check-type thing (or symbol string))
         (ensure-namestring-for thing #',name-op)))))

(define-cpp-name namespace  lower-snake-case-name)
(define-cpp-name class      pascal-case-name)
(define-cpp-name enum       pascal-case-name)
(define-cpp-name type-param pascal-case-name)
(define-cpp-name variable   lower-snake-case-name)
(define-cpp-name enumerator upper-snake-case-name)

(defun cpp-name-for-member (thing &key from-style structp)
  "Just like CPP-NAME-FOR-VARIABLE except that the suffix \"_\" is added unless
  STRUCTP is true."
  (check-type thing (or symbol string))
  (format nil "~A~@[_~]"
          (cpp-name-for-variable thing :from-style from-style)
          (not structp)))

(defun ensure-namestring-for-member (thing &key structp)
  (check-type thing (or symbol string))
  (ensure-namestring-for
   thing (lambda (symbol) (cpp-name-for-member symbol :structp structp))))

(setf (documentation 'ensure-namestring-for-member 'function)
      (format nil +cpp-namestring-docstring+
              "member" "member" 'cpp-member-name))
