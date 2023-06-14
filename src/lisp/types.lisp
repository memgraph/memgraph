;;;; This file contains definitions of types used to store meta information on
;;;; C++ types.  Along with data definitions, you will find various functions
;;;; and methods for operating on that data.

(in-package #:lcp)
(named-readtables:in-readtable lcp:lcp-syntax)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Supported and unsupported C++ types

(deftype general-cpp-type ()
  '(or cpp-type unsupported-cpp-type))

(defgeneric cpp-type-decl (cpp-type &key namespacep globalp enclosing-classes-p
                                      type-params-p)
  (:documentation "Return the C++ type declaration corresponding to the given
object."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Supported C++ types

(defvar +cpp-primitive-type-names+
  '("bool" "char" "int" "int16_t" "int32_t" "int64_t" "uint" "uint16_t"
    "uint32_t" "uint64_t" "float" "double"))

(defclass cpp-type ()
  ((documentation
    :type (or null string)
    :initarg :documentation
    :initform nil
    :reader cpp-type-documentation
    :documentation "Documentation string for this C++ type.")
   (namespace
    :type list
    :initarg :namespace
    :initform nil
    :reader cpp-type-namespace
    :documentation "A list of strings naming the individual namespace parts of
the namespace of this type. Enclosing classes aren't included, even though they
form valid C++ namespaces.")
   (enclosing-classes
    :type list
    :initarg :enclosing-classes
    :initform nil
    :reader cpp-type-enclosing-classes
    :accessor %cpp-type-enclosing-classes
    :documentation "A list of strings naming the enclosing classes of this
type.")
   (name
    :type string
    :initarg :name
    :reader cpp-type-name
    :documentation "The name of this type.")
   (type-params
    :type list
    :initarg :type-params
    :initform nil
    :reader cpp-type-type-params
    :documentation "A list of strings naming the template parameters that are
needed to instantiate a concrete type. For example, in `template <TValue> class
vector`, `TValue' is the type parameter.")
   (type-args
    :type list
    :initarg :type-args
    :initform nil
    :reader cpp-type-type-args
    :accessor %cpp-type-type-args
    :documentation "A list of `CPP-TYPE' instances that represent the template
type arguments used within the instantiation of the template. For example in
`std::vector<int>`, `int' is a template type argument."))
  (:documentation "Base class for meta information on C++ types."))

(defun make-cpp-type (name &key namespace enclosing-classes type-params
                             type-args)
  "Create an instance of CPP-TYPE. The keyword arguments correspond to the slots
of the class CPP-TYPE and expect values according to their type and
documentation, except as noted below.

If the first element of NAMESPACE is an empty string, it is removed. NAMESPACE
parts must not contain characters from +WHITESPACE-CHARS+.

TYPE-ARGS can be a list of CPP-TYPE designators, each of which will be coerced
into a CPP-TYPE instance as if by ENSURE-CPP-TYPE.

TYPE-PARAMS and TYPE-ARGS cannot be provided simultaneously."
  (check-type name string)
  (check-type namespace list)
  (check-type enclosing-classes list)
  (check-type type-params list)
  (check-type type-args list)
  (dolist (list (list namespace enclosing-classes type-params))
    (dolist (elem list)
      (check-type elem string)))
  (let ((namespace (if (and namespace (string= (car namespace) ""))
                       (cdr namespace)
                       namespace)))
    (dolist (part namespace)
      (when (or (string= part "")
                (find-if
                 (lambda (c) (member c +whitespace-chars+ :test #'char=))
                 part))
        (error "~@<Invalid namespace part ~S in ~S~@:>" part namespace)))
    (when (and type-params type-args)
      (error "~@<A CPP-TYPE can't have both of TYPE-PARAMS and TYPE-ARGS~@:>"))
    (make-instance 'cpp-type
                   :name name
                   :namespace namespace
                   :enclosing-classes enclosing-classes
                   :type-params type-params
                   :type-args (mapcar #'ensure-cpp-type type-args))))

(defmethod print-object ((cpp-type cpp-type) stream)
  (print-unreadable-object (cpp-type stream :type t)
    (format stream "~A" (cpp-type-decl cpp-type))))

(defun cpp-type= (a b)
  (check-type a cpp-type)
  (check-type b cpp-type)
  "Test whether two instances of CPP-TYPE, A and B, represent the same C++ type.

For the test to return true, the following must hold:

- The CPP-TYPE-NAME of A and B must be STRING=.

- The CPP-TYPE-NAMESPACE of A and B must be EQUAL.

- The CPP-TYPE-ENCLOSING-CLASSES of A and B must be EQUAL.

- The CPP-TYPE-TYPE-PARAMS of A and B must be pairwise STRING=.

- The CPP-TYPE-TYPE-ARGS of A and B must be pairwise CPP-TYPE=."
  (and (string= (cpp-type-name a) (cpp-type-name b))
       (equal (cpp-type-namespace a) (cpp-type-namespace b))
       (equal (cpp-type-enclosing-classes a) (cpp-type-enclosing-classes b))
       (not (mismatch (cpp-type-type-params a) (cpp-type-type-params b)
                      :test #'string=))
       (not (mismatch (cpp-type-type-args a) (cpp-type-type-args b)
                      :test #'cpp-type=))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Unsupported C++ types

(defclass unsupported-cpp-type ()
  ((typestring
    :type string
    :initarg :typestring
    :initform nil
    :reader unsupported-cpp-type-typestring
    :documentation "The typestring for this type that LCP couldn't
parse (doesn't support)."))
  (:documentation "A class that represents unsupported C++ types."))

(defun make-unsupported-cpp-type (typestring)
  (make-instance 'unsupported-cpp-type :typestring typestring))

(defmethod print-object ((cpp-type unsupported-cpp-type) stream)
  (print-unreadable-object (cpp-type stream :type t)
    (princ (unsupported-cpp-type-typestring cpp-type) stream)))

(macrolet ((define-unsupported-cpp-type-methods ()
             (let ((names '(documentation namespace enclosing-classes
                            type-params type-args name)))
               `(progn
                  ,@(loop :for name :in names
                          :for fname := (alexandria:symbolicate 'cpp-type- name)
                          :collect
                          `(defmethod ,fname ((cpp-type unsupported-cpp-type))
                             (error ,(format
                                      nil "~S doesn't support the method ~S"
                                      'unsupported-cpp-type fname))))))))
  (define-unsupported-cpp-type-methods))

(defmethod cpp-type-decl ((cpp-type unsupported-cpp-type)
                          &key &allow-other-keys)
  "Return the captured typestring for the instance of UNSUPPORTED-CPP-TYPE."
  (unsupported-cpp-type-typestring cpp-type))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Known C++ enums

(defclass cpp-enum (cpp-type)
  ((values
    :type list
    :initarg :values
    :initform nil
    :reader cpp-enum-values)
   ;; If true, generate serialization code for this enum.
   (serializep
    :type boolean
    :initarg :serializep
    :initform nil
    :reader cpp-enum-serializep))
  (:documentation "Meta information on a C++ enum."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Known C++ classes

(defstruct cpp-member
  "Meta information on a C++ class (or struct) member variable."
  ;; The class that contains this member.
  (name nil :type string :read-only t)
  (type nil :type (or string general-cpp-type))
  (initval nil :type (or null string integer float) :read-only t)
  (scope :private :type (member :public :protected :private) :read-only t)
  ;; TODO: Support giving a name for reader function.
  (reader nil :type boolean :read-only t)
  (documentation nil :type (or null string) :read-only t)
  ;; If T, skips this member in serialization code generation. The member may
  ;; still be deserialized with custom load hook.
  (dont-save nil :type boolean :read-only t)
  ;; May be a function which takes 1 argument, member-name. It needs to return
  ;; C++ code.
  (slk-save nil :type (or null function) :read-only t)
  (slk-load nil :type (or null function) :read-only t)
  (clone t :type (or boolean (eql :copy) function) :read-only t))

(defstruct slk-opts
  "SLK serialization options for C++ class."
  ;; BASE is T if the class should be treated as a base class for SLK, even
  ;; though it may have parents.
  (base nil :type boolean :read-only t)
  ;; Extra arguments to the generated save function. List of (name cpp-type).
  (save-args nil)
  (load-args nil)
  ;; In case of multiple inheritance, pretend we only inherit the 1st base
  ;; class.
  (ignore-other-base-classes nil :type boolean :read-only t))

(defstruct clone-opts
  "Cloning options for C++ class."
  ;; Extra arguments to the generated clone function. List of (name cpp-type).
  (args nil)
  (return-type nil :type (or null function) :read-only t)
  (base nil :read-only t)
  (ignore-other-base-classes nil :read-only t)
  (init-object nil :read-only t))

(defstruct type-info-opts
  "Options for generating TypeInfo of C++ class."
  (base nil :read-only t)
  (ignore-other-base-classes nil :read-only t))

(defclass cpp-class (cpp-type)
  ((structp
    :type boolean
    :initarg :structp
    :initform nil
    :reader cpp-class-structp)
   (super-classes
    :initarg :super-classes
    :initform nil
    :accessor %cpp-class-super-classes)
   (members
    :initarg :members
    :initform nil
    :accessor %cpp-class-members)
   ;; Custom C++ code in 3 scopes. May be a list of C++ meta information or a
   ;; single element.
   (public
    :initarg :public
    :initform nil
    :accessor cpp-class-public)
   (protected
    :initarg :protected
    :initform nil
    :reader cpp-class-protected)
   (private
    :initarg :private
    :initform nil
    :accessor cpp-class-private)
   (slk-opts
    :type (or null slk-opts)
    :initarg :slk-opts
    :initform nil
    :reader %cpp-class-slk-opts)
   (clone-opts
    :type (or null clone-opts)
    :initarg :clone-opts
    :initform nil
    :reader %cpp-class-clone-opts)
   (type-info-opts
    :type type-info-opts
    :initarg :type-info-opts
    :initform (make-type-info-opts)
    :reader cpp-class-type-info-opts)
   (inner-types
    :initarg :inner-types
    :initform nil
    :reader cpp-class-inner-types)
   (abstractp
    :initarg :abstractp
    :initform nil
    :reader cpp-class-abstractp))
  (:documentation "Meta information on a C++ class (or struct)."))

(defmethod cpp-type-decl ((cpp-type cpp-type) &rest kwargs
                          &key (namespacep t) (globalp nil)
                            (enclosing-classes-p t) (type-params-p t))
  "Return the C++ type declaration corresponding to the given CPP-TYPE.

If NAMESPACEP is true, the namespace (excluding enclosing classes) is included
in the declaration. If GLOBALP is true, the namespace (if included) is fully
qualified.

If ENCLOSING-CLASSES-P is true, the namespace formed by the enclosing classes is
included in the declaration.

If TYPE-PARAMS-P is true, type parameters are included when CPP-TYPE has type
parameters.

If CPP-TYPE has type arguments, type arguments are included in the declaration
and formatted by recursively calling CPP-TYPE-DECL with the same keyword
arguments."
  (flet ((rec (cpp-type)
           (apply #'cpp-type-decl cpp-type kwargs)))
    (with-output-to-string (s)
      (cond
        ;; Handle const.
        ((cpp-type-const-p cpp-type)
         (format s "~A " (cpp-type-name cpp-type))
         (write-string (rec (car (cpp-type-type-args cpp-type))) s))
        ;; Handle pointers and references.
        ((or (cpp-type-raw-pointer-p cpp-type)
             (cpp-type-reference-p cpp-type))
         (write-string (rec (car (cpp-type-type-args cpp-type))) s)
         (format s " ~A" (cpp-type-name cpp-type)))
        (t
         (when namespacep
           (when globalp
             (write-string "::" s))
           (write-string (cpp-type-namespace-string cpp-type) s))
         (when enclosing-classes-p
           (write-string (cpp-type-enclosing-classes-string cpp-type) s))
         (write-string (cpp-type-name cpp-type) s)
         (cond
           ((cpp-type-type-args cpp-type)
            (format s "<~{~A~^, ~}>"
                    (mapcar #'rec (cpp-type-type-args cpp-type))))
           ((and type-params-p (cpp-type-type-params cpp-type))
            (format s "<~{~A~^, ~}>" (cpp-type-type-params cpp-type)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; C++ type parsing

(defun parse-cpp-type-declaration (type-decl)
  "Try to construct a CPP-TYPE instance from the string TYPE-DECL representing a
C++ type declaration.

The function assumes that TYPE-DECL is a well-formed C++ type declaration. No
attempt is made to handle erroneous declarations.

Note that the function doesn't aim to support the whole of C++'s type
declaration syntax. Certain declarations just aren't supported.

If the declaration is successfully parsed, the resulting CPP-TYPE instance is
returned. Otherwise, if the string is empty or if unsupported constructs were
used, NIL is returned."
  (check-type type-decl string)
  ;; A C++ type declaration for our purposes is of the form:
  ;;
  ;; namespace::namespace::type<type-arg, type-arg> <* or &>
  ;; |^^^^^^^^^^^^^^^^^^^^|    |^^^^^^^^^^^^^^^^^^| |^^^^^|
  ;;       optional                 optional        optional
  ;;
  ;; The type arguments are recursively parsed.

  (when (string= "" type-decl)
    (return-from parse-cpp-type-declaration nil))
  ;; Unsupported: `typename' and array syntax
  (when (or (search "typename" type-decl)
            (cl-ppcre:scan "[[\\]]" type-decl))
    (return-from parse-cpp-type-declaration nil))
  (setf type-decl (string-trim +whitespace-chars+ type-decl))
  ;; Check if the type is a primitive type
  (let ((type-keyword (member type-decl +cpp-primitive-type-names+
                              :test #'string=)))
    (when type-keyword
      (return-from parse-cpp-type-declaration
        (make-cpp-type (car type-keyword)))))
  ;; Check if the type is a pointer
  (let ((ptr-pos (position-if (lambda (c) (or (char= c #\*) (char= c #\&)))
                              type-decl :from-end t)))
    (when (and ptr-pos (not (cl-ppcre:scan "[()<>]" type-decl :start ptr-pos)))
      (return-from parse-cpp-type-declaration
        (let ((type-arg (parse-cpp-type-declaration
                         (subseq type-decl 0 ptr-pos))))
          (when type-arg
            (make-cpp-type (subseq type-decl ptr-pos)
                           :type-args (list type-arg)))))))
  ;; Other cases
  (destructuring-bind (full-name &optional template)
      (cl-ppcre:split "<" type-decl :limit 2)
    ;; Unsupported: Function or array syntax
    (let ((pos (if template
                   (position-of-closing-delimiter type-decl #\< #\>)
                   0)))
      (when (or (cl-ppcre:scan "[()]" full-name)
                (cl-ppcre:scan "[()]" type-decl :start (1+ pos)))
        (return-from parse-cpp-type-declaration nil)))
    (let* ((parts (cl-ppcre:split "::" full-name))
           (name (car (last parts)))
           (namespace (butlast parts))
           (type-args nil))
      (when template
        ;; A class template instantiation ends with the '>' character
        (let ((arg-start 0))
          (cl-ppcre:do-scans (match-start match-end reg-starts reg-ends
                              "[a-zA-Z0-9_:<>() *&]+[,>]" template)
            (flet ((matchedp (open-char close-char)
                     "Return T if the TEMPLATE[ARG-START:MATCH-END] contains
                     matched OPEN-CHAR and CLOSE-CHAR."
                     (= (count open-char template :start arg-start :end match-end)
                        (count close-char template :start arg-start :end match-end))))
              (when (or (= match-end (length template)) ;; We are at the end
                        (and (matchedp #\< #\>) (matchedp #\( #\))))
                (let ((type-arg (parse-cpp-type-declaration
                                 ;; Take the arg and omit the final [,>]
                                 (subseq template arg-start (1- match-end)))))
                  (if type-arg
                      (push type-arg type-args)
                      (return-from parse-cpp-type-declaration nil)))
                (setf arg-start match-end))))))
      ;; Treat the first capitalized namespace and all the ones after that as
      ;; enclosing classes, whether or not they're known to LCP.
      (let ((pos (or (position-if
                      (lambda (part)
                        (and (string/= "" part) (upper-case-p (aref part 0))))
                      namespace)
                     (length namespace))))
        (make-cpp-type name
                       :namespace (subseq namespace 0 pos)
                       :enclosing-classes (subseq namespace pos)
                       :type-args (reverse type-args))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Typestrings

(defun ensure-typestring (thing)
  "Return the typestring corresponding to the typestring designator THING.

- If THING is a symbol whose name is STRING-EQUAL to an element in
  +CPP-PRIMITIVE-TYPE-NAMES+, return it.

- If THING is any other symbol, return the result of (cpp-name-for-class thing).

- If THING is a string, return it."
  (check-type thing (or symbol string))
  (ctypecase thing
    (symbol (if (member thing +cpp-primitive-type-names+ :test #'string-equal)
                (string-downcase thing)
                (cpp-name-for-class thing)))
    (string thing)))

(defun typestring-supported-p (typestring)
  "Test whether the typestring TYPESTRING would resolve to a supported
CPP-TYPE."
  (and (parse-cpp-type-declaration typestring) t))

(defun typestring-class-template-instantiation-p (typestring)
  "Return whether the typestring TYPESTRING would resolve to a CPP-TYPE which is
a class template instantiation."
  (and (cl-ppcre:scan "<|>" typestring) t))

(defun typestring-fully-qualified-p (typestring)
  "Test whether the supported typestring TYPESTRING is fully qualified. If the
typestring is unsupported, return NIL."
  (and (>= (length typestring) 2)
       (string= "::" typestring :end2 2)))

(defun typestring-qualified-p (typestring)
  "Test whether the supported typestring TYPESTRING is qualified. If the
typestring is unsupported, return NIL.

Note that the test only checks the topmost type and doesn't recurse into its
type arguments."
  (or
   ;; NOTE: Checking whether the typestring is fully qualified is not just an
   ;; optimization. Since PARSE-CPP-TYPE-DECLARATION drops any qualifiers for
   ;; the global namespace, without this check we wouldn't be able to tell e.g.
   ;; whether the typestring "::MyClass" is fully qualified or not.
   (typestring-fully-qualified-p typestring)
   (let ((cpp-type (parse-cpp-type-declaration typestring)))
     (and cpp-type (cpp-type-extended-namespace cpp-type) t))))

(define-condition typestring-warning (simple-warning)
  ())

(defun typestring-warn (control &rest args)
  (warn 'typestring-warning :format-control control :format-arguments args))

(defun process-typestring (typestring)
  "Process the typestring TYPESTRING.

To process the typestring means to:

- Leave it as is if it's fully qualified, unqualified or unsupported.

- Fully qualify it if it's partially qualified."
  (check-type typestring string)
  (cond
    ((or (not (typestring-supported-p typestring))
         (typestring-fully-qualified-p typestring))
     typestring)
    ((typestring-qualified-p typestring)
     (let ((cpp-type (parse-cpp-type-declaration typestring)))
       (unless (string= (first (cpp-type-namespace cpp-type)) "std")
         (typestring-warn
          "Treating qualified type \"~A\" as the fully qualified type \"::~A\"."
          typestring typestring)))
     (format nil "::~A" typestring))
    ;; Unqualified.
    (t
     typestring)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Class and Enum Registry

(defvar *cpp-classes* nil
  "List of defined classes from LCP file.")

(defvar *cpp-enums* nil
  "List of defined enums from LCP file.")

(defun split-namespace-string (namespace)
  (let ((parts (cl-ppcre:split "::" namespace)))
    (if (string= (car parts) "")
        (cdr parts)
        parts)))

(defun find-cpp-class (name &optional namespace)
  "Find an instance of CPP-CLASS in the class registry by searching for a
specific type.

NAME must be either a string, a symbol or a CPP-TYPE instance:

- If NAME is a string or a symbol, it is treated as a designator for a class
  namestring.

- If NAME is a CPP-TYPE instance, it is treated as the string produced
  by (cpp-type-decl name).

If the resulting string is qualified, it is split into parts by \"::\", trimming
any empty strings on both sides. Every part but the last one is used to form a
list of strings that is the namespace, while the last string is used as the
name. NAMESPACE is ignored in this case.

If the resulting string is not qualified, it is taken to be the name, while the
namespace is formed according to the value of NAMESPACE.

NAMESPACE can be either a string or a list of strings:

- If NAMESPACE is a string, it is treated as the list that's the result of
  splitting the string by \"::\", trimming any empty strings on both sides. If
  the string is empty, it designates the empty list.

- If NAMESPACE is a list, it must be a list of strings, each naming a single
  namespace.

Finally, the name and the namespace are compared as follows:

- The names are compared using STRING=.

- The namespaces are compared pairwise using STRING=. The empty list designates
  the global namespace.

Return a CPP-CLASS instance if one is found, otherwise return NIL."
  (check-type name (or symbol string cpp-type))
  (check-type namespace (or nil string list))
  (multiple-value-bind (name namespace)
      (let ((name (ctypecase name
                    ((or symbol string) (ensure-namestring-for-class name))
                    (cpp-type (cpp-type-decl name)))))
        (if (typestring-qualified-p name)
            (let ((parts (split-namespace-string name)))
              (values (car (last parts)) (butlast parts)))
            (values name (ctypecase namespace
                           (list namespace)
                           (string (split-namespace-string namespace))))))
    (find-if
     (lambda (cpp-type)
       (and (string= name (cpp-type-name cpp-type))
            (equal namespace (cpp-type-extended-namespace cpp-type))))
     *cpp-classes*)))

(defun find-cpp-class-ascending (name namespace)
  "Find an instance of CPP-CLASS in the class registry by searching upwards from
the given namespace.

The arguments NAME and NAMESPACE work just the same as in FIND-CPP-CLASS, except
that:

- NAME cannot be a qualified name.

- If NAME is a CPP-TYPE instance, then it is treated as the
  string (cpp-type-name name)."
  (check-type name (or symbol string cpp-type))
  (check-type namespace (or nil string list))
  (let ((name (ctypecase name
                ((or symbol string) (ensure-namestring-for-class name))
                (cpp-type (cpp-type-name name))))
        (namespace (ctypecase namespace
                     (list namespace)
                     (string (split-namespace-string namespace)))))
    (when (typestring-qualified-p name)
      (error "Using the qualified name ~S with ~S" name
             'find-cpp-class-ascending))
    (let ((cpp-classes
            (remove-if-not
             (lambda (cpp-type)
               (and (string= name (cpp-type-name cpp-type))
                    (prefix-of-p (cpp-type-extended-namespace cpp-type)
                                 namespace :test #'string=)))
             *cpp-classes*)))
      (and cpp-classes
           (minimize
            cpp-classes
            :test #'>
            :key (lambda (cpp-type)
                   (length (cpp-type-extended-namespace cpp-type))))))))

(defun find-cpp-class-descending (name &optional namespace)
  "Find an instance of CPP-CLASS in the class registry by searching downwards
from the given namespace.

The arguments NAME and NAMESPACE work just the same as in FIND-CPP-CLASS, except
that:

- NAME cannot be a qualified name.

- If NAME is a CPP-TYPE instance, then it is treated as the
  string (cpp-type-name name)."
  (check-type name (or symbol string cpp-type))
  (check-type namespace (or nil string list))
  (let ((name (ctypecase name
                ((or symbol string) (ensure-namestring-for-class name))
                (cpp-type (cpp-type-name name))))
        (namespace (ctypecase namespace
                     (list namespace)
                     (string (split-namespace-string namespace)))))
    (when (typestring-qualified-p name)
      (error "Using the qualified name ~S with ~S" name
             'find-cpp-class-descending))
    (let ((cpp-classes
            (remove-if-not
             (lambda (cpp-type)
               (and (string= name (cpp-type-name cpp-type))
                    (prefix-of-p namespace
                                 (cpp-type-extended-namespace cpp-type)
                                 :test #'string=)))
             *cpp-classes*)))
      (and cpp-classes
           (minimize
            cpp-classes
            :key (lambda (cpp-type)
                   (length (cpp-type-extended-namespace cpp-type))))))))

(defun find-cpp-enum (name &optional namespace)
  "Find an instance of CPP-ENUM in the enum registry.

NAME must be either a string, a symbol or a CPP-TYPE instance:

- If NAME is a string or a symbol, it is treated as a designator for a class
namestring.

- If NAME is a CPP-TYPE instance, it is treated as the string produced
  by (cpp-type-decl name).

If the resulting string is qualified, it is split into parts by \"::\", trimming
any empty strings on both sides. Every part but the last one is used to form a
list of strings that is the namespace, while the last string is used as the
name. NAMESPACE is ignored in this case.

If the resulting string is not qualified, it is taken to be the name, while the
namespace is formed according to the value of NAMESPACE.

NAMESPACE can be either a string or a list of strings:

- If NAMESPACE is a string, it is treated as the list that's the result of
  splitting the string by \"::\", trimming any empty strings on both sides. If
  the string is empty, it designates the empty list.

- If NAMESPACE is a list, it must be a list of strings, each naming a single
  namespace.

Finally, the name and the namespace are compared as follows:

- The names are compared using STRING=.

- The namespaces are compared pairwise using STRING=. The empty list designates
  the global namespace.

Return a CPP-CLASS instance if one is found, otherwise return NIL."
  (check-type name (or symbol string cpp-type))
  (check-type namespace (or nil string list))
  (multiple-value-bind (name namespace)
      (let ((name (ctypecase name
                    ((or symbol string) (ensure-namestring-for-class name))
                    (cpp-type (cpp-type-decl name)))))
        (if (typestring-qualified-p name)
            (let ((parts (split-namespace-string name)))
              (values (car (last parts)) (butlast parts)))
            (values name (ctypecase namespace
                           (list namespace)
                           (string (split-namespace-string namespace))))))
    (find-if
     (lambda (cpp-type)
       (and (string= name (cpp-type-name cpp-type))
            (equal namespace (cpp-type-extended-namespace cpp-type))))
     *cpp-enums* :from-end t)))

(defun find-cpp-enum-ascending (name namespace)
  "Find an instance of CPP-ENUM in the enum registry by searching upwards from
the given namespace.

The arguments NAME and NAMESPACE work just the same as in FIND-CPP-ENUM, except
that:

- NAME cannot be a qualified name.

- If NAME is a CPP-TYPE instance, then it is treated as the
  string (cpp-type-name name)."
  (check-type name (or symbol string cpp-type))
  (check-type namespace (or nil string list))
  (let ((name (ctypecase name
                ((or symbol string) (ensure-namestring-for-class name))
                (cpp-type (cpp-type-name name))))
        (namespace (ctypecase namespace
                     (list namespace)
                     (string (split-namespace-string namespace)))))
    (when (typestring-qualified-p name)
      (error "Using the qualified name ~S with ~S" name
             'find-cpp-enum-ascending))
    (let ((cpp-enums
            (remove-if-not
             (lambda (cpp-type)
               (and (string= name (cpp-type-name cpp-type))
                    (prefix-of-p (cpp-type-extended-namespace cpp-type)
                                 namespace :test #'string=)))
             *cpp-enums*)))
      (and cpp-enums
           (minimize
            cpp-enums
            :test #'>
            :key (lambda (cpp-type)
                   (length (cpp-type-extended-namespace cpp-type))))))))

(defun find-cpp-enum-descending (name &optional namespace)
  "Find an instance of CPP-ENUM in the enum registry by searching downwards
from the given namespace.

The arguments NAME and NAMESPACE work just the same as in FIND-CPP-ENUM, except
that:

- NAME cannot be a qualified name.

- If NAME is a CPP-TYPE instance, then it is treated as the
  string (cpp-type-name name)."
  (check-type name (or symbol string cpp-type))
  (check-type namespace (or nil string list))
  (let ((name (ctypecase name
                (string name)
                (symbol (cpp-name-for-class name))))
        (namespace (ctypecase namespace
                     (list namespace)
                     (string (split-namespace-string namespace)))))
    (when (typestring-qualified-p name)
      (error "Using the qualified name ~S with an iterative traversal" name))
    (let ((cpp-enums
            (remove-if-not
             (lambda (cpp-type)
               (and (string= name (cpp-type-name cpp-type))
                    (prefix-of-p namespace
                                 (cpp-type-extended-namespace cpp-type)
                                 :test #'string=)))
             *cpp-enums*)))
      (and cpp-enums
           (minimize
            cpp-enums
            :key (lambda (cpp-type)
                   (length (cpp-type-extended-namespace cpp-type))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Type queries

(defun cpp-enum-p (object)
  "Test whether OBJECT is an instance of CPP-ENUM."
  (typep object 'cpp-enum))

(defun cpp-class-p (object)
  "Test whether OBJECT is an instance of CPP-CLASS."
  (typep object 'cpp-class))

(defun cpp-type-supported-p (general-cpp-type)
  "Test whether the given GENERAL-CPP-TYPE instance is a supported type (i.e.
not an instance of UNSUPPORTED-CPP-TYPE)."
  (check-type general-cpp-type general-cpp-type)
  (not (typep general-cpp-type 'unsupported-cpp-type)))

(defun cpp-type-known-p (general-cpp-type)
  "Test whether the given GENERAL-CPP-TYPE instance is a known type."
  (check-type general-cpp-type general-cpp-type)
  (or (cpp-class-p general-cpp-type) (cpp-enum-p general-cpp-type)))

(defun cpp-type-primitive-p (cpp-type)
  "Test whether CPP-TYPE represents a primitive C++ type."
  (check-type cpp-type cpp-type)
  (and (null (cpp-type-namespace cpp-type))
       (null (cpp-type-enclosing-classes cpp-type))
       (null (cpp-type-type-params cpp-type))
       (null (cpp-type-type-args cpp-type))
       (member (cpp-type-name cpp-type) +cpp-primitive-type-names+
               :test #'string=)
       t))

(defun cpp-type-raw-pointer-p (cpp-type)
  "Test whether CPP-TYPE represents a raw pointer type."
  (check-type cpp-type cpp-type)
  (string= (cpp-type-name cpp-type) "*"))

(defun cpp-type-reference-p (cpp-type)
  "Test whether CPP-TYPE represents a reference type."
  (check-type cpp-type cpp-type)
  (string= (cpp-type-name cpp-type) "&"))

(defun cpp-type-const-p (cpp-type)
  "Test whether CPP-TYPE represents a constant type."
  (check-type cpp-type cpp-type)
  (string= (cpp-type-name cpp-type) "const"))

(defun cpp-type-smart-pointer-p (cpp-type)
  "Test whether CPP-TYPE represents a smart pointer type."
  (check-type cpp-type cpp-type)
  (and (cpp-type-class-template-instantiation-p cpp-type)
       (member (cpp-type-name cpp-type) '("shared_ptr" "unique_ptr")
               :test #'string=)
       t))

(defun cpp-type-pointer-p (cpp-type)
  "Test whether CPP-TYPE represents either a raw or a smart pointer type."
  (check-type cpp-type cpp-type)
  (or (cpp-type-raw-pointer-p cpp-type)
      (cpp-type-smart-pointer-p cpp-type)))

(defun cpp-type-simple-class-p (cpp-type)
  "Test whether CPP-TYPE represents a simple class (class which is not a class
template instantiation)."
  (check-type cpp-type cpp-type)
  (and (not (cpp-type-primitive-p cpp-type))
       (not (cpp-type-type-params cpp-type))
       (not (cpp-type-type-args cpp-type))))

(defun cpp-type-class-template-p (cpp-type)
  "Test whether CPP-TYPE represents a class template."
  (check-type cpp-type cpp-type)
  (and (not (cpp-type-raw-pointer-p cpp-type))
       (not (cpp-type-reference-p cpp-type))
       (cpp-type-type-params cpp-type)
       t))

(defun cpp-type-class-template-instantiation-p (cpp-type)
  "Test whether CPP-TYPE represents a class template instantiation."
  (check-type cpp-type cpp-type)
  (and (not (member (cpp-type-name cpp-type) '("*" "&") :test #'string=))
       (cpp-type-type-args cpp-type)
       t))

(defun cpp-type-class-p (cpp-type)
  "Test whether CPP-TYPE represents either a simple class or a class template
instantiation."
  (check-type cpp-type cpp-type)
  (or (cpp-type-simple-class-p cpp-type)
      (cpp-type-class-template-instantiation-p cpp-type)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Resolution

(defun resolve-typestring-for-super-class (typestring cpp-class)
  "Resolve the typestring TYPESTRING for a superclass. CPP-CLASS is the
CPP-CLASS instance that subclass the class named by the typestring and is used
to perform proper relative lookup, if any."
  (flet ((rec (cpp-type)
           ;; NOTE: This will surely produce the original typestring because
           ;; we only ever resolve typestrings that are either fully qualified
           ;; or not qualified at all, both of which are preserved by the
           ;; declaration parsing process.
           (resolve-typestring-for-super-class
            (cpp-type-decl
             cpp-type :globalp (cpp-type-extended-namespace cpp-type))
            cpp-class)))
    (let* ((cpp-type (parse-cpp-type-declaration typestring))
           (resolved (cond
                       ((not cpp-type)
                        (make-unsupported-cpp-type typestring))
                       ((typestring-fully-qualified-p typestring)
                        (or (find-cpp-class typestring)
                            cpp-type))
                       (t
                        (or (find-cpp-class-ascending
                             typestring (cpp-type-extended-namespace cpp-class))
                            cpp-type)))))
      (prog1 resolved
        ;; Recursively resolve any type arguments, but only for supported types.
        (when cpp-type
          (setf (%cpp-type-type-args resolved)
                (mapcar #'rec (cpp-type-type-args resolved))))))))

(defun resolve-typestring-for-member (typestring cpp-class)
  "Resolve the typestring TYPESTRING for the type of a member. CPP-CLASS is a
CPP-CLASS instance that contains the CPP-MEMBER and is used in order to perform
proper relative lookup, if any."
  (flet ((rec (cpp-type)
           (resolve-typestring-for-member
            ;; NOTE: This will surely produce the original typestring because
            ;; we only ever resolve typestrings that are either fully
            ;; qualified or not qualified at all, both of which are preserved
            ;; by the declaration parsing process.
            (cpp-type-decl
             cpp-type :globalp (cpp-type-extended-namespace cpp-type))
            cpp-class)))
    (let* ((cpp-type (parse-cpp-type-declaration typestring))
           (resolved (cond
                       ((not cpp-type)
                        (make-unsupported-cpp-type typestring))
                       ((typestring-fully-qualified-p typestring)
                        (or (find-cpp-class typestring)
                            (find-cpp-enum typestring)
                            cpp-type))
                       ((cpp-type-primitive-p cpp-type)
                        cpp-type)
                       (t
                        ;; The types of members may be defined within the class
                        ;; itself.
                        (let ((namespace
                                (append (cpp-type-extended-namespace cpp-class)
                                        (list (cpp-type-name cpp-class)))))
                          (or (find-cpp-class-ascending typestring namespace)
                              (find-cpp-enum-ascending typestring namespace)
                              cpp-type))))))
      (prog1 resolved
        ;; Recursively resolve any type arguments, but only for supported types.
        (when cpp-type
          (setf (%cpp-type-type-args resolved)
                (mapcar #'rec (cpp-type-type-args resolved))))))))

(defmethod cpp-class-super-classes ((cpp-class cpp-class))
  "Return a list of GENERAL-CPP-TYPE instances which are the superclasses of the
C++ class CPP-CLASS."
  (mapcar
   (lambda (typestring)
     (resolve-typestring-for-super-class typestring cpp-class))
   (%cpp-class-super-classes cpp-class)))

(defmethod cpp-class-members (cpp-class)
  (mapcar
   (lambda (member)
     (let ((member (copy-cpp-member member)))
       (prog1 member
         (setf (cpp-member-type member)
               (resolve-typestring-for-member
                (cpp-member-type member) cpp-class)))))
   (%cpp-class-members cpp-class)))

(defmethod cpp-class-slk-opts (cpp-class)
  (alexandria:when-let ((opts (%cpp-class-slk-opts cpp-class)))
    (let ((opts (copy-slk-opts opts)))
      (prog1 opts
        (setf (slk-opts-save-args opts)
              (mapcar
               (lambda (arg)
                 (destructuring-bind (namestring typestring) arg
                   (list namestring (resolve-typestring-for-member
                                     typestring cpp-class))))
               (slk-opts-save-args opts)))
        (setf (slk-opts-load-args opts)
              (mapcar
               (lambda (arg)
                 (destructuring-bind (namestring typestring) arg
                   (list namestring (resolve-typestring-for-member
                                     typestring cpp-class))))
               (slk-opts-load-args opts)))))))

(defmethod cpp-class-clone-opts (cpp-class)
  (alexandria:when-let ((opts (%cpp-class-clone-opts cpp-class)))
    (let ((opts (copy-clone-opts opts)))
      (prog1 opts
        (setf (clone-opts-args opts)
              (mapcar
               (lambda (arg)
                 (destructuring-bind (namestring typestring) arg
                   (list namestring (resolve-typestring-for-member
                                     typestring cpp-class))))
               (clone-opts-args opts)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Type utilities

(defun ensure-cpp-type (thing)
  "Return a CPP-TYPE instance corresponding to the CPP-TYPE designator
THING.

- If THING is of type CPP-TYPE, return it.

- If THING is a typestring designator it is coerced into a typestring as if by
  ENSURE-TYPESTRING. The typestring is then parsed using
  PARSE-CPP-TYPE-DECLARATION. If it is successfully parsed, return the resulting
  CPP-TYPE instance. Otherwise, return an instance of UNSUPPORTED-CPP-TYPE."
  (ctypecase thing
    (cpp-type
     thing)
    ((or symbol string)
     (let ((thing (ensure-typestring thing)))
       (or (parse-cpp-type-declaration thing)
           (make-unsupported-cpp-type thing))))))

(defun cpp-class-direct-subclasses (cpp-class)
  "Return a list of CPP-CLASS instances which are the direct subclasses of the
C++ class CPP-CLASS."
  (check-type cpp-class cpp-class)
  ;; Reverse to get them in definition order.
  (reverse
   (remove-if-not
    (lambda (subclass)
      (member cpp-class
              (remove-if-not #'cpp-type-supported-p
                             (cpp-class-super-classes subclass))
              :test #'cpp-type=))
    *cpp-classes*)))

(defun cpp-type-extended-namespace (cpp-type)
  (check-type cpp-type cpp-type)
  (append (cpp-type-namespace cpp-type) (cpp-type-enclosing-classes cpp-type)))

(defun cpp-type-namespace-string (cpp-type)
  "Return the namespace part of CPP-TYPE as a string ending with \"::\". When
CPP-TYPE has no namespace, return an empty string."
  (check-type cpp-type cpp-type)
  (format nil "~{~A::~}" (cpp-type-namespace cpp-type)))

(defun cpp-type-enclosing-classes-string (cpp-type)
  "Return as a string the concatenation of the names of the enclosing classes of
the type CPP-TYPE. The names are delimited with \"::\" and a trailing delimiter
is included."
  (check-type cpp-type cpp-type)
  (format nil "~{~A::~}" (cpp-type-enclosing-classes cpp-type)))

(defun cpp-class-members-for-save (cpp-class)
  (check-type cpp-class cpp-class)
  (remove-if #'cpp-member-dont-save (cpp-class-members cpp-class)))

(defun cpp-type-wrap (cpp-type class-templates)
  (check-type cpp-type lcp::cpp-type)
  (reduce (lambda (cpp-type class-template)
            (lcp::make-cpp-type
             (lcp::ensure-namestring-for-class class-template)
             :type-args (list cpp-type)))
          class-templates :initial-value cpp-type))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Macros
;;;
;;; These provide a small DSL for defining enums and classes. The defined enums
;;; and classes are automatically added to the global enum and class registries.
;;;
;;; *CPP-INNER-TYPES* and *CPP-ENCLOSING-CLASSES* are used to communicate (at
;;; run-time, not macroexpansion-time) information between nested usages of the
;;; macros. The expansions are such that any nested expansions will be evaluated
;;; within a dynamic environment set up by the parent macro.

(defvar *cpp-inner-types* :toplevel
  "A list of CPP-TYPE instances defined within the current class being
defined.")

(defvar *cpp-enclosing-classes* nil
  "A list of strings naming the enclosing classes of the current class being
defined. The names are ordered from outermost to innermost enclosing class.")

(defun cons-or-replace (element list &key (test #'eql) (key #'identity))
  "Cons the ELEMENT to the LIST and remove existing elements.

All elements that compare equal (under TEST) with ELEMENT are removed. KEY will
be applied to each element of LIST before calling TEST."
  (cons element (remove-if (lambda (other) (funcall test element other))
                           list :key key)))

(defun register-enum (cpp-enum)
  "Register the given CPP-ENUM instance with the enum registry."
  (check-type cpp-enum cpp-enum)
  (prog1 cpp-enum
    ;; Add or redefine the enum.
    (setf *cpp-enums*
          (cons cpp-enum
                (delete-if
                 (lambda (other)
                   (string= (cpp-type-decl cpp-enum) (cpp-type-decl other)))
                 *cpp-enums*)))
    ;; Add to the parent's inner types.
    (unless (eq *cpp-inner-types* :toplevel)
      (push cpp-enum *cpp-inner-types*))))

(defun register-class (cpp-class)
  "Register the given CPP-CLASS instance with the class registry."
  (check-type cpp-class cpp-class)
  (prog1 cpp-class
    ;; Add or redefine the class.
    (setf *cpp-classes*
          (cons cpp-class
                (delete-if
                 (lambda (other)
                   (string= (cpp-type-decl cpp-class) (cpp-type-decl other)))
                 *cpp-classes*)))
    ;; Add to the parent's inner types.
    (unless (eq *cpp-inner-types* :toplevel)
      (push cpp-class *cpp-inner-types*))))

(defmacro define-enum (name values &rest options)
  "Define a C++ enum.

The syntax is:

  (define-enum <name>
    (<value>*)
    <enum-option>*)

NAME should be a designator for a class namestring. VALUE should be a designator
for an enumerator namestring.

Each ENUM-OPTION is of the type (KEY VALUE). The possible values of KEY are:

- :DOCUMENTATION -- String specifying the Doxygen documentation for the enum.

- :SERIALIZE -- If T, generate serialization code for this enum."
  `(register-enum
    (make-instance
     'cpp-enum
     :documentation ',(assoc-second :documentation options)
     :name ',(ensure-namestring-for-class name)
     :values ',(mapcar #'ensure-namestring-for-enumerator values)
     :namespace (reverse *cpp-namespaces*)
     :enclosing-classes (reverse *cpp-enclosing-classes*)
     :serializep ',(if (assoc :serialize options) t))))

(defun generate-make-cpp-member (slot-definition &key structp)
  (destructuring-bind (name type &rest kwargs &key reader scope
                       &allow-other-keys)
      slot-definition
    (let ((scope (or scope (if structp :public :private))))
      (when (and structp reader (eq :private scope))
        (error "~A is a private member with a getter, but a struct is being defined" name))
      (when (and structp reader (eq :public scope))
        (error "~A is a public member, but a getter was defined" name))
      `(make-cpp-member
        :name ,(ensure-namestring-for-member name :structp structp)
        :type (process-typestring ,(ensure-typestring type))
        :scope ',scope
        ,@kwargs))))

(defun ensure-arg-list (args)
  (mapcar (lambda (arg)
            (destructuring-bind (namestring typestring) arg
              (list (ensure-namestring-for-variable namestring)
                    (process-typestring (ensure-typestring typestring)))))
          args))

(defun generate-slk-opts (opts)
  (destructuring-bind (&rest kwargs &key save-args load-args
                       &allow-other-keys)
      opts
    (let ((save-args (and save-args `(:save-args (ensure-arg-list ,save-args))))
          (load-args (and load-args `(:load-args (ensure-arg-list ,load-args)))))
      `(make-slk-opts ,@save-args
                      ,@load-args
                      ,@(alexandria:remove-from-plist
                         kwargs
                         (and save-args :save-args)
                         (and load-args :load-args))))))

(defun generate-clone-opts (opts)
  (destructuring-bind (&rest kwargs &key args &allow-other-keys)
      opts
    (let ((args (and args `(:args (ensure-arg-list ,args)))))
      `(make-clone-opts ,@args
                        ,@(alexandria:remove-from-plist
                           kwargs (and args :args))))))

(defun generate-define-class (name super-classes slots options)
  "Generate the expansion for DEFINE-CLASS."
  (let* ((name (alexandria:ensure-list name))
         (class-name (ensure-namestring-for-class (car name)))
         (type-params (mapcar #'ensure-namestring-for-type-param (cdr name)))
         (structp (assoc-second :structp options))
         (abstractp (assoc-second :abstractp options))
         (members (mapcar (lambda (s) (generate-make-cpp-member s :structp structp)) slots))
         (super-classes (mapcar #'ensure-typestring super-classes))
         (serialize (assoc-body :serialize options))
         (slk (assoc :slk serialize))
         (clone (assoc :clone options))
         (type-info (assoc-body :type-info options))
         (documentation (assoc-second :documentation options))
         (public (concat (assoc-body-all :public options)))
         (protected (concat (assoc-body-all :protected options)))
         (private (concat (assoc-body-all :private options))))
    ;; Call REGISTER-CLASS within the original context.
    `(register-class
      ;; Save our original context.
      ,(alexandria:once-only ((cpp-enclosing-classes '*cpp-enclosing-classes*))
         ;; Evaluate the subforms of DEFINE-CLASS within a nested context, so
         ;; that recursive invocations of the same macro are handled properly.
         `(let ((*cpp-inner-types* '())
                (*cpp-enclosing-classes* (cons ,class-name *cpp-enclosing-classes*)))
            ;; Explicitly sequence the evaluation of user-provided subforms to aid clarity.
            ,(alexandria:once-only
                 ((public `(list ,@public))
                  (protected `(list ,@protected))
                  (private `(list ,@private))
                  (slk (and slk (generate-slk-opts (cdr slk))))
                  (clone (and clone (generate-clone-opts (cdr clone))))
                  (type-info `(make-type-info-opts ,@type-info)))
               `(make-instance
                 'cpp-class
                 :documentation ',documentation
                 :namespace (reverse *cpp-namespaces*)
                 :enclosing-classes (reverse ,cpp-enclosing-classes)
                 :name ,class-name
                 :type-params ',type-params
                 :structp ',structp
                 :super-classes (mapcar #'process-typestring ',super-classes)
                 :members (list ,@members)
                 :public ,public
                 :protected ,protected
                 :private ,private
                 :slk-opts ,slk
                 :clone-opts ,clone
                 :type-info-opts ,type-info
                 :inner-types *cpp-inner-types*
                 :abstractp ',abstractp)))))))

(defmacro define-class (name super-classes slots &rest options)
  "Define a simple C++ class or a C++ class template.

The syntax is:

  (define-class <name> (<super-class>*)
    (<cpp-slot-definition>*)
    <class-option>*)

NAME is either an atom ATOM or a list of the form (ATOM TYPE-PARAM+). If NAME is
an atom, the invocation defines a simple C++ class. Otherwise, a class template
is defined. In both cases ATOM must be a designator for a class namestring.
TYPE-PARAM must be designator for a type parameter namestring.

Each SUPER-CLASS is a typestring representing a superclass of the class being
defined.

Each CPP-SLOT-DEFINITION is of the form (NAME CPP-TYPE . SLOT-OPTIONS). NAME
must be a designator for a member namestring. CPP-TYPE must be a typestring
designator.

SLOT-OPTIONS is a plist whose values are not evaluated by default. The possible
keys are:

- :INITVAL -- Evaluated. A number or a string representing a C++ expression that
  will be used to initialize the member using the member initializer list.

- :READER -- If T, generates a public getter for the member.

- :SCOPE -- The class scope of the member. One of :PUBLIC, :PROTECTED
  or :PRIVATE (default).

- :DOCUMENTATION -- String specifying the Doxygen documentation for the member.

The SLK serialization backend also introduces the following member options:

- :SLK-SAVE -- Evaluated. A function that accepts a single argument, a
  namestring corresponding to the member. The function should return a RAW-CPP
  object representing the C++ code that saves the member.

- :SLK-LOAD -- Evaluated. A function that accepts a single argument, a
  namestring corresponding to the member. The function should return a RAW-CPP
  object representing the C++ code that loads the member.

CLASS-OPTION is a pair (KEY VALUE*). VALUE is by default not evaluated. Options
by default have overriding behavior, meaning that if a key appears multiple
times, the value associated with the leftmost one is taken. Options might
instead have aggregating behavior, meaning that the value is formed by
collecting the values associated with all of the appearances of the key. The
possible values of KEY are:

- :DOCUMENTATION -- String specifying the Doxygen documentation for the class.

- :PUBLIC, :PROTECTED, :PRIVATE -- Evaluated. Aggregated. Lisp forms that
  evaluate to RAW-CPP objects representing C++ code that is to be included
  within the public (or protected or private) scope of the class body. Results
  that are not of type RAW-CPP are ignored.

- :SERIALIZE -- Generate serialization code for the class using the given
  serialization backend.

  Each VALUE should be of the form (BACKEND . BACKEND-OPTIONS), where BACKEND is
  a keyword corresponding to the serialization backend. BACKEND-OPTIONS is a
  plist specifying backend-specific options.

  For now, only the SLK (:slk) backend is supported. Its options are:

  - :SAVE-ARGS -- Evaluated. A list of (NAME TYPE) pairs that designate extra
    arguments of the generated serialization function. NAME should be a variable
    namestring designator while TYPE should be a typestring designator.

  - :LOAD-ARGS -- Evaluated. A list of (NAME TYPE) pairs that designate
    arguments of the generated deserialization function. NAME should be a
    variable namestring designator while TYPE should be a typestring designator.

  - :BASE -- If T, treat the class as the root of a class hierarchy for the
    purpose of serialization.

  - :IGNORE-OTHER-BASE-CLASSES -- If T, treat the class as if it inherits just
    the first of its superclasses, ignoring the others.

- :CLONE -- Generate cloning code for the class.

  All VALUEs should form a plist of clone options. The following options are
  supported:

  - :RETURN-TYPE -- Evaluated. A function that accepts a single argument, a
    typestring corresponding to the class being defined. The function should
    return a typestring that represents the return type of the cloning function.

  - :ARGS -- Evaluated. A list of (NAME TYPE) pairs that designate arguments of
    the generated cloning function. NAME should be a variable namestring
    designator while TYPE should be a typestring designator.

  - :INIT-OBJECT -- Evaluated. A function that accepts two arguments, NAME and
    TYPE. NAME is a variable namestring while TYPE is a typestring corresponding
    to the class being defined. The function should return, as a string, C++
    code that declares and initializes the C++ variable NAME, of type TYPE.

  - :IGNORE-OTHER-BASE-CLASSES -- If T, treat the class as if it inherits just
    the first of its superclasses, ignoring the others.

- :TYPE-INFO -- Specify additional type information options. Type information
  code for the class is generated unconditionally, whether or not this option is
  present.

  All VALUEs should form a plist of type information options. The following
  options are supported:

  - :BASE -- If T, treat the class as the root of a class hierarchy for the
    purpose of serialization.

  - :IGNORE-OTHER-BASE-CLASSES -- If T, treat the class as if it inherits just
    the first of its superclasses, ignoring the others.

- :ABSTRACTP -- If T, marks that this class cannot be instantiated (currently
  only useful in serialization code).

- :STRUCTP -- If T, define a struct instead of a class."
  (generate-define-class name super-classes slots options))

(defmacro define-struct (name super-classes slots &rest options)
  "The same as DEFINE-CLASS, except that a struct is defined instead (by passing
T to the :STRUCTP option)."
  `(define-class ,name ,super-classes ,slots (:structp t) ,@options))

(defun rpc-constructors (class-name members)
  "Generate C++ code for an RPC's constructors.

CLASS-NAME is the name of the class whose constructors to generate. MEMBERS
should be a list of members as in DEFINE-RPC. Detailed documentation regarding
the constructors and various options can be found within DEFINE-RPC."
  (let* ((members (remove-if (lambda (member)
                               (let ((initarg (member :initarg member)))
                                 (and initarg (null (second initarg)))))
                             members))
         (args
           (mapcar
            (lambda (member)
              (list (ensure-typestring (second member))
                    (ensure-namestring-for-member (first member) :structp t)))
            members))
         (init-list
           (mapcar
            (lambda (member)
              (let ((var (ensure-namestring-for-variable (first member)))
                    (movep (eq :move (second (member :initarg member)))))
                (list var (if movep
                              (format nil "std::move(~A)" var)
                              var))))
            members))
         (full-constructor
           (with-output-to-string (s)
             (when members
               (format s "~A ~A(~:{~A ~A~:^, ~}) : ~:{~A(~A)~:^, ~} {}"
                       (if (= (length members) 1) "explicit" "")
                       class-name args init-list)))))
    #>cpp
    ${class-name}() {}
    ${full-constructor}
    cpp<#))

(defun rpc-save-load (name)
  "Generate SLK's `Save` and `Load` functions for a request or response RPC
structure named by the string NAME."
  ;; TODO: Replace FIND-CPP-CLASS-DESCENDING.
  `(let ((class (find-cpp-class-descending ,name)))
     (unless (lcp.slk::save-extra-args class)
       (push ,(progn
                #>cpp
                  static void Save(const ${name} &self, memgraph::slk::Builder *builder);
                cpp<#)
             (cpp-class-public class))
       (in-impl
        ,(progn
           #>cpp
             void ${name}::Save(const ${name} &self, memgraph::slk::Builder *builder) {
               memgraph::slk::Save(self, builder);
             }
           cpp<#)))
     (unless (lcp.slk::load-extra-args class)
       (push ,(progn #>cpp
                       static void Load(${name} *self, memgraph::slk::Reader *reader);
                     cpp<#)
             (cpp-class-public class))
       (in-impl
        ,(progn
           #>cpp
             void ${name}::Load(${name} *self, memgraph::slk::Reader *reader) {
               memgraph::slk::Load(self, reader);
             }
           cpp<#)))))

(defmacro define-rpc (name &body options)
  "Define an RPC. Two structures are defined, representing the request and
the response for the given RPC.

The syntax is:

  (define-rpc <name>
    (:request (<slot>*) <struct-option>*)
    (:response (<slot>*) <struct-option>*))

NAME should designate a namestring for a class, which is used to produce the
names of the two structures.

The names of the structures are formed by concatenating the namestring NAME with
\"Req\" and \"Res\".

The two options :REQUEST and :RESPONSE are mandatory. Their bodies should follow
the same syntax and conventions of DEFINE-STRUCT's (i.e. DEFINE-CLASS's) class
and member options.

DEFINE-RPC introduces the following additional class options:

- :CTOR -- If NIL, inhibits the generation of constructors.

  For both structures two constructors are generated:

  - A default constructor that does no explicit initialization of members.

  - A custom constructor that accepts values and initializes members according
    to their :INITARG option, in order of appearance.

    If the constructor ends up accepting just one member, it is marked
    `explicit`.

    If the constructor ends up accepting no members, it is not generated.

DEFINE-RPC introduces the following additional member options:

- :INITARG -- Controls the way in which the corresponding member participates in
  the custom constructor.

  If the :INITARG option is omitted or NIL, the constructor doesn't accept a
  value for the member and the member is not explicitly initialized.

  If the :INITARG option is true, the constructor accepts a value for the member
  and the member is copy-initialized.

  If the :INITARG option is :MOVE, the constructor accepts a value for the
  member and the member is move-initialized using `std::move`."
  (flet ((remove-rpc-options (body)
           `(,(mapcar
               (lambda (member)
                 `(,(first member)
                   ,(second member)
                   ,@(alexandria:remove-from-plist (cddr member) :initarg)))
               (car body))
             ,@(remove :ctor (cdr body) :key #'car))))
    (let* ((name (ensure-namestring-for-class name))
           (rpc-name (format nil "~ARpc" name))
           (req-name (format nil "~AReq" name))
           (res-name (format nil "~ARes" name))
           (rpc-decl
             #>cpp
             using ${rpc-name} = rpc::RequestResponse<${req-name}, ${res-name}>;
             cpp<#)
           (request-body (cdr (assoc :request options)))
           (response-body (cdr (assoc :response options)))
           (req-ctor (assoc :ctor (cdr request-body)))
           (res-ctor (assoc :ctor (cdr response-body))))
      `(cpp-list
        (define-struct ,req-name ()
          ,@(remove-rpc-options request-body)
          ,@(when (or (not req-ctor) (not (cdr req-ctor)))
              `((:public
                 ,(rpc-constructors req-name (first request-body)))))
          (:serialize (:slk)))
        ,(rpc-save-load req-name)
        (define-struct ,res-name ()
          ,@(remove-rpc-options response-body)
          ,@(when (or (not res-ctor) (not (cdr res-ctor)))
              `((:public
                 ,(rpc-constructors res-name (first response-body)))))
          (:serialize (:slk)))
        ,(rpc-save-load res-name)
        ,rpc-decl))))
