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

(in-package #:lcp)

(defconstant +whitespace-chars+ '(#\Newline #\Space #\Return #\Linefeed #\Tab))
(defconstant +vim-read-only+ "vim: readonly")
(defconstant +emacs-read-only+ "-*- buffer-read-only: t; -*-")

(defstruct raw-cpp
  (string "" :type string :read-only t))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun |#>-reader| (stream sub-char numarg)
    "Reads the #>cpp ... cpp<# block into `raw-cpp'.
    The block supports string interpolation of variables by using the syntax
    similar to shell interpolation. For example, ${variable} will be
    interpolated to use the value of VARIABLE."
    (declare (ignore sub-char numarg))
    (let ((begin-cpp (read stream nil :eof t)))
      (unless (and (symbolp begin-cpp) (string= begin-cpp 'cpp))
        (error "Expected #>cpp, got '#>~A'" begin-cpp)))
    (let ((output (make-array 0 :element-type 'character :adjustable t :fill-pointer 0))
          (end-cpp "cpp<#")
          interpolated-args)
      (flet ((interpolate-argument ()
               "Parse argument for interpolation after $."
               (when (char= #\$ (peek-char nil stream t nil t))
                 ;; $$ is just $
                 (vector-push-extend (read-char stream t nil t) output)
                 (return-from interpolate-argument))
               (unless (char= #\{ (peek-char nil stream t nil t))
                 (error "Expected { after $"))
               (read-char stream t nil t) ;; consume {
               (let ((form (let ((*readtable* (copy-readtable)))
                             ;; Read form to }
                             (set-macro-character #\} (get-macro-character #\)))
                             (read-delimited-list #\} stream t))))
                 (unless (and (not (null form)) (null (cdr form)) (symbolp (car form)))
                   (error "Expected a variable inside ${..}, got ~A" form))
                 ;; Push the variable symbol
                 (push (car form) interpolated-args))
               ;; Push the format directive
               (vector-push-extend #\~ output)
               (vector-push-extend #\A output)))
        (handler-case
            (do (curr
                 (pos 0))
                ((= pos (length end-cpp)))
              (setf curr (read-char stream t nil t))
              (if (and (< pos (length end-cpp))
                       (char= (char-downcase curr) (aref end-cpp pos)))
                  (incf pos)
                  (setf pos 0))
              (if (char= #\$ curr)
                  (interpolate-argument)
                  (vector-push-extend curr output)))
          (end-of-file () (error "Missing closing '#>cpp .. cpp<#' block"))))
      (let ((trimmed-string
             (string-trim +whitespace-chars+
                          (subseq output
                                  0 (- (length output) (length end-cpp))))))
        `(make-raw-cpp
          :string ,(if interpolated-args
                       `(format nil ,trimmed-string ,@(reverse interpolated-args))
                       trimmed-string))))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (set-dispatch-macro-character #\# #\> #'|#>-reader|))

(defclass cpp-type ()
  ((documentation :type (or null string) :initarg :documentation :initform nil
                  :reader cpp-type-documentation
                  :documentation "Documentation string for this C++ type.")
   (namespace :type list :initarg :ns :initarg :namespace :initform nil
              :reader cpp-type-namespace
              :documentation "A list of symbols or strings defining the full
              namespace. A single symbol may refer to a `CPP-CLASS' which
              encloses this type.")
   (enclosing-class :type (or null symbol) :initarg :enclosing-class
                    :initform nil :accessor cpp-type-enclosing-class)
   (name :type (or symbol string) :initarg :name :reader cpp-type-base-name
         :documentation "Base name of this type.")
   (type-params :type list :initarg :type-params :initform nil
                :reader cpp-type-type-params
                :documentation "List of template parameters that are needed to
                instantiate a concrete type. For example, in `template
                <TValue> class vector`, 'TValue' is type parameter.")
   (type-args :type list :initarg :type-args :initform nil
              :reader cpp-type-type-args
              :documentation "List of already applied template arguments. For
              example in `std::vector<int>`, 'int' is a type argument."))
  (:documentation "Base class for meta information on C++ types."))

(defgeneric cpp-type-name (cpp-type)
  (:documentation "Get C++ style type name from `CPP-TYPE' as a string."))

(defmethod cpp-type-name ((cpp-type string))
  "Return CPP-TYPE string as is."
  cpp-type)

(defmethod cpp-type-name ((cpp-type cpp-type))
  "Return `CPP-TYPE' name as PascalCase or if string, as is."
  (cpp-type-name (cpp-type-base-name cpp-type)))

(deftype cpp-primitive-type-keywords ()
  "List of keywords that specify a primitive type in C++."
  `(member :bool :int :int16_t :int32_t :int64_t :uint :uint16_t :uint32_t :uint64_t :float :double))

(defconstant +cpp-primitive-type-keywords+
  '(:bool :int :int16_t :int32_t :int64_t :uint :uint16_t :uint32_t :uint64_t :float :double))

(defmethod cpp-type-name ((cpp-type symbol))
  "Return PascalCase of CPP-TYPE symbol or lowercase if it is a primitive type."
  (if (typep cpp-type 'cpp-primitive-type-keywords)
      (string-downcase (string cpp-type))
      (remove #\- (string-capitalize (string cpp-type)))))

(defclass cpp-primitive-type (cpp-type)
  ((name :type cpp-primitive-type-keywords))
  (:documentation "Represents a primitive type in C++."))

(defun parse-cpp-type-declaration (type-decl)
  "Parse C++ type from TYPE-DECL string and return CPP-TYPE.

For example:

::std::pair<my_space::MyClass<std::function<void(int, bool)>, double>, char>

produces:

;; (cpp-type
;;  :name pair
;;  :type-args ((cpp-type
;;              :name MyClass
;;              :type-args ((cpp-type :name function
;;                                    :type-args (cpp-type :name void(int, bool)))
;;                          (cpp-type :name double)))
;;              (cpp-type :name char)))"
  (declare (type string type-decl))
  ;; C++ type can be declared as follows:
  ;; namespace::namespace::type<type-arg, type-arg> *
  ;; |^^^^^^^^^^^^^^^^^^^^|    |^^^^^^^^^^^^^^^^^^| | optional
  ;;       optional                 optional
  ;; type-args in template are recursively parsed
  ;; C++ may contain dependent names with 'typename' keyword, these aren't
  ;; supported here.
  (when (search "typename" type-decl)
    (error "'typename' not supported in '~A'" type-decl))
  (when (find #\& type-decl)
    (error "References not supported in '~A'" type-decl))
  (setf type-decl (string-trim +whitespace-chars+ type-decl))
  ;; Check if primitive type
  (let ((type-keyword (member type-decl +cpp-primitive-type-keywords+
                              :test #'string-equal)))
    (when type-keyword
      (return-from parse-cpp-type-declaration
        (make-instance 'cpp-primitive-type :name (car type-keyword)))))
  ;; Check if pointer
  (let ((ptr-pos (position #\* type-decl :from-end t)))
    (when (and ptr-pos (not (cl-ppcre:scan "[()<>]" type-decl :start ptr-pos)))
      (return-from parse-cpp-type-declaration
        (make-instance 'cpp-type
                       :name (subseq type-decl ptr-pos)
                       :type-args (list (parse-cpp-type-declaration
                                         (subseq type-decl 0 ptr-pos)))))))
  ;; Other cases
  (destructuring-bind (full-name &optional template)
      (cl-ppcre:split "<" type-decl :limit 2)
    (let* ((namespace-split (cl-ppcre:split "::" full-name))
           (name (car (last namespace-split)))
           type-args)
      (when template
        ;; template ends with '>' character
        (let ((arg-start 0))
          (cl-ppcre:do-scans (match-start match-end reg-starts reg-ends
                                          "[a-zA-Z0-9_:<>() *]+[,>]" template)
            (flet ((matchedp (open-char close-char)
                     "Return T if the TEMPLATE[ARG-START:MATCH-END] contains
                     matched OPEN-CHAR and CLOSE-CHAR."
                     (= (count open-char template :start arg-start :end match-end)
                        (count close-char template :start arg-start :end match-end))))
              (when (or (= match-end (length template)) ;; we are at the end
                        (and (matchedp #\< #\>) (matchedp #\( #\))))
                (push (parse-cpp-type-declaration
                       ;; take the arg and omit final [,>]
                       (subseq template arg-start (1- match-end)))
                      type-args)
                (setf arg-start (1+ match-end)))))))
      (make-instance 'cpp-type
                     :ns (when (cdr namespace-split)
                           (butlast namespace-split))
                     :name name
                     :type-args (reverse type-args)))))

(defun cpp-type-decl (cpp-type &key (type-args t) (namespace t))
  "Return the fully qualified name of given CPP-TYPE."
  (declare (type cpp-type cpp-type))
  (flet ((enclosing-classes (cpp-type)
           (declare (type cpp-type cpp-type))
           (let (enclosing)
             (loop
                for class = cpp-type
                then (find-cpp-class (cpp-type-enclosing-class class))
                while class
                do (push (cpp-type-name class) enclosing))
             enclosing)))
    (with-output-to-string (s)
      (let ((ptr-pos (position #\* (cpp-type-name cpp-type))))
        (cond
          ((and ptr-pos (= 0 ptr-pos))
           ;; Special handle pointer
           (write-string (cpp-type-decl (car (cpp-type-type-args cpp-type))) s)
           (format s " ~A" (cpp-type-name cpp-type)))
          (t
           (when namespace
             (format s "~{~A::~}" (cpp-type-namespace cpp-type)))
           (format s "~{~A~^::~}" (enclosing-classes cpp-type))
           (when (and type-args (cpp-type-type-params cpp-type))
             ;; TODO: What about applied type args?
             (format s "<~{~A~^, ~}>" (mapcar #'cpp-type-name (cpp-type-type-params cpp-type))))))))))

(defclass cpp-enum (cpp-type)
  ((values :type list :initarg :values :initform nil :reader cpp-enum-values)
   ;; If true, generate the schema for this enum.
   (capnp-schema :type boolean :initarg :capnp-schema :initform nil
                 :reader cpp-enum-capnp-schema))
  (:documentation "Meta information on a C++ enum."))

(defstruct cpp-member
  "Meta information on a C++ class (or struct) member variable."
  (symbol nil :type symbol :read-only t)
  (type nil :type (or cpp-primitive-type-keywords string) :read-only t)
  (initarg nil :type symbol :read-only t)
  (initval nil :type (or null string integer float) :read-only t)
  (scope :private :type (member :public :protected :private) :read-only t)
  ;; TODO: Support giving a name for reader function.
  (reader nil :type boolean :read-only t)
  (documentation nil :type (or null string) :read-only t)
  ;; CAPNP-TYPE may be a string specifying the type, or a list of
  ;; (member-symbol "capnp-type") specifying a union type.
  (capnp-type nil :type (or null string list) :read-only t)
  (capnp-init t :type boolean :read-only t)
  ;; Custom saving and loading code. May be a function which takes 2
  ;; args: (builder-or-reader member-name) and needs to return C++ code.
  (capnp-save nil :type (or null function (eql :dont-save)) :read-only t)
  (capnp-load nil :type (or null function) :read-only t))

(defstruct capnp-opts
  "Cap'n Proto serialization options for C++ class."
  ;; BASE is T if the class should be treated as a base class for capnp, even
  ;; though it may have parents.
  (base nil :type boolean :read-only t)
  ;; Extra arguments to the generated save function. List of (name cpp-type).
  (save-args nil :read-only t)
  (load-args nil :read-only t)
  ;; Explicit instantiation of template to generate schema with enum.
  (type-args nil :read-only t)
  ;; In case of multiple inheritance, list of classes which should be handled
  ;; as a composition.
  (inherit-compose nil :read-only t))

(defclass cpp-class (cpp-type)
  ((structp :type boolean :initarg :structp :initform nil
            :reader cpp-class-structp)
   (super-classes :initarg :super-classes :initform nil
                  :reader cpp-class-super-classes)
   (members :initarg :members :initform nil :reader cpp-class-members)
   ;; Custom C++ code in 3 scopes. May be a list of C++ meta information or a
   ;; single element.
   (public :initarg :public :initform nil :reader cpp-class-public)
   (protected :initarg :protected :initform nil :reader cpp-class-protected)
   (private :initarg :private :initform nil :accessor cpp-class-private)
   (capnp-opts :type (or null capnp-opts) :initarg :capnp-opts :initform nil
               :reader cpp-class-capnp-opts)
   (inner-types :initarg :inner-types :initform nil :reader cpp-class-inner-types))
  (:documentation "Meta information on a C++ class (or struct)."))

(defvar *cpp-classes* nil "List of defined classes from LCP file")
(defvar *cpp-enums* nil "List of defined enums from LCP file")

(defun find-cpp-class (cpp-class-name)
  "Find CPP-CLASS in *CPP-CLASSES* by CPP-CLASS-NAME"
  (declare (type (or symbol string) cpp-class-name))
  ;; TODO: Find by full name
  (if (stringp cpp-class-name)
      (find cpp-class-name *cpp-classes* :key #'cpp-type-name :test #'string=)
      (find cpp-class-name *cpp-classes* :key #'cpp-type-base-name)))

(defun find-cpp-enum (cpp-enum-name)
  "Find CPP-ENUM in *CPP-ENUMS* by CPP-ENUM-NAME"
  (declare (type (or symbol string) cpp-enum-name))
  (if (stringp cpp-enum-name)
      (find cpp-enum-name *cpp-enums* :key #'cpp-type-name :test #'string=)
      (find cpp-enum-name *cpp-enums* :key #'cpp-type-base-name)))

(defun direct-subclasses-of (cpp-class)
  "Find direct subclasses of CPP-CLASS from *CPP-CLASSES*"
  (declare (type (or symbol cpp-class) cpp-class))
  (let ((name (if (symbolp cpp-class) cpp-class (cpp-type-base-name cpp-class))))
    (reverse ;; reverse to get them in definition order
     (remove-if (lambda (subclass)
                  (not (member name (cpp-class-super-classes subclass))))
                *cpp-classes*))))

(defun cpp-documentation (documentation)
  "Convert DOCUMENTATION to Doxygen style string."
  (declare (type string documentation))
  (format nil "/// ~A"
          (cl-ppcre:regex-replace-all
           (string #\Newline) documentation (format nil "~%/// "))))

(defun cpp-variable-name (symbol)
  "Get C++ style name of SYMBOL as a string."
  (declare (type (or string symbol) symbol))
  (cl-ppcre:regex-replace-all "-" (string-downcase symbol) "_"))

(defun cpp-constant-name (symbol)
  "Get C++ style constant name of SYMBOL as a string. This is like
`CPP-VARIABLE-NAME' but upcased."
  (declare (type (or string symbol) symbol))
  (cl-ppcre:regex-replace-all "-" (string-upcase symbol) "_"))

(defun cpp-member-name (cpp-member &key struct)
  "Get C++ style name of the `CPP-MEMBER' as a string."
  (declare (type cpp-member cpp-member)
           (type boolean struct))
  (let ((cpp-name (cpp-variable-name (cpp-member-symbol cpp-member))))
    (if struct cpp-name (format nil "~A_" cpp-name))))

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

(defun cpp-enum-definition (cpp-enum)
  "Get C++ style `CPP-ENUM' definition as a string."
  (declare (type cpp-enum cpp-enum))
  (with-output-to-string (s)
    (when (cpp-type-documentation cpp-enum)
      (write-line (cpp-documentation (cpp-type-documentation cpp-enum)) s))
    (with-cpp-block-output (s :name (format nil "enum class ~A" (cpp-type-name cpp-enum))
                              :semicolonp t)
      (format s "~{ ~A~^,~%~}~%" (mapcar #'cpp-constant-name (cpp-enum-values cpp-enum))))))

(defun cpp-member-declaration (cpp-member &key struct)
  "Get C++ style `CPP-MEMBER' declaration as a string."
  (declare (type cpp-member cpp-member)
           (type boolean struct))
  (let ((type-name (cpp-type-name (cpp-member-type cpp-member))))
    (with-output-to-string (s)
      (when (cpp-member-documentation cpp-member)
        (write-line (cpp-documentation (cpp-member-documentation cpp-member)) s))
      (if (cpp-member-initval cpp-member)
          (format s "~A ~A{~A};" type-name
                  (cpp-member-name cpp-member :struct struct) (cpp-member-initval cpp-member))
          (format s "~A ~A;" type-name (cpp-member-name cpp-member :struct struct))))))

(defun cpp-member-reader-definition (cpp-member)
  "Get C++ style `CPP-MEMBER' getter (reader) function."
  (declare (type cpp-member cpp-member))
  (if (typep (cpp-member-type cpp-member) 'cpp-primitive-type-keywords)
      (format nil "auto ~A() const { return ~A; }" (cpp-member-name cpp-member :struct t) (cpp-member-name cpp-member))
      (format nil "const auto &~A() const { return ~A; }" (cpp-member-name cpp-member :struct t) (cpp-member-name cpp-member))))

(defun cpp-template (type-params &optional stream)
  "Generate C++ template declaration from provided TYPE-PARAMS. If STREAM is
NIL, returns a string."
  (format stream "template <~{class ~A~^,~^ ~}>"
          (mapcar #'cpp-type-name type-params)))

(defun cpp-class-definition (cpp-class)
  "Get C++ definition of the CPP-CLASS as a string."
  (declare (type cpp-class cpp-class))
  (flet ((cpp-class-members-scoped (scope)
           (remove-if (lambda (m) (not (eq scope (cpp-member-scope m))))
                      (cpp-class-members cpp-class)))
         (member-declaration (member)
           (cpp-member-declaration member :struct (cpp-class-structp cpp-class))))
    (with-output-to-string (s)
      (terpri s)
      (when (cpp-type-documentation cpp-class)
        (write-line (cpp-documentation (cpp-type-documentation cpp-class)) s))
      (when (cpp-type-type-params cpp-class)
        (cpp-template (cpp-type-type-params cpp-class) s))
      (if (cpp-class-structp cpp-class)
          (write-string "struct " s)
          (write-string "class " s))
      (format s "~A" (cpp-type-name cpp-class))
      (when (cpp-class-super-classes cpp-class)
        (format s " : ~{public ~A~^, ~}"
                (mapcar #'cpp-type-name (cpp-class-super-classes cpp-class))))
      (with-cpp-block-output (s :semicolonp t)
        (let ((reader-members (remove-if (complement #'cpp-member-reader)
                                         (cpp-class-members cpp-class))))
          (when (or (cpp-class-public cpp-class) (cpp-class-members-scoped :public) reader-members)
            (unless (cpp-class-structp cpp-class)
              (write-line " public:" s))
            (format s "~{~A~%~}" (mapcar #'cpp-code (cpp-class-public cpp-class)))
            (format s "~{~%~A~}~%" (mapcar #'cpp-member-reader-definition reader-members))
            (format s "~{  ~%~A~}~%"
                    (mapcar #'member-declaration (cpp-class-members-scoped :public)))))
        (when (or (cpp-class-protected cpp-class) (cpp-class-members-scoped :protected))
          (write-line " protected:" s)
          (format s "~{~A~%~}" (mapcar #'cpp-code (cpp-class-protected cpp-class)))
          (format s "~{  ~%~A~}~%"
                  (mapcar #'member-declaration (cpp-class-members-scoped :protected))))
        (when (or (cpp-class-private cpp-class) (cpp-class-members-scoped :private))
          (write-line " private:" s)
          (format s "~{~A~%~}" (mapcar #'cpp-code (cpp-class-private cpp-class)))
          (format s "~{  ~%~A~}~%"
                  (mapcar #'member-declaration (cpp-class-members-scoped :private))))))))

(defun cpp-function-declaration (name &key args (returns "void") type-params)
  "Generate a C++ top level function declaration named NAME as a string.  ARGS
is a list of (variable type) function arguments. RETURNS is the return type of
the function.  TYPE-PARAMS is a list of names for template argments"
  (declare (type string name))
  (declare (type string returns))
  (let ((template (if type-params (cpp-template type-params) ""))
        (args (format nil "~:{~A ~A~:^, ~}"
                      (mapcar (lambda (name-and-type)
                                (list (cpp-type-name (second name-and-type))
                                      (cpp-variable-name (first name-and-type))))
                              args))))
    (raw-cpp-string
     #>cpp
     ${template}
     ${returns} ${name}(${args})
     cpp<#)))

(defun cpp-method-declaration (class method-name
                               &key args (returns "void") (inline t) static
                                 virtual const override)
  "Generate a C++ method declaration as a string for the given METHOD-NAME on
CLASS.  ARGS is a list of (variable type) arguments to method.  RETURNS is the
return type of the function.  When INLINE is set to NIL, generates a
declaration to be used outside of class definition.  Remaining keys are flags
which generate the corresponding C++ keywords."
  (declare (type cpp-class class)
           (type string method-name))
  (let* ((type-params (cpp-type-type-params class))
         (template (if (or inline (not type-params)) "" (cpp-template type-params)))
         (static/virtual (cond
                           ((and inline static) "static")
                           ((and inline virtual) "virtual")
                           (t "")))
         (namespace (if inline "" (format nil "~A::" (cpp-type-decl class :namespace nil))))
         (args (format nil "~:{~A ~A~:^, ~}"
                       (mapcar (lambda (name-and-type)
                                 (list (cpp-type-name (second name-and-type))
                                       (cpp-variable-name (first name-and-type))))
                               args)))
         (const (if const "const" ""))
         (override (if (and override inline) "override" "")))
    (raw-cpp-string
     #>cpp
     ${template} ${static/virtual}
     ${returns} ${namespace}${method-name}(${args}) ${const} ${override}
     cpp<#)))

(defstruct cpp-list
  values)

(defun cpp-list (&rest args)
  (make-cpp-list
   :values (remove-if (lambda (a)
                        (not (typep a '(or raw-cpp cpp-type cpp-list))))
                      args)))

(defun cpp-code (cpp)
  "Get a C++ string from given CPP meta information."
  (typecase cpp
    (raw-cpp (raw-cpp-string cpp))
    (cpp-class (cpp-class-definition cpp))
    (cpp-enum (cpp-enum-definition cpp))
    (string cpp)
    (cpp-list (format nil "~{~A~^~%~}" (mapcar #'cpp-code (cpp-list-values cpp))))
    (null "")
    (otherwise (error "Unknown conversion to C++ for ~S" (type-of cpp)))))

(defun count-newlines (stream &key stop-position)
  (loop for pos = (file-position stream)
     and char = (read-char stream nil nil)
     until (or (not char) (and stop-position (> pos stop-position)))
     when (char= #\Newline char) count it))

;;; Cap'n Proto schema and C++ serialization code generation

;;; Schema generation
;;;
;;; The basic algorthm should work as follows.
;;;   1) C++ class (or struct) is converted to the same named Capnp struct.
;;;   2) C++ class members are converted to camelCased members of Capnp struct.
;;;      a) Primitive C++ types are mapped to primitive Capnp types.
;;;      b) C++ std types are converted to our Capnp wrappers found in
;;;         utils/serialization.capnp. This process is hardcoded.
;;;      c) For other composite types, we assume that a Capnp struct exists
;;;         with the same PascalCase name as the *top-most* base class.
;;;      d) The user may provide a :CAPNP-TYPE string, which overrides our
;;;         decision making. Alternatively, a user may register the conversion
;;;         with `CAPNP-TYPE-CONVERSION'.
;;;   3) Handle C++ inheritance by checking direct subclasses of C++ class.
;;;      * Inheritance can be modeled with a union or composition.
;;;
;;;        Since with Capnp we cannot use the pointer casting trick from C
;;;        when modeling inheritance with composition, we are only left with
;;;        union. This is an ok solution for single inheritance trees, but C++
;;;        allows for multiple inheritance. This poses a problem.
;;;
;;;        Luckily, most of our use cases of multiple inheritance are only for
;;;        mixin classes to reuse some functionality. This allows us to model
;;;        multiple inheritance via composition. These classes need to be
;;;        marked as such for Capnp. Obviously, this precludes serialization
;;;        of pointers to mixin classes, but we should deal with that in C++
;;;        code on a case by case basis.
;;;
;;;        The algorithm is then the following.
;;;      a) If C++ class is the only parent of the direct sublcass, add the
;;;         direct subclass to union.
;;;      b) If C++ class isn't the only parent (multiple inheritance), then
;;;         check whether our class marked to be composed in the derived. If
;;;         yes, don't generate the union for direct subclass. Otherwise,
;;;         behave as 3.a)
;;;   4) Handle C++ template parameters of a class
;;;      Currently we require that explicit instantiations be listed in
;;;      TYPE-ARGS of `CAPNP-OPTS'. These arguments are then used to generate
;;;      a union, similarly to how inheritance is handled. This approach does
;;;      not support inheriting from template classes.

(defun capnp-union-subclasses (cpp-class)
  "Get direct subclasses of CPP-CLASS which should be modeled as a union in
Cap'n Proto schema."
  (declare (type (or symbol cpp-class) cpp-class))
  (let ((class-name (if (symbolp cpp-class) cpp-class (cpp-type-base-name cpp-class))))
    (remove-if (lambda (subclass)
                 (member class-name
                         (capnp-opts-inherit-compose (cpp-class-capnp-opts subclass))))
               (direct-subclasses-of cpp-class))))

(defun capnp-union-and-compose-parents (cpp-class)
  "Get direct parents of CPP-CLASS that model the inheritance via union. The
secondary value contains parents which are modeled as being composed inside
CPP-CLASS."
  (declare (type (or symbol cpp-class) cpp-class))
  (let* ((class (if (symbolp cpp-class) (find-cpp-class cpp-class) cpp-class))
         (capnp-opts (cpp-class-capnp-opts class))
         union compose)
    (when (not capnp-opts)
      (error "Class ~A should be marked for capnp serialization,
or its derived classes set as :CAPNP :BASE T" (cpp-type-base-name class)))
    (when (not (capnp-opts-base capnp-opts))
      (dolist (parent (cpp-class-super-classes class))
        (if (member parent (capnp-opts-inherit-compose capnp-opts))
            (push parent compose)
            (push parent union))))
    (values union compose)))

(defun capnp-union-parents-rec (cpp-class)
  "Return a list of all parent clases recursively for CPP-CLASS that should be
encoded as union inheritance in Cap'n Proto."
  (declare (type cpp-class cpp-class))
  (labels ((capnp-base-p (class)
             (declare (type cpp-class class))
             (let ((opts (cpp-class-capnp-opts class)))
               (and opts (capnp-opts-base opts))))
           (rec (class)
             (declare (type cpp-class class))
             (cons (cpp-type-base-name class)
                   ;; Continue to supers only if this isn't marked as capnp base class.
                   (when (and (not (capnp-base-p class))
                              (capnp-union-and-compose-parents class))
                     (let ((first-parent (find-cpp-class
                                          (first (capnp-union-and-compose-parents class)))))
                       (if first-parent
                           (rec first-parent)
                           (list (first (capnp-union-and-compose-parents class)))))))))
    (cdr (rec cpp-class))))

(defvar *capnp-serialize-p* nil
  "True if we should generate Cap'n Proto serialization code")

(defvar *capnp-type-converters* nil
  "Pairs of (cpp-type capnp-type) which map the conversion of C++ types to
  Cap'n Proto types.")

(defun capnp-type-conversion (cpp-type capnp-type)
  (declare (type string cpp-type capnp-type))
  (push (cons cpp-type capnp-type) *capnp-type-converters*))

(defun capnp-type<-cpp-type (cpp-type &key boxp)
  (flet ((convert-primitive-type (name)
           (when (member name '(:int :uint))
             (error "Unable to get Capnp type for integer without specified width."))
           (let ((capnp-type
                  (case name
                    (:bool "Bool")
                    (:float "Float32")
                    (:double "Float64")
                    (otherwise
                     (let ((pos-of-i (position #\I (string name))))
                       ;; Delete the _t suffix
                       (cl-ppcre:regex-replace
                        "_t$" (string-downcase name :start (1+ pos-of-i)) ""))))))
             (if boxp
                 (concatenate 'string "Utils.Box" capnp-type)
                 capnp-type))))
    (typecase cpp-type
      (cpp-primitive-type-keywords (convert-primitive-type cpp-type))
      (cpp-primitive-type (convert-primitive-type (cpp-type-base-name cpp-type)))
      (string
       (let ((type (parse-cpp-type-declaration cpp-type)))
         (cond
           ((typep type 'cpp-primitive-type)
            (convert-primitive-type (cpp-type-base-name type)))
           ((string= "string" (cpp-type-base-name type))
            "Text")
           ((string= "shared_ptr" (cpp-type-base-name type))
            (let ((class (find-cpp-class
                          ;; TODO: Use full type
                          (cpp-type-base-name (first (cpp-type-type-args type))))))
              (unless class
                (error "Unable to determine base type for '~A'; use :capnp-type"
                       cpp-type))
              (let* ((parents (capnp-union-parents-rec class))
                     (top-parent (if parents (car (last parents)) (cpp-type-base-name class))))
                (format nil "Utils.SharedPtr(~A)" (cpp-type-name top-parent)))))
           ((string= "vector" (cpp-type-base-name type))
            (format nil "List(~A)"
                    (capnp-type<-cpp-type
                     (cpp-type-decl (first (cpp-type-type-args type))))))
           ((string= "optional" (cpp-type-base-name type))
            (format nil "Utils.Optional(~A)"
                    (capnp-type<-cpp-type (cpp-type-decl (first (cpp-type-type-args type)))
                                          :boxp t)))
           ((assoc cpp-type *capnp-type-converters* :test #'string=)
            (cdr (assoc cpp-type *capnp-type-converters* :test #'string=)))
           (t (cpp-type-name cpp-type)))))
      ;; Capnp only accepts uppercase first letter in types (PascalCase), so
      ;; this is the same as our conversion to C++ type name.
      (otherwise (cpp-type-name cpp-type)))))

(defun capnp-type-of-member (member)
  (declare (type cpp-member member))
  (if (cpp-member-capnp-type member)
      (cpp-member-capnp-type member)
      (capnp-type<-cpp-type (cpp-member-type member))))

(defun capnp-primitive-type-p (capnp-type)
  (declare (type (or list string) capnp-type))
  (and (stringp capnp-type)
       (member capnp-type
               '("Bool"
                 "Int8" "Int16" "Int32" "Int64"
                 "UInt8" "UInt16" "UInt32" "UInt64"
                 "Float32" "Float64"
                 "Text" "Void")
               :test #'string=)))

(defun capnp-schema-for-enum (cpp-enum)
  "Generate Cap'n Proto serialization schema for CPP-ENUM"
  (declare (type cpp-enum cpp-enum))
  (with-output-to-string (s)
    (with-cpp-block-output (s :name (format nil "enum ~A" (cpp-type-name cpp-enum)))
      (loop for val in (cpp-enum-values cpp-enum) and field-number from 0
         do (format s "  ~A @~A;~%"
                    (string-downcase (cpp-type-name val) :end 1)
                    field-number)))))

(defun capnp-schema (cpp-class)
  "Generate Cap'n Proto serialiation schema for CPP-CLASS"
  (declare (type (or cpp-class cpp-enum symbol) cpp-class))
  (when (null cpp-class)
    (return-from capnp-schema))
  (when (typep cpp-class 'cpp-enum)
    (return-from capnp-schema (capnp-schema-for-enum cpp-class)))
  (let ((class-name (if (symbolp cpp-class) cpp-class (cpp-type-base-name cpp-class)))
        (members (when (typep cpp-class 'cpp-class) (cpp-class-members cpp-class)))
        (inner-types (when (typep cpp-class 'cpp-class) (cpp-class-inner-types cpp-class)))
        (union-subclasses (capnp-union-subclasses cpp-class))
        (type-params (when (typep cpp-class 'cpp-class) (cpp-type-type-params cpp-class)))
        (capnp-type-args (when (typep cpp-class 'cpp-class)
                           (capnp-opts-type-args (cpp-class-capnp-opts cpp-class))))
        (field-number 0))
    (when (and type-params (not capnp-type-args))
      (error "Don't know how to create schema for template class '~A'" class-name))
    (when (and capnp-type-args union-subclasses)
      (error "Don't know how to handle templates and inheritance of ~A" class-name))
    (flet ((field-name<-symbol (symbol)
             "Get Capnp compatible field name (camelCase)."
             (string-downcase (cpp-type-name symbol) :end 1)))
      (multiple-value-bind (union-parents compose-parents)
          (capnp-union-and-compose-parents cpp-class)
        (when (> (list-length union-parents) 1)
          (error "Class ~A has multiple inheritance. Use :inherit-compose for
          remaining parents." class-name))
        (with-output-to-string (s)
          (with-cpp-block-output (s :name (format nil "struct ~A" (cpp-type-name class-name)))
            (dolist (compose compose-parents)
              (format s "  ~A @~A :~A;~%"
                      (field-name<-symbol compose)
                      field-number
                      (capnp-type<-cpp-type compose))
              (incf field-number))
            (when capnp-type-args
              (with-cpp-block-output (s :name "union")
                (dolist (type-arg capnp-type-args)
                  (format s "  ~A @~A :Void;~%" (field-name<-symbol type-arg) field-number)
                  (incf field-number))))
            (dolist (member members)
              (unless (eq :dont-save (cpp-member-capnp-save member))
                (let ((capnp-type (capnp-type-of-member member))
                      (field-name (field-name<-symbol (cpp-member-symbol member))))
                  (if (stringp capnp-type)
                      (progn
                        (format s "  ~A @~A :~A;~%"
                                field-name field-number capnp-type)
                        (incf field-number))
                      ;; capnp-type is a list specifying a union type
                      (progn
                        (with-cpp-block-output (s :name (format nil "  ~A :union" field-name))
                          (dolist (union-member capnp-type)
                            (format s "    ~A @~A :~A;~%"
                                    (field-name<-symbol (first union-member))
                                    field-number (second union-member))
                            (incf field-number))))))))
            (dolist (inner inner-types)
              (when (or (and (typep inner 'cpp-class) (cpp-class-capnp-opts inner))
                        (and (typep inner 'cpp-enum) (cpp-enum-capnp-schema inner)))
                (write-line (capnp-schema inner) s)))
            (when union-subclasses
              (with-cpp-block-output (s :name "union")
                (when union-parents
                  ;; Allow instantiating classes in the middle of inheritance
                  ;; hierarchy.
                  (format s "    ~A @~A :Void;~%"
                          (field-name<-symbol class-name) field-number)
                  (incf field-number))
                (dolist (subclass union-subclasses)
                  (format s "    ~A @~A :~A;~%"
                          (field-name<-symbol (cpp-type-base-name subclass))
                          field-number
                          (capnp-type<-cpp-type (cpp-type-base-name subclass)))
                  (incf field-number))))))))))

;;; Capnp C++ serialization code generation
;;;
;;; Algorithm is closely tied with the generated schema (see above).
;;;
;;; 1) Generate the function declaration.
;;;
;;; We are using top level functions, so that we can easily decouple the
;;; serialization code from class definitions.  This requires the class to
;;; have public access to its serializable fields.
;;;
;;; Two problems arise:
;;;
;;;  * inheritance and
;;;  * helper arguments (for tracking pointers or similar).
;;;
;;; The function will always take a `const &T` and a pointer to a
;;; `capnp::<T>::Builder` class.  Additional arguments are optional, and are
;;; supplied when declaring that the class should be serialized with capnp.
;;;
;;; To determine the concrete T we need to know whether this class is a
;;; derived one or is inherited from.  If it is, then T needs to be the
;;; top-most parent that is modeled by union and not composition.  (For the
;;; inheritance modeling problem, refer to the description of schema
;;; generation.)  Since we opted for using top level functions, we cannot use
;;; virtual call dispatch to get the concrete type.  (We could use the visitor
;;; pattern, but that introduces the coupling we are avoiding with regular
;;; functions.)  Therefore, we use dynamic_cast in functions to determine the
;;; concrete serialization code.  If this class has no inheritance in any
;;; direction, then we just serialize T to its corresponding capnp::T schema
;;; type.
;;;
;;; Helper arguments are obtained from SAVE-ARGS of `CAPNP-OPTS'.
;;;
;;; 2) Generate parent calls for serialization (if we have parent classes).
;;;
;;; For the first (and only) parent which is modeled through union, generate
;;; the parent serialization code.  This is done recursively for each union
;;; parent.  The generated code sees all of the arguments from our function
;;; declaration.
;;;
;;; Then, find our own concrete builder by traversing through the union schema
;;; of the base builder.  It is expected (and required) that the parent code
;;; has initialized them correctly.  We just need to initialize the most
;;; concrete builder.
;;;
;;; Other parents are required to be modeled through composition.  Therefore,
;;; we generate calls to parents by passing builders for the composed structs.
;;;
;;;     auto parent_builder = builder->initParent();
;;;     // Parent Save code
;;;
;;; Any additional helper arguments are also visited in the generated code.
;;;
;;; 3) Generate member serialization.
;;;
;;; For primitive typed members, generate `builder->setMember(member);` calls.
;;;
;;; For `std` types, generate hard-coded calls to our wrapper functions.  Most
;;; of these require a lambda function which serializes the element inside the
;;; `std` class.  This can be done recursively with this step.
;;;
;;; For composite types, check whether we have been given a custom save
;;; invocation.  If not, assume that the type has an accompanying function
;;; called `Save` which expects an instance of that type and a builder for it,
;;; as well as any additional helper arguments.

(defun capnp-extra-args (cpp-class save-or-load)
  "Get additional arguments to Save/Load function for CPP-CLASS."
  (declare (type cpp-class cpp-class)
           (type (member :save :load) save-or-load))
  (loop for parent in (cons (cpp-type-base-name cpp-class) (capnp-union-parents-rec cpp-class))
     for opts = (cpp-class-capnp-opts (find-cpp-class parent))
     for args = (ecase save-or-load
                  (:save (capnp-opts-save-args opts))
                  (:load (capnp-opts-load-args opts)))
     when args return args))

(defun capnp-save-function-declaration (cpp-class)
  "Generate Cap'n Proto save function declaration for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (let* ((parents (capnp-union-parents-rec cpp-class))
         (top-parent-class
          (if parents
              (cpp-type-decl (find-cpp-class (car (last parents))) :type-args nil :namespace nil)
              (cpp-type-decl cpp-class :type-args nil :namespace nil)))
         (self-arg
          (list 'self (format nil "const ~A &"
                              (cpp-type-decl cpp-class :namespace nil))))
         (builder-arg
          (list (if parents 'base-builder 'builder)
                (format nil "capnp::~A::Builder *" top-parent-class))))
    (cpp-function-declaration
     "Save"
     :args (cons self-arg
                 (cons builder-arg (capnp-extra-args cpp-class :save)))
     :type-params (cpp-type-type-params cpp-class))))

(defun capnp-cpp-type<-cpp-type (cpp-type &key boxp)
  (declare (type cpp-type cpp-type))
  (when (cpp-type-type-params cpp-type)
    (error "Don't know how to convert '~A' to capnp equivalent"
           (cpp-type-decl cpp-type)))
  (let ((name (cpp-type-base-name cpp-type))
        (namespace (cpp-type-namespace cpp-type)))
    (cond
      ((and boxp (typep cpp-type 'cpp-primitive-type))
       (setf name (concatenate 'string "Box" (capnp-type<-cpp-type cpp-type))
             namespace '("utils" "capnp")))
      ((string= "string" (cpp-type-base-name cpp-type))
       (setf name "Text"
             namespace '("capnp")))
      (t ;; Just append capnp as final namespace
       (setf namespace (append namespace '("capnp")))))
    (make-instance 'cpp-type
                   :name name :namespace namespace
                   :enclosing-class (cpp-type-enclosing-class cpp-type))))

(defun capnp-save-default (member-name member-type member-builder capnp-name)
  "Generate the default call to save for member.  MEMBER-NAME and MEMBER-TYPE
are strings describing the member being serialized.  MEMBER-BUILDER is the
name of the builder variable.  CAPNP-NAME is the name of the member in Cap'n
Proto schema."
  (declare (type string member-name member-type member-builder capnp-name))
  (let* ((type (parse-cpp-type-declaration member-type))
         (type-name (cpp-type-base-name type))
         (cpp-enum (find-cpp-enum member-type)))
    (cond
      (cpp-enum
       (funcall
        (capnp-save-enum (cpp-type-decl (capnp-cpp-type<-cpp-type cpp-enum))
                         (cpp-type-decl cpp-enum) (cpp-enum-values cpp-enum))
        member-builder member-name capnp-name))
      ((string= "vector" type-name)
       (let* ((elem-type (car (cpp-type-type-args type)))
              (capnp-cpp-type (capnp-cpp-type<-cpp-type elem-type)))
         (if (capnp-primitive-type-p (capnp-type<-cpp-type (cpp-type-base-name elem-type)))
             (raw-cpp-string
              #>cpp
              utils::SaveVector(${member-name}, &${member-builder});
              cpp<#)
             (raw-cpp-string
              (funcall (capnp-save-vector (cpp-type-decl capnp-cpp-type) (cpp-type-decl elem-type))
                       member-builder member-name capnp-name)))))
      ((string= "optional" type-name)
       (let* ((elem-type (car (cpp-type-type-args type)))
              (capnp-cpp-type (capnp-cpp-type<-cpp-type elem-type :boxp t))
              (lambda-code (when (typep elem-type 'cpp-primitive-type)
                             "[](auto *builder, const auto &v){ builder->setValue(v); }")))
         (raw-cpp-string
          (funcall (capnp-save-optional
                    (cpp-type-decl capnp-cpp-type) (cpp-type-decl elem-type) lambda-code)
                   member-builder member-name capnp-name))))
      ((member type-name '("unique_ptr" "shared_ptr" "vector") :test #'string=)
       (error "Use a custom :capnp-save function for ~A ~A" type-name member-name))
      (t
       (let* ((cpp-class (find-cpp-class type-name)) ;; TODO: full type-name search
              (extra-args (when cpp-class
                            (mapcar (lambda (name-and-type)
                                      (cpp-variable-name (first name-and-type)))
                                    (capnp-extra-args cpp-class :save)))))
         (format nil "Save(~A, &~A~{, ~A~});"
                 member-name member-builder extra-args))))))

(defun capnp-save-members (cpp-class &key instance-access)
  "Generate Cap'n Proto saving code for members of CPP-CLASS.  INSTANCE-ACCESS
  is a C++ string which is prefixed to member access.  For example,
  INSTANCE-ACCESS could be `my_struct->`"
  (declare (type cpp-class cpp-class))
  (declare (type string instance-access))
  (with-output-to-string (s)
    (dolist (member (cpp-class-members cpp-class))
      (unless (eq :dont-save (cpp-member-capnp-save member))
        (let ((member-access
               (concatenate 'string instance-access
                            (if (eq :public (cpp-member-scope member))
                                (cpp-member-name member :struct (cpp-class-structp cpp-class))
                                (format nil "~A()" (cpp-member-name member :struct t)))))
              (member-builder (format nil "~A_builder" (cpp-member-name member :struct t)))
              (capnp-name (cpp-type-name (cpp-member-symbol member))))
          (cond
            ((and (not (cpp-member-capnp-save member))
                  (capnp-primitive-type-p (capnp-type-of-member member)))
             (format s "  builder->set~A(~A);~%" capnp-name member-access))
            (t
             ;; Enclose larger save code in new scope
             (with-cpp-block-output (s)
               (let ((size (if (string= "vector" (cpp-type-base-name
                                                  (parse-cpp-type-declaration
                                                   (cpp-member-type member))))
                               (format nil "~A.size()" member-access)
                               "")))
                 (if (and (cpp-member-capnp-init member)
                          (not (find-cpp-enum (cpp-member-type member))))
                     (format s "  auto ~A = builder->init~A(~A);~%"
                             member-builder capnp-name size)
                     (setf member-builder "builder")))
               (if (cpp-member-capnp-save member)
                   (format s "  ~A~%"
                           (cpp-code (funcall (cpp-member-capnp-save member)
                                              member-builder member-access capnp-name)))
                   (write-line (capnp-save-default member-access (cpp-member-type member)
                                                   member-builder capnp-name)
                               s))))))))))

(defun capnp-save-parents (cpp-class save-parent)
  "Generate Cap'n Proto code for serializing parent classes of CPP-CLASS.
SAVE-PARENT is a function which generates the code for serializing a parent.
It takes a parent class symbol."
  (declare (type cpp-class cpp-class))
  (declare (type (function (symbol) string) save-parent))
  (multiple-value-bind (direct-union-parents compose-parents)
      (capnp-union-and-compose-parents cpp-class)
    (declare (ignore direct-union-parents))
    ;; Handle the union inheritance calls first.
    (with-output-to-string (s)
      (let ((parents (capnp-union-parents-rec cpp-class)))
        (when parents
          (let ((first-parent (first parents)))
            (write-line (funcall save-parent first-parent) s))
          (if (or compose-parents (cpp-class-members cpp-class))
              (progn
                (format s "  auto ~A_builder = base_builder->~{get~A().~}init~A();~%"
                        (cpp-variable-name (cpp-type-base-name cpp-class))
                        (mapcar #'cpp-type-name (cdr (reverse parents)))
                        (cpp-type-name cpp-class))
                (format s "  auto *builder = &~A_builder;~%"
                        (cpp-variable-name (cpp-type-base-name cpp-class))))
              (format s "  base_builder->~{get~A().~}init~A();~%"
                      (mapcar #'cpp-type-name (cdr (reverse parents)))
                      (cpp-type-name cpp-class)))
          (when (capnp-union-subclasses cpp-class)
            ;; We are in the middle of inheritance hierarchy, so set our
            ;; union Void field.
            (format s "  builder->set~A();" (cpp-type-name cpp-class)))))
      ;; Now handle composite inheritance calls.
      (dolist (parent compose-parents)
        (with-cpp-block-output (s)
          (let* ((builder (format nil "~A_builder" (cpp-variable-name parent))))
            (format s "  auto ~A = builder->init~A();~%" builder (cpp-type-name parent))
            (format s "  auto *builder = &~A;~%" builder)
            (write-line (funcall save-parent parent) s)))))))

(defun capnp-save-function-code (cpp-class)
  "Generate Cap'n Proto save code for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (labels ((save-class (cpp-class cpp-out)
             "Output the serialization code for members of this and parent class."
             (write-line (capnp-save-parents cpp-class #'save-parent) cpp-out)
             ;; Set the template instantiations
             (when (and (capnp-opts-type-args (cpp-class-capnp-opts cpp-class))
                        (/= 1 (list-length (cpp-type-type-params cpp-class))))
               (error "Don't know how to save templated class ~A" (cpp-type-base-name cpp-class)))
             (let ((type-param (first (mapcar #'cpp-type-name (cpp-type-type-params cpp-class)))))
               (dolist (type-arg (mapcar #'cpp-type-name
                                         (capnp-opts-type-args (cpp-class-capnp-opts cpp-class))))
                 (write-string
                  (raw-cpp-string
                   #>cpp
                   if (std::is_same<${type-arg}, ${type-param}>::value) {
                   builder->set${type-arg}();
                   }
                   cpp<#)
                  cpp-out)))
             (write-line (capnp-save-members cpp-class :instance-access "self.") cpp-out))
           (save-parent (parent)
             "Generate serialization code for parent class."
             (let ((cpp-class (find-cpp-class parent)))
               (with-output-to-string (s)
                 (with-cpp-block-output (s)
                   (format s "// Save base class ~A~%" (cpp-type-name parent))
                   (save-class cpp-class s))))))
    (with-output-to-string (cpp-out)
      (let ((subclasses (direct-subclasses-of cpp-class)))
        (when subclasses
          (write-line "// Forward serialization to most derived type" cpp-out)
          (dolist (subclass subclasses)
            (let ((derived-name (cpp-type-name subclass))
                  (save-args
                   (format nil "~A~{, ~A~}"
                           (if (capnp-union-parents-rec cpp-class)
                               "base_builder"
                               "builder")
                           (mapcar (lambda (name-and-type)
                                     (cpp-variable-name (first name-and-type)))
                                   (capnp-extra-args cpp-class :save))))
                  (type-args (capnp-opts-type-args (cpp-class-capnp-opts subclass))))
              (if type-args
                  ;; Handle template instantiation
                  (dolist (type-arg (mapcar #'cpp-type-name type-args))
                    (write-string
                     (raw-cpp-string
                      #>cpp
                      if (const auto *derived = dynamic_cast<const ${derived-name}<${type-arg}> *>(&self)) {
                      return Save(*derived, ${save-args});
                      }
                      cpp<#)
                     cpp-out))
                  ;; Just forward the serialization normally.
                  (write-string
                   (raw-cpp-string
                    #>cpp
                    if (const auto *derived = dynamic_cast<const ${derived-name} *>(&self)) {
                    return Save(*derived, ${save-args});
                    }
                    cpp<#)
                   cpp-out))))))
      (save-class cpp-class cpp-out))))

(defun capnp-save-function-definition (cpp-class)
  "Generate Cap'n Proto save function."
  (declare (type cpp-class cpp-class))
  (with-output-to-string (cpp-out)
    (with-cpp-block-output (cpp-out :name (capnp-save-function-declaration cpp-class))
      (write-line (capnp-save-function-code cpp-class) cpp-out))))

;;; Capnp C++ deserialization code generation
;;;
;;; This is almost the same as serialization, but with a special case for
;;; handling serialized pointers to base classes.
;;;
;;; The usual function signature for data types with no inheritance is:
;;;
;;;   void Load(Data *self, const capnp::Data::Reader &, <extra args>)
;;;
;;; The function expects that the `Data` type is already allocated and can be
;;; modified via pointer.  This way the user can allocate `Data` any way they
;;; see fit.
;;;
;;; With inheritance, the user doesn't know the concrete type to allocate.
;;; Therefore, the signature is changed so that the `Load` can heap allocate
;;; and construct the correct type.  The downside of this approach is that the
;;; user of the `Load` function has no control over the allocation.  The
;;; signature for loading types with inheritance is:
;;;
;;;   void Load(std::unique_ptr<Data> *self, const capnp::Data::Reader &,
;;;             <extra args>)
;;;
;;; The user can now only provide a pointer to unique_ptr which should take
;;; the ownership of the concrete type.

(defun capnp-load-function-declaration (cpp-class)
  "Generate Cap'n Proto load function declaration for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (let* ((parents (capnp-union-parents-rec cpp-class))
         (top-parent-class (if parents
                               (cpp-type-decl (find-cpp-class (car (last parents))) :type-args nil :namespace nil)
                               (cpp-type-decl cpp-class :type-args nil :namespace nil)))
         (reader-arg (list (if (or parents (capnp-union-subclasses cpp-class))
                               'base-reader
                               'reader)
                           (format nil "const capnp::~A::Reader &" top-parent-class)))
         (out-arg (list 'self
                        (if (or parents (direct-subclasses-of cpp-class))
                            (format nil "std::unique_ptr<~A> *" (cpp-type-decl cpp-class :namespace nil))
                            (format nil "~A *" (cpp-type-decl cpp-class :namespace nil))))))
    (cpp-function-declaration
     "Load"
     :args (cons out-arg (cons reader-arg (capnp-extra-args cpp-class :load)))
     :returns "void"
     :type-params (cpp-type-type-params cpp-class))))

(defun capnp-load-default (member-name member-type member-reader capnp-name)
  "Generate default load call for member.  MEMBER-NAME and MEMBER-TYPE are
strings describing the member being loaded.  MEMBER-READER is the name of the
reader variable.  CAPNP-NAME is the name of the member in Cap'n Proto schema."
  (declare (type string member-name member-type member-reader))
  (let* ((type (parse-cpp-type-declaration member-type))
         (type-name (cpp-type-base-name type))
         (cpp-enum (find-cpp-enum member-type)))
    (cond
      (cpp-enum
       (funcall
        (capnp-load-enum (cpp-type-decl (capnp-cpp-type<-cpp-type cpp-enum))
                         (cpp-type-decl cpp-enum) (cpp-enum-values cpp-enum))
        member-reader member-name capnp-name))
      ((string= "vector" type-name)
       (let* ((elem-type (car (cpp-type-type-args type)))
              (capnp-cpp-type (capnp-cpp-type<-cpp-type elem-type)))
         (if (capnp-primitive-type-p (capnp-type<-cpp-type (cpp-type-base-name elem-type)))
             (raw-cpp-string
              #>cpp
              utils::LoadVector(&${member-name}, ${member-reader});
              cpp<#)
             (raw-cpp-string
              (funcall (capnp-load-vector (cpp-type-decl capnp-cpp-type) (cpp-type-decl elem-type))
                       member-reader member-name capnp-name)))))
      ((string= "optional" type-name)
       (let* ((elem-type (car (cpp-type-type-args type)))
              (capnp-cpp-type (capnp-cpp-type<-cpp-type elem-type :boxp t))
              (lambda-code (when (string= "Box" (cpp-type-name capnp-cpp-type) :end2 (length "Box"))
                             "[](const auto &reader){ return reader.getValue(); }")))
         (raw-cpp-string
          (funcall (capnp-load-optional
                    (cpp-type-decl capnp-cpp-type) (cpp-type-decl elem-type) lambda-code)
                   member-reader member-name capnp-name))))
      ((member type-name '("unique_ptr" "shared_ptr" "vector") :test #'string=)
       (error "Use a custom :capnp-load function for ~A ~A" type-name member-name))
      (t
       (let* ((cpp-class (find-cpp-class type-name)) ;; TODO: full type-name search
              (extra-args (when cpp-class
                            (mapcar (lambda (name-and-type)
                                      (cpp-variable-name (first name-and-type)))
                                    (capnp-extra-args cpp-class :load)))))
         (format nil "Load(&~A, ~A~{, ~A~});"
                 member-name member-reader extra-args))))))

(defun capnp-load-members (cpp-class &key instance-access)
  "Generate Cap'n Proto loading code for members of CPP-CLASS.
INSTANCE-ACCESS is a C++ string which will be prefixed to member access.  For
example, INSTANCE-ACCESS could be `my_struct->`"
  (declare (type cpp-class cpp-class))
  (declare (type string instance-access))
  (with-output-to-string (s)
    (dolist (member (cpp-class-members cpp-class))
      (unless (and (eq :dont-save (cpp-member-capnp-save member))
                   (not (cpp-member-capnp-load member)))
        (let ((member-access
               (concatenate 'string instance-access
                            (cpp-member-name member :struct (cpp-class-structp cpp-class))))
              (member-reader (format nil "~A_reader" (cpp-member-name member :struct t)))
              (capnp-name (cpp-type-name (cpp-member-symbol member))))
          (cond
            ((and (not (cpp-member-capnp-load member))
                  (capnp-primitive-type-p (capnp-type-of-member member)))
             (format s "  ~A = reader.get~A();~%" member-access capnp-name))
            (t
             ;; Enclose larger load code in new scope
             (with-cpp-block-output (s)
               (if (and (cpp-member-capnp-init member)
                        (not (find-cpp-enum (cpp-member-type member))))
                   (format s "  auto ~A = reader.get~A();~%" member-reader capnp-name)
                   (setf member-reader "reader"))
               (if (cpp-member-capnp-load member)
                   (format s "  ~A~%"
                           (cpp-code (funcall (cpp-member-capnp-load member)
                                              member-reader member-access capnp-name)))
                   (write-line (capnp-load-default member-access
                                                   (cpp-member-type member)
                                                   member-reader capnp-name) s))))))))))

(defun capnp-load-parents (cpp-class load-parent)
  "Generate Cap'n Proto code for loading parent classes of CPP-CLASS.
LOAD-PARENT is a function which generates the code for loading parent members.
It takes a parent class symbol."
  (declare (type cpp-class cpp-class))
  (declare (type (function (symbol) string) load-parent))
  (with-output-to-string (s)
    (multiple-value-bind (direct-union-parents compose-parents)
        (capnp-union-and-compose-parents cpp-class)
      (declare (ignore direct-union-parents))
      ;; Handle the union inheritance calls first.
      (let ((parents (capnp-union-parents-rec cpp-class)))
        (when parents
          (let ((first-parent (first parents)))
            (write-line (funcall load-parent first-parent) s))
          (when (or compose-parents (cpp-class-members cpp-class))
            (format s "  auto reader = base_reader.~{get~A().~}get~A();~%"
                    (mapcar #'cpp-type-name (cdr (reverse parents)))
                    (cpp-type-name cpp-class)))
          ;; Now handle composite inheritance calls.
          (dolist (parent compose-parents)
            (with-cpp-block-output (s)
              (let ((reader (format nil "~A_reader" (cpp-variable-name parent))))
                (format s "  auto ~A = reader.get~A();~%" reader (cpp-type-name parent))
                (format s "  const auto &reader = ~A;" reader)
                (write-line (funcall load-parent parent) s)))))))))

(defun capnp-load-function-code (cpp-class)
  "Generate Cap'n Proto loadd code for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (let ((instance-access (if (or (direct-subclasses-of cpp-class)
                                 (capnp-union-parents-rec cpp-class))
                             "self->get()->"
                             "self->")))
    (labels ((load-class (cpp-class)
               (with-output-to-string (s)
                 (write-line (capnp-load-parents cpp-class #'load-parent) s)
                 (write-line (capnp-load-members cpp-class :instance-access instance-access) s)))
             (load-parent (parent)
               (with-output-to-string (s)
                 (with-cpp-block-output (s)
                   (format s "// Load base class ~A~%" (cpp-type-name parent))
                   (write-line (load-class (find-cpp-class parent)) s)))))
      (with-output-to-string (s)
        (cond
          ((and (capnp-union-and-compose-parents cpp-class)
                (not (direct-subclasses-of cpp-class)))
           ;; CPP-CLASS is the most derived class, so construct and load.
           (format s "*self = std::make_unique<~A>();~%" (cpp-type-decl cpp-class :namespace nil))
           (write-line (load-class cpp-class) s))
          ((capnp-union-subclasses cpp-class)
           ;; Forward the load to most derived class by switching on reader.which()
           (let ((parents (capnp-union-parents-rec cpp-class)))
             (if parents
                 (format s "switch (base_reader.~{get~A().~}get~A().which()) "
                         (mapcar #'cpp-type-name (cdr (reverse parents)))
                         (cpp-type-name cpp-class))
                 (write-string "switch (base_reader.which()) " s)))
           (with-cpp-block-output (s)
             (dolist (subclass (capnp-union-subclasses cpp-class))
               (format s "  case capnp::~A::~A: "
                       (cpp-type-name cpp-class)
                       (cpp-constant-name (cpp-type-base-name subclass)))
               (flet ((load-derived (derived-type-name)
                        (with-cpp-block-output (s)
                          (format s "std::unique_ptr<~A> derived;~%" derived-type-name)
                          (format s "Load(&derived, base_reader~{, ~A~});~%"
                                  (mapcar (lambda (name-and-type)
                                            (cpp-variable-name (first name-and-type)))
                                          (capnp-extra-args cpp-class :load)))
                          (write-line "*self = std::move(derived);" s))))
                 (if (capnp-opts-type-args (cpp-class-capnp-opts subclass))
                     ;; Handle template instantiation
                     (progn
                       (format s "switch (base_reader.get~A().which()) " (cpp-type-name subclass))
                       (with-cpp-block-output (s)
                         (dolist (type-arg (capnp-opts-type-args (cpp-class-capnp-opts subclass)))
                           (format s "  case capnp::~A::~A: "
                                   (cpp-type-name subclass) (cpp-constant-name type-arg))
                           (load-derived (format nil "~A<~A>"
                                                 (cpp-type-name subclass)
                                                 (cpp-type-name type-arg))))))
                     ;; Regular forward to derived
                     (load-derived (cpp-type-name subclass))))
               (write-line "break;" s))
             (when (capnp-union-and-compose-parents cpp-class)
               ;; We are in the middle of the hierarchy, so allow constructing and loading us.
               (with-cpp-block-output (s :name (format nil "case capnp::~A::~A:"
                                                       (cpp-type-name cpp-class)
                                                       (cpp-constant-name (cpp-type-base-name cpp-class))))
                 (format s "*self = std::make_unique<~A>();~%"
                         (cpp-type-decl cpp-class :namespace nil))
                 (write-line (load-class cpp-class) s)))))
          (t
           ;; Regular load for absolutely no inheritance class
           (assert (not (capnp-union-subclasses cpp-class)))
           (assert (not (capnp-union-and-compose-parents cpp-class)))
           (write-line (load-class cpp-class) s)))))))

(defun capnp-load-function-definition (cpp-class)
  "Generate Cap'n Proto load function."
  (declare (type cpp-class cpp-class))
  (with-output-to-string (cpp-out)
    (with-cpp-block-output (cpp-out :name (capnp-load-function-declaration cpp-class))
      (write-line (capnp-load-function-code cpp-class) cpp-out))))

(defvar *capnp-imports* nil
  "List of pairs (namespace, import-file), which will be imported in Cap'n
  Proto schema with the syntax 'using Namespace = import import-file'")

(defun capnp-import (namespace import-file)
  "Import the IMPORT-FILE to Cap'n Proto aliased using NAMESPACE."
  (declare (type symbol namespace)
           (type string import-file))
  (push (cons namespace import-file) *capnp-imports*))

(defvar *capnp-namespace* nil
  "Name of the namespace where Cap'n Proto generated C++ will be.")

(defun capnp-namespace (namespace)
  "Set the Cap'n Proto generated c++ namespace."
  (declare (type string namespace))
  (setf *capnp-namespace* namespace))

(defun capnp-save-optional (capnp-type cpp-type &optional lambda-code)
  "Generate the C++ code calling utils::SaveOptional. CAPNP-TYPE and CPP-TYPE
are passed as template parameters, while the optional LAMBDA-CODE is used to
save the value inside the std::optional."
  (declare (type string capnp-type cpp-type)
           (type (or null string) lambda-code))
  ;; TODO: Try using `capnp-save-default'
  (let* ((namespace (format nil "~{~A::~}"
                            (cpp-type-namespace (parse-cpp-type-declaration cpp-type))))
         (lambda-code (if lambda-code
                         lambda-code
                         (format nil
                                 "[](auto *builder, const auto &val) { ~ASave(val, builder); }"
                                 namespace))))
    (lambda (builder member capnp-name)
      (declare (ignore capnp-name))
      #>cpp
      utils::SaveOptional<${capnp-type}, ${cpp-type}>(${member}, &${builder}, ${lambda-code});
      cpp<#)))

(defun capnp-load-optional (capnp-type cpp-type &optional lambda-code)
  "Generate the C++ code calling utils::LoadOptional. CAPNP-TYPE and CPP-TYPE
are passed as template parameters, while the optional LAMBDA-CODE is used to
load the value of std::optional."
  (declare (type string capnp-type cpp-type)
           (type (or null string) lambda-code))
  (let* ((namespace (format nil "~{~A::~}"
                            (cpp-type-namespace (parse-cpp-type-declaration cpp-type))))
         (lambda-code (if lambda-code
                         lambda-code
                         (format nil
                                 "[](const auto &reader) { ~A val; ~ALoad(&val, reader); return val; }"
                                 cpp-type namespace))))
    (lambda (reader member capnp-name)
      (declare (ignore capnp-name))
      #>cpp
      ${member} = utils::LoadOptional<${capnp-type}, ${cpp-type}>(${reader}, ${lambda-code});
      cpp<#)))

(defun capnp-save-vector (capnp-type cpp-type &optional lambda-code)
  "Generate the C++ code calling utils::SaveVector. CAPNP-TYPE and CPP-TYPE
are passed as template parameters, while LAMBDA-CODE is used to save each
element."
  (declare (type string capnp-type cpp-type)
           (type (or null string) lambda-code))
  ;; TODO: Why not use our `capnp-save-default' for this?
  (let* ((namespace (format nil "~{~A::~}"
                           (cpp-type-namespace (parse-cpp-type-declaration cpp-type))))
        (lambda-code (if lambda-code
                         lambda-code
                         (format nil
                                 "[](auto *builder, const auto &val) { ~ASave(val, builder); }"
                                 namespace))))
    (lambda (builder member-name capnp-name)
      (declare (ignore capnp-name))
      #>cpp
      utils::SaveVector<${capnp-type}, ${cpp-type}>(${member-name}, &${builder}, ${lambda-code});
      cpp<#)))

(defun capnp-load-vector (capnp-type cpp-type &optional lambda-code)
  "Generate the C++ code calling utils::LoadVector. CAPNP-TYPE and CPP-TYPE
are passed as template parameters, while LAMBDA-CODE is used to load each
element."
  (declare (type string capnp-type cpp-type)
           (type (or null string) lambda-code))
  (let* ((namespace (format nil "~{~A::~}"
                            (cpp-type-namespace (parse-cpp-type-declaration cpp-type))))
         (lambda-code (if lambda-code
                         lambda-code
                         (format nil
                                 "[](const auto &reader) { ~A val; ~ALoad(&val, reader); return val; }"
                                 cpp-type namespace))))
    (lambda (reader member-name capnp-name)
      (declare (ignore capnp-name))
      #>cpp
      utils::LoadVector<${capnp-type}, ${cpp-type}>(&${member-name}, ${reader}, ${lambda-code});
      cpp<#)))

(defun capnp-save-enum (capnp-type cpp-type &optional enum-values)
  "Generate C++ code for saving the enum specified by CPP-TYPE by converting
the values to CAPNP-TYPE. If ENUM-VALUES are not specified, tries to find the
CPP-TYPE among defined enums."
  (declare (type string capnp-type)
           (type (or symbol string) cpp-type))
  (lambda (builder member capnp-name)
    (let* ((enum-values (if enum-values
                            enum-values
                            (cpp-enum-values (find-cpp-enum cpp-type))))
           (cases (mapcar (lambda (value-symbol)
                            (let ((value (cl-ppcre:regex-replace-all "-" (string value-symbol) "_")))
                              #>cpp
                              case ${cpp-type}::${value}:
                                ${builder}->set${capnp-name}(${capnp-type}::${value});
                                break;
                              cpp<#))
                          enum-values)))
      (format nil "switch (~A) {~%~{~A~%~}}" member (mapcar #'raw-cpp-string cases)))))

(defun capnp-load-enum (capnp-type cpp-type &optional enum-values)
  "Generate C++ code for loading the enum specified by CPP-TYPE by converting
the values from CAPNP-TYPE. If ENUM-VALUES are not specified, tries to find the
CPP-TYPE among defined enums."
  (declare (type string capnp-type)
           (type (or symbol string) cpp-type))
  (lambda (reader member capnp-name)
    (let* ((enum-values (if enum-values
                            enum-values
                            (cpp-enum-values (find-cpp-enum cpp-type))))
           (cases (mapcar (lambda (value-symbol)
                            (let ((value (cl-ppcre:regex-replace-all "-" (string value-symbol) "_")))
                              #>cpp
                              case ${capnp-type}::${value}:
                                ${member} = ${cpp-type}::${value};
                                break;
                              cpp<#))
                          enum-values)))
      (format nil "switch (~A.get~A()) {~%~{~A~%~}}"
              reader capnp-name (mapcar #'raw-cpp-string cases)))))

(defvar *cpp-namespaces* nil
  "Stack of C++ namespaces we are generating the code in.")

(defmacro namespace (name)
  "Push the NAME to currently set namespaces."
  (declare (type symbol name))
  (let ((cpp-namespace (cpp-variable-name name)))
    `(progn
       (push ,cpp-namespace *cpp-namespaces*)
       (make-raw-cpp
        :string ,(format nil "~%namespace ~A {~%" cpp-namespace)))))

(defun pop-namespace ()
  (pop *cpp-namespaces*)
  #>cpp } cpp<#)

(defvar *cpp-impl* nil "List of (namespace . C++ code) pairs that should be
  written in the implementation (.cpp) file.")

(defun in-impl (&rest args)
  (let ((namespaces (reverse *cpp-namespaces*)))
    (setf *cpp-impl*
          (append *cpp-impl* (mapcar (lambda (cpp) (cons namespaces cpp))
                                     args)))))

(defvar *cpp-inner-types* nil
  "List of cpp types defined inside an enclosing class or struct")

(defvar *cpp-enclosing-class* nil
  "Symbol name of the `CPP-CLASS' inside which inner types are defined.")

(defmacro define-enum (name values &rest options)
  "Define a C++ enum. Documentation is optional. The only options are
  :documentation and :serialize. Syntax is:

;; (define-enum name
;;   (value1 value2 ...)
;;   (:enum-option option-value)*)"
  (declare (type symbol name))
  (let ((documentation (second (assoc :documentation options)))
        (enum (gensym (format nil "ENUM-~A" name))))
    `(let ((,enum (make-instance 'cpp-enum
                                 :name ',name
                                 :documentation ,documentation
                                 :values ',values
                                 :namespace (reverse *cpp-namespaces*)
                                 :enclosing-class *cpp-enclosing-class*
                                 :capnp-schema (and *capnp-serialize-p* ',(assoc :serialize options)))))
       (prog1 ,enum
         (push ,enum *cpp-enums*)
         (push ,enum *cpp-inner-types*)))))

(defmacro define-class (name super-classes slots &rest options)
  "Define a C++ class. Syntax is:

;; (define-class name (list-of-super-classes)
;;   ((c++-slot-definition)*)
;;   (:class-option option-value)*)

Class name may be a list where the first element is the class name, while
others are template arguments.

For example:

;; (define-class (optional t-value)
;;    ...)

defines a templated C++ class:

template <class TValue>
class Optional { ... };

Each C++ member/slot definition is of the form:
;; (name cpp-type slot-options)

slot-options are keyword arguments. Currently supported options are:
  * :initval -- initializer value for the member, a C++ string or a number.
  * :reader -- if t, generates a public getter for the member.
  * :scope -- class scope of the member, either :public, :protected or :private (default).
  * :documentation -- Doxygen documentation of the member.
  * :capnp-type -- String or list specifying which Cap'n Proto type to use for
    serialization.  If a list of (member-symbol \"capnp-type\") then a union
    type is specified.
  * :capnp-init -- Boolean indicating whether the member needs to be
    initialized in Cap'n Proto structure, by calling `builder.init<member>`.
    This is T by default, you may need to set it to NIL if the LCP doesn't
    correctly recognize a primitive type or you wish to call `init<member>`
    yourself.
  * :capnp-save -- Custom code for serializing this member.
  * :capnp-load -- Custom code for deserializing this member.

Currently supported class-options are:
  * :documentation -- Doxygen documentation of the class.
  * :public -- additional C++ code in public scope.
  * :protected -- additional C++ code in protected scope.
  * :private -- additional C++ code in private scope.
  * :serialize -- only :capnp is a valid value.  Setting :capnp will generate
    the Cap'n Proto serialization code for the class members.  You may
    specifiy additional options after :capnp to fill the `CAPNP-OPTS' slots.

Larger example:

;; (lcp:define-class derived (base)
;;   ((val :int :reader t :initval 42))
;;   (:public #>cpp void set_val(int new_val) { val_ = new_val; } cpp<#)
;;   (:serialize :capnp))

Generates C++:

;; class Derived : public Base {
;;  public:
;;   void set_val(int new_val) { val_ = new_val; }
;;   auto val() { return val_; } // autogenerated from :reader t
;;
;;   void Save(capnp::Base::Builder *builder) const;
;;   static std::unique_ptr<Derived> Construct(const capnp::Base::Reader &reader);
;;   void Load(const capnp::Base::Reader &reader);
;;
;;  private:
;;   int val_ = 42; // :initval is assigned
;; };"
  (let ((structp (second (assoc :structp options))))
    (flet ((parse-slot (slot-name type &rest kwargs
                                  &key reader scope &allow-other-keys)
             (let ((scope (if scope scope (if structp :public :private))))
               (when (and structp reader (eq :private scope))
                 (error "Slot ~A is declared private with reader in a struct. You should use define-class" slot-name))
               (when (and structp reader (eq :public scope))
                 (error "Slot ~A is public, you shouldn't specify :reader" slot-name))
               `(make-cpp-member :symbol ',slot-name :type ,type :scope ,scope
                                 ,@kwargs))))
      (let ((members (mapcar (lambda (s) (apply #'parse-slot s)) slots))
            (class-name (if (consp name) (car name) name))
            (type-params (when (consp name) (cdr name)))
            (class (gensym (format nil "CLASS-~A" name)))
            (serialize (cdr (assoc :serialize options))))
        `(let ((,class
                (let ((*cpp-inner-types* nil)
                      (*cpp-enclosing-class* ',class-name))
                  (make-instance 'cpp-class
                                 :name ',class-name :super-classes ',super-classes
                                 :type-params ',type-params
                                 :structp ,(second (assoc :structp options))
                                 :members (list ,@members)
                                 :documentation ,(second (assoc :documentation options))
                                 :public (list ,@(cdr (assoc :public options)))
                                 :protected (list ,@(cdr (assoc :protected options)))
                                 :private (list ,@(cdr (assoc :private options)))
                                 :capnp-opts ,(when (member :capnp serialize)
                                                `(and *capnp-serialize-p*
                                                      (make-capnp-opts ,@(cdr (member :capnp serialize)))))
                                 :namespace (reverse *cpp-namespaces*)
                                 ;; Set inner types at the end. This works
                                 ;; because CL standard specifies order of
                                 ;; evaluation from left to right.
                                 :inner-types *cpp-inner-types*))))
           (prog1 ,class
             (push ,class *cpp-classes*)
             ;; Set the parent's inner types
             (push ,class *cpp-inner-types*)
             (setf (cpp-type-enclosing-class ,class) *cpp-enclosing-class*)))))))

(defmacro define-struct (name super-classes slots &rest options)
  `(define-class ,name ,super-classes ,slots (:structp t) ,@options))

(defmacro define-rpc (name request response)
  (declare (type list request response))
  (assert (eq :request (car request)))
  (assert (eq :response (car response)))
  (flet ((decl-type-info (class-name)
           #>cpp
           using Capnp = capnp::${class-name};
           static const communication::rpc::MessageType TypeInfo;
           cpp<#)
         (def-type-info (class-name)
           #>cpp
           const communication::rpc::MessageType
           ${class-name}::TypeInfo{::capnp::typeId<${class-name}::Capnp>(), "${class-name}"};
           cpp<#)
         (def-constructor (class-name members)
           (let ((full-constructor
                  (let ((init-members (remove-if (lambda (slot-def)
                                                   ;; TODO: proper initarg
                                                   (let ((initarg (member :initarg slot-def)))
                                                     (and initarg (null (second initarg)))))
                                                 members)))
                    (with-output-to-string (s)
                      (when init-members
                        (format s "~A ~A(~:{~A ~A~:^, ~}) : ~:{~A(~A)~:^, ~} {}"
                                (if (= 1 (list-length init-members)) "explicit" "")
                                class-name
                                (mapcar (lambda (member)
                                          (list (cpp-type-name (second member))
                                                (cpp-variable-name (first member))))
                                        init-members)
                                (mapcar (lambda (member)
                                          (let ((var (cpp-variable-name (first member)))
                                                (movep (eq :move (second (member :initarg member)))))
                                            (list var (if movep
                                                          (format nil "std::move(~A)" var)
                                                          var))))
                                        init-members)))))))
             #>cpp
             ${class-name}() {}
             ${full-constructor}
             cpp<#)))
    (let* ((req-sym (intern (format nil "~A-~A" name 'req)))
           (req-name (cpp-type-name req-sym))
           (res-sym (intern (format nil "~A-~A" name 'res)))
           (res-name (cpp-type-name res-sym))
           (rpc-name (format nil "~ARpc" (cpp-type-name name)))
           (rpc-decl
            #>cpp
             using ${rpc-name} = communication::rpc::RequestResponse<${req-name}, ${res-name}>;
             cpp<#))
      `(cpp-list
        (define-struct ,req-sym ()
          ,@(cdr request)
          (:public
           ,(decl-type-info req-name)
           ,(def-constructor req-name (second request)))
          (:serialize :capnp :base t))
        (in-impl ,(def-type-info req-name))
        (define-struct ,res-sym ()
          ,@(cdr response)
          (:public
           ,(decl-type-info res-name)
           ,(def-constructor res-name (second response)))
          (:serialize :capnp :base t))
        (in-impl ,(def-type-info res-name))
        ,rpc-decl))))

(defun read-lcp (filepath)
  "Read the FILEPATH and return a list of C++ meta information that should be
formatted and output."
  (with-open-file (in-stream filepath)
    (let ((stream-pos 0))
      (handler-case
          (loop for form = (read-preserving-whitespace in-stream nil 'eof)
             until (eq form 'eof)
             for res = (handler-case (eval form)
                         (error (err)
                           (file-position in-stream 0) ;; start of stream
                           (error "~%~A:~A: error:~2%~A~2%in:~2%~A"
                                  (uiop:native-namestring filepath)
                                  (count-newlines in-stream :stop-position (1+ stream-pos))
                                  err form)))
             do (setf stream-pos (file-position in-stream))
             when (typep res '(or raw-cpp cpp-type cpp-list))
             collect res)
        (end-of-file ()
          (file-position in-stream 0) ;; start of stream
          (error "~%~A:~A:error: READ error, did you forget a closing ')'?"
                 (uiop:native-namestring filepath)
                 (count-newlines in-stream
                                 :stop-position (1+ stream-pos))))))))

(defun call-with-namespaced-output (out fun)
  "Invoke FUN with a function for opening C++ namespaces.  The function takes
care to write namespaces to OUT without redundantly opening already open
namespaces."
  (declare (type stream out))
  (declare (type (function (function)) fun))
  (let (open-namespaces)
    (funcall fun (lambda (namespaces)
                   ;; Check if we need to open or close namespaces
                   (loop for namespace in namespaces
                      with unmatched = open-namespaces do
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

(defun generate-capnp (cpp-types &key capnp-file capnp-id cpp-out lcp-file)
  "Generate Cap'n Proto serialization code for given CPP-TYPES.  The schema
is written to CAPNP-FILE using the CAPNP-ID.  The C++ serialization code is
written to CPP-OUT stream.  This source file will include the provided HPP-FILE.
Original LCP-FILE is used just to insert a comment about the source of the
code generation."
  (with-open-file (out capnp-file :direction :output :if-exists :supersede)
    (format out "~@{# ~A~%~}" +emacs-read-only+ +vim-read-only+)
    (format out "# DO NOT EDIT! Generated using LCP from '~A'~2%"
            (file-namestring lcp-file))
    (format out "~A;~2%" capnp-id)
    (write-line "using Cxx = import \"/capnp/c++.capnp\";" out)
    (format out "$Cxx.namespace(\"~A::capnp\");~2%" *capnp-namespace*)
    (dolist (capnp-import *capnp-imports* (terpri out))
      (format out "using ~A = import ~S;~%"
              (remove #\- (string-capitalize (car capnp-import)))
              (cdr capnp-import)))
    (dolist (cpp-type cpp-types)
      ;; Generate schema only for top level classes, inner classes are handled
      ;; inside the generation of the enclosing class.
      (unless (cpp-type-enclosing-class cpp-type)
        (let ((schema (capnp-schema cpp-type)))
          (when schema (write-line schema out))))))
  ;; Now generate the save/load C++ code in the cpp file.
  (write-line "// Autogenerated Cap'n Proto serialization code" cpp-out)
  (write-line "#include \"utils/serialization.hpp\"" cpp-out)
  (with-namespaced-output (cpp-out open-namespace)
    (dolist (cpp-class (remove-if (lambda (cpp-type) (not (typep cpp-type 'cpp-class))) cpp-types))
      (open-namespace (cpp-type-namespace cpp-class))
      (format cpp-out "// Serialize code for ~A~2%" (cpp-type-name cpp-class))
      ;; Top level functions
      (write-line (capnp-save-function-definition cpp-class) cpp-out)
      (write-line (capnp-load-function-definition cpp-class) cpp-out))))

(defun process-file (lcp-file &key capnp-id)
  "Process a LCP-FILE and write the output to .hpp file in the same directory.
If CAPNP-ID is passed, generates the Cap'n Proto schema to .capnp file in the
same directory, while the loading code is generated in LCP-FILE.cpp source
file."
  (multiple-value-bind (filename extension)
      (uiop:split-name-type lcp-file)
    (assert (string= (string-downcase extension) "lcp"))
    (let ((hpp-file (concatenate 'string filename ".hpp"))
          ;; Unlike hpp, for cpp file use the full path. This allows us to
          ;; have our own accompanying .cpp files
          (cpp-file (concatenate 'string lcp-file ".cpp"))
          (capnp-file (concatenate 'string filename ".capnp"))
          ;; Reset globals
          (*capnp-serialize-p* capnp-id)
          (*capnp-namespace* nil)
          (*capnp-imports* nil)
          (*capnp-type-converters* nil)
          (*cpp-inner-types* nil)
          (*cpp-impl*)
          ;; Don't reset *cpp-classes* if we want to have support for
          ;; procesing multiple files.
          ;; (*cpp-classes* nil)
          ;; (*cpp-enums* nil)
          )
      ;; First read and evaluate the whole file, then output the evaluated
      ;; cpp-code. This allows us to generate code which may rely on
      ;; evaluation done after the code definition.
      (with-open-file (out hpp-file :direction :output :if-exists :supersede)
        (format out "~@{// ~A~%~}" +emacs-read-only+ +vim-read-only+)
        (format out "// DO NOT EDIT! Generated using LCP from '~A'~2%"
                (file-namestring lcp-file))
        (dolist (res (read-lcp lcp-file))
          (write-line (cpp-code res) out)))
      (when *cpp-namespaces*
        (error "Unclosed namespaces: ~A" (reverse *cpp-namespaces*)))
      ;; If we have a capnp-id, generate the schema and serialization code
      (let ((types-for-capnp (when capnp-id
                               (append (remove-if (complement #'cpp-class-capnp-opts) *cpp-classes*)
                                       (remove-if (complement #'cpp-enum-capnp-schema) *cpp-enums*)))))
        ;; Append top-level declarations for Cap'n Proto serialization
        (with-open-file (out hpp-file :direction :output :if-exists :append)
          (terpri out)
          (write-line "// Cap'n Proto serialization declarations" out)
          (with-namespaced-output (out open-namespace)
            (dolist (type-for-capnp types-for-capnp)
              (when (typep type-for-capnp 'cpp-class)
                (open-namespace (cpp-type-namespace type-for-capnp))
                (format out "~A;~%" (capnp-save-function-declaration type-for-capnp))
                (format out "~A;~%" (capnp-load-function-declaration type-for-capnp))))))
        ;; When we have either capnp or C++ code for the .cpp file, generate the .cpp file
        (when (or *cpp-impl* types-for-capnp)
          (with-open-file (out cpp-file :direction :output :if-exists :supersede)
            (format out "~@{// ~A~%~}" +emacs-read-only+ +vim-read-only+)
            (format out "// DO NOT EDIT! Generated using LCP from '~A'~2%"
                    (file-namestring lcp-file))
            (format out "#include \"~A\"~2%" (file-namestring hpp-file))
            ;; First output the C++ code from the user
            (with-namespaced-output (out open-namespace)
              (dolist (cpp *cpp-impl*)
                (destructuring-bind (namespaces . code) cpp
                  (open-namespace namespaces)
                  (write-line (cpp-code code) out))))
            (when types-for-capnp
              (generate-capnp types-for-capnp :capnp-file capnp-file :capnp-id capnp-id
                              :cpp-out out :lcp-file lcp-file))))))))
