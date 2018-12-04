;;; This file is an entry point for processing LCP files and generating the
;;; C++ code.

(in-package #:lcp)

(defvar +vim-read-only+ "vim: readonly")
(defvar +emacs-read-only+ "-*- buffer-read-only: t; -*-")

(defvar *generating-cpp-impl-p* nil
  "T if we are currently writing the .cpp file.")

(eval-when (:compile-toplevel :load-toplevel :execute)
  (set-dispatch-macro-character #\# #\> #'|#>-reader|))

(defun fnv1a64-hash-string (string)
  "Produce (UNSIGNED-BYTE 64) hash of the given STRING using FNV-1a algorithm.
See https://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash."
  (check-type string string)
  (let ((hash 14695981039346656037) ;; offset basis
        (prime 1099511628211))
    (declare (type (unsigned-byte 64) hash prime))
    (loop for c across string do
         (setf hash (mod (* (boole boole-xor hash (char-code c)) prime)
                         (expt 2 64) ;; Fit to 64bit
                         )))
    hash))

(defun cpp-enum-definition (cpp-enum)
  "Get C++ style `CPP-ENUM' definition as a string."
  (declare (type cpp-enum cpp-enum))
  (with-output-to-string (s)
    (when (cpp-type-documentation cpp-enum)
      (write-line (cpp-documentation (cpp-type-documentation cpp-enum)) s))
    (with-cpp-block-output (s :name (format nil "enum class ~A" (cpp-type-name cpp-enum))
                              :semicolonp t)
      (format s "~{ ~A~^,~%~}~%" (mapcar #'cpp-enumerator-name (cpp-enum-values cpp-enum))))))

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
          (when (or (cpp-class-public cpp-class) (cpp-class-members-scoped :public) reader-members
                    ;; We at least have public TypeInfo object for non-template classes.
                    (not (cpp-type-type-params cpp-class)))
            (unless (cpp-class-structp cpp-class)
              (write-line " public:" s))
            (unless (cpp-type-type-params cpp-class)
              ;; Skip generating TypeInfo for template classes.
              (write-line "static const utils::TypeInfo kType;" s))
            (format s "~%~{~A~%~}" (mapcar #'cpp-code (cpp-class-public cpp-class)))
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
                  (mapcar #'member-declaration (cpp-class-members-scoped :private)))))
      ;; Define the TypeInfo object.  Relies on the fact that *CPP-IMPL* is
      ;; processed later.
      (unless (cpp-type-type-params cpp-class)
        (let ((typeinfo-def
               (format nil "const utils::TypeInfo ~A::kType{0x~XULL, \"~a\"};~%"
                       (if *generating-cpp-impl-p*
                           (cpp-type-name cpp-class)
                           ;; Use full type declaration if class definition
                           ;; isn't inside the .cpp file.
                           (cpp-type-decl cpp-class))
                       ;; Use full type declaration for hash
                       (fnv1a64-hash-string (cpp-type-decl cpp-class))
                       (cpp-type-name cpp-class))))
          (if *generating-cpp-impl-p*
              (write-line typeinfo-def s)
              (in-impl typeinfo-def)))))))

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
                 (let ((capnp-opts (cpp-class-capnp-opts subclass)))
                   (or
                    ;; Remove if we are a parent that should be ignored (not
                    ;; the 1st in the list).
                    (and (capnp-opts-ignore-other-base-classes capnp-opts)
                         (not (eq class-name (car (cpp-class-super-classes subclass)))))
                    ;; Remove if we are a parent that should be treated as
                    ;; composition.
                    (member class-name
                            (capnp-opts-inherit-compose (cpp-class-capnp-opts subclass))))))
               (direct-subclasses-of cpp-class))))

(defun capnp-union-and-compose-parents (cpp-class)
  "Get direct parents of CPP-CLASS that model the inheritance via union. The
secondary value contains parents which are modeled as being composed inside
CPP-CLASS."
  (declare (type (or symbol cpp-class) cpp-class))
  (let* ((class (if (symbolp cpp-class) (find-cpp-class cpp-class) cpp-class))
         (capnp-opts (cpp-class-capnp-opts class)))
    (when (not capnp-opts)
      (error "Class ~A should be marked for capnp serialization,
or its derived classes set as :CAPNP :BASE T" (cpp-type-base-name class)))
    (when (not (capnp-opts-base capnp-opts))
      (if (capnp-opts-ignore-other-base-classes capnp-opts)
          ;; Since we are ignoring multiple inheritance, return the 1st class
          ;; (as union parent).
          (list (car (cpp-class-super-classes class)))
          ;; We aren't ignoring multiple inheritance, collect union and
          ;; compose parents.
          (let (union compose)
            (dolist (parent (cpp-class-super-classes class))
              (if (member parent (capnp-opts-inherit-compose capnp-opts))
                  (push parent compose)
                  (push parent union)))
            (values union compose))))))

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
        (members (when (typep cpp-class 'cpp-class)
                   (cpp-class-members-for-save cpp-class)))
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
                          (incf field-number)))))))
            (dolist (inner inner-types)
              (when (or (and (typep inner 'cpp-class) (cpp-class-capnp-opts inner))
                        (and (typep inner 'cpp-enum) (cpp-enum-capnp-schema inner)))
                (write-line (capnp-schema inner) s)))
            (when union-subclasses
              (with-cpp-block-output (s :name "union")
                (when (not (cpp-class-abstractp cpp-class))
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
              (cpp-type-decl (find-cpp-class (car (last parents))) :type-params nil :namespace nil)
              (cpp-type-decl cpp-class :type-params nil :namespace nil)))
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
    (make-cpp-type name :namespace namespace
                   :enclosing-class (cpp-type-enclosing-class cpp-type))))

(defun cpp-enum-to-capnp-function-name (cpp-enum &key namespace)
  "Generate the name of the C++ function for converting a C++ enum to
equivalent Cap'n Proto schema enum.  If NAMESPACE is T, prepend the name with
namespace of CPP-ENUM."
  (concatenate
   'string
   (if namespace (cpp-type-namespace-string cpp-enum) "")
   ;; Remove namespace demarkations of any potential enclosing classses.
   (remove #\: (cpp-type-decl cpp-enum :namespace nil))
   "ToCapnp"))

(defun cpp-enum-to-capnp-function-declaration (cpp-enum)
  "Generate C++ function declaration for converting a C++ enum to equivalent
Cap'n Proto schema enum."
  (let ((name (cpp-enum-to-capnp-function-name cpp-enum))
        (capnp-type (cpp-type-decl (capnp-cpp-type<-cpp-type cpp-enum))))
    (cpp-function-declaration
     name :args `((value ,(cpp-type-decl cpp-enum)))
     :returns capnp-type)))

(defun cpp-enum-to-capnp-function-definition (cpp-enum)
  "Generate C++ function for converting a C++ enum to equivalent Cap'n Proto
schema enum."
  (with-output-to-string (out)
    (with-cpp-block-output (out :name (cpp-enum-to-capnp-function-declaration cpp-enum))
      (let* ((cpp-type (cpp-type-decl cpp-enum))
             (capnp-type (cpp-type-decl (capnp-cpp-type<-cpp-type cpp-enum)))
             (cases (mapcar (lambda (value-symbol)
                             (let ((value (cl-ppcre:regex-replace-all "-" (string value-symbol) "_")))
                               #>cpp
                               case ${cpp-type}::${value}:
                                 return ${capnp-type}::${value};
                               cpp<#))
                            (cpp-enum-values cpp-enum))))
        (format out "switch (value) {~%~{~A~%~}}" (mapcar #'raw-cpp-string cases))))))

(defun cpp-enum-from-capnp-function-name (cpp-enum &key namespace)
  "Generate the name of the C++ function for converting a C++ enum from
equivalent Cap'n Proto schema enum.  If NAMESPACE is T, prepend the name with
namespace of CPP-ENUM."
  (concatenate
   'string
   (if namespace (cpp-type-namespace-string cpp-enum) "")
   ;; Remove namespace demarkations of any potential enclosing classses.
   (remove #\: (cpp-type-decl cpp-enum :namespace nil))
   "FromCapnp"))

(defun cpp-enum-from-capnp-function-declaration (cpp-enum)
  "Generate C++ function declaration for converting a C++ enum from equivalent
Cap'n Proto schema enum."
  (let ((name (cpp-enum-from-capnp-function-name cpp-enum))
        (capnp-type (cpp-type-decl (capnp-cpp-type<-cpp-type cpp-enum))))
    (cpp-function-declaration
     name :args `((value ,capnp-type))
     :returns (cpp-type-decl cpp-enum))))

(defun cpp-enum-from-capnp-function-definition (cpp-enum)
  "Generate C++ function for converting a C++ enum from equivalent Cap'n Proto
schema enum."
  (with-output-to-string (out)
    (with-cpp-block-output (out :name (cpp-enum-from-capnp-function-declaration cpp-enum))
      (let* ((cpp-type (cpp-type-decl cpp-enum))
             (capnp-type (cpp-type-decl (capnp-cpp-type<-cpp-type cpp-enum)))
             (cases (mapcar (lambda (value-symbol)
                             (let ((value (cl-ppcre:regex-replace-all "-" (string value-symbol) "_")))
                               #>cpp
                               case ${capnp-type}::${value}:
                                 return ${cpp-type}::${value};
                               cpp<#))
                            (cpp-enum-values cpp-enum))))
        (format out "switch (value) {~%~{~A~%~}}" (mapcar #'raw-cpp-string cases))))))

(defun capnp-save-enum-vector (builder-name member-name cpp-enum)
  (let ((enum-to-capnp (cpp-enum-to-capnp-function-name cpp-enum :namespace t)))
    (raw-cpp-string
     #>cpp
     for (size_t i = 0;
          i < ${member-name}.size();
          ++i) {
       ${builder-name}.set(i, ${enum-to-capnp}(${member-name}[i]));
     }
     cpp<#)))

(defun capnp-save-default (member-name member-type member-builder capnp-name &key cpp-class)
  "Generate the default call to save for member.  MEMBER-NAME and MEMBER-TYPE
are strings describing the member being serialized.  MEMBER-BUILDER is the
name of the builder variable.  CAPNP-NAME is the name of the member in Cap'n
Proto schema."
  (declare (type string member-name member-type member-builder capnp-name))
  (declare (type cpp-class cpp-class))
  (let* ((type (parse-cpp-type-declaration member-type))
         (type-name (cpp-type-base-name type))
         (cpp-enum (or
                    ;; Look for potentially nested enum first
                    (find-cpp-enum
                     (concatenate 'string (cpp-type-decl cpp-class) "::" member-type))
                    (find-cpp-enum member-type))))
    (cond
      (cpp-enum
       (let ((enum-to-capnp (cpp-enum-to-capnp-function-name cpp-enum :namespace t)))
         (raw-cpp-string
          #>cpp
          ${member-builder}->set${capnp-name}(${enum-to-capnp}(${member-name}));
          cpp<#)))
      ((string= "vector" type-name)
       (let* ((elem-type (car (cpp-type-type-args type)))
              (elem-type-enum (or (find-cpp-enum (concatenate 'string (cpp-type-decl cpp-class)
                                                              "::" (cpp-type-decl elem-type)))
                                  (find-cpp-enum (cpp-type-decl elem-type))))
              (capnp-cpp-type (capnp-cpp-type<-cpp-type (or elem-type-enum elem-type))))
         (cond
           ((capnp-primitive-type-p (capnp-type<-cpp-type (cpp-type-base-name elem-type)))
            (raw-cpp-string
             #>cpp
             utils::SaveVector(${member-name}, &${member-builder});
             cpp<#))
           (elem-type-enum
            (capnp-save-enum-vector member-builder member-name elem-type-enum))
           (t
            (raw-cpp-string
             (funcall (capnp-save-vector (cpp-type-decl capnp-cpp-type) (cpp-type-decl elem-type))
                      member-builder member-name capnp-name))))))
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

(defun capnp-save-members (cpp-class builder &key instance-access)
  "Generate Cap'n Proto saving code for members of CPP-CLASS.  INSTANCE-ACCESS
  is a C++ string which is prefixed to member access.  For example,
  INSTANCE-ACCESS could be `my_struct->`"
  (declare (type cpp-class cpp-class))
  (declare (type string instance-access))
  (with-output-to-string (s)
    (dolist (member (cpp-class-members-for-save cpp-class))
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
           (format s "  ~A->set~A(~A);~%" builder capnp-name member-access))
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
                   (format s "  auto ~A = ~A->init~A(~A);~%"
                           member-builder builder capnp-name size)
                   (setf member-builder builder)))
             (if (cpp-member-capnp-save member)
                 (format s "  ~A~%"
                         (cpp-code (funcall (cpp-member-capnp-save member)
                                            member-builder member-access capnp-name)))
                 (write-line (capnp-save-default member-access (cpp-member-type member)
                                                 member-builder capnp-name
                                                 :cpp-class cpp-class)
                             s)))))))))

(defun capnp-save-function-code (cpp-class)
  "Generate Cap'n Proto save code for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (labels ((save-class (cpp-class builder cpp-out &key (force-builder nil))
             "Output the serialization code for CPP-CLASS and its parent classes."
             (let* ((compose-parents (nth-value 1 (capnp-union-and-compose-parents cpp-class)))
                    (parents (capnp-union-parents-rec cpp-class))
                    (first-parent (find-cpp-class (first parents))))
               (when first-parent
                 (with-cpp-block-output (cpp-out)
                   (format cpp-out "// Save parent class ~A~%" (cpp-type-name first-parent))
                   (save-class first-parent builder cpp-out)))
               ;; Initialize CPP-CLASS builder
               (when parents
                 (if (or force-builder compose-parents
                         (cpp-class-members-for-save cpp-class))
                     (progn
                       (format cpp-out "auto ~A_builder = ~A->~{get~A().~}init~A();~%"
                               (cpp-variable-name (cpp-type-base-name cpp-class))
                               builder
                               (mapcar #'cpp-type-name (cdr (reverse parents)))
                               (cpp-type-name cpp-class))
                       (format cpp-out "auto *builder = &~A_builder;~%"
                               (cpp-variable-name (cpp-type-base-name cpp-class)))
                       (setf builder "builder"))
                     (format cpp-out "~A->~{get~A().~}init~A();~%"
                             builder
                             (mapcar #'cpp-type-name (cdr (reverse parents)))
                             (cpp-type-name cpp-class))))
               ;; Save composed parent classes
               (dolist (parent compose-parents)
                 (with-cpp-block-output (cpp-out)
                   (let* ((parent-builder (format nil "~A_builder" (cpp-variable-name parent))))
                     (format cpp-out "// Save composed class ~A~%" (cpp-type-name parent))
                     (format cpp-out "auto ~A = ~A->init~A();~%"
                             parent-builder builder (cpp-type-name parent))
                     (format cpp-out "Save(self, &~A~{, ~A~});"
                             parent-builder
                             (mapcar (lambda (name-and-type)
                                       (cpp-variable-name (first name-and-type)))
                                     (capnp-extra-args (find-cpp-class parent) :save))))))
               ;; Save members
               (write-string (capnp-save-members cpp-class builder :instance-access "self.") cpp-out)
               ;; Call post-save function if necessary
               (let ((capnp-opts (cpp-class-capnp-opts cpp-class)))
                 (when (capnp-opts-post-save capnp-opts)
                   (write-string (cpp-code (funcall (capnp-opts-post-save capnp-opts) builder)) cpp-out))))))
    (with-output-to-string (cpp-out)
      (let ((subclasses (capnp-union-subclasses cpp-class))
            (builder (if (capnp-union-parents-rec cpp-class)
                         "base_builder"
                         "builder")))
        (when subclasses
          (write-line "// Forward serialization to most derived type" cpp-out)
          (dolist (subclass subclasses)
            (let ((derived-name (cpp-type-name subclass))
                  (save-args
                   (format nil "~A~{, ~A~}"
                           builder
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
                   cpp-out)))))
        (cond
          ((cpp-class-abstractp cpp-class)
           (format cpp-out
                   "LOG(FATAL) << \"Should not get here -- `~A` should be an abstract class!\";"
                   (cpp-type-name cpp-class)))
          ((capnp-union-subclasses cpp-class)
           ;; We are in the middle of inheritance hierarchy, so set our
           ;; union Void field.
           (save-class cpp-class builder cpp-out :force-builder t)
           (format cpp-out "builder->set~A();~%" (cpp-type-name cpp-class)))
          (t (save-class cpp-class builder cpp-out)))))))

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

(defun cpp-class-members-for-capnp-load (cpp-class)
  (remove-if (lambda (m) (and (cpp-member-dont-save m)
                              (not (cpp-member-capnp-load m))))
             (cpp-class-members cpp-class)))

(defun capnp-load-function-declaration (cpp-class)
  "Generate Cap'n Proto load function declaration for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (let* ((parents (capnp-union-parents-rec cpp-class))
         (top-parent-class (if parents
                               (cpp-type-decl (find-cpp-class (car (last parents))) :type-params nil :namespace nil)
                               (cpp-type-decl cpp-class :type-params nil :namespace nil)))
         (reader-arg (list (if (or parents (capnp-union-subclasses cpp-class))
                               'base-reader
                               'reader)
                           (format nil "const capnp::~A::Reader &" top-parent-class)))
         (out-arg (list 'self
                        (if (or parents (capnp-union-subclasses cpp-class))
                            (format nil "std::unique_ptr<~A> *" (cpp-type-decl cpp-class :namespace nil))
                            (format nil "~A *" (cpp-type-decl cpp-class :namespace nil))))))
    (cpp-function-declaration
     "Load"
     :args (cons out-arg (cons reader-arg (capnp-extra-args cpp-class :load)))
     :returns "void"
     :type-params (cpp-type-type-params cpp-class))))

(defun capnp-load-enum-vector (reader-name member-name cpp-enum)
  (let ((enum-from-capnp (cpp-enum-from-capnp-function-name cpp-enum :namespace t)))
    (raw-cpp-string
     #>cpp
     ${member-name}.resize(${reader-name}.size());
     for (size_t i = 0;
          i < ${reader-name}.size();
          ++i) {
       ${member-name}[i] = ${enum-from-capnp}(${reader-name}[i]);
     }
     cpp<#)))

(defun capnp-load-default (member-name member-type member-reader capnp-name &key cpp-class)
  "Generate default load call for member.  MEMBER-NAME and MEMBER-TYPE are
strings describing the member being loaded.  MEMBER-READER is the name of the
reader variable.  CAPNP-NAME is the name of the member in Cap'n Proto schema."
  (declare (type string member-name member-type member-reader))
  (let* ((type (parse-cpp-type-declaration member-type))
         (type-name (cpp-type-base-name type))
         (cpp-enum (or
                    ;; Look for potentially nested enum first
                    (find-cpp-enum
                     (concatenate 'string (cpp-type-decl cpp-class) "::" member-type))
                    (find-cpp-enum member-type))))
    (cond
      (cpp-enum
       (let ((enum-from-capnp (cpp-enum-from-capnp-function-name cpp-enum :namespace t)))
         (raw-cpp-string
          #>cpp
          ${member-name} = ${enum-from-capnp}(${member-reader}.get${capnp-name}());
          cpp<#)))
      ((string= "vector" type-name)
       (let* ((elem-type (car (cpp-type-type-args type)))
              (elem-type-enum (or (find-cpp-enum (concatenate 'string (cpp-type-decl cpp-class)
                                                              "::" (cpp-type-decl elem-type)))
                                  (find-cpp-enum (cpp-type-decl elem-type))))
              (capnp-cpp-type (capnp-cpp-type<-cpp-type (or elem-type-enum elem-type))))
         (cond
           ((capnp-primitive-type-p (capnp-type<-cpp-type (cpp-type-base-name elem-type)))
            (raw-cpp-string
             #>cpp
             utils::LoadVector(&${member-name}, ${member-reader});
             cpp<#))
           (elem-type-enum
            (capnp-load-enum-vector member-reader member-name elem-type-enum))
           (t
            (raw-cpp-string
             (funcall (capnp-load-vector (cpp-type-decl capnp-cpp-type) (cpp-type-decl elem-type))
                      member-reader member-name capnp-name))))))
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

(defun capnp-load-members (cpp-class reader &key instance-access)
  "Generate Cap'n Proto loading code for members of CPP-CLASS.
INSTANCE-ACCESS is a C++ string which will be prefixed to member access.  For
example, INSTANCE-ACCESS could be `my_struct->`"
  (declare (type cpp-class cpp-class))
  (declare (type string instance-access))
  (with-output-to-string (s)
    (dolist (member (cpp-class-members-for-capnp-load cpp-class))
      (let ((member-access
             (concatenate 'string instance-access
                          (cpp-member-name member :struct (cpp-class-structp cpp-class))))
            (member-reader (format nil "~A_reader" (cpp-member-name member :struct t)))
            (capnp-name (cpp-type-name (cpp-member-symbol member))))
        (cond
          ((and (not (cpp-member-capnp-load member))
                (capnp-primitive-type-p (capnp-type-of-member member)))
           (format s "  ~A = ~A.get~A();~%" member-access reader capnp-name))
          (t
           ;; Enclose larger load code in new scope
           (with-cpp-block-output (s)
             (if (and (cpp-member-capnp-init member)
                      (not (find-cpp-enum (cpp-member-type member))))
                 (format s "  auto ~A = ~A.get~A();~%" member-reader reader capnp-name)
                 (setf member-reader reader))
             (if (cpp-member-capnp-load member)
                 (format s "  ~A~%"
                         (cpp-code (funcall (cpp-member-capnp-load member)
                                            member-reader member-access capnp-name)))
                 (write-line (capnp-load-default member-access
                                                 (cpp-member-type member)
                                                 member-reader capnp-name :cpp-class cpp-class)
                             s)))))))))

(defun capnp-load-function-code (cpp-class)
  "Generate Cap'n Proto load code for CPP-CLASS."
  (declare (type cpp-class cpp-class))
  (let ((instance-access (if (or (capnp-union-subclasses cpp-class)
                                 (capnp-union-parents-rec cpp-class))
                             "self->get()->"
                             "self->")))
    (labels ((load-class (cpp-class reader cpp-out)
               (let* ((compose-parents (nth-value 1 (capnp-union-and-compose-parents cpp-class)))
                      (parents (capnp-union-parents-rec cpp-class))
                      (first-parent (find-cpp-class (first parents))))
                 (when first-parent
                   (with-cpp-block-output (cpp-out)
                     (format cpp-out "// Load parent class ~A~%" (cpp-type-name first-parent))
                     (load-class first-parent reader cpp-out)))
                 ;; Initialize CPP-CLASS reader
                 (when (and parents (or compose-parents
                                        (cpp-class-members-for-capnp-load cpp-class)))
                       (progn
                         (format cpp-out "auto reader = ~A.~{get~A().~}get~A();"
                                 reader
                                 (mapcar #'cpp-type-name (cdr (reverse parents)))
                                 (cpp-type-name cpp-class))
                         (setf reader "reader")))
                 ;; Load composed parent classes
                 (dolist (parent compose-parents)
                   (with-cpp-block-output (cpp-out)
                     (let ((parent-reader (format nil "~A_reader" (cpp-variable-name parent))))
                       (format cpp-out "// Load composed class ~A~%" (cpp-type-name parent))
                       (format cpp-out "auto ~A = ~A.get~A();~%"
                               parent-reader reader (cpp-type-name parent))
                       (format cpp-out "Load(self->get(), ~A~{, ~A~});~%"
                               parent-reader
                               (mapcar (lambda (name-and-type)
                                         (cpp-variable-name (first name-and-type)))
                                       (capnp-extra-args (find-cpp-class parent) :load))))))
                 ;; Load members
                 (write-string (capnp-load-members cpp-class reader :instance-access instance-access) cpp-out))))
      (with-output-to-string (s)
        (cond
          ((and (capnp-union-and-compose-parents cpp-class)
                (not (direct-subclasses-of cpp-class)))
           ;; CPP-CLASS is the most derived class, so construct and load.
           (if (and (cpp-class-capnp-opts cpp-class)
                    (capnp-opts-construct (cpp-class-capnp-opts cpp-class)))
               (write-line (funcall
                            (capnp-opts-construct (cpp-class-capnp-opts cpp-class))
                            (cpp-type-decl cpp-class :namespace nil))
                           s)
               (format s "*self = std::make_unique<~A>();~%" (cpp-type-decl cpp-class :namespace nil)))
           (load-class cpp-class "base_reader" s))
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
                       (cpp-enumerator-name (cpp-type-base-name subclass)))
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
                                   (cpp-type-name subclass) (cpp-enumerator-name type-arg))
                           (load-derived (format nil "~A<~A>"
                                                 (cpp-type-name subclass)
                                                 (cpp-type-name type-arg))))))
                     ;; Regular forward to derived
                     (load-derived (cpp-type-name subclass))))
               (write-line "break;" s))
             (when (not (cpp-class-abstractp cpp-class))
               ;; We are in the middle of the hierarchy, so allow constructing and loading us.
               (with-cpp-block-output (s :name (format nil "case capnp::~A::~A:"
                                                       (cpp-type-name cpp-class)
                                                       (cpp-enumerator-name (cpp-type-base-name cpp-class))))
                 (format s "*self = std::make_unique<~A>();~%"
                         (cpp-type-decl cpp-class :namespace nil))
                 (load-class cpp-class "base_reader" s)))))
          (t
           ;; Regular load for absolutely no inheritance class
           (assert (not (capnp-union-subclasses cpp-class)))
           (assert (not (capnp-union-and-compose-parents cpp-class)))
           (load-class cpp-class "reader" s)))))))

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
  (let* ((namespace (cpp-type-namespace-string (parse-cpp-type-declaration cpp-type)))
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
  (let* ((namespace (cpp-type-namespace-string (parse-cpp-type-declaration cpp-type)))
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
  ;; TODO: namespace doesn't work for enums nested in classes
  (let* ((namespace (cpp-type-namespace-string (parse-cpp-type-declaration cpp-type)))
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
  (let* ((namespace (cpp-type-namespace-string (parse-cpp-type-declaration cpp-type)))
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

(defun capnp-save-enum (capnp-type cpp-type enum-values)
  "Generate C++ code for saving the enum specified by CPP-TYPE by converting
ENUM-VALUES to CAPNP-TYPE.  This function should only be used for saving enums
which aren't defined in LCP."
  (check-type capnp-type string)
  (check-type cpp-type (or symbol string))
  (check-type enum-values list)
  (lambda (builder member capnp-name)
    (let ((cases (mapcar (lambda (value-symbol)
                           (let ((value (cpp-enumerator-name value-symbol)))
                             #>cpp
                             case ${cpp-type}::${value}:
                               ${builder}->set${capnp-name}(${capnp-type}::${value});
                               break;
                             cpp<#))
                         enum-values)))
      (format nil "switch (~A) {~%~{~A~%~}}" member (mapcar #'raw-cpp-string cases)))))

(defun capnp-load-enum (capnp-type cpp-type enum-values)
  "Generate C++ code for loading the enum specified by CPP-TYPE by converting
ENUM-VALUES from CAPNP-TYPE.  This function should only be used for saving
enums which aren't defined in LCP."
  (check-type capnp-type string)
  (check-type cpp-type (or symbol string))
  (check-type enum-values list)
  (lambda (reader member capnp-name)
    (let ((cases (mapcar (lambda (value-symbol)
                           (let ((value (cpp-enumerator-name value-symbol)))
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

(defmacro define-rpc (name request response)
  (declare (type list request response))
  (assert (eq :request (car request)))
  (assert (eq :response (car response)))
  (flet ((decl-type-info (class-name)
           #>cpp
           using Capnp = capnp::${class-name};
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
        (define-struct ,res-sym ()
          ,@(cdr response)
          (:public
           ,(decl-type-info res-name)
           ,(def-constructor res-name (second response)))
          (:serialize :capnp :base t))
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
    (dolist (cpp-type cpp-types)
      (open-namespace (cpp-type-namespace cpp-type))
      (ctypecase cpp-type
        (cpp-class
         (format cpp-out "// Serialize code for ~A~2%" (cpp-type-name cpp-type))
         ;; Top level functions
         (write-line (capnp-save-function-definition cpp-type) cpp-out)
         (write-line (capnp-load-function-definition cpp-type) cpp-out))
        (cpp-enum
         (write-line (cpp-enum-to-capnp-function-definition cpp-type) cpp-out)
         (write-line (cpp-enum-from-capnp-function-definition cpp-type) cpp-out))))))

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
              (open-namespace (cpp-type-namespace type-for-capnp))
              (ctypecase type-for-capnp
                (cpp-class
                 (format out "~A;~%" (capnp-save-function-declaration type-for-capnp))
                 (format out "~A;~%" (capnp-load-function-declaration type-for-capnp)))
                (cpp-enum
                 (format out "~A;~%" (cpp-enum-to-capnp-function-declaration type-for-capnp))
                 (format out "~A;~%" (cpp-enum-from-capnp-function-declaration type-for-capnp)))))))
        ;; When we have either capnp or C++ code for the .cpp file, generate
        ;; the .cpp file.  Note, that some code may rely on the fact that .cpp
        ;; file is generated after .hpp.
        (when (or *cpp-impl* types-for-capnp)
          (let ((*generating-cpp-impl-p* t))
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
                                :cpp-out out :lcp-file lcp-file)))))))))
