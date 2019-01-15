;;;; This file contains definitions of types used to store meta information on
;;;; C++ types.  Along with data defintions, you will find various functions
;;;; and methods for operating on that data.

(in-package #:lcp)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defvar +whitespace-chars+ '(#\Newline #\Space #\Return #\Linefeed #\Tab)))

(defstruct raw-cpp
  "Represents a raw character string of C++ code."
  (string "" :type string :read-only t))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun |#>-reader| (stream sub-char numarg)
    "Reads the #>cpp ... cpp<# block into `RAW-CPP'.
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

(deftype cpp-primitive-type-keywords ()
  "List of keywords that specify a primitive type in C++."
  `(member :bool :char :int :int16_t :int32_t :int64_t :uint :uint16_t
           :uint32_t :uint64_t :float :double))

(defvar +cpp-primitive-type-keywords+
  '(:bool :char :int :int16_t :int32_t :int64_t :uint :uint16_t
    :uint32_t :uint64_t :float :double))

(defclass cpp-type ()
  ((documentation :type (or null string) :initarg :documentation :initform nil
                  :reader cpp-type-documentation
                  :documentation "Documentation string for this C++ type.")
   (namespace :type list :initarg :ns :initarg :namespace :initform nil
              :reader cpp-type-namespace
              :documentation "A list of symbols or strings defining the full
              namespace.  A single symbol may refer to a `CPP-CLASS' which
              encloses this type.")
   (enclosing-class :type (or null symbol string) :initarg :enclosing-class
                    :initform nil :accessor cpp-type-enclosing-class
                    :documentation "A symbol or a string that is a designator
                    for the type of the enclosing class of this type, or NIL if
                    the type has no enclosing class.")
   (name :type (or symbol string) :initarg :name :reader cpp-type-base-name
         :documentation "Base name of this type.")
   (type-params :type list :initarg :type-params :initform nil
                :reader cpp-type-type-params
                :documentation "A list of strings naming the template parameters
                that are needed to instantiate a concrete type. For example, in
                `template <TValue> class vector`, 'TValue' is the type
                parameter.")
   (type-args :type list :initarg :type-args :initform nil
              :reader cpp-type-type-args
              :documentation "A list of `CPP-TYPE' instances that represent the
              template type arguments used within the instantiation of the
              template. For example in `std::vector<int>`, 'int' is a template
              type argument."))
  (:documentation "Base class for meta information on C++ types."))

(defclass cpp-primitive-type (cpp-type)
  ((name :type cpp-primitive-type-keywords))
  (:documentation "Represents a primitive type in C++."))

(defclass cpp-enum (cpp-type)
  ((values :type list :initarg :values :initform nil :reader cpp-enum-values)
   ;; If true, generate serialization code for this enum.
   (serializep :type boolean :initarg :serializep :initform nil :reader cpp-enum-serializep))
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
  ;; If T, skips this member in serialization code generation.  The member may
  ;; still be deserialized with custom load hook.
  (dont-save nil :type boolean :read-only t)
  ;; CAPNP-TYPE may be a string specifying the type, or a list of
  ;; (member-symbol "capnp-type") specifying a union type.
  (capnp-type nil :type (or null string list) :read-only t)
  (capnp-init t :type boolean :read-only t)
  ;; Custom saving and loading code. May be a function which takes 2
  ;; args: (builder-or-reader member-name) and needs to return C++ code.
  (capnp-save nil :type (or null function (eql :dont-save)) :read-only t)
  (capnp-load nil :type (or null function) :read-only t)
  ;; May be a function which takes 1 argument, member-name.  It needs to
  ;; return C++ code.
  (slk-save nil :type (or null function) :read-only t)
  (slk-load nil :type (or null function) :read-only t))

(defstruct capnp-opts
  "Cap'n Proto serialization options for C++ class."
  ;; BASE is T if the class should be treated as a base class for capnp, even
  ;; though it may have parents.
  (base nil :type boolean :read-only t)
  ;; Extra arguments to the generated save function. List of (name cpp-type).
  (save-args nil :read-only t)
  (load-args nil :read-only t)
  ;; Function to be called after saving the instance. Lambda taking builder name as only argument.
  (post-save nil :read-only t)
  (construct nil :read-only t)
  ;; Explicit instantiation of template to generate schema with enum.
  (type-args nil :read-only t)
  ;; In case of multiple inheritance, list of classes which should be handled
  ;; as a composition.
  (inherit-compose nil :read-only t)
  ;; In case of multiple inheritance, pretend we only inherit the 1st base class.
  (ignore-other-base-classes nil :type boolean :read-only t))

(defstruct slk-opts
  "SLK serialization options for C++ class."
  ;; BASE is T if the class should be treated as a base class for SLK, even
  ;; though it may have parents.
  (base nil :type boolean :read-only t)
  ;; Extra arguments to the generated save function. List of (name cpp-type).
  (save-args nil :read-only t)
  (load-args nil :read-only t)
  ;; In case of multiple inheritance, pretend we only inherit the 1st base class.
  (ignore-other-base-classes nil :type boolean :read-only t))

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
   (slk-opts :type (or null slk-opts) :initarg :slk-opts :initform nil
             :reader cpp-class-slk-opts)
   (inner-types :initarg :inner-types :initform nil :reader cpp-class-inner-types)
   (abstractp :initarg :abstractp :initform nil :reader cpp-class-abstractp))
  (:documentation "Meta information on a C++ class (or struct)."))

(defvar *cpp-classes* nil "List of defined classes from LCP file")
(defvar *cpp-enums* nil "List of defined enums from LCP file")

(defun cpp-class-members-for-save (cpp-class)
  (check-type cpp-class cpp-class)
  (remove-if #'cpp-member-dont-save (cpp-class-members cpp-class)))

(defun make-cpp-primitive-type (name)
  "Create an instance of CPP-PRIMITIVE-TYPE given the arguments."
  (check-type name cpp-primitive-type-keywords)
  (make-instance 'cpp-primitive-type :name name))

(defun make-cpp-type (name &key namespace enclosing-class type-params type-args)
  "Create an instance of `CPP-TYPE' given the arguments.  Check the
documentation on `CPP-TYPE' members for function arguments."
  (check-type name (or symbol string))
  (check-type namespace list)
  (check-type enclosing-class (or null symbol string))
  (check-type type-params list)
  (check-type type-args list)
  (when (and type-params type-args)
    (error "A CPP-TYPE can't have both of TYPE-PARAMS and TYPE-ARGS"))
  (let ((namespace (if (and namespace
                            (string= (string-trim +whitespace-chars+ (car namespace)) ""))
                       (cdr namespace)
                       namespace)))
    (loop for ns in namespace
       when (or (find-if (lambda (c) (member c +whitespace-chars+ :test #'char=)) ns)
                (string= ns ""))
       do (error "Invalid namespace name ~S in ~S" ns namespace))
    (make-instance 'cpp-type
                   :name name
                   :namespace namespace
                   :enclosing-class enclosing-class
                   :type-params type-params
                   :type-args (mapcar #'cpp-type type-args))))

(defun cpp-type= (a b)
  (let ((a (cpp-type a))
        (b (cpp-type b)))
    (with-accessors ((args1 cpp-type-type-args)) a
      (with-accessors ((args2 cpp-type-type-args)) b
        (and (equalp (cpp-type-namespace a) (cpp-type-namespace b))
             (equalp (cpp-type-name a) (cpp-type-name b))
             (and (= (length args1) (length args2))
                  (every #'cpp-type= args1 args2))
             (string=
              (cpp-type-name (cpp-type-enclosing-class a))
              (cpp-type-name (cpp-type-enclosing-class b))))))))

(defmethod print-object ((cpp-type cpp-type) stream)
  (print-unreadable-object (cpp-type stream :type t)
    (with-accessors ((name cpp-type-base-name)
                     (ns cpp-type-namespace)
                     (params cpp-type-type-params)
                     (args cpp-type-type-args))
        cpp-type
      (format stream "~a" (cpp-type-decl cpp-type)))))

(defgeneric cpp-type-name (cpp-type)
  (:documentation "Get C++ style type name from `CPP-TYPE' as a string."))

(defmethod cpp-type-name ((cpp-type string))
  "Return CPP-TYPE string as is."
  cpp-type)

(defmethod cpp-type-name ((cpp-type cpp-type))
  "Return `CPP-TYPE' name as PascalCase or if string, as is."
  (cpp-type-name (cpp-type-base-name cpp-type)))

(defmethod cpp-type-name ((cpp-type symbol))
  "Return PascalCase of CPP-TYPE symbol or lowercase if it is a primitive type."
  (if (typep cpp-type 'cpp-primitive-type-keywords)
      (string-downcase cpp-type)
      (remove #\- (string-capitalize cpp-type))))

(defun cpp-primitive-type-p (type-decl)
  "Whether the C++ type designated by TYPE-DECL is a primitive type."
  (typep (cpp-type type-decl) 'cpp-primitive-type))

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
  (check-type type-decl string)
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
        (make-instance 'cpp-primitive-type :name (string-downcase
                                                  (car type-keyword))))))
  ;; Check if pointer
  (let ((ptr-pos (position #\* type-decl :from-end t)))
    (when (and ptr-pos (not (cl-ppcre:scan "[()<>]" type-decl :start ptr-pos)))
      (return-from parse-cpp-type-declaration
        (make-cpp-type (subseq type-decl ptr-pos)
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
      (let (namespace enclosing-class namespace-done-p)
        (when (cdr namespace-split)
          (dolist (ns (butlast namespace-split))
            ;; Treat capitalized namespace as designating an enclosing class.
            ;; Only the final enclosing class is taken, because we assume that
            ;; we can get enclosing classes recursively via `FIND-CPP-CLASS'.
            ;; This won't work if the classes are not defined in LCP.
            (cond
              ((and (string/= "" ns) (upper-case-p (aref ns 0)))
               (setf namespace-done-p t)
               (setf enclosing-class ns))
              ((not namespace-done-p)
               (push ns namespace))))
          (setf namespace (reverse namespace)))
        (make-cpp-type name
                       :namespace namespace
                       :enclosing-class enclosing-class
                       :type-args (reverse type-args))))))

(defun cpp-type-namespace-string (cpp-type)
  "Return the namespace part of CPP-TYPE as a string ending with '::'.  When
CPP-TYPE has no namespace, return an empty string."
  (format nil "~{~A::~}" (cpp-type-namespace cpp-type)))

;; TODO: use CPP-TYPE, CPP-TYPE= and CPP-PRIMITIVE-TYPE-P in the rest of the
;; code
(defun cpp-type (type-designator)
  "Coerce the CPP-TYPE designator TYPE-DESIGNATOR into a CPP-TYPE instance.

- If TYPE-DESIGNATOR is an instance of CPP-TYPE, CPP-PRIMITIVE-TYPE or
  CPP-CLASS, just return it.

- If TYPE-DESIGNATOR is one of the keywords in +CPP-PRIMITIVE-TYPE-KEYWORDS+,
  return an instance of CPP-PRIMITIVE-TYPE with the name being the result
  of (string-downcase type-designator).

- If TYPE-DESIGNATOR is any other symbol, return an instance of CPP-TYPE with
  the name being the result of (remove #\- (string-capitalize type-designator)).

- If TYPE-DESIGNATOR is a string, return an instance of CPP-TYPE with the name
  being that string."
  (ctypecase type-designator
    ((or cpp-type cpp-primitive-type cpp-class)
     type-designator)
    (cpp-primitive-type-keywords
     (make-cpp-primitive-type type-designator))
    ((or symbol string)
     (let ((primitive-type
            (member type-designator +cpp-primitive-type-keywords+ :test #'string-equal)))
       (if primitive-type
           (make-cpp-primitive-type (car primitive-type))
           (make-cpp-type
            (if (symbolp type-designator)
                (remove #\- (string-capitalize type-designator))
                type-designator)))))))

(defun find-cpp-class (cpp-class-name)
  "Find `CPP-CLASS' in *CPP-CLASSES* by CPP-CLASS-NAME"
  (check-type cpp-class-name (or symbol string))
  ;; TODO: Find by full name
  (if (stringp cpp-class-name)
      (find cpp-class-name *cpp-classes* :key #'cpp-type-name :test #'string=)
      (find cpp-class-name *cpp-classes* :key #'cpp-type-base-name)))

(defun find-cpp-enum (cpp-enum-name)
  "Find `CPP-ENUM' in *CPP-ENUMS* by CPP-ENUM-NAME"
  (check-type cpp-enum-name (or symbol string))
  (if (stringp cpp-enum-name)
      (or (find (parse-cpp-type-declaration cpp-enum-name) *cpp-enums* :test #'cpp-type=)
          (find cpp-enum-name *cpp-enums* :key #'cpp-type-name :test #'string=))
      (find cpp-enum-name *cpp-enums* :key #'cpp-type-base-name)))

(defun direct-subclasses-of (cpp-class)
  "Find direct subclasses of CPP-CLASS from *CPP-CLASSES*"
  (check-type cpp-class (or symbol cpp-class))
  (let ((name (if (symbolp cpp-class) cpp-class (cpp-type-base-name cpp-class))))
    (reverse ;; reverse to get them in definition order
     (remove-if (lambda (subclass)
                  (not (member name (cpp-class-super-classes subclass))))
                *cpp-classes*))))

(defun cpp-type-decl (cpp-type &key (type-params t) (namespace t))
  "Return the fully qualified name of given CPP-TYPE."
  (check-type cpp-type cpp-type)
  (flet ((enclosing-classes (cpp-type)
           (declare (type cpp-type cpp-type))
           (let ((enclosing '()))
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
             (write-string (cpp-type-namespace-string cpp-type) s))
           (format s "~{~A~^::~}" (enclosing-classes cpp-type))
           (cond
             ((cpp-type-type-args cpp-type)
              (format s "<~{~A~^, ~}>" (mapcar #'cpp-type-decl
                                               (cpp-type-type-args cpp-type))))
             ((and type-params (cpp-type-type-params cpp-type))
              (format s "<~{~A~^, ~}>" (cpp-type-type-params cpp-type))))))))))

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
                                 :serializep ,(if (assoc :serialize options) t))))
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
  * :serialize -- either (:capnp) or (:slk).  Setting :capnp will generate
    the Cap'n Proto serialization code for the class members.  You may
    specifiy additional options after :capnp to fill the `CAPNP-OPTS' slots.
    Similarly, you may specify `SLK-OPTS' after :slk.
  * :abstractp -- if t, marks that this class cannot be instantiated
    (currently only useful in serialization code)

Larger example:

;; (lcp:define-class derived (base)
;;   ((val :int :reader t :initval 42))
;;   (:public #>cpp void set_val(int new_val) { val_ = new_val; } cpp<#)
;;   (:serialize (:capnp)))

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
            (serialize (cdr (assoc :serialize options)))
            (abstractp (second (assoc :abstractp options))))
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
                                 :capnp-opts ,(when (assoc :capnp serialize)
                                                `(make-capnp-opts ,@(cdr (assoc :capnp serialize))))
                                 :slk-opts ,(when (assoc :slk serialize)
                                              `(make-slk-opts ,@(cdr (assoc :slk serialize))))
                                 :abstractp ,abstractp
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
