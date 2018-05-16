(defpackage #:lcp
  (:use #:cl)
  (:export #:define-class
           #:define-struct
           #:define-enum
           #:namespace
           #:pop-namespace
           #:capnp-namespace
           #:capnp-import
           #:capnp-type-conversion
           #:process-file))

(in-package #:lcp)

(defconstant +whitespace-chars+ '(#\Newline #\Space #\Return #\Linefeed #\Tab))

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

(deftype cpp-primitive-type ()
  `(member :bool :int :int32_t :int64_t :uint :uint32_t :uint64_t :float :double))

(defun cpp-primitive-type-p (type)
  (member type '(:bool :int :int32_t :int64_t :uint :uint32_t :uint64_t :float :double)))

;; TODO: Rename this to cpp-type and set it as the base class/struct for all
;; the other cpp type metainformation (`CPP-CLASS', `CPP-ENUM' ...)
(defstruct cpp-type-decl
  namespace
  name
  type-args)

(defun parse-cpp-type-declaration (type-decl)
  "Parse C++ type from TYPE-DECL string and return CPP-TYPE-DECL.

For example:

::std::pair<my_space::MyClass<std::function<void(int, bool)>, double>, char>

produces:

;; (cpp-type-decl
;;  :name pair
;;  :type-args ((cpp-type-decl
;;              :name MyClass
;;              :type-args ((cpp-type-decl :name function
;;                                         :type-args (cpp-type-decl :name void(int, bool)))
;;                          (cpp-type-decl :name double)))
;;              (cpp-type-decl :name char)))"
  (declare (type string type-decl))
  ;; C++ type can be declared as follows:
  ;; namespace::namespace::type<type-arg, type-arg> *
  ;; |^^^^^^^^^^^^^^^^^^^^|    |^^^^^^^^^^^^^^^^^^| | optional
  ;;       optional                 optional
  ;; type-args in template are recursively parsed
  ;; C++ may contain dependent names with 'typename' keyword, these aren't
  ;; supported here.
  ;; TODO: Add support for raw pointers as if they are templated
  ;; type. I.e. 'char *' should generate
  ;; (cpp-type-decl :name * :type-args (cpp-type-decl :name "char"))
  (when (search "typename" type-decl)
    (error "'typename' not supported in '~A'" type-decl))
  (destructuring-bind (full-name &optional template)
      (cl-ppcre:split "<" type-decl :limit 2)
    (let* ((namespace-split (cl-ppcre:split "::" full-name))
           (name (car (last namespace-split)))
           type-args)
      (when template
        ;; template ends with '>' character
        (let ((arg-start 0))
          (cl-ppcre:do-scans (match-start match-end reg-starts reg-ends
                                          "[a-zA-Z0-9_:<>()]+[,>]" template)
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
      (make-cpp-type-decl :namespace (when (cdr namespace-split)
                                       (butlast namespace-split))
                          :name name
                          :type-args (reverse type-args)))))

(defun string<-cpp-type-decl (type-decl)
  (declare (type cpp-type-decl type-decl))
  (with-output-to-string (s)
    (format s "~{~A::~}" (cpp-type-decl-namespace type-decl))
    (write-string (cpp-type-decl-name type-decl) s)
    (when (cpp-type-decl-type-args type-decl)
      (format s "<~{~A~^, ~}>"
              (mapcar #'string<-cpp-type-decl (cpp-type-decl-type-args type-decl))))))

(defstruct cpp-enum
  "Meta information on a C++ enum."
  (symbol nil :type symbol :read-only t)
  (documentation nil :type (or null string) :read-only t)
  (values nil :read-only t)
  (enclosing-class nil :type (or null symbol) :read-only t))

(defstruct cpp-member
  "Meta information on a C++ class (or struct) member variable."
  (symbol nil :type symbol :read-only t)
  (type nil :type (or cpp-primitive-type string) :read-only t)
  (initval nil :type (or null string integer float) :read-only t)
  (scope :private :type (member :public :protected :private) :read-only t)
  ;; TODO: Support giving a name for reader function.
  (reader nil :type boolean :read-only t)
  (documentation nil :type (or null string) :read-only t)
  ;; Custom saving and loading code. May be a function which takes 2
  ;; args: (archive member-name) and needs to return C++ code.
  (save-fun nil :type (or null string raw-cpp function) :read-only t)
  (load-fun nil :type (or null string raw-cpp function) :read-only t)
  (capnp-type nil :type (or null string) :read-only t)
  (capnp-init t :type boolean :read-only t)
  (capnp-save nil :type (or null function) :read-only t)
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

(defstruct cpp-class
  "Meta information on a C++ class (or struct)."
  (structp nil :type boolean :read-only t)
  (name nil :type symbol :read-only t)
  (super-classes nil :read-only t)
  (type-params nil :read-only t)
  (documentation "" :type (or null string) :read-only t)
  (members nil :read-only t)
  ;; Custom C++ code in 3 scopes. May be a list of C++ meta information or a
  ;; single element.
  (public nil :read-only t)
  (protected nil :read-only t)
  (private nil)
  (capnp-opts nil :type (or null capnp-opts) :read-only t)
  (namespace nil :read-only t)
  (inner-types nil)
  (enclosing-class nil :type (or null symbol)))

(defun cpp-type-name (name)
  "Get C++ style type name from NAME as a string."
  (typecase name
    (cpp-primitive-type (string-downcase (string name)))
    (symbol (remove #\- (string-capitalize (string name))))
    (string name)
    (otherwise (error "Unkown conversion to C++ type for ~S" (type-of name)))))

(defvar *cpp-classes* nil "List of defined classes from LCP file")

(defun find-cpp-class (cpp-class-name)
  "Find CPP-CLASS in *CPP-CLASSES* by CPP-CLASS-NAME"
  (declare (type (or symbol string) cpp-class-name))
  (if (stringp cpp-class-name)
      (find cpp-class-name *cpp-classes*
            :key (lambda (class) (cpp-type-name (cpp-class-name class)))
            :test #'string=)
      (find cpp-class-name *cpp-classes* :key #'cpp-class-name)))

(defun direct-subclasses-of (cpp-class)
  "Find direct subclasses of CPP-CLASS from *CPP-CLASSES*"
  (declare (type (or symbol cpp-class) cpp-class))
  (let ((name (if (symbolp cpp-class) cpp-class (cpp-class-name cpp-class))))
    (reverse ;; reverse to get them in definition order
     (remove-if (lambda (subclass)
                  (not (member name (cpp-class-super-classes subclass))))
                *cpp-classes*))))

(defun cpp-documentation (documentation)
  "Convert DOCUMENTATION to Doxygen style string."
  (declare (type string documentation))
  (format nil "/// ~A"
          (cl-ppcre:regex-replace-all
           (string #\Newline)
           documentation
           (format nil "~%/// "))))

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

(defun cpp-enum-definition (cpp-enum)
  "Get C++ style `CPP-ENUM' definition as a string."
  (declare (type cpp-enum cpp-enum))
  (with-output-to-string (s)
    (when (cpp-enum-documentation cpp-enum)
      (write-line (cpp-documentation (cpp-enum-documentation cpp-enum)) s))
    (format s "enum class ~A {~%" (cpp-type-name (cpp-enum-symbol cpp-enum)))
    (format s "~{ ~A~^,~%~}~%" (mapcar #'cpp-constant-name (cpp-enum-values cpp-enum)))
    (write-line "};" s)))

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
  (if (cpp-primitive-type-p (cpp-member-type cpp-member))
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
      (when (cpp-class-documentation cpp-class)
        (write-line (cpp-documentation (cpp-class-documentation cpp-class)) s))
      (when (cpp-class-type-params cpp-class)
        (cpp-template (cpp-class-type-params cpp-class) s))
      (if (cpp-class-structp cpp-class)
          (write-string "struct " s)
          (write-string "class " s))
      (format s "~A" (cpp-type-name (cpp-class-name cpp-class)))
      (when (cpp-class-super-classes cpp-class)
        (format s " : ~{public ~A~^, ~}"
                (mapcar #'cpp-type-name (cpp-class-super-classes cpp-class))))
      (write-line " {" s)
      (let ((reader-members (remove-if (lambda (m) (not (cpp-member-reader m)))
                                         (cpp-class-members cpp-class))))
        (when (or (cpp-class-public cpp-class) (cpp-class-members-scoped :public) reader-members)
          (unless (cpp-class-structp cpp-class)
            (write-line " public:" s))
          (format s "~{~A~%~}" (mapcar #'cpp-code (cpp-class-public cpp-class)))
          (format s "~{~%~A~}~%" (mapcar #'cpp-member-reader-definition reader-members))
          (format s "~{  ~%~A~}~%"
                  (mapcar #'member-declaration (cpp-class-members-scoped :public)))))
      (when (cpp-class-capnp-opts cpp-class)
        (let ((save (capnp-save-declaration cpp-class))
              (construct (capnp-construct-declaration cpp-class))
              (load (capnp-load-declaration cpp-class)))
          (when save (format s "  ~A;~%" save))
          (when construct (format s "  ~A;~%" construct))
          (when load (format s "  ~A;~%" load))))
      (when (or (cpp-class-protected cpp-class) (cpp-class-members-scoped :protected))
        (write-line " protected:" s)
        (format s "~{~A~%~}" (mapcar #'cpp-code (cpp-class-protected cpp-class)))
        (format s "~{  ~%~A~}~%"
                (mapcar #'member-declaration (cpp-class-members-scoped :protected))))
      (when (or (cpp-class-private cpp-class) (cpp-class-members-scoped :private))
        (write-line " private:" s)
        (format s "~{~A~%~}" (mapcar #'cpp-code (cpp-class-private cpp-class)))
        (format s "~{  ~%~A~}~%"
                (mapcar #'member-declaration (cpp-class-members-scoped :private))))
      (write-line "};" s))))

(defun cpp-class-full-name (class &key (type-args t))
  "Return the fully namespaced name of given CLASS."
  (declare (type cpp-class class))
  (flet ((enclosing-classes (cpp-class)
           (declare (type cpp-class cpp-class))
           (let (enclosing)
             (loop
                for class = cpp-class
                then (find-cpp-class (cpp-class-enclosing-class class))
                while class
                do (push (cpp-type-name (cpp-class-name class)) enclosing))
             enclosing)))
    (let* ((full-name (format nil "~{~A~^::~}" (enclosing-classes class)))
           (type-args (if (or (not type-args) (not (cpp-class-type-params class)))
                          ""
                          (format nil "<~{~A~^, ~}>" (mapcar #'cpp-type-name (cpp-class-type-params class))))))
      (concatenate 'string full-name type-args))))

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
  (let* ((type-params (cpp-class-type-params class))
         (template (if (or inline (not type-params)) "" (cpp-template type-params)))
         (static/virtual (cond
                           ((and inline static) "static")
                           ((and inline virtual) "virtual")
                           (t "")))
         (namespace (if inline "" (format nil "~A::" (cpp-class-full-name class))))
         (args (format nil "~{~A~^, ~}"
                       (mapcar (lambda (name-and-type)
                                 (format nil "~A ~A"
                                         (cpp-type-name (second name-and-type))
                                         (cpp-variable-name (first name-and-type))))
                               args)))
         (const (if const "const" ""))
         (override (if (and override inline) "override" "")))
    (raw-cpp-string
     #>cpp
     ${template} ${static/virtual}
     ${returns} ${namespace}${method-name}(${args}) ${const} ${override}
     cpp<#)))

(defun cpp-code (cpp)
  "Get a C++ string from given CPP meta information."
  (typecase cpp
    (raw-cpp (raw-cpp-string cpp))
    (cpp-class (cpp-class-definition cpp))
    (cpp-enum (cpp-enum-definition cpp))
    (string cpp)
    (null "")
    (otherwise (error "Unknown conversion to C++ for ~S" (type-of cpp)))))

(defun count-newlines (stream &key stop-position)
  (loop for pos = (file-position stream)
     and char = (read-char stream nil nil)
     until (or (not char) (and stop-position (> pos stop-position)))
     when (char= #\Newline char) count it))

(defun boost-serialization (cpp-class)
  "Add boost serialization code to `CPP-CLASS'."
  (labels ((get-serialize-code (member-name serialize-fun)
             (if serialize-fun
                 ;; Invoke or use serialize-fun
                 (ctypecase serialize-fun
                   ((or string raw-cpp) serialize-fun)
                   (function
                    (let ((res (funcall serialize-fun "ar" member-name)))
                      (check-type res (or raw-cpp string))
                      res)))
                 ;; Else use the default serialization
                 #>cpp
                 ar & ${member-name};
                 cpp<#))
           (save-member (member)
             (get-serialize-code
              (cpp-member-name member :struct (cpp-class-structp cpp-class))
              (cpp-member-save-fun member)))
           (load-member (member)
             (get-serialize-code
              (cpp-member-name member :struct (cpp-class-structp cpp-class))
              (cpp-member-load-fun member))))
    (let* ((members (cpp-class-members cpp-class))
           (split-serialization (some (lambda (m) (or (cpp-member-save-fun m)
                                                      (cpp-member-load-fun m)))
                                      members))
           (serialize-declaration
            (cond
              (split-serialization
               #>cpp
               BOOST_SERIALIZATION_SPLIT_MEMBER();

               template <class TArchive>
               void save(TArchive &ar, const unsigned int) const {
               cpp<#)
              (t ;; otherwise a single serialization function for save + load
               #>cpp
               template <class TArchive>
               void serialize(TArchive &ar, const unsigned int) {
               cpp<#)))
           (serialize-bases
            (when (cpp-class-super-classes cpp-class)
              (make-raw-cpp
               :string (format nil "~{ar & boost::serialization::base_object<~A>(*this);~^~%~}"
                               (mapcar #'cpp-type-name (cpp-class-super-classes cpp-class)))))))
      (append (list
               (make-raw-cpp
                :string (format nil "~%friend class boost::serialization::access;"))
               serialize-declaration
               ;; save
               serialize-bases)
              (mapcar #'save-member members)
              (when split-serialization
                ;; load
                (cons
                 #>cpp
                 }

                 template <class TArchive>
                 void load(TArchive &ar, const unsigned int) {
                 cpp<#
                 (cons serialize-bases
                       (mapcar #'load-member members))))
              (list #>cpp } cpp<#)))))

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
  (let ((class-name (if (symbolp cpp-class) cpp-class (cpp-class-name cpp-class))))
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
    (dolist (parent (cpp-class-super-classes class))
      (if (member parent (capnp-opts-inherit-compose capnp-opts))
          (push parent compose)
          (push parent union)))
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
             (cons (cpp-class-name class)
                   ;; Continue to supers only if this isn't marked as capnp base class.
                   (when (and (not (capnp-base-p class))
                              (capnp-union-and-compose-parents class))
                     (let ((first-parent (find-cpp-class
                                          (first (capnp-union-and-compose-parents class)))))
                       (if first-parent
                           (rec first-parent)
                           (list (first (capnp-union-and-compose-parents class)))))))))
    (cdr (rec cpp-class))))

(defvar *capnp-type-converters* nil
  "Pairs of (cpp-type capnp-type) which map the conversion of C++ types to
  Cap'n Proto types.")

(defun capnp-type-conversion (cpp-type capnp-type)
  (declare (type string cpp-type capnp-type))
  (push (cons cpp-type capnp-type) *capnp-type-converters*))

(defun capnp-type<-cpp-type (cpp-type)
  (typecase cpp-type
    (cpp-primitive-type
     (when (member cpp-type '(:int :uint))
       (error "Unable to get Capnp type for integer without specified width."))
     ;; Delete the _t suffix
     (cl-ppcre:regex-replace "_t$" (string-downcase cpp-type :start 1) ""))
    (string
     (let ((type-decl (parse-cpp-type-declaration cpp-type)))
       (cond
         ((string= "shared_ptr" (cpp-type-decl-name type-decl))
          (let ((class (find-cpp-class
                        ;; TODO: Use full type
                        (cpp-type-decl-name (first (cpp-type-decl-type-args type-decl))))))
            (unless class
              (error "Unable to determine base type for '~A'; use :capnp-type"
                     cpp-type))
            (let* ((parents (capnp-union-parents-rec class))
                   (top-parent (if parents (car (last parents)) (cpp-class-name class))))
                   (format nil "Utils.SharedPtr(~A)" (cpp-type-name top-parent)))))
         ((string= "vector" (cpp-type-decl-name type-decl))
          (format nil "List(~A)"
                  (capnp-type<-cpp-type
                   (string<-cpp-type-decl (first (cpp-type-decl-type-args type-decl))))))
         ((string= "optional" (cpp-type-decl-name type-decl))
          (format nil "Utils.Optional(~A)"
                  (capnp-type<-cpp-type
                   (string<-cpp-type-decl (first (cpp-type-decl-type-args type-decl))))))
         ((assoc cpp-type *capnp-type-converters* :test #'string=)
          (cdr (assoc cpp-type *capnp-type-converters* :test #'string=)))
         (t (cpp-type-name cpp-type)))))
    ;; Capnp only accepts uppercase first letter in types (PascalCase), so
    ;; this is the same as our conversion to C++ type name.
    (otherwise (cpp-type-name cpp-type))))

(defun capnp-schema-for-enum (cpp-enum)
  "Generate Cap'n Proto serialization schema for CPP-ENUM"
  (declare (type cpp-enum cpp-enum))
  (with-output-to-string (s)
    (format s "enum ~A {~%" (cpp-type-name (cpp-enum-symbol cpp-enum)))
    (loop for val in (cpp-enum-values cpp-enum) and field-number from 0
       do (format s "  ~A @~A;~%"
                  (string-downcase (cpp-type-name val) :end 1)
                  field-number))
    (write-line "}" s)))

(defun capnp-schema (cpp-class)
  "Generate Cap'n Proto serialiation schema for CPP-CLASS"
  (declare (type (or cpp-class cpp-enum symbol) cpp-class))
  (when (null cpp-class)
    (return-from capnp-schema))
  (when (cpp-enum-p cpp-class)
    (return-from capnp-schema (capnp-schema-for-enum cpp-class)))
  (let ((class-name (if (symbolp cpp-class) cpp-class (cpp-class-name cpp-class)))
        (members (when (cpp-class-p cpp-class) (cpp-class-members cpp-class)))
        (inner-types (when (cpp-class-p cpp-class) (cpp-class-inner-types cpp-class)))
        (union-subclasses (capnp-union-subclasses cpp-class))
        (type-params (when (cpp-class-p cpp-class) (cpp-class-type-params cpp-class)))
        (capnp-type-args (when (cpp-class-p cpp-class)
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
          (format s "struct ~A {~%" (cpp-type-name class-name))
          (dolist (compose compose-parents)
            (format s "  ~A @~A :~A;~%"
                    (field-name<-symbol compose)
                    field-number
                    (capnp-type<-cpp-type compose))
            (incf field-number))
          (when capnp-type-args
            (write-line "  union {" s)
            (dolist (type-arg capnp-type-args)
              (format s "  ~A @~A :Void;~%" (field-name<-symbol type-arg) field-number)
              (incf field-number))
            (write-line "  }" s))
          (dolist (member members)
            (format s "  ~A @~A :~A;~%"
                    (field-name<-symbol (cpp-member-symbol member))
                    field-number
                    (if (cpp-member-capnp-type member)
                        (cpp-member-capnp-type member)
                        (capnp-type<-cpp-type (cpp-member-type member))))
            (incf field-number))
          (dolist (inner inner-types)
            (write-line (capnp-schema inner) s))
          (when union-subclasses
            (write-line "  union {" s)
            (dolist (subclass union-subclasses)
              (format s "    ~A @~A :~A;~%"
                      (field-name<-symbol (cpp-class-name subclass))
                      field-number
                      (capnp-type<-cpp-type (cpp-class-name subclass)))
              (incf field-number))
            (write-line "  }" s))
          (write-line "}" s))))))

;;; Capnp C++ serialization code generation
;;;
;;; Algorithm is closely tied with the generated schema (see above).
;;;
;;; 1) Generate the method declaration.
;;;
;;; Two problems arise:
;;;  * inheritance and
;;;  * helper arguments (for tracking pointers or similar).
;;;
;;; The method will always take a pointer to a capnp::<T>::Builder
;;; class. Additional arguments are optional, and are supplied when declaring
;;; that the class should be serialized with capnp.
;;;
;;; To determine the concrete T we need to know whether this class is a
;;; derived one or is inherited from. If it is, then T needs to be the
;;; top-most parent that is modeled by union and not composition. (For the
;;; inheritance modeling problem, refer to the description of schema
;;; generation.) Obviously, the method now needs to be virtual. If this class
;;; has no inheritance in any direction, then we just use the class name for
;;; T prepended with capnp:: namespace.
;;;
;;; Helper arguments are obtained from SAVE-ARGS of `CAPNP-OPTS'.
;;;
;;; 2) Generate parent calls for serialization (if we have parent classes).
;;;
;;; For the first (and only) parent which is modeled through union, generate a
;;; <Parent>::Save call. The call is passed all of the arguments from out
;;; function declaration.
;;;
;;; Find our own concrete builder by traversing through the union schema of
;;; the base builder. It is expected (and required) that the parent call has
;;; initialized them correctly. We just need to initialize the most concrete
;;; builder.
;;;
;;; Other parents are required to be modelled through composition. Therefore,
;;; we generate calls to parents by passing builders for the composed structs.
;;;     auto parent_builder = builder->initParent();
;;;     Parent::Save(&parent_builder);
;;; Any additional helper arguments are also passed to the above call.
;;;
;;; 3) Generate member serialization.
;;;
;;; For primitive typed member, generate builder->setMember(member_); calls.
;;;
;;; For std types, generate hardcoded calls to our wrapper functions. Most of
;;; these require a lambda function which serializes the element inside the
;;; std class. This can be done recursively with this step.
;;;
;;; For composite types, check whether we have been given a custom save
;;; invocation. If not, assume that the type has a member function called Save
;;; which expects a builder for that type and any additional helper arguments.

(defun capnp-extra-args (cpp-class save-or-load)
  "Get additional arguments to Save/Load function for CPP-CLASS."
  (declare (type cpp-class cpp-class)
           (type (member :save :load) save-or-load))
  (loop for parent in (cons (cpp-class-name cpp-class) (capnp-union-parents-rec cpp-class))
     for opts = (cpp-class-capnp-opts (find-cpp-class parent))
     for args = (ecase save-or-load
                  (:save (capnp-opts-save-args opts))
                  (:load (capnp-opts-load-args opts)))
     when args return args))

(defun capnp-save-declaration (cpp-class &key (inline t))
  "Generate Cap'n Proto save function declaration for CPP-CLASS. If
INLINE is NIL, the declaration is namespaced for the class so that it can be
used for outside definition."
  (declare (type cpp-class cpp-class))
  (let* ((parents (capnp-union-parents-rec cpp-class))
         (top-parent-class (if parents
                               (cpp-class-full-name (find-cpp-class (car (last parents))))
                               (cpp-class-full-name cpp-class)))
         (builder-arg
          (list (if parents 'base-builder 'builder)
                (format nil "capnp::~A::Builder *" top-parent-class))))
    (cpp-method-declaration
     cpp-class "Save" :args (cons builder-arg (capnp-extra-args cpp-class :save))
     :virtual (and (not parents) (capnp-union-subclasses cpp-class))
     :const t :override parents :inline inline)))

(defun capnp-save-default (member-name member-type member-builder)
  "Generate the default call to save for member."
  (declare (type string member-name member-type member-builder))
  (let* ((type (parse-cpp-type-declaration member-type))
         (type-name (cpp-type-decl-name type)))
    (when (member type-name '("unique_ptr" "shared_ptr" "vector") :test #'string=)
      (error "Use a custom :capnp-save function for ~A ~A" type-name member-name))
    (let* ((cpp-class (find-cpp-class type-name)) ;; TODO: full type-name search
           (extra-args (when cpp-class
                         (mapcar (lambda (name-and-type)
                                   (cpp-variable-name (first name-and-type)))
                                 (capnp-extra-args cpp-class :save)))))
      (format nil "~A.Save(&~A~{, ~A~});"
              member-name member-builder extra-args))))

(defun capnp-save-code (cpp-class)
  "Generate Cap'n Proto saving code for CPP-CLASS"
  (declare (type cpp-class cpp-class))
  (with-output-to-string (s)
    (format s "~A {~%" (capnp-save-declaration cpp-class :inline nil))
    (flet ((parent-args (parent)
             (mapcar (lambda (name-and-type)
                       (cpp-variable-name (first name-and-type)))
                     (capnp-extra-args (find-cpp-class parent) :save))))
      (multiple-value-bind (direct-union-parents compose-parents)
          (capnp-union-and-compose-parents cpp-class)
        (declare (ignore direct-union-parents))
        ;; Handle the union inheritance calls first.
        (let ((parents (capnp-union-parents-rec cpp-class)))
          (when parents
            (let ((first-parent (first parents)))
              (format s " ~A::Save(base_builder~{, ~A~});~%"
                      (cpp-type-name first-parent) (parent-args first-parent)))
            (when (or compose-parents (cpp-class-members cpp-class))
              (format s "  auto ~A_builder = base_builder->~{get~A().~}init~A();~%"
                      (cpp-variable-name (cpp-class-name cpp-class))
                      (mapcar #'cpp-type-name (cdr (reverse parents)))
                      (cpp-type-name (cpp-class-name cpp-class)))
              (format s "  auto *builder = &~A_builder;~%"
                      (cpp-variable-name (cpp-class-name cpp-class))))))
        ;; Now handle composite inheritance calls.
        (dolist (parent compose-parents)
          (write-line "{" s)
          (format s "  auto ~A_builder = builder->init~A();~%"
                  (cpp-variable-name parent) (cpp-type-name parent))
          (format s " ~A::Save(&~A_builder~{, ~A~});~%"
                  (cpp-type-name parent) (cpp-variable-name parent) (parent-args parent))
          (write-line "}" s))))
    ;; Set the template instantiations
    (when (and (capnp-opts-type-args (cpp-class-capnp-opts cpp-class))
               (/= 1 (list-length (cpp-class-type-params cpp-class))))
      (error "Don't know how to save templated class ~A" (cpp-class-name cpp-class)))
    (let ((type-param (first (cpp-class-type-params cpp-class))))
      (dolist (type-arg (capnp-opts-type-args (cpp-class-capnp-opts cpp-class)))
        (format s "  if (std::is_same<~A, ~A>::value) { builder->set~A(); }"
                (cpp-type-name type-arg) (cpp-type-name type-param) (cpp-type-name type-arg))))
    (dolist (member (cpp-class-members cpp-class))
      (let ((member-name (cpp-member-name member :struct (cpp-class-structp cpp-class)))
            (member-builder (format nil "~A_builder" (cpp-member-name member :struct t)))
            (capnp-name (cpp-type-name (cpp-member-symbol member))))
        (cond
          ((cpp-primitive-type-p (cpp-member-type member))
           (format s "  builder->set~A(~A);~%" capnp-name member-name))
          (t
           (write-line "{" s) ;; Enclose larger save code in new scope
           (let ((size (if (string= "vector" (cpp-type-decl-name
                                              (parse-cpp-type-declaration
                                               (cpp-member-type member))))
                           (format nil "~A.size()" member-name)
                           "")))
             (if (cpp-member-capnp-init member)
                 (format s "  auto ~A = builder->init~A(~A);~%"
                         member-builder capnp-name size)
                 (setf member-builder "builder")))
           (if (cpp-member-capnp-save member)
               (format s "  ~A~%"
                       (cpp-code (funcall (cpp-member-capnp-save member)
                                          member-builder member-name)))
               (write-line (capnp-save-default member-name
                                               (cpp-member-type member)
                                               member-builder)
                           s))
           (write-line "}" s)))))
    (write-line "}" s)))

;;; Capnp C++ deserialization code generation
;;;
;;; This is almost the same as serialization, but with a special case for
;;; handling serialized pointers to base classes.
;;;
;;; We need to generate a static Construct function, which dispatches on the
;;; union of the Capnp struct schema. This needs to recursively traverse
;;; unions to instantiate a concrete C++ class.
;;;
;;; When loading such a pointer, first we call Construct and then follow it
;;; with calling Load on the constructed instance.

(defun capnp-construct-declaration (cpp-class &key (inline t))
  "Generate Cap'n Proto construct function declaration for CPP-CLASS. If
INLINE is NIL, the declaration is namespaced for the class so that it can be
used for outside definition."
  (declare (type cpp-class cpp-class))
  ;; Don't generate Construct if this class is only inherited as composite.
  (when (and (not (capnp-union-subclasses cpp-class))
             (direct-subclasses-of cpp-class))
    (return-from capnp-construct-declaration))
  (let ((reader-type (format nil "const capnp::~A::Reader &"
                             (cpp-class-full-name cpp-class :type-args nil))))
    (cpp-method-declaration
     cpp-class "Construct" :args (list (list 'reader reader-type))
     :returns (format nil "std::unique_ptr<~A>" (cpp-class-full-name cpp-class))
     :inline inline :static t)))

(defun capnp-construct-code (cpp-class)
  "Generate Cap'n Proto construct code for CPP-CLASS"
  (declare (type cpp-class cpp-class))
  (let ((construct-declaration (capnp-construct-declaration cpp-class :inline nil)))
    (unless construct-declaration
      (return-from capnp-construct-code))
    (let ((class-name (cpp-class-name cpp-class))
          (union-subclasses (capnp-union-subclasses cpp-class)))
      (with-output-to-string (s)
        (format s "~A {~%" construct-declaration)
        (if (not union-subclasses)
            ;; No inheritance, just instantiate the concrete class.
            (format s "  return std::unique_ptr<~A>(new ~A());~%"
                    (cpp-type-name class-name) (cpp-type-name class-name))
            ;; Inheritance, so forward the Construct.
            (progn
              (write-line "  switch (reader.which()) {" s)
              (dolist (subclass union-subclasses)
                (format s "    case capnp::~A::~A:~%"
                        (cpp-type-name class-name)
                        (cpp-constant-name (cpp-class-name subclass)))
                (let ((subclass-name (cpp-type-name (cpp-class-name subclass))))
                  (if (capnp-opts-type-args (cpp-class-capnp-opts subclass))
                      ;; Handle template instantiation
                      (progn
                        (format s "  switch (reader.get~A().which()) {~%" subclass-name)
                        (dolist (type-arg (capnp-opts-type-args (cpp-class-capnp-opts subclass)))
                          (format s "  case capnp::~A::~A: return ~A<~A>::Construct(reader.get~A());~%"
                                  subclass-name (cpp-constant-name type-arg)
                                  subclass-name (cpp-type-name type-arg)
                                  subclass-name))
                        (write-line "  }" s))
                      ;; Just forward the construct normally.
                      (format s "      return ~A::Construct(reader.get~A());~%"
                              subclass-name subclass-name))))
              (write-line "  }" s))) ;; close switch
        (write-line "}" s))))) ;; close function

(defun capnp-load-default (member-name member-type member-reader)
  "Generate default load call for member."
  (declare (type string member-name member-type member-reader))
  (let* ((type (parse-cpp-type-declaration member-type))
         (type-name (cpp-type-decl-name type)))
    (when (member type-name '("unique_ptr" "shared_ptr" "vector") :test #'string=)
      (error "Use a custom :capnp-load function for ~A ~A" type-name member-name))
    (let* ((cpp-class (find-cpp-class type-name)) ;; TODO: full type-name search
           (extra-args (when cpp-class
                         (mapcar (lambda (name-and-type)
                                   (cpp-variable-name (first name-and-type)))
                                 (capnp-extra-args cpp-class :load)))))
      (format nil "~A.Load(~A~{, ~A~});"
              member-name member-reader extra-args))))

(defun capnp-load-declaration (cpp-class &key (inline t))
  "Generate Cap'n Proto load function declaration for CPP-CLASS. If
INLINE is NIL, the declaration is namespaced for the class so that it can be
used for outside definition."
  (declare (type cpp-class cpp-class))
  (let* ((parents (capnp-union-parents-rec cpp-class))
         (top-parent-class (if parents
                               (cpp-class-full-name (find-cpp-class (car (last parents))))
                               (cpp-class-full-name cpp-class)))
         (reader-arg
          (list (if parents 'base-reader 'reader)
                (format nil "const capnp::~A::Reader &" top-parent-class))))
    (cpp-method-declaration
     cpp-class "Load" :args (cons reader-arg (capnp-extra-args cpp-class :load))
     :virtual (and (not parents) (capnp-union-subclasses cpp-class))
     :override parents :inline inline)))

(defun capnp-load-code (cpp-class)
  "Generate Cap'n Proto loading code for CPP-CLASS"
  (declare (type cpp-class cpp-class))
  (with-output-to-string (s)
    (format s "~A {~%" (capnp-load-declaration cpp-class :inline nil))
    (flet ((parent-args (parent)
             (mapcar (lambda (name-and-type)
                       (cpp-variable-name (first name-and-type)))
                     (capnp-extra-args (find-cpp-class parent) :load))))
      (multiple-value-bind (direct-union-parents compose-parents)
          (capnp-union-and-compose-parents cpp-class)
        (declare (ignore direct-union-parents))
        ;; Handle the union inheritance calls first.
        (let ((parents (capnp-union-parents-rec cpp-class)))
          (when parents
            (let ((first-parent (first parents)))
              (format s "  ~A::Load(base_reader~{, ~A~});~%"
                      (cpp-type-name first-parent) (parent-args first-parent)))
            (when (or compose-parents (cpp-class-members cpp-class))
              (format s "  auto reader = base_reader.~{get~A().~}get~A();~%"
                      (mapcar #'cpp-type-name (cdr (reverse parents)))
                      (cpp-type-name (cpp-class-name cpp-class))))
            ;; Now handle composite inheritance calls.
            (dolist (parent compose-parents)
              (write-line "{" s)
              (format s "  auto ~A_reader = reader.get~A();~%"
                      (cpp-variable-name parent) (cpp-type-name parent))
              (format s "  ~A::Load(~A_reader~{, ~A~});~%"
                      (cpp-type-name parent) (cpp-variable-name parent) (parent-args parent))
              (write-line "}" s))))))
    (dolist (member (cpp-class-members cpp-class))
      (let ((member-name (cpp-member-name member :struct (cpp-class-structp cpp-class)))
            (member-reader (format nil "~A_reader" (cpp-member-name member :struct t)))
            (capnp-name (cpp-type-name (cpp-member-symbol member))))
        (cond
          ((cpp-primitive-type-p (cpp-member-type member))
           (format s "  ~A = reader.get~A();~%" member-name capnp-name))
          (t
           (write-line "{" s) ;; Enclose larger load code in new scope
           (if (cpp-member-capnp-init member)
               (format s "  auto ~A = reader.get~A();~%" member-reader capnp-name)
               (setf member-reader "reader"))
           (if (cpp-member-capnp-load member)
               (format s "  ~A~%"
                       (cpp-code (funcall (cpp-member-capnp-load member)
                                          member-reader member-name)))
               (write-line (capnp-load-default member-name
                                               (cpp-member-type member)
                                               member-reader) s))
           (write-line "}" s)))))
    (write-line "}" s)))

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

(defvar *cpp-inner-types* nil
  "List of cpp types defined inside an enclosing class or struct")

(defvar *cpp-enclosing-class* nil
  "Symbol name of the `CPP-CLASS' inside which inner types are defined.")

(defmacro define-enum (name maybe-documentation &rest values)
  "Define a C++ enum. Documentation is optional. Syntax is:

;; (define-enum name
;;   [documentation-string]
;;   value1 value2 ...)"
  (declare (type symbol name)
           (type (or string symbol) maybe-documentation))
  (let ((documentation (when (stringp maybe-documentation) maybe-documentation))
        (all-values (if (symbolp maybe-documentation)
                        (cons maybe-documentation values)
                        values))
        (enum (gensym (format nil "ENUM-~A" name))))
    `(let ((,enum (make-cpp-enum :symbol ',name
                                 :documentation ,documentation
                                 :values ',all-values
                                 :enclosing-class *cpp-enclosing-class*)))
       (prog1 ,enum
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
  * :save-fun -- Custom code for serializing this member.
  * :load-fun -- Custom code for deserializing this member.

Currently supported class-options are:
  * :documentation -- Doxygen documentation of the class.
  * :public -- additional C++ code in public scope.
  * :protected -- additional C++ code in protected scope.
  * :private -- additional C++ code in private scope.
  * :serialize -- only :boost and :capnp are valid values.  Setting :boost
    will generate boost serialization code for the class members.  Setting
    :capnp will generate the Cap'n Proto serialization code for the class
    members.  You may specifiy additional options after :capnp to fill the
    `CAPNP-OPTS' slots.

Larger example:

;; (lcp:define-class derived (base)
;;   ((val :int :reader t :initval 42))
;;   (:public #>cpp void set_val(int new_val) { val_ = new_val; } cpp<#)
;;   (:serialize :boost))

Generates C++:

;; class Derived : public Base {
;;  public:
;;   void set_val(int new_val) { val_ = new_val; }
;;   auto val() { return val_; } // autogenerated from :reader t
;;
;;  private:
;;   friend class boost::serialization::access;
;;   template <class TArchive>
;;   void serialize(TArchive &ar, unsigned int) {
;;     ar & boost::serialization::base_object<Base>(*this);
;;     ar & val_;
;;   }
;;
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
                  (make-cpp-class :name ',class-name :super-classes ',super-classes
                                  :type-params ',type-params
                                  :structp ,(second (assoc :structp options))
                                  :members (list ,@members)
                                  :documentation ,(second (assoc :documentation options))
                                  :public (list ,@(cdr (assoc :public options)))
                                  :protected (list ,@(cdr (assoc :protected options)))
                                  :private (list ,@(cdr (assoc :private options)))
                                  :capnp-opts ,(when (member :capnp serialize)
                                                 `(make-capnp-opts ,@(cdr (member :capnp serialize))))
                                  :namespace (reverse *cpp-namespaces*)
                                  ;; Set inner types at the end. This works
                                  ;; because CL standard specifies order of
                                  ;; evaluation from left to right.
                                  :inner-types *cpp-inner-types*))))
           (prog1 ,class
             (push ,class *cpp-classes*)
             ;; Set the parent's inner types
             (push ,class *cpp-inner-types*)
             (setf (cpp-class-enclosing-class ,class) *cpp-enclosing-class*)
             ,(when (eq :boost (car serialize))
                `(setf (cpp-class-private ,class)
                       (append (cpp-class-private ,class) (boost-serialization ,class))))))))))

(defmacro define-struct (name super-classes slots &rest options)
  `(define-class ,name ,super-classes ,slots (:structp t) ,@options))

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
                           (error "~%~A:~A: error:~%~%~A~%~%in:~%~%~A"
                                  (uiop:native-namestring filepath)
                                  (count-newlines in-stream :stop-position (1+ stream-pos))
                                  err form)))
             do (setf stream-pos (file-position in-stream))
             when (typep res '(or raw-cpp cpp-class cpp-enum))
             collect res)
        (end-of-file ()
          (file-position in-stream 0) ;; start of stream
          (error "~%~A:~A:error: READ error, did you forget a closing ')'?"
                 (uiop:native-namestring filepath)
                 (count-newlines in-stream
                                 :stop-position (1+ stream-pos))))))))

(defun generate-capnp (cpp-classes &key capnp-file capnp-id cpp-file hpp-file lcp-file)
  "Generate Cap'n Proto serialization code for given CPP-CLASSES.  The schema
is written to CAPNP-FILE using the CAPNP-ID.  The C++ serialization code is
written to CPP-FILE.  This source file will include the provided HPP-FILE.
Original LCP-FILE is used just to insert a comment about the source of the
code generation."
  (with-open-file (out capnp-file :direction :output :if-exists :supersede)
    (format out "# Autogenerated using LCP from '~A'~%~%" lcp-file)
    (format out "~A;~%~%" capnp-id)
    (write-line "using Cxx = import \"/capnp/c++.capnp\";" out)
    (format out "$Cxx.namespace(\"~A::capnp\");~%~%" *capnp-namespace*)
    (dolist (capnp-import *capnp-imports* (terpri out))
      (format out "using ~A = import ~S;~%"
              (remove #\- (string-capitalize (car capnp-import)))
              (cdr capnp-import)))
    (dolist (cpp-class cpp-classes)
      ;; Generate schema only for top level classes, inner classes are handled
      ;; inside the generation of the enclosing class.
      (unless (cpp-class-enclosing-class cpp-class)
        (let ((schema (capnp-schema cpp-class)))
          (when schema (write-line schema out))))))
  ;; Now generate the save/load C++ code in the cpp file.
  (with-open-file (out cpp-file :direction :output :if-exists :supersede)
    (format out "// Autogenerated using LCP from '~A'~%~%" lcp-file)
    (write-line "#include \"utils/serialization.hpp\"" out)
    (format out "#include \"~A\"~%~%" hpp-file)
    (let (open-namespaces)
      (dolist (cpp-class cpp-classes)
        ;; Check if we need to open or close namespaces
        (loop for namespace in (cpp-class-namespace cpp-class)
           with unmatched = open-namespaces do
             (if (string= namespace (car unmatched))
                 (setf unmatched (cdr unmatched))
                 (progn
                   (dolist (to-close unmatched)
                     (declare (ignore to-close))
                     (write-line "}" out))
                   (format out "namespace ~A {~%~%" namespace))))
        (setf open-namespaces (cpp-class-namespace cpp-class))
        ;; Output the serialization code
        (format out "// Serialize code for ~A~%~%"
                (cpp-type-name (cpp-class-name cpp-class)))
        (let ((save-code (capnp-save-code cpp-class))
              (construct-code (capnp-construct-code cpp-class))
              (load-code (capnp-load-code cpp-class)))
          (when save-code (write-line save-code out))
          (when construct-code (write-line construct-code out))
          (when load-code (write-line load-code out))))
      ;; Close remaining namespaces
      (dolist (to-close open-namespaces)
        (declare (ignore to-close))
        (write-line "}" out)))))

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
          (*capnp-namespace* nil)
          (*capnp-imports* nil)
          (*capnp-type-converters* nil)
          (*cpp-inner-types* nil)
          ;; Don't reset *cpp-classes* if we want to have support for
          ;; procesing multiple files.
          ;; (*cpp-classes* nil)
          )
      ;; First read and evaluate the whole file, then output the evaluated
      ;; cpp-code. This allows us to generate code which may rely on
      ;; evaluation done after the code definition.
      (with-open-file (out hpp-file :direction :output :if-exists :supersede)
        (format out "// Autogenerated using LCP from '~A'~%~%" lcp-file)
        (dolist (res (read-lcp lcp-file))
          (write-line (cpp-code res) out)))
      (when *cpp-namespaces*
        (error "Unclosed namespaces: ~A" (reverse *cpp-namespaces*)))
      ;; If we have a capnp-id, generate the schema
      (when capnp-id
        (let ((classes-for-capnp
               (remove-if (complement #'cpp-class-capnp-opts) *cpp-classes*)))
          (when classes-for-capnp
            (generate-capnp classes-for-capnp :capnp-file capnp-file :capnp-id capnp-id
                            :cpp-file cpp-file :hpp-file (file-namestring hpp-file)
                            :lcp-file lcp-file)))))))
