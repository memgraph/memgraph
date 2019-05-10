;;; This file is an entry point for processing LCP files and generating the
;;; C++ code.

(in-package #:lcp)
(named-readtables:in-readtable lcp-syntax)

(defvar +vim-read-only+ "vim: readonly")
(defvar +emacs-read-only+ "-*- buffer-read-only: t; -*-")

(defvar *generating-cpp-impl-p* nil
  "T if we are currently writing the .cpp file.")

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

(defun type-info-declaration-for-class (cpp-class)
  (assert (and (not (cpp-type-type-params cpp-class))
               (not (cpp-type-type-args cpp-class))))
  (with-output-to-string (s)
    (write-line "static const utils::TypeInfo kType;" s)
    (let* ((type-info-basep (type-info-opts-base (cpp-class-type-info-opts cpp-class)))
           (virtual (if (and (or type-info-basep (not (cpp-class-super-classes cpp-class)))
                             (direct-subclasses-of cpp-class))
                        "virtual"
                        ""))
           (override (if (and (not type-info-basep)
                              (cpp-class-super-classes cpp-class))
                         "override"
                         "")))
      (format s "~A const utils::TypeInfo &GetTypeInfo() const ~A { return kType; }"
              virtual override))))

(defun type-info-definition-for-class (cpp-class)
  (assert (and (not (cpp-type-type-params cpp-class))
               (not (cpp-type-type-args cpp-class))))
  (with-output-to-string (s)
    (let ((super-classes (when (not (type-info-opts-base (cpp-class-type-info-opts cpp-class)))
                           (cpp-class-super-classes cpp-class))))
      (when (type-info-opts-ignore-other-base-classes (cpp-class-type-info-opts cpp-class))
        (setf super-classes (list (first super-classes))))
      (when (> (length super-classes) 1)
        (error "Unable to generate TypeInfo for class '~A' due to multiple inheritance!"
               (cpp-type-base-name cpp-class)))
      (flet ((get-super-type-info (super)
               (let ((super-class (find-cpp-class super)))
                 (format nil "&~A::kType"
                         (if super-class
                             (cpp-type-decl super-class)
                             (cpp-type-name super))))))
        (format s "const utils::TypeInfo ~A::kType{0x~XULL, \"~A\", ~A};~%"
                (if *generating-cpp-impl-p*
                    (cpp-type-name cpp-class)
                    ;; Use full type declaration if class definition
                    ;; isn't inside the .cpp file.
                    (cpp-type-decl cpp-class))
                ;; Use full type declaration for hash
                (fnv1a64-hash-string (cpp-type-decl cpp-class))
                (cpp-type-name cpp-class)
                (if super-classes (get-super-type-info (first super-classes)) "nullptr"))))))

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
              (write-line (type-info-declaration-for-class cpp-class) s))
            (format s "~%~{~A~%~}" (mapcar #'cpp-code (cpp-class-public cpp-class)))
            (format s "~{~%~A~}~%" (mapcar #'cpp-member-reader-definition reader-members))
            (format s "~{  ~%~A~}~%"
                    (mapcar #'member-declaration (cpp-class-members-scoped :public)))
            (when (cpp-class-clone-opts cpp-class)
              (format s "~%~A" (lcp.clone:clone-function-definition-for-class cpp-class)))))
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
        (let ((typeinfo-def (type-info-definition-for-class cpp-class)))
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
                                 virtual const override delete)
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
         (override (if (and override inline) "override" ""))
         (delete (if delete "= 0" "")))
    (raw-cpp-string
     #>cpp
     ${template} ${static/virtual}
     ${returns} ${namespace}${method-name}(${args}) ${const} ${override} ${delete}
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

;;;


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
  (flet ((decl-type-info (class-name))
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
          (:serialize (:slk)))
        (let ((req-class (find-cpp-class ',req-sym)))
          (unless (lcp.slk::save-extra-args req-class)
            (push ,(progn
                     #>cpp
                     static void Save(const ${req-name} &self, slk::Builder *builder);
                     cpp<#)
                  (cpp-class-public req-class))
            (in-impl
             ,(progn
                #>cpp
                void ${req-name}::Save(const ${req-name} &self, slk::Builder *builder) {
                  slk::Save(self, builder);
                }
                cpp<#)))
          (unless (lcp.slk::load-extra-args req-class)
            (push ,(progn #>cpp
                          static void Load(${req-name} *self, slk::Reader *reader);
                          cpp<#)
                  (cpp-class-public req-class))
            (in-impl
             ,(progn
                #>cpp
                void ${req-name}::Load(${req-name} *self, slk::Reader *reader) {
                  slk::Load(self, reader);
                }
                cpp<#))))
        (define-struct ,res-sym ()
          ,@(cdr response)
          (:public
           ,(decl-type-info res-name)
           ,(def-constructor res-name (second response)))
          (:serialize (:slk)))
        (let ((res-class (find-cpp-class ',res-sym)))
          (unless (lcp.slk::save-extra-args res-class)
            (push ,(progn
                     #>cpp
                     static void Save(const ${res-name} &self, slk::Builder *builder);
                     cpp<#)
                  (cpp-class-public res-class))
            (in-impl
             ,(progn
                #>cpp
                void ${res-name}::Save(const ${res-name} &self, slk::Builder *builder) {
                  slk::Save(self, builder);
                }
                cpp<#)))
          (unless (lcp.slk::load-extra-args res-class)
            (push ,(progn #>cpp
                          static void Load(${res-name} *self, slk::Reader *reader);
                          cpp<#)
                  (cpp-class-public res-class))
            (in-impl
             ,(progn
                #>cpp
                void ${res-name}::Load(${res-name} *self, slk::Reader *reader) {
                  slk::Load(self, reader);
                }
                cpp<#))))
        ,rpc-decl))))

(defun read-lcp (filepath)
  "Read the FILEPATH and return a list of C++ meta information that should be
formatted and output."
  (with-open-file (in-stream filepath)
    (let ((*readtable* (named-readtables:find-readtable 'lcp-syntax))
          (stream-pos 0))
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

(defun process-file (lcp-file &key slk-serialize)
  "Process a LCP-FILE and write the output to .hpp file in the same directory."
  (multiple-value-bind (filename extension)
      (uiop:split-name-type lcp-file)
    (assert (string= (string-downcase extension) "lcp"))
    (let ((hpp-file (concatenate 'string filename ".hpp"))
          ;; Unlike hpp, for cpp file use the full path. This allows us to
          ;; have our own accompanying .cpp files
          (cpp-file (concatenate 'string lcp-file ".cpp"))
          (serializep slk-serialize)
          ;; Reset globals
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
      ;; Collect types for serialization
      (let ((types-for-slk (when serializep
                             (append (remove-if (complement #'cpp-class-slk-opts) *cpp-classes*)
                                     (remove-if (complement #'cpp-enum-serializep) *cpp-enums*)))))
        (when types-for-slk
          ;; Append top-level declarations for SLK serialization
          (with-open-file (out hpp-file :direction :output :if-exists :append)
            (terpri out)
            (write-line "// SLK serialization declarations" out)
            (write-line "#include \"slk/serialization.hpp\"" out)
            (with-namespaced-output (out open-namespace)
              (open-namespace '("slk"))
              (dolist (type-for-slk types-for-slk)
                (ctypecase type-for-slk
                  (cpp-class
                   (format out "~A;~%" (lcp.slk:save-function-declaration-for-class type-for-slk))
                   (when (or (cpp-class-super-classes type-for-slk)
                             (direct-subclasses-of type-for-slk))
                     (format out "~A;~%" (lcp.slk:construct-and-load-function-declaration-for-class type-for-slk)))
                   (unless (cpp-class-abstractp type-for-slk)
                     (format out "~A;~%" (lcp.slk:load-function-declaration-for-class type-for-slk))))
                  (cpp-enum
                   (format out "~A;~%" (lcp.slk:save-function-declaration-for-enum type-for-slk))
                   (format out "~A;~%" (lcp.slk:load-function-declaration-for-enum type-for-slk))))))))
        ;; Generate the .cpp file.  Note, that some code may rely on the fact
        ;; that .cpp file is generated after .hpp.
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
            ;; Generate SLK serialization
            (when types-for-slk
              (write-line "// Autogenerated SLK serialization code" out)
              (with-namespaced-output (out open-namespace)
                (open-namespace '("slk"))
                (dolist (cpp-type types-for-slk)
                  (ctypecase cpp-type
                    (cpp-class
                     (format out "// Serialize code for ~A~2%" (cpp-type-name cpp-type))
                     ;; Top level functions
                     (write-line (lcp.slk:save-function-definition-for-class cpp-type) out)
                     (when (or (cpp-class-super-classes cpp-type)
                               (direct-subclasses-of cpp-type))
                       (format out "~A;~%" (lcp.slk:construct-and-load-function-definition-for-class cpp-type)))
                     (unless (cpp-class-abstractp cpp-type)
                       (write-line (lcp.slk:load-function-definition-for-class cpp-type) out)))
                    (cpp-enum
                     (write-line (lcp.slk:save-function-definition-for-enum cpp-type) out)
                     (write-line (lcp.slk:load-function-definition-for-enum cpp-type) out))))))))))))
