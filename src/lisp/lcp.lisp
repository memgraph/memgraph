;;; This file is an entry point for processing LCP files and generating the
;;; C++ code.

(in-package #:lcp)
(named-readtables:in-readtable lcp-syntax)

(defvar *generating-cpp-impl-p* nil
  "T if we are currently writing the .cpp file.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; C++ code generation

(defun cpp-enum-definition (cpp-enum)
  "Get C++ style `CPP-ENUM' definition as a string."
  (check-type cpp-enum cpp-enum)
  (with-output-to-string (s)
    (when (cpp-type-documentation cpp-enum)
      (write-line (cpp-documentation (cpp-type-documentation cpp-enum)) s))
    (with-cpp-block-output (s :name (format nil "enum class ~A" (cpp-type-name cpp-enum))
                              :semicolonp t)
      (format s "~{ ~A~^,~%~}~%" (cpp-enum-values cpp-enum)))))

(defun cpp-member-declaration (cpp-member)
  "Get C++ style `CPP-MEMBER' declaration as a string."
  (check-type cpp-member cpp-member)
  (let ((type-name (cpp-type-decl (cpp-member-type cpp-member))))
    (with-output-to-string (s)
      (when (cpp-member-documentation cpp-member)
        (write-line (cpp-documentation (cpp-member-documentation cpp-member)) s))
      (if (cpp-member-initval cpp-member)
          (format s "~A ~A{~A};"
                  type-name
                  (cpp-member-name cpp-member)
                  (cpp-member-initval cpp-member))
          (format s "~A ~A;"
                  type-name
                  (cpp-member-name cpp-member))))))

(defun cpp-member-reader-definition (cpp-member)
  "Get C++ style `CPP-MEMBER' getter (reader) function."
  (check-type cpp-member cpp-member)
  (if (cpp-type-primitive-p (cpp-member-type cpp-member))
      (format nil "auto ~A() const { return ~A; }"
              (cpp-member-reader-name cpp-member)
              (cpp-member-name cpp-member))
      (format nil "const auto &~A() const { return ~A; }"
              (cpp-member-reader-name cpp-member)
              (cpp-member-name cpp-member))))

(defun cpp-template (type-params &optional stream)
  "Generate C++ template declaration from provided TYPE-PARAMS. If STREAM is
NIL, returns a string."
  (format stream "template <~{class ~A~^,~^ ~}>" type-params))

(defun type-info-declaration-for-class (cpp-class)
  (assert (cpp-type-simple-class-p cpp-class))
  (with-output-to-string (s)
    (write-line "static const utils::TypeInfo kType;" s)
    (let* ((type-info-basep (type-info-opts-base
                             (cpp-class-type-info-opts cpp-class)))
           (virtual (if (and (or type-info-basep
                                 (not (cpp-class-super-classes cpp-class)))
                             (cpp-class-direct-subclasses cpp-class))
                        "virtual"
                        ""))
           (override (if (and (not type-info-basep)
                              (cpp-class-super-classes cpp-class))
                         "override"
                         "")))
      (format s "~A const utils::TypeInfo &GetTypeInfo() const ~A { return kType; }"
              virtual override))))

(defun type-info-definition-for-class (cpp-class)
  (assert (cpp-type-simple-class-p cpp-class))
  (with-output-to-string (s)
    (let ((super-classes (when (not (type-info-opts-base
                                     (cpp-class-type-info-opts cpp-class)))
                           (cpp-class-super-classes cpp-class))))
      (when (type-info-opts-ignore-other-base-classes
             (cpp-class-type-info-opts cpp-class))
        (setf super-classes (list (first super-classes))))
      (when (> (length super-classes) 1)
        (error "Unable to generate TypeInfo for class '~A' due to multiple inheritance!"
               (cpp-type-name cpp-class)))
      (format s "const utils::TypeInfo ~A::kType{0x~XULL, \"~A\", ~A};~%"
              (if *generating-cpp-impl-p*
                  (cpp-type-name cpp-class)
                  ;; Use full type declaration if class definition
                  ;; isn't inside the .cpp file.
                  (cpp-type-decl cpp-class))
              ;; Use full type declaration for hash
              (fnv1a64-hash-string (cpp-type-decl cpp-class))
              (cpp-type-name cpp-class)
              (if super-classes
                  (format nil "&~A::kType"
                          (cpp-type-decl (first super-classes)))
                  "nullptr")))))

(defun cpp-class-definition (cpp-class)
  "Get C++ definition of the CPP-CLASS as a string."
  (check-type cpp-class cpp-class)
  (flet ((cpp-class-members-scoped (scope)
           (remove-if (lambda (m) (not (eq scope (cpp-member-scope m))))
                      (cpp-class-members cpp-class)))
         (member-declaration (member)
           (cpp-member-declaration member)))
    (with-output-to-string (s)
      (terpri s)
      (when (cpp-type-documentation cpp-class)
        (write-line (cpp-documentation (cpp-type-documentation cpp-class)) s))
      (when (cpp-type-class-template-p cpp-class)
        (cpp-template (cpp-type-type-params cpp-class) s))
      (if (cpp-class-structp cpp-class)
          (write-string "struct " s)
          (write-string "class " s))
      (format s "~A" (cpp-type-name cpp-class))
      (let ((super-classes (cpp-class-super-classes cpp-class)))
        (when super-classes
          (format s " : ~{public ~A~^, ~}"
                  (mapcar #'cpp-type-decl super-classes))))
      (with-cpp-block-output (s :semicolonp t)
        (let ((reader-members (remove-if (complement #'cpp-member-reader)
                                         (cpp-class-members cpp-class))))
          (when (or (cpp-class-public cpp-class)
                    (cpp-class-members-scoped :public)
                    reader-members
                    ;; We at least have public TypeInfo object for non-template
                    ;; classes.
                    (not (cpp-type-class-template-p cpp-class)))
            (unless (cpp-class-structp cpp-class)
              (write-line " public:" s))
            ;; Skip generating TypeInfo for class templates.
            (unless (cpp-type-class-template-p cpp-class)
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
      ;; Define the TypeInfo object. Relies on the fact that *CPP-IMPL* is
      ;; processed later.
      (unless (cpp-type-class-template-p cpp-class)
        (let ((typeinfo-def (type-info-definition-for-class cpp-class)))
          (if *generating-cpp-impl-p*
              (write-line typeinfo-def s)
              (in-impl typeinfo-def)))))))

(defun cpp-function-declaration (name &key args (returns "void") type-params)
  "Generate, as a string, a top level C++ function declaration for the
function (or function template) named by NAME.

NAME is a namestring for a function.

ARGS is a list of (NAME TYPE) pairs representing the function's arguments, where
NAME is a namestring for a variable and TYPE is a CPP-TYPE.

RETURNS is a typestring for the return type of the function.

TYPE-PARAMS is a list of strings naming the template type parameters if NAME
names a function template rather than a function."
  (check-type name string)
  (check-type returns string)
  (let ((template (if type-params (cpp-template type-params) ""))
        (args (format nil "~:{~A ~A~:^, ~}"
                      (mapcar (lambda (name-and-type)
                                (list (cpp-type-decl (second name-and-type))
                                      (first name-and-type)))
                              args))))
    (raw-cpp-string
     #>cpp
     ${template}
     ${returns} ${name}(${args})
     cpp<#)))

(defun cpp-method-declaration (class method-name
                               &key args (returns "void") (inline t) static
                                 virtual const override delete)
  "Generate, as a string, a C++ method declaration for the method named by
METHOD-NAME of the C++ class CLASS.

ARGS is a list of (NAME TYPE) pairs representing the method's arguments, where
NAME is a variable namestring and TYPE is a CPP-TYPE.

RETURNS is a typestring for the return type of the method.

If INLINE is T, a declaration appropriate for inclusion into the body of a class
declaration is generated. Otherwise, a top level declaration is generated.

If VIRTUAL, CONST, OVERRIDE or DELETE is T, the corresponding C++ keyword is
included in the method declaration."
  (check-type class cpp-class)
  (check-type method-name string)
  (let* ((type-params (cpp-type-type-params class))
         (template (if (or inline (not type-params)) "" (cpp-template type-params)))
         (static/virtual (cond
                           ((and inline static) "static")
                           ((and inline virtual) "virtual")
                           (t "")))
         (namespace
           (if inline "" (format nil "~A::" (cpp-type-decl
                                             class :namespacep nil))))
         (args (format nil "~:{~A ~A~:^, ~}"
                       (mapcar (lambda (name-and-type)
                                 (list (cpp-type-decl (second name-and-type))
                                       (first name-and-type)))
                               args)))
         (const (if const "const" ""))
         (override (if (and override inline) "override" ""))
         (delete (if delete "= 0" "")))
    (raw-cpp-string
     #>cpp
     ${template} ${static/virtual}
     ${returns} ${namespace}${method-name}(${args}) ${const} ${override} ${delete}
     cpp<#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; C++ elements

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; The LCP Driver

(defvar +vim-read-only+ "vim: readonly")
(defvar +emacs-read-only+ "-*- buffer-read-only: t; -*-")

(defvar *cpp-namespaces* nil
  "Stack of C++ namespaces we are generating the code in.")

(defmacro namespace (name)
  "Push the NAME to currently set namespaces."
  (check-type name symbol)
  (let ((cpp-namespace (cpp-name-for-variable name)))
    `(progn
       (push ,cpp-namespace *cpp-namespaces*)
       (make-raw-cpp
        :string ,(format nil "~%namespace ~A {~%" cpp-namespace)))))

(defun pop-namespace ()
  (pop *cpp-namespaces*)
  #>cpp } cpp<#)

(defvar *cpp-impl* nil
  "List of (namespace . C++ code) pairs that should be written in the
  implementation (.cpp) file.")

(defun in-impl (&rest args)
  (let ((namespaces (reverse *cpp-namespaces*)))
    (setf *cpp-impl*
          (append *cpp-impl* (mapcar (lambda (cpp) (cons namespaces cpp))
                                     args)))))

(defun process-lcp (cpp-elements &key cpp hpp lcp-file hpp-file cpp-file slk-serialize-p)
  "Process a list of C++ elements.

To process a C++ element means to generate code for it and any of the
additional functionalities that have been specified.

A C++ element can be:

- an instance of CPP-CLASS or CPP-ENUM (e.g. returned by DEFINE-CLASS or
  DEFINE-ENUM) -- specifies the definition of a C++ class or enum, along with
  any additional functionality

- an instance of RAW-CPP -- specifies raw C++ code to include into the generated
  code

- an instance of CPP-LIST -- specifies a list of C++ elements which are
  processed recursively

CPP and HPP are streams to which the generated hpp and cpp code will be output.
Note that these can be any streams, not just file streams.

LCP-FILE, HPP-FILE and CPP-FILE are pathname designators that will be used for
the purposes of C++ includes or other such functionality. They can be omitted,
in which case the string \"<UNKNOWN>\" will be used.

SLK-SERIALIZE-P determines whether SLK serialization code is generated."
  (generate-hpp cpp-elements hpp
                :lcp-file lcp-file
                :hpp-file hpp-file
                :cpp-file cpp-file
                :slk-serialize-p slk-serialize-p)
  ;; NOTE: Some code may rely on the fact that the .cpp file is generated after
  ;; the .hpp.
  (let ((*generating-cpp-impl-p* t))
    (generate-cpp cpp-elements cpp
                  :lcp-file lcp-file
                  :hpp-file hpp-file
                  :cpp-file cpp-file
                  :slk-serialize-p slk-serialize-p)))

(defun generate-hpp (cpp-elements out &key lcp-file hpp-file cpp-file slk-serialize-p)
  "Process a list of C++ elements to generate C++ header file code.

OUT is a stream to write the generated code to.

LCP-FILE, HPP-FILE, CPP-FILE and SLK-SERIALIZE-P are as in PROCESS-LCP."
  (declare (ignore hpp-file cpp-file))
  (format out "~@{// ~A~%~}" +emacs-read-only+ +vim-read-only+)
  (format out "// DO NOT EDIT! Generated using LCP from '~A'~2%"
          (or lcp-file "<UNKNOWN>"))
  (dolist (res cpp-elements)
    (write-line (cpp-code res) out))
  (alexandria:when-let
      ((types-for-slk
        (when slk-serialize-p
          (append (remove-if (complement #'cpp-class-slk-opts) *cpp-classes*)
                  (remove-if (complement #'cpp-enum-serializep) *cpp-enums*)))))
    ;; Append top-level declarations for SLK serialization
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
                     (cpp-class-direct-subclasses type-for-slk))
             (format out "~A;~%" (lcp.slk:construct-and-load-function-declaration-for-class type-for-slk)))
           (unless (cpp-class-abstractp type-for-slk)
             (format out "~A;~%" (lcp.slk:load-function-declaration-for-class type-for-slk))))
          (cpp-enum
           (format out "~A;~%" (lcp.slk:save-function-declaration-for-enum type-for-slk))
           (format out "~A;~%" (lcp.slk:load-function-declaration-for-enum type-for-slk))))))))

(defun generate-cpp (cpp-elements out &key lcp-file hpp-file cpp-file slk-serialize-p)
  "Process a list of C++ elements to generate C++ source file code.

OUT is a stream to write the generated code to.

LCP-FILE, HPP-FILE, CPP-FILE and SLK-SERIALIZE-P are as in PROCESS-LCP."
  (declare (ignore cpp-elements cpp-file))
  (format out "~@{// ~A~%~}" +emacs-read-only+ +vim-read-only+)
  (format out "// DO NOT EDIT! Generated using LCP from '~A'~2%"
          (or lcp-file "<UNKNOWN>"))
  (format out "#include \"~A\"~2%" (or hpp-file "<UNKNOWN>"))
  ;; First output the C++ code from the user
  (with-namespaced-output (out open-namespace)
    (dolist (cpp *cpp-impl*)
      (destructuring-bind (namespaces . code) cpp
        (open-namespace namespaces)
        (write-line (cpp-code code) out))))
  ;; Generate SLK serialization
  (alexandria:when-let
      ((types-for-slk
        (when slk-serialize-p
          (append (remove-if (complement #'cpp-class-slk-opts) *cpp-classes*)
                  (remove-if (complement #'cpp-enum-serializep) *cpp-enums*)))))
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
                     (cpp-class-direct-subclasses cpp-type))
             (format out "~A;~%" (lcp.slk:construct-and-load-function-definition-for-class cpp-type)))
           (unless (cpp-class-abstractp cpp-type)
             (write-line (lcp.slk:load-function-definition-for-class cpp-type) out)))
          (cpp-enum
           (write-line (lcp.slk:save-function-definition-for-enum cpp-type) out)
           (write-line (lcp.slk:load-function-definition-for-enum cpp-type) out)))))))

(defun read-lcp (stream)
  "Read and evaluate LCP forms.

Forms are read from the stream STREAM and immediately evaluated, one by one. The
given stream is read until EOF is reached. In the case of a reading error, a
condition of type END-OF-FILE is signaled.

Return a list of results."
  (let ((*readtable* (named-readtables:find-readtable 'lcp-syntax)))
    (loop :for form := (read-preserving-whitespace stream nil 'eof)
          :until (eq form 'eof)
          :for res := (eval form)
          :when (typep res '(or raw-cpp cpp-type cpp-list))
            :collect res)))

(defun read-lcp-file (stream)
  "Read and evaluate LCP forms from a file stream.

The behavior is just as in READ-LCP, except that STREAM must be a file stream.
The reported error messages will contain the name of the file and the line
number on which the error ocurred.

In case of a reading error, a condition of type ERROR is signaled reporting the
file and the line number of the erroneous form.

In case of an evaluation error, a condition of type ERROR is signaled reporting
the file and the line number of the erroneous form. Additionally, a restart
named DECLINE will be established around the newly signaled condition which can
be used by a handler to force the signalling of the original error instead.

Return a list of results."
  (let ((filepath (pathname stream))
        (*readtable* (named-readtables:find-readtable 'lcp-syntax))
        (stream-pos 0))
    (handler-case
        (loop :for form := (read-preserving-whitespace stream nil 'eof)
              :until (eq form 'eof)
              :for res
                := (decline-case (eval form)
                     (error (err)
                       ;; Seek to the start of the stream.
                       (file-position stream 0)
                       (error "~%~A:~A: error:~2%~A~2%in:~2%~A"
                              (uiop:native-namestring filepath)
                              (count-newlines
                               stream
                               :stop-position (1+ stream-pos))
                              err form)))
              :do (setf stream-pos (file-position stream))
              :when (typep res '(or raw-cpp cpp-type cpp-list))
                :collect res)
      (end-of-file ()
        ;; Seek to the start of the stream.
        (file-position stream 0)
        (error "~%~A:~A:error: READ error, did you forget a closing ')'?"
               (uiop:native-namestring filepath)
               (count-newlines stream :stop-position (1+ stream-pos)))))))

(defun process-lcp-string (string &key slk-serialize-p)
  "Process the C++ elements produced by reading and evaluating LCP forms from
the string STRING.

SLK-SERIALIZE-P is as in PROCESS-LCP.

The generated code is returned as two values, both of which are strings. The
strings represent the C++ source file and C++ header file code respectively."
  (with-retry-restart (reprocess-lcp-string "Reprocess the LCP string")
    (restart-case
        (let* (;; Reset globals that influence the evaluation of LCP forms
               (*cpp-inner-types* :toplevel)
               (*cpp-impl* '())
               (*cpp-namespaces* '())
               (cpp-elements (with-input-from-string (lcp string)
                               (read-lcp lcp))))
          ;; Check for unclosed namespaces in the LCP file
          (when *cpp-namespaces*
            (error "Unclosed namespaces: ~A" (reverse *cpp-namespaces*)))
          ;; Process the result
          (with-output-to-string (hpp)
            (with-output-to-string (cpp)
              (process-lcp cpp-elements
                           :cpp cpp
                           :hpp hpp
                           :lcp-file "<PROCESS-LCP-STRING>"
                           :hpp-file "<PROCESS-LCP-STRING>"
                           :cpp-file "<PROCESS-LCP-STRING>"
                           :slk-serialize-p slk-serialize-p)
              (return-from process-lcp-string
                (values (get-output-stream-string hpp)
                        (get-output-stream-string cpp))))))
      (clean-reprocess-lcp-string ()
        :report "Reprocess the LCP string with a clean registry"
        (setf *cpp-classes* '()
              *cpp-enums* '())
        (invoke-restart 'reprocess-lcp-string)))))

(defun process-lcp-file (lcp-file &key slk-serialize-p)
  "Process the C++ elements produced by reading and evaluating LCP forms from
the file named by the pathname designator LCP-FILE.

SLK-SERIALIZE-P is as in PROCESS-LCP.

The generated code is written into two files. The files are in the same
directory as the LCP file. C++ source file code is written to
\"<LCP-FILENAME>.lcp.cpp\" while C++ header file code is written to
\"<LCP-FILENAME>.hpp\"."
  (multiple-value-bind (filename extension)
      (uiop:split-name-type lcp-file)
    (assert (string= (string-downcase extension) "lcp"))
    (let ((hpp-file (concatenate 'string filename ".hpp"))
          (cpp-file (concatenate 'string lcp-file ".cpp")))
      (with-retry-restart (reprocess-lcp-file "Reprocess the LCP file")
        (restart-case
            (let* (;; Reset globals that influence the evaluation of LCP forms
                   (*cpp-inner-types* :toplevel)
                   (*cpp-impl* '())
                   (*cpp-namespaces* '())
                   (cpp-elements (with-open-file (lcp lcp-file)
                                   (read-lcp-file lcp))))
              ;; Check for unclosed namespaces in the LCP file
              (when *cpp-namespaces*
                (error "Unclosed namespaces: ~A" (reverse *cpp-namespaces*)))
              ;; Process the results
              (with-open-file (hpp hpp-file :direction :output :if-exists :supersede)
                (with-open-file (cpp cpp-file :direction :output :if-exists :supersede)
                  (process-lcp cpp-elements
                               :cpp cpp
                               :hpp hpp
                               :lcp-file lcp-file
                               :hpp-file hpp-file
                               :cpp-file cpp-file
                               :slk-serialize-p slk-serialize-p))))
          (clean-reprocess-lcp-file ()
            :report "Reprocess the LCP file with a clean registry"
            (setf *cpp-classes* '()
                  *cpp-enums* '())
            (invoke-restart 'reprocess-lcp-file)))))))
