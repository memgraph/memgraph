;;;; This file contains code generation for serialization to our Save Load
;;;; Kit (SLK).  It works very similarly to Cap'n Proto serialization, but
;;;; without the schema generation.

(in-package #:lcp.slk)

(define-condition slk-error (error)
  ((message :type string :initarg :message :reader slk-error-message))
  (:report (lambda (condition stream)
             (write-string (slk-error-message condition) stream))))

(defun save-function-declaration-for-class (cpp-class)
  "Generate SLK save function declaration for CPP-CLASS.  Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-class lcp::cpp-class)
  (when (lcp::cpp-type-type-params cpp-class)
    (error 'slk-error :message
           (format nil "Don't know how to save templated class '~A'"
                   (lcp::cpp-type-base-name cpp-class))))
  (when (< 1 (list-length (lcp::cpp-class-super-classes cpp-class)))
    (error 'slk-error :message
           (format nil "Don't know how to save multiple parents of '~A'"
                   (lcp::cpp-type-base-name cpp-class))))
  (let ((self-arg
         (list 'self (format nil "const ~A &"
                             (lcp::cpp-type-decl cpp-class :namespace nil))))
        (builder-arg (list 'builder "slk::Builder *")))
    (lcp::cpp-function-declaration
     "Save" :args (list self-arg builder-arg)
     :type-params (lcp::cpp-type-type-params cpp-class))))

(defun save-members (cpp-class)
  "Generate code for saving members of CPP-CLASS.  Raise `SLK-ERROR' if the
serializable member has no public access."
  (with-output-to-string (s)
    (dolist (member (lcp::cpp-class-members-for-save cpp-class))
      (let ((member-name (lcp::cpp-member-name member :struct (lcp::cpp-class-structp cpp-class))))
        (when (not (eq :public (lcp::cpp-member-scope member)))
          (error 'slk-error :message
                 (format nil "Cannot save non-public member '~A' of '~A'"
                         (lcp::cpp-member-symbol member) (lcp::cpp-type-base-name cpp-class))))
        (cond
          ((lcp::cpp-member-slk-save member)
           ;; Custom save function
           (write-line (lcp::cpp-code (funcall (lcp::cpp-member-slk-save member)
                                               member-name))
                       s))
          ;; TODO: Extra args for cpp-class members
          (t
           (format s "slk::Save(self.~A, builder);~%" member-name)))))))

(defun save-parents-recursively (cpp-class)
  "Generate code for saving members of all parents, recursively.  Raise
`SLK-ERROR' if trying to save templated parent class or if using multiple
inheritance."
  (when (< 1 (list-length (lcp::cpp-class-super-classes cpp-class)))
    (error 'slk-error :message
           (format nil "Don't know how to save multiple parents of '~A'"
                   (lcp::cpp-type-base-name cpp-class))))
  (with-output-to-string (s)
    ;; TODO: Stop recursing to parents if CPP-CLASS is marked as base for
    ;; serialization.
    (dolist (parent (lcp::cpp-class-super-classes cpp-class))
      (let ((parent-class (lcp::find-cpp-class parent)))
        (assert parent-class)
        (when (lcp::cpp-type-type-params parent-class)
          (error 'slk-error :message
                 (format nil "Don't know how to save templated parent class '~A'"
                         (lcp::cpp-type-base-name parent-class))))
        (format s "// Save parent ~A~%" (lcp::cpp-type-name parent))
        (lcp::with-cpp-block-output (s)
          (write-string (save-parents-recursively parent-class) s)
          (write-string (save-members parent-class) s))))))

(defun forward-save-to-subclasses (cpp-class)
  "Generate code which forwards the serialization to derived classes of
CPP-CLASS.  Raise `SLK-ERROR' if a derived class has template parameters."
  (with-output-to-string (s)
    (let ((subclasses (lcp::direct-subclasses-of cpp-class)))
      (dolist (subclass subclasses)
        (when (lcp::cpp-type-type-params subclass)
          (error 'slk-error :message
                 (format nil "Don't know how to save derived templated class '~A'"
                         (lcp::cpp-type-base-name subclass))))
        (let ((derived-class (lcp::cpp-type-name subclass))
              (derived-var (lcp::cpp-variable-name (lcp::cpp-type-base-name subclass)))
              ;; TODO: Extra save arguments
              (extra-args nil))
          (format s "if (const auto &~A_derived = dynamic_cast<const ~A &>(self)) {
                       return slk::Save(~A_derived, builder~{, ~A~}); }~%"
                  derived-var derived-class derived-var extra-args))))))

(defun save-function-code-for-class (cpp-class)
  "Generate code for serializing CPP-CLASS.  Raise `SLK-ERROR' on unsupported
C++ constructs, mostly related to templates."
  (when (lcp::cpp-type-type-params cpp-class)
    (error 'slk-error :message
           (format nil "Don't know how to save templated class '~A'"
                   (lcp::cpp-type-base-name cpp-class))))
  (with-output-to-string (s)
    (cond
      ((lcp::direct-subclasses-of cpp-class)
       (write-string (forward-save-to-subclasses cpp-class) s)
       (if (lcp::cpp-class-abstractp cpp-class)
           (format s "LOG(FATAL) << \"`~A` is marked as an abstract class!\";"
                   (lcp::cpp-type-name cpp-class))
           (progn
             (write-string (save-parents-recursively cpp-class) s)
             (write-string (save-members cpp-class) s))))
      (t
       ;; TODO: Write some sort of type ID for derived classes
       (write-string (save-parents-recursively cpp-class) s)
       (write-string (save-members cpp-class) s)))))

(defun save-function-definition-for-class (cpp-class)
  "Generate SLK save function.  Raise `SLK-ERROR' if an unsupported or invalid
class definition is encountered during code generation.  Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-class lcp::cpp-class)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (save-function-declaration-for-class cpp-class))
      (write-line (save-function-code-for-class cpp-class) cpp-out))))

(defun save-function-declaration-for-enum (cpp-enum)
  "Generate SLK save function declaration for CPP-ENUM.  Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (let ((self-arg
         (list 'self (format nil "const ~A &" (lcp::cpp-type-decl cpp-enum))))
        (builder-arg (list 'builder "slk::Builder *")))
    (lcp::cpp-function-declaration "Save" :args (list self-arg builder-arg))))

(defun save-function-code-for-enum (cpp-enum)
  (with-output-to-string (s)
    (write-line "uint8_t enum_value;" s)
    (lcp::with-cpp-block-output (s :name "switch (self)")
      (loop for enum-value in (lcp::cpp-enum-values cpp-enum)
         and enum-ix from 0 do
           (format s "case ~A::~A: enum_value = ~A; break;"
                   (lcp::cpp-type-decl cpp-enum)
                   (lcp::cpp-enumerator-name enum-value)
                   enum-ix)))
    (write-line "slk::Save(enum_value, builder);" s)))

(defun save-function-definition-for-enum (cpp-enum)
  "Generate SLK save function.  Note that the code generation expects the
declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (save-function-declaration-for-enum cpp-enum))
      (write-line (save-function-code-for-enum cpp-enum) cpp-out))))

(defun load-function-declaration-for-enum (cpp-enum)
  "Generate SLK load function declaration for CPP-ENUM.  Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (let ((self-arg
         (list 'self (format nil "~A *" (lcp::cpp-type-decl cpp-enum))))
        (reader-arg (list 'reader "slk::Reader *")))
    (lcp::cpp-function-declaration "Load" :args (list self-arg reader-arg))))

(defun load-function-code-for-enum (cpp-enum)
  (with-output-to-string (s)
    (write-line "uint8_t enum_value;" s)
    (write-line "slk::Load(&enum_value, reader);" s)
    (lcp::with-cpp-block-output (s :name "switch (enum_value)")
      (loop for enum-value in (lcp::cpp-enum-values cpp-enum)
         and enum-ix from 0 do
           (format s "case static_cast<uint8_t>(~A): *self = ~A::~A; break;"
                   enum-ix
                   (lcp::cpp-type-decl cpp-enum)
                   (lcp::cpp-enumerator-name enum-value)))
      (write-line "default: LOG(FATAL) << \"Trying to load unknown enum value!\";" s))))

(defun load-function-definition-for-enum (cpp-enum)
  "Generate SLK save function.  Note that the code generation expects the
declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (load-function-declaration-for-enum cpp-enum))
      (write-line (load-function-code-for-enum cpp-enum) cpp-out))))
