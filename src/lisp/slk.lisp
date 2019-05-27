;;;; This file contains code generation for serialization to our Save Load Kit
;;;; (SLK). It works very similarly to Cap'n Proto serialization, but without
;;;; the schema generation.

(in-package #:lcp.slk)

(define-condition slk-error (simple-error)
  ())

(defun slk-error (format-control &rest format-arguments)
  (error 'slk-error :format-control format-control
                    :format-arguments format-arguments))

;;; CPP-CLASS serialization generation

(defun cpp-class-super-classes-for-slk (cpp-class)
  (let ((supers (lcp::cpp-class-super-classes cpp-class))
        (opts (lcp::cpp-class-slk-opts cpp-class)))
    (unless (and opts (lcp::slk-opts-base opts))
      (if (and supers opts (lcp::slk-opts-ignore-other-base-classes opts))
          (list (car supers))
          supers))))

(defun save-extra-args (cpp-class)
  "Get additional arguments to Save function for CPP-CLASS. Note, returned extra
arguments are of the first class encountered when traversing the hierarchy from
CPP-CLASS to parents."
  (let ((opts (lcp::cpp-class-slk-opts cpp-class)))
    (if (and opts (lcp::slk-opts-save-args opts))
        (lcp::slk-opts-save-args opts)
        (let ((parents (cpp-class-super-classes-for-slk cpp-class)))
          (dolist (parent parents)
            (when (lcp::cpp-type-known-p parent)
              (return (save-extra-args parent))))))))

(defun load-extra-args (cpp-class)
  "Get additional arguments to Load function for CPP-CLASS. Note, returned extra
arguments are of the first class encountered when traversing the hierarchy from
CPP-CLASS to parents."
  (let ((opts (lcp::cpp-class-slk-opts cpp-class)))
    (if (and opts (lcp::slk-opts-load-args opts))
        (lcp::slk-opts-load-args opts)
        (let ((parents (cpp-class-super-classes-for-slk cpp-class)))
          (dolist (parent parents)
            (when (lcp::cpp-type-known-p parent)
              (return (load-extra-args parent))))))))

(defun save-function-declaration-for-class (cpp-class)
  "Generate SLK save function declaration for CPP-CLASS. Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-class lcp::cpp-class)
  (when (lcp::cpp-type-class-template-p cpp-class)
    (slk-error "Don't know how to save class template '~A'"
               (lcp::cpp-type-name cpp-class)))
  (when (> (length (cpp-class-super-classes-for-slk cpp-class)) 1)
    (slk-error "Don't know how to save multiple parents of '~A'"
               (lcp::cpp-type-name cpp-class)))
  (let ((self-arg (list (lcp::ensure-namestring-for-variable 'self)
                        (lcp::cpp-type-wrap cpp-class '("const" "&"))))
        (builder-arg (list (lcp::ensure-namestring-for-variable 'builder)
                           (lcp::ensure-cpp-type "slk::Builder *"))))
    (lcp::cpp-function-declaration
     "Save" :args (list* self-arg builder-arg (save-extra-args cpp-class))
            :type-params (lcp::cpp-type-type-params cpp-class))))

(defun construct-and-load-function-declaration-for-class (cpp-class)
  "Generate SLK construct and load function declaration for CPP-CLASS. This
function needs to be used to load pointers to polymorphic types. Note that the
code generation expects the declarations and definitions to be in `slk`
namespace."
  (check-type cpp-class lcp::cpp-class)
  (when (lcp::cpp-type-class-template-p cpp-class)
    (slk-error "Don't know how to load class template '~A'"
               (lcp::cpp-type-name cpp-class)))
  (when (> (length (cpp-class-super-classes-for-slk cpp-class)) 1)
    (slk-error "Don't know how to load multiple parents of '~A'"
               (lcp::cpp-type-name cpp-class)))
  (let ((self-arg (list (lcp::ensure-namestring-for-variable 'self)
                        (lcp::cpp-type-wrap cpp-class '("std::unique_ptr" "*"))))
        (reader-arg (list (lcp::ensure-namestring-for-variable 'reader)
                          (lcp::ensure-cpp-type "slk::Reader *"))))
    (lcp::cpp-function-declaration
     "ConstructAndLoad"
     :args (list* self-arg reader-arg (load-extra-args cpp-class))
     :type-params (lcp::cpp-type-type-params cpp-class))))

(defun load-function-declaration-for-class (cpp-class)
  "Generate SLK load function declaration for CPP-CLASS. Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-class lcp::cpp-class)
  (when (lcp::cpp-type-class-template-p cpp-class)
    (slk-error "Don't know how to load class template '~A'"
               (lcp::cpp-type-name cpp-class)))
  (when (> (length (cpp-class-super-classes-for-slk cpp-class)) 1)
    (slk-error "Don't know how to load multiple parents of '~A'"
               (lcp::cpp-type-name cpp-class)))
  (let ((self-arg (list (lcp::ensure-namestring-for-variable 'self)
                        (lcp::cpp-type-wrap cpp-class '("*"))))
        (reader-arg (list (lcp::ensure-namestring-for-variable 'reader)
                          (lcp::ensure-cpp-type "slk::Reader *"))))
    (lcp::cpp-function-declaration
     "Load" :args (list* self-arg reader-arg (load-extra-args cpp-class))
            :type-params (lcp::cpp-type-type-params cpp-class))))

(defun save-members (cpp-class)
  "Generate code for saving members of CPP-CLASS. Raise `SLK-ERROR' if the
serializable member has no public access."
  (with-output-to-string (s)
    (dolist (member (lcp::cpp-class-members-for-save cpp-class))
      (let ((member-name (lcp::cpp-member-name member)))
        (when (not (eq :public (lcp::cpp-member-scope member)))
          (slk-error "Cannot save non-public member '~A' of '~A'"
                     (lcp::cpp-member-name member) (lcp::cpp-type-name cpp-class)))
        (cond
          ((lcp::cpp-member-slk-save member)
           ;; Custom save function
           (lcp::with-cpp-block-output (s)
             (write-line (lcp::cpp-code (funcall (lcp::cpp-member-slk-save member)
                                                 member-name))
                         s)))
          ;; TODO: Maybe support saving (but not loading) unique_ptr.
          ((lcp::cpp-type-pointer-p (lcp::cpp-member-type member))
           (slk-error "Don't know how to save pointer '~A' in '~A'"
                      (lcp::cpp-type-decl (lcp::cpp-member-type member))
                      (lcp::cpp-type-name cpp-class)))
          ;; TODO: Extra args for cpp-class members
          (t
           (format s "slk::Save(self.~A, builder);~%" member-name)))))))

(defun members-for-load (cpp-class)
  (remove-if (lambda (m)
               (and (lcp::cpp-member-dont-save m)
                    (not (lcp::cpp-member-slk-load m))))
             (lcp::cpp-class-members cpp-class)))

(defun load-members (cpp-class)
  "Generate code for loading members of CPP-CLASS. Raise `SLK-ERROR' if the
serializable member has no public access."
  (with-output-to-string (s)
    (dolist (member (members-for-load cpp-class))
      (let ((member-name (lcp::cpp-member-name member)))
        (when (not (eq :public (lcp::cpp-member-scope member)))
          (slk-error "Cannot save non-public member '~A' of '~A'"
                     (lcp::cpp-member-name member) (lcp::cpp-type-name cpp-class)))
        (cond
          ((lcp::cpp-member-slk-load member)
           ;; Custom load function
           (lcp::with-cpp-block-output (s)
             (write-line (lcp::cpp-code (funcall (lcp::cpp-member-slk-load member)
                                                 member-name))
                         s)))
          ((lcp::cpp-type-pointer-p (lcp::cpp-member-type member))
           (slk-error "Don't know how to load pointer '~A' in '~A'"
                      (lcp::cpp-type-decl (lcp::cpp-member-type member))
                      (lcp::cpp-type-name cpp-class)))
          ;; TODO: Extra args for cpp-class members
          (t
           (format s "slk::Load(&self->~A, reader);~%" member-name)))))))

(defun save-parents-recursively (cpp-class)
  "Generate code for saving members of all parents, recursively. Raise
`SLK-ERROR' if CPP-CLASS has multiple superclasses or if any ancestor is a class
template."
  (when (> (length (cpp-class-super-classes-for-slk cpp-class)) 1)
    (slk-error "Don't know how to save multiple parents of '~A'"
               (lcp::cpp-type-name cpp-class)))
  (with-output-to-string (s)
    (dolist (parent (cpp-class-super-classes-for-slk cpp-class))
      (cond
        ((not (lcp::cpp-type-known-p parent))
         (slk-error
          "Class '~A' has an unknown parent '~A', serialization is incomplete. Did you forget to mark '~A' as base?"
          (lcp::cpp-type-name cpp-class)
          (lcp::cpp-type-name parent)
          (lcp::cpp-type-name cpp-class)))
        ((lcp::cpp-type-class-template-p parent)
         (slk-error "Don't know how to save parent class template '~A'"
                    (lcp::cpp-type-name parent)))
        (t
         (format s "// Save parent ~A~%" (lcp::cpp-type-name parent))
         (lcp::with-cpp-block-output (s)
           (write-string (save-parents-recursively parent) s)
           (write-string (save-members parent) s)))))))

(defun load-parents-recursively (cpp-class)
  "Generate code for loading members of all parents, recursively. Raise
`SLK-ERROR' if CPP-CLASS has multiple superclasses or if any ancestor is a class
template."
  (when (> (length (cpp-class-super-classes-for-slk cpp-class)) 1)
    (slk-error "Don't know how to load multiple parents of '~A'"
               (lcp::cpp-type-name cpp-class)))
  (with-output-to-string (s)
    (dolist (parent (cpp-class-super-classes-for-slk cpp-class))
      (cond
        ((not (lcp::cpp-type-known-p parent))
         (slk-error
          "Class '~A' has an unknown parent '~A', serialization is incomplete. Did you forget to mark '~A' as base?"
          (lcp::cpp-type-name cpp-class)
          (lcp::cpp-type-name parent)
          (lcp::cpp-type-name cpp-class)))
        ((lcp::cpp-type-type-params parent)
         (slk-error "Don't know how to load parent class template '~A'"
                    (lcp::cpp-type-name parent)))
        (t
         (format s "// Load parent ~A~%" (lcp::cpp-type-name parent))
         (lcp::with-cpp-block-output (s)
           (write-string (load-parents-recursively parent) s)
           (write-string (load-members parent) s)))))))

(defun forward-save-to-subclasses (cpp-class)
  "Generate code which forwards the serialization to derived classes of
CPP-CLASS. Raise `SLK-ERROR' if a derived class has template parameters."
  (with-output-to-string (s)
    (let ((subclasses (lcp::cpp-class-direct-subclasses cpp-class)))
      (dolist (subclass subclasses)
        (when (lcp::cpp-type-class-template-p subclass)
          (slk-error "Don't know how to save derived class template '~A'"
                     (lcp::cpp-type-name subclass)))
        (let ((derived-class (lcp::cpp-type-decl subclass))
              (derived-var (lcp::cpp-name-for-variable (lcp::cpp-type-name subclass)))
              (extra-args (mapcar #'first (save-extra-args cpp-class))))
          (format s "if (const auto *~A_derived = utils::Downcast<const ~A>(&self)) {
                       return slk::Save(*~A_derived, builder~{, ~A~}); }~%"
                  derived-var derived-class derived-var extra-args))))))

(defun save-function-code-for-class (cpp-class)
  "Generate code for serializing CPP-CLASS. Raise `SLK-ERROR' on unsupported C++
constructs, mostly related to templates."
  (when (lcp::cpp-type-class-template-p cpp-class)
    (slk-error "Don't know how to save class template '~A'"
               (lcp::cpp-type-name cpp-class)))
  (with-output-to-string (s)
    (cond
      ((lcp::cpp-class-direct-subclasses cpp-class)
       ;; We have more derived classes, so forward the call to them.
       (write-string (forward-save-to-subclasses cpp-class) s)
       (if (lcp::cpp-class-abstractp cpp-class)
           (format s "LOG(FATAL) << \"`~A` is marked as an abstract class!\";"
                   (lcp::cpp-type-name cpp-class))
           (progn
             ;; We aren't abstract, so save our data.
             (format s "slk::Save(~A::kType.id, builder);~%"
                     (lcp::cpp-type-decl cpp-class))
             (write-string (save-parents-recursively cpp-class) s)
             (write-string (save-members cpp-class) s))))
      (t
       (when (cpp-class-super-classes-for-slk cpp-class)
         ;; Write type ID for the (final) derived classes.
         (format s "slk::Save(~A::kType.id, builder);~%"
                 (lcp::cpp-type-decl cpp-class)))
       (write-string (save-parents-recursively cpp-class) s)
       (write-string (save-members cpp-class) s)))))

(defun construct-and-load-function-code-for-class (cpp-class)
  "Generate code for serializing CPP-CLASS. Raise `SLK-ERROR' on unsupported C++
constructs, mostly related to templates."
  (assert (or (cpp-class-super-classes-for-slk cpp-class)
              (lcp::cpp-class-direct-subclasses cpp-class)))
  (when (lcp::cpp-type-class-template-p cpp-class)
    (slk-error "Don't know how to load class template '~A'"
               (lcp::cpp-type-name cpp-class)))
  (labels ((concrete-subclasses-rec (class)
             (let ((concrete-classes nil))
               (dolist (subclass (lcp::cpp-class-direct-subclasses class) concrete-classes)
                 (unless (lcp::cpp-class-abstractp subclass)
                   (push subclass concrete-classes))
                 (setf concrete-classes
                       (append concrete-classes (concrete-subclasses-rec subclass)))))))
    (with-output-to-string (s)
      (write-line "uint64_t type_id;" s)
      (write-line "slk::Load(&type_id, reader);" s)
      (let ((concrete-classes (concrete-subclasses-rec cpp-class)))
        (unless (lcp::cpp-class-abstractp cpp-class)
          (push cpp-class concrete-classes))
        (dolist (concrete-class concrete-classes)
          (let ((type-decl (lcp::cpp-type-decl concrete-class))
                (var-name (lcp::cpp-name-for-variable (lcp::cpp-type-name concrete-class)))
                (extra-args (mapcar #'first (load-extra-args cpp-class))))
            (lcp::with-cpp-block-output
                (s :name (format nil "if (~A::kType.id == type_id)" type-decl))
              (format s "auto ~A_instance = std::make_unique<~A>();~%" var-name type-decl)
              (format s "slk::Load(~A_instance.get(), reader~{, ~A~});~%" var-name extra-args)
              (format s "*self = std::move(~A_instance); return;~%" var-name))))
        (write-line "throw slk::SlkDecodeException(\"Trying to load unknown derived type!\");" s)))))

(defun load-function-code-for-class (cpp-class)
  "Generate code for serializing CPP-CLASS. Raise `SLK-ERROR' on unsupported C++
constructs, mostly related to templates."
  (when (lcp::cpp-type-class-template-p cpp-class)
    (slk-error "Don't know how to load class template '~A'"
               (lcp::cpp-type-name cpp-class)))
  (assert (not (lcp::cpp-class-abstractp cpp-class)))
  (with-output-to-string (s)
    ;; We are assuming that the generated code is called only in cases when we
    ;; really have this particular class instantiated and not any of the
    ;; derived ones.
    (when (lcp::cpp-class-direct-subclasses cpp-class)
      (format s "if (self->GetTypeInfo() != ~A::kType)~%" (lcp::cpp-type-decl cpp-class))
      (write-line "throw slk::SlkDecodeException(\"Trying to load incorrect derived type!\");" s))
    (write-string (load-parents-recursively cpp-class) s)
    (write-string (load-members cpp-class) s)))

(defun save-function-definition-for-class (cpp-class)
  "Generate SLK save function. Raise `SLK-ERROR' if an unsupported or invalid
class definition is encountered during code generation. Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-class lcp::cpp-class)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (save-function-declaration-for-class cpp-class))
      (write-line (save-function-code-for-class cpp-class) cpp-out))))

(defun load-function-definition-for-class (cpp-class)
  "Generate SLK load function. Raise `SLK-ERROR' if an unsupported or invalid
class definition is encountered during code generation. Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-class lcp::cpp-class)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (load-function-declaration-for-class cpp-class))
      (write-line (load-function-code-for-class cpp-class) cpp-out))))

(defun construct-and-load-function-definition-for-class (cpp-class)
  "Generate SLK construct and load function. This function needs to be used to
load pointers to polymorphic types. Raise `SLK-ERROR' if an unsupported or
invalid class definition is encountered during code generation. Note that the
code generation expects the declarations and definitions to be in `slk`
namespace."
  (check-type cpp-class lcp::cpp-class)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (construct-and-load-function-declaration-for-class cpp-class))
      (write-line (construct-and-load-function-code-for-class cpp-class) cpp-out))))

;;; CPP-ENUM serialization generation

(defun save-function-declaration-for-enum (cpp-enum)
  "Generate SLK save function declaration for CPP-ENUM. Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (let ((self-arg (list (lcp::ensure-namestring-for-variable 'self)
                        (lcp::cpp-type-wrap cpp-enum '("const" "&"))))
        (builder-arg (list (lcp::ensure-namestring-for-variable 'builder)
                           (lcp::ensure-cpp-type "slk::Builder *"))))
    (lcp::cpp-function-declaration "Save" :args (list self-arg builder-arg))))

(defun save-function-code-for-enum (cpp-enum)
  (with-output-to-string (s)
    (write-line "uint8_t enum_value;" s)
    (lcp::with-cpp-block-output (s :name "switch (self)")
      (loop :for enum-value :in (lcp::cpp-enum-values cpp-enum)
            :and enum-ix :from 0 :do
              (format s "case ~A::~A: enum_value = ~A; break;"
                      (lcp::cpp-type-decl cpp-enum)
                      enum-value
                      enum-ix)))
    (write-line "slk::Save(enum_value, builder);" s)))

(defun save-function-definition-for-enum (cpp-enum)
  "Generate SLK save function. Note that the code generation expects the
declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (save-function-declaration-for-enum cpp-enum))
      (write-line (save-function-code-for-enum cpp-enum) cpp-out))))

(defun load-function-declaration-for-enum (cpp-enum)
  "Generate SLK load function declaration for CPP-ENUM. Note that the code
generation expects the declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (let ((self-arg (list (lcp::ensure-namestring-for-variable 'self)
                        (lcp::cpp-type-wrap cpp-enum '("*"))))
        (reader-arg (list (lcp::ensure-namestring-for-variable 'reader)
                          (lcp::ensure-cpp-type "slk::Reader *"))))
    (lcp::cpp-function-declaration "Load" :args (list self-arg reader-arg))))

(defun load-function-code-for-enum (cpp-enum)
  (with-output-to-string (s)
    (write-line "uint8_t enum_value;" s)
    (write-line "slk::Load(&enum_value, reader);" s)
    (lcp::with-cpp-block-output (s :name "switch (enum_value)")
      (loop :for enum-value :in (lcp::cpp-enum-values cpp-enum)
            :and enum-ix :from 0 :do
              (format s "case static_cast<uint8_t>(~A): *self = ~A::~A; break;"
                      enum-ix
                      (lcp::cpp-type-decl cpp-enum)
                      enum-value))
      (write-line "default: throw slk::SlkDecodeException(\"Trying to load unknown enum value!\");" s))))

(defun load-function-definition-for-enum (cpp-enum)
  "Generate SLK save function. Note that the code generation expects the
declarations and definitions to be in `slk` namespace."
  (check-type cpp-enum lcp::cpp-enum)
  (with-output-to-string (cpp-out)
    (lcp::with-cpp-block-output
        (cpp-out :name (load-function-declaration-for-enum cpp-enum))
      (write-line (load-function-code-for-enum cpp-enum) cpp-out))))
