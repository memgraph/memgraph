(in-package #:lcp.clone)

(defvar *variable-idx* 0 "Used to generate unique variable names")

(defmacro with-vars (vars &body body)
  "Generates unique variable names for use in generated code by
appending an index to desired variable names. Useful when generating
loops which might reuse counter names.

Usage example:
  (with-vars ((loop-counter \"i\"))
    (format nil \"for (auto ~A = 0; ~A < v.size(); ++~A) {
                    // do something
                  }\"
            loop-counter loop-counter loop-counter))"
  `(let* ((*variable-idx* (1+ *variable-idx*))
          ,@(loop for var in vars collecting
                 `(,(first var)
                    (format nil "~A~A" ,(second var) *variable-idx*))))
     ,@body))

(define-condition clone-error (error)
  ((message :type string :initarg :message :reader clone-error-message)
   (format-args :type list :initform nil :initarg :format-args :reader clone-error-format-args))
  (:report (lambda (condition stream)
             (apply #'format stream
                    (clone-error-message condition)
                    (clone-error-format-args condition)))))

(defun clone-error (message &rest format-args)
  (error 'clone-error :message message :format-args format-args))

(defun cloning-parent (cpp-class)
  (let ((supers (lcp::cpp-class-super-classes cpp-class))
        (opts (lcp::cpp-class-clone-opts cpp-class)))
    (unless opts
      (clone-error "Class ~A isn't cloneable" (lcp::cpp-type-base-name cpp-class)))
    (cond
      ((lcp::clone-opts-base opts) nil)
      ((lcp::clone-opts-ignore-other-base-classes opts) (car supers))
      (t
       (when (> (length supers) 1)
         (clone-error "Cloning doesn't support multiple inheritance (class '~A', parents: '~A')"
                      (lcp::cpp-type-base-name cpp-class) supers))
       (car supers)))))

(defun cloning-root (cpp-class)
  (let ((parent-class (cloning-parent cpp-class)))
    (if parent-class
        (cloning-root (lcp::find-cpp-class parent-class))
        cpp-class)))

(defun members-for-cloning (cpp-class)
  (do ((current-class cpp-class) members)
      ((not current-class) members)
    (setf members (append (remove-if-not #'lcp::cpp-member-clone
                                         (lcp::cpp-class-members current-class))
                          members))
    (setf current-class (lcp::find-cpp-class (cloning-parent current-class)))))

(defun copy-object (source-name dest-name)
  (format nil "~A = ~A;" dest-name source-name))

;; TODO: This could be a common function in types.lisp if it were improved
;; a bit. It probably won't be necessary once we refactor LCP to use uniform
;; type designators.
(defun get-type (type-designator)
  (ctypecase type-designator
    (lcp::cpp-type type-designator)
    (string (lcp::parse-cpp-type-declaration type-designator))
    (symbol (lcp::cpp-type type-designator))))

(defun clone-by-copy-p (object-type)
  (let ((object-type (get-type object-type)))
    (cond
      ((string= "vector" (lcp::cpp-type-name object-type))
       (clone-by-copy-p (car (lcp::cpp-type-type-args object-type))))
      ((string= "optional" (lcp::cpp-type-name object-type))
       (clone-by-copy-p (car (lcp::cpp-type-type-args object-type))))
      ((string= "unordered_map" (lcp::cpp-type-name object-type))
       (and (clone-by-copy-p (first (lcp::cpp-type-type-args object-type)))
            (clone-by-copy-p (second (lcp::cpp-type-type-args object-type)))))
      ((string= "pair" (lcp::cpp-type-name object-type))
       (and (clone-by-copy-p (first (lcp::cpp-type-type-args object-type)))
            (clone-by-copy-p (second (lcp::cpp-type-type-args object-type)))))
      ((lcp::cpp-type-type-args object-type) nil)
      ((or (lcp::find-cpp-enum (lcp::cpp-type-name object-type))
           (typep object-type 'lcp::cpp-primitive-type)
           (string= "string" (lcp::cpp-type-name object-type))
           ;; TODO: We might want to forbid implicit copying of unknown types once
           ;; there's a way to globally mark type as trivially copyable. Now it is
           ;; too annoying to add (:clone :copy) option everywhere.
           (not (lcp::find-cpp-class (lcp::cpp-type-name object-type))))
       t)
      (t
       ;; We know now that we're dealing with a C++ class defined in
       ;; LCP.  A class is cloneable by copy only if it doesn't have
       ;; `Clone` function defined, all of its members are cloneable
       ;; by copy and it is not a member of inheritance hierarchy.
       (let ((cpp-class (lcp::find-cpp-class (lcp::cpp-type-name object-type))))
         (assert cpp-class)
         (and (not (lcp::cpp-class-clone-opts cpp-class))
              (not (lcp::direct-subclasses-of cpp-class))
              (not (lcp::cpp-class-super-classes cpp-class))
              (every (lambda (member)
                       (or (eq (lcp::cpp-member-clone member) :copy)
                           (clone-by-copy-p (lcp::cpp-member-type member))))
                     (lcp::cpp-class-members cpp-class))))))))

(defun clone-object (object-type source-name dest-name &key args)
  (let ((object-type (get-type object-type))
        (arg-list (format nil "~{~A~^, ~}"
                          (mapcar (lambda (name-and-type)
                                    (lcp::cpp-variable-name (first name-and-type)))
                                  args))))
    (cond
      ((clone-by-copy-p object-type)
       (copy-object source-name dest-name))
      ((lcp::cpp-pointer-type-p object-type)
       (format nil "~A = ~A ? ~A->Clone(~A) : nullptr;"
               dest-name source-name source-name arg-list))
      ((string= "optional" (lcp::cpp-type-name object-type))
       (let ((value-type (car (lcp::cpp-type-type-args object-type))))
         (clone-optional value-type source-name dest-name :args args)))
      ((string= "vector" (lcp::cpp-type-name object-type))
       (let ((elem-type (car (lcp::cpp-type-type-args object-type))))
         (clone-vector elem-type source-name dest-name :args args)))
      ((string= "unordered_map" (lcp::cpp-type-name object-type))
       (let ((key-type (first (lcp::cpp-type-type-args object-type)))
             (value-type (second (lcp::cpp-type-type-args object-type))))
             (clone-map key-type value-type source-name dest-name :args args)))
      ((string= "pair" (lcp::cpp-type-name object-type))
       (let ((first-type (first (lcp::cpp-type-type-args object-type)))
             (second-type (second (lcp::cpp-type-type-args object-type))))
             (clone-pair first-type second-type source-name dest-name :args args)))
      ((and (lcp::find-cpp-class (lcp::cpp-type-name object-type))
            (lcp::cpp-class-clone-opts (lcp::find-cpp-class (lcp::cpp-type-name object-type))))
       (format nil "~A = ~A.Clone(~A);" dest-name source-name arg-list))
      (t (clone-error "Don't know how to clone object of type ~A"
                      (lcp::cpp-type-decl object-type))))))

(defun clone-vector (elem-type source-name dest-name &key args)
  (with-vars ((loop-counter "i"))
    (format nil
            "~A.resize(~A.size());
             for (auto ~A = 0; ~A < ~A.size(); ++~A) { ~A }"
            dest-name source-name
            loop-counter loop-counter source-name loop-counter
            (clone-object elem-type
                          (format nil "~A[~A]" source-name loop-counter)
                          (format nil "~A[~A]" dest-name loop-counter)
                          :args args))))

(defun clone-map (key-type value-type source-name dest-name &key args)
  (with-vars ((loop-var "kv") (entry-var "entry"))
    (let ((entry-type (lcp::make-cpp-type "pair"
                                          :namespace '("std")
                                          :type-args (list key-type value-type))))
      (format nil
              "for (const auto &~A : ~A) {
                 ~A ~A;
                 ~A
                 ~A.emplace(std::move(~A));
               }"
              loop-var source-name
              (lcp::cpp-type-decl entry-type) entry-var
              (clone-object entry-type loop-var entry-var :args args)
              dest-name entry-var))))

(defun clone-optional (value-type source-name dest-name &key args)
  (with-vars ((value-var "value"))
    (format nil
            "if (~A) {
               ~A ~A;
               ~A
               ~A.emplace(std::move(~A));
             } else {
               ~A = std::nullopt;
             }"
          source-name
          (lcp::cpp-type-decl value-type) value-var
          (clone-object value-type
                        (format nil "(*~A)" source-name)
                        value-var
                        :args args)
          dest-name value-var
          dest-name)))

(defun clone-pair (first-type second-type source-name dest-name &key args)
  (with-vars ((first-var "first") (second-var "second"))
    (with-output-to-string (cpp-out)
      (lcp::with-cpp-block-output (cpp-out)
        (format cpp-out
                "~A ~A;
                 ~A
                 ~A ~A;
                 ~A
                 ~A = std::make_pair(std::move(~A), std::move(~A));"
                (lcp::cpp-type-decl first-type) first-var
                (clone-object first-type
                              (format nil "~A.first" source-name)
                              first-var
                              :args args)
                (lcp::cpp-type-decl second-type) second-var
                (clone-object second-type
                              (format nil "~A.second" source-name)
                              second-var
                              :args args)
                dest-name first-var second-var))
      cpp-out)))

(defun clone-function-definition-for-class (cpp-class)
  (check-type cpp-class lcp::cpp-class)
  (when (lcp::cpp-type-type-params cpp-class)
    (clone-error "Don't know how to clone templated class '~A'"
                 (lcp::cpp-type-base-name cpp-class)))
  (let* ((cloning-root (cloning-root cpp-class))
         (root-opts (lcp::cpp-class-clone-opts cloning-root))
         (inheritancep (or (lcp::direct-subclasses-of cpp-class)
                           (cloning-parent cpp-class)))
         (return-type (cond
                        ((lcp::clone-opts-return-type root-opts)
                         (lcp::cpp-code
                          (funcall (lcp::clone-opts-return-type root-opts)
                                   (lcp::cpp-type-name cpp-class))))
                        (inheritancep (format nil "std::unique_ptr<~A>"
                                              (lcp::cpp-type-name (cloning-root cpp-class))))
                        (t (lcp::cpp-type-name cpp-class))))
         (declaration
          (lcp::cpp-method-declaration cpp-class "Clone"
                                       :args (lcp::clone-opts-args root-opts)
                                       :returns return-type
                                       :virtual (and inheritancep
                                                     (eq cpp-class cloning-root))
                                       :inline t
                                       :const t
                                       :override (and inheritancep
                                                      (not (eq cpp-class cloning-root)))
                                       :delete (lcp::cpp-class-abstractp cpp-class))))
    (if (lcp::cpp-class-abstractp cpp-class)
        (return-from clone-function-definition-for-class (format nil "~A;" declaration)))
    (with-output-to-string (cpp-out)
      (lcp::with-cpp-block-output (cpp-out :name declaration :semicolonp nil)
        (let (object-access)
          (cond
            ((lcp::clone-opts-init-object root-opts)
             (setf object-access "object->")
             (write-line
              (lcp::cpp-code
               (funcall (lcp::clone-opts-init-object root-opts)
                        "object" (lcp::cpp-type-name cpp-class)))
               cpp-out))
            (inheritancep
             (setf object-access "object->")
             (format cpp-out "~&auto object = std::make_unique<~A>();"
                     (lcp::cpp-type-name cpp-class)))
            (t
             (setf object-access "object.")
             (format cpp-out "~&~A object;"
                     (lcp::cpp-type-name cpp-class))))
          (dolist (member (members-for-cloning cpp-class))
            (let* ((source (lcp::cpp-member-name member :struct (lcp::cpp-class-structp cpp-class)))
                   (dest (format nil "~A~A" object-access source)))
              (cond
                ((eq (lcp::cpp-member-clone member) :copy)
                 (format cpp-out "~&~A" (copy-object source dest)))
                ((functionp (lcp::cpp-member-clone member))
                 (format cpp-out "~&~A"
                         (lcp::cpp-code (funcall (lcp::cpp-member-clone member) source dest))))
                 (t
                  (format cpp-out "~&~A"
                          (clone-object (lcp::cpp-member-type member)
                                        source dest
                                        :args (lcp::clone-opts-args root-opts)))))))
              (format cpp-out "~&return object;"))))))
