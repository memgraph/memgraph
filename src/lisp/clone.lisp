(in-package #:lcp.clone)

(define-condition clone-error (simple-error)
  ())

(defun clone-error (format-control &rest format-arguments)
  (error 'clone-error :format-control format-control
                      :format-arguments format-arguments))

(defun cloning-parent (cpp-class)
  (check-type cpp-class lcp::cpp-class)
  (let ((supers (lcp::cpp-class-super-classes cpp-class))
        (opts (lcp::cpp-class-clone-opts cpp-class)))
    (unless opts
      (clone-error "Class ~A isn't cloneable" (lcp::cpp-type-name cpp-class)))
    (cond
      ((lcp::clone-opts-base opts)
       nil)
      ((lcp::clone-opts-ignore-other-base-classes opts)
       (car supers))
      (t
       (when (> (length supers) 1)
         (clone-error "Cloning doesn't support multiple inheritance (class '~A', parents: '~A')"
                      (lcp::cpp-type-name cpp-class) supers))
       (car supers)))))

(defun cloning-root (cpp-class)
  (check-type cpp-class lcp::cpp-class)
  (let ((parent-class (cloning-parent cpp-class)))
    (if parent-class
        (cloning-root parent-class)
        cpp-class)))

(defun members-for-cloning (cpp-class)
  (check-type cpp-class lcp::cpp-class)
  (alexandria:flatten
   (reverse
    (loop :for current := cpp-class :then (cloning-parent current)
          :while current
          :collect (remove-if-not #'lcp::cpp-member-clone
                                  (lcp::cpp-class-members current))))))

(defun copy-object (source-name dest-name)
  (format nil "~A = ~A;" dest-name source-name))

(defun clone-by-copy-p (object-type)
  (check-type object-type lcp::cpp-type)
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
    ((or (lcp::cpp-enum-p object-type)
         (lcp::cpp-type-primitive-p object-type)
         (string= "string" (lcp::cpp-type-name object-type))
         ;; TODO: We might want to forbid implicit copying of unknown types once
         ;; there's a way to globally mark type as trivially copyable. Now it is
         ;; too annoying to add (:clone :copy) option everywhere.
         (not (lcp::cpp-class-p object-type)))
     t)
    (t
     ;; We know now that we're dealing with a C++ class defined in LCP. A class
     ;; is cloneable by copy only if it doesn't have `Clone` function defined,
     ;; all of its members are cloneable by copy and it is not a member of a
     ;; class hierarchy.
     (assert (and (lcp::cpp-type-known-p object-type)
                  (lcp::cpp-type-class-p object-type)))
     (and (not (lcp::cpp-class-clone-opts object-type))
          (not (lcp::cpp-class-direct-subclasses object-type))
          (not (lcp::cpp-class-super-classes object-type))
          (every (lambda (member)
                   (or (eq (lcp::cpp-member-clone member) :copy)
                       (clone-by-copy-p (lcp::cpp-member-type member))))
                 (lcp::cpp-class-members object-type))))))

(defun clone-object (object-type source-name dest-name &key args)
  (check-type object-type lcp::cpp-type)
  (let ((arg-list (format nil "~{~A~^, ~}"
                          (mapcar (lambda (name-and-type)
                                    (lcp::cpp-name-for-variable (first name-and-type)))
                                  args))))
    (cond
      ((clone-by-copy-p object-type)
       (copy-object source-name dest-name))
      ((lcp::cpp-type-pointer-p object-type)
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
      ((and (lcp::cpp-class-p object-type)
            (lcp::cpp-class-clone-opts object-type))
       (format nil "~A = ~A.Clone(~A);" dest-name source-name arg-list))
      (t (clone-error "Don't know how to clone object of type ~A"
                      (lcp::cpp-type-decl object-type))))))

(defun clone-vector (elem-type source-name dest-name &key args)
  (lcp::with-cpp-gensyms ((loop-counter "i"))
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
  (lcp::with-cpp-gensyms ((loop-var "kv")
                          (entry-var "entry"))
    (let ((entry-type (lcp::make-cpp-type
                       "pair"
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
  (lcp::with-cpp-gensyms ((value-var "value"))
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
  (lcp::with-cpp-gensyms ((first-var "first")
                          (second-var "second"))
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
  (when (lcp::cpp-type-class-template-p cpp-class)
    (clone-error "Don't know how to clone class template '~A'"
                 (lcp::cpp-type-name cpp-class)))
  (let* ((cloning-root (cloning-root cpp-class))
         (root-opts (lcp::cpp-class-clone-opts cloning-root))
         (inheritancep (or (lcp::cpp-class-direct-subclasses cpp-class)
                           (cloning-parent cpp-class)))
         (return-type
           (cond
             ((lcp::clone-opts-return-type root-opts)
              (lcp::cpp-code
               (funcall (lcp::clone-opts-return-type root-opts)
                        (lcp::cpp-type-name cpp-class))))
             (inheritancep
              (format nil "std::unique_ptr<~A>"
                      (lcp::cpp-type-name (cloning-root cpp-class))))
             (t
              (lcp::cpp-type-name cpp-class))))
         (declaration
           (lcp::cpp-method-declaration
            cpp-class "Clone"
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
        (return-from clone-function-definition-for-class
          (format nil "~A;" declaration)))
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
            (let* ((source (lcp::cpp-member-name member))
                   (dest (format nil "~A~A" object-access source)))
              (cond
                ((eq (lcp::cpp-member-clone member) :copy)
                 (format cpp-out "~&~A" (copy-object source dest)))
                ((functionp (lcp::cpp-member-clone member))
                 (format cpp-out "~&~A"
                         (lcp::cpp-code (funcall (lcp::cpp-member-clone member)
                                                 source dest))))
                (t
                 (format cpp-out "~&~A"
                         (clone-object
                          (lcp::cpp-member-type member)
                          source dest
                          :args (lcp::clone-opts-args root-opts)))))))
          (format cpp-out "~&return object;"))))))
