(defpackage #:lcp
  (:use #:cl)
  (:export #:define-class
           #:define-struct
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
                             (set-syntax-from-char #\} #\))
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
  (load-fun nil :type (or null string raw-cpp function) :read-only t))

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
  (private nil))

(defun cpp-documentation (documentation)
  "Convert DOCUMENTATION to Doxygen style string."
  (declare (type string documentation))
  (format nil "/// ~A"
          (cl-ppcre:regex-replace-all
           (string #\Newline)
           documentation
           (format nil "~%/// "))))

(defun cpp-member-name (cpp-member &key struct)
  "Get C++ style name of the `CPP-MEMBER' as a string."
  (declare (type cpp-member cpp-member)
           (type boolean struct))
  (let ((cpp-name (cl-ppcre:regex-replace-all
                   "-" (string-downcase (cpp-member-symbol cpp-member)) "_")))
    (if struct
        cpp-name
        (format nil "~A_" cpp-name))))

(defun cpp-member-declaration (cpp-member &key struct)
  "Get C++ style `CPP-MEMBER' declaration as a string."
  (declare (type cpp-member cpp-member)
           (type boolean struct))
  (flet ((cpp-type-name ()
           (cond
             ((stringp (cpp-member-type cpp-member))
              (cpp-member-type cpp-member))
             ((keywordp (cpp-member-type cpp-member))
              (string-downcase (string (cpp-member-type cpp-member))))
             (t (error "Unknown conversion to C++ type for ~S" (type-of (cpp-member-type cpp-member)))))))
    (with-output-to-string (s)
      (when (cpp-member-documentation cpp-member)
        (write-line (cpp-documentation (cpp-member-documentation cpp-member)) s))
      (if (cpp-member-initval cpp-member)
          (format s "~A ~A{~A};" (cpp-type-name)
                  (cpp-member-name cpp-member :struct struct) (cpp-member-initval cpp-member))
          (format s "~A ~A;" (cpp-type-name) (cpp-member-name cpp-member :struct struct))))))

(defun cpp-member-reader-definition (cpp-member)
  "Get C++ style `CPP-MEMBER' getter (reader) function."
  (declare (type cpp-member cpp-member))
  (if (cpp-primitive-type-p (cpp-member-type cpp-member))
      (format nil "auto ~A() const { return ~A; }" (cpp-member-name cpp-member :struct t) (cpp-member-name cpp-member))
      (format nil "const auto &~A() const { return ~A; }" (cpp-member-name cpp-member :struct t) (cpp-member-name cpp-member))))

(defun cpp-type-name (symbol-name)
  "Get C++ style type name from lisp SYMBOL-NAME as a string."
  (remove #\- (string-capitalize (string symbol-name))))

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
        (format s "template <~{class ~A~^,~^ ~}>~%"
                (mapcar #'cpp-type-name (cpp-class-type-params cpp-class))))
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

(defun cpp-code (cpp)
  "Get a C++ string from given CPP meta information."
  (typecase cpp
    (raw-cpp (raw-cpp-string cpp))
    (cpp-class (cpp-class-definition cpp))
    (string cpp)
    (null "")
    (otherwise (error "Unknown conversion to C++ for ~S" (type-of cpp)))))

(defun count-newlines (stream &key stop-position)
  (loop for pos = (file-position stream)
     and char = (read-char stream nil nil)
     until (or (not char) (and stop-position (> pos stop-position)))
     when (char= #\Newline char) count it))

(defun process-file (filepath &key out-stream)
  "Process a LCP file from FILEPATH and send the output to OUT-STREAM."
  (flet ((process-to (out)
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
                      when (typep res '(or raw-cpp cpp-class))
                      do (write-line (cpp-code res) out))
                 (end-of-file ()
                   (file-position in-stream 0) ;; start of stream
                   (error "~%~A:~A:error: READ error, did you forget a closing ')'?"
                          (uiop:native-namestring filepath)
                          (count-newlines in-stream
                                          :stop-position (1+ stream-pos)))))))))
    (if out-stream
        (process-to out-stream)
        (with-output-to-string (string)
          (process-to string)))))

(defun boost-serialization (cpp-class)
  "Add boost serialization code to `CPP-CLASS'."
  (labels ((get-serialize-code (member-name serialize-fun)
             (make-raw-cpp
              :string
              (if serialize-fun
                  (etypecase serialize-fun
                    (string serialize-fun)
                    (raw-cpp (raw-cpp-string serialize-fun))
                    (function
                     (let ((res (funcall serialize-fun "ar" member-name)))
                       (check-type res (or raw-cpp string))
                       res)))
                  (format nil "ar & ~A;" member-name))))
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


(defmacro define-class (name super-classes slots &rest options)
  "Define a C++ class. Syntax is:

(define-class name (list-of-super-classes)
  ((c++-slot-definition)*)
  (:class-option option-value)*)

Class name may be a list where the first element is the class name, while
others are template arguments.

For example:

(define-class (optional t-value)
   ...)

defines a templated C++ class:

template <class TValue>
class Optional { ... };

Each C++ member/slot definition is of the form:
  (name cpp-type slot-options)

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
  * :serialize -- only :boost is a valid value, setting this will generate
    boost serialization code for the class members.

Larger example:

(lcp:define-class derived (base)
  ((val :int :reader t :initval 42))
  (:public #>cpp void set_val(int new_val) { val_ = new_val; } cpp<#)
  (:serialize :boost))

Generates C++:

class Derived : public Base {
 public:
  void set_val(int new_val) { val_ = new_val; }
  auto val() { return val_; } // autogenerated from :reader t

 private:
  friend class boost::serialization::access;
  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar & boost::serialization::base_object<Base>(*this);
    ar & val_;
  }

  int val_ = 42; // :initval is assigned
};"
  (let ((structp (second (assoc :structp options))))
    (flet ((parse-slot (slot-name type &key initval reader scope
                                  documentation save-fun load-fun)
             (let ((scope (if scope
                              scope
                              (if structp :public :private))))
               (when (and structp reader (eq :private scope))
                 (error "Slot ~A is declared private with reader in a struct. You should use define-class" slot-name))
               (when (and structp reader (eq :public scope))
                 (error "Slot ~A is public, you shouldn't specify :reader" slot-name))
               `(make-cpp-member :symbol ',slot-name :type ,type :initval ,initval
                                 :reader ,reader :scope ,scope
                                 :documentation ,documentation
                                 :save-fun ,save-fun :load-fun ,load-fun))))
      (let ((members (mapcar (lambda (s) (apply #'parse-slot s)) slots))
            (class-name (if (consp name) (car name) name))
            (type-params (when (consp name) (cdr name)))
            (class (gensym (format nil "CLASS-~A" name))))
        `(let ((,class
                (make-cpp-class :name ',class-name :super-classes ',super-classes
                                :type-params ',type-params
                                :structp ,(second (assoc :structp options))
                                :members (list ,@members)
                                :documentation ,(second (assoc :documentation options))
                                :public (list ,@(cdr (assoc :public options)))
                                :protected (list ,@(cdr (assoc :protected options)))
                                :private (list ,@(cdr (assoc :private options))))))
           (prog1 ,class
             ,(when (eq :boost (cadr (assoc :serialize options)))
                `(setf (cpp-class-private ,class)
                       (append (cpp-class-private ,class) (boost-serialization ,class))))))))))

(defmacro define-struct (name super-classes slots &rest options)
  `(define-class ,name ,super-classes ,slots (:structp t) ,@options))
