(defpackage #:lcp.test
  (:use #:cl #:prove))

(in-package #:lcp.test)
(named-readtables:in-readtable lcp:lcp-syntax)

;;; NOTE: Fix Prove's PROVE.TEST::IS-ERROR. It is implemented as an alias to
;;; IS-CONDITION which uses HANDLER-CASE to catch *any* condition (any subclass
;;; of CONDITION). This is wrong, because not every condition is an error.
;;; Because of this, Prove used to catch LCP's warnings and would fail because
;;; an LCP error was expected (which would have been signaled).

(in-package #:prove.test)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defmacro is-error (form condition &optional desc)
    (with-gensyms (error duration)
      `(with-duration ((,duration ,error) (handler-case ,form
                                            (error (,error) ,error)))
         (test ,error
               ,(if (and (listp condition) (eq 'quote (car condition)))
                    condition
                    `(quote ,condition))
               ,desc
               :duration ,duration
               :got-form ',form
               :test-fn #'typep
               :report-expected-label "raise an error")))))

(in-package #:lcp.test)

(defun same-type-test (a b)
  "Test whether two CPP-TYPE designators, A and B, designate CPP-TYPE= CPP-TYPE
instances."
  (is (lcp::ensure-cpp-type a) (lcp::ensure-cpp-type b) :test #'lcp::cpp-type=))

(defun different-type-test (a b)
  "Test whether two CPP-TYPE designators, A and B, designate non-CPP-TYPE=
CPP-TYPE instances."
  (isnt (lcp::ensure-cpp-type a) (lcp::ensure-cpp-type b)
        :test #'lcp::cpp-type=))

(defun parse-test (type-decl cpp-type)
  "Test whether TYPE-DECL parses as the C++ type designated by CPP-TYPE."
  (is (lcp::parse-cpp-type-declaration type-decl) cpp-type
      :test #'lcp::cpp-type=))

(defun fail-parse-test (type-decl)
  "Test whether TYPE-DECL fails to parse."
  (is (lcp::parse-cpp-type-declaration type-decl) nil))

(defun decl-test (type-decl cpp-type &key (namespacep t) (type-params-p t))
  "Test whether the C++ type declaration of CPP-TYPE, as produced by
CPP-TYPE-DECL, matches the string TYPE-DECL.

The keyword arguments NAMESPACEP and TYPE-PARAMS-P are forwarded to
CPP-TYPE-DECL."
  (is (lcp::cpp-type-decl cpp-type :type-params-p type-params-p
                                   :namespacep namespacep)
      type-decl))

(defun different-parse-test (type-decl1 type-decl2)
  (isnt (lcp::parse-cpp-type-declaration type-decl1)
        (lcp::parse-cpp-type-declaration type-decl2)
        :test #'lcp::cpp-type=))

(deftest "types"
  (subtest "primitive"
    (mapc (lambda (name)
            (is (string-downcase name) name))
          lcp::+cpp-primitive-type-names+))
  (subtest "designators"
    (mapc (lambda (name)
            (same-type-test name name)
            (same-type-test (string-downcase name) name)
            (different-type-test (string-upcase name) name)
            (different-type-test (string-capitalize name) name)
            (same-type-test (make-symbol (string name)) name)
            (same-type-test (make-symbol (string-downcase name)) name)
            (same-type-test (make-symbol (string-upcase name)) name)
            (same-type-test (make-symbol (string-capitalize name)) name)
            (same-type-test
             (lcp::make-cpp-type (string-downcase name)) name)
            (different-type-test
             (lcp::make-cpp-type (string-upcase name)) name)
            (different-type-test
             (lcp::make-cpp-type (string-capitalize name)) name))
          lcp::+cpp-primitive-type-names+)
    (mapc (lambda (sym)
            (let ((type (lcp::make-cpp-type "MyClass")))
              (same-type-test sym type)))
          `(:my-class :|mY-cLASS| my-class "MyClass"
                      ,(lcp::make-cpp-type "MyClass"))))

  (subtest "parsing"
    (parse-test "char*"
                (lcp::make-cpp-type "*" :type-args '(:char)))

    (parse-test "char *"
                (lcp::make-cpp-type "*" :type-args '(:char)))

    (parse-test "::my_namespace::EnclosingClass::Thing"
                (lcp::make-cpp-type
                 "Thing"
                 :namespace '("" "my_namespace")
                 :enclosing-classes '("EnclosingClass")))

    ;; Unsupported constructs
    (fail-parse-test
     "::std::pair<my_space::MyClass<std::function<void(int, bool)>, double>, char>")
    (fail-parse-test "char (*)[]")
    (fail-parse-test "char (*)[4]")

    ;; We don't handle ordering
    (different-parse-test "const char" "char const")
    (different-parse-test "volatile char" "char volatile")
    (different-parse-test "const volatile char" "char const volatile")
    (different-parse-test "const char *" "char const *")
    (different-parse-test "volatile char *" "char volatile *"))

  (subtest "printing"
    (decl-test "pair<T1, T2>"
               (lcp::make-cpp-type
                "pair"
                :type-args
                (list
                 (lcp::make-cpp-type "T1")
                 (lcp::make-cpp-type "T2"))))

    (decl-test "pair<int, double>"
               (lcp::make-cpp-type "pair" :type-args '(:int :double)))

    (decl-test "pair<TIntegral1, TIntegral2>"
               (lcp::make-cpp-type
                "pair" :type-params '("TIntegral1" "TIntegral2")))

    (decl-test "pair"
               (lcp::make-cpp-type
                "pair" :type-params '("TIntegral1" "TIntegral2"))
               :type-params-p nil))

  (subtest "finding defined enums"
    (let ((lcp::*cpp-classes* nil)
          (lcp::*cpp-enums* nil))
      (lcp:define-enum action (val1 val2))
      (lcp:define-class enclosing-class ()
        ()
        (:public
         (lcp:define-enum action (other-val1 other-val2))
         (lcp:define-class another-enclosing ()
           ()
           (:public
            (lcp:define-enum action (deep-val1 deep-val2))))))
      (lcp:namespace my-namespace)
      (lcp:define-enum action (third-val1 third-val2))
      (lcp:pop-namespace)

      (decl-test "Action" (lcp::find-cpp-enum "::Action"))
      (decl-test "EnclosingClass::Action" (lcp::find-cpp-enum "EnclosingClass::Action"))
      (decl-test "EnclosingClass::Action" (lcp::find-cpp-enum "::EnclosingClass::Action"))
      (decl-test "EnclosingClass::AnotherEnclosing::Action"
                 (lcp::find-cpp-enum "EnclosingClass::AnotherEnclosing::Action"))
      (decl-test "my_namespace::Action" (lcp::find-cpp-enum "my_namespace::Action"))
      (decl-test "my_namespace::Action" (lcp::find-cpp-enum "::my_namespace::Action"))
      (ok (lcp::find-cpp-enum "Action"))
      (ok (lcp::find-cpp-enum 'action))
      (ok (not (lcp::find-cpp-enum "NonExistent")))
      (ok (not (lcp::find-cpp-enum "")))
      (ok (not (lcp::find-cpp-enum "my_namespace::NonExistent")))
      (ok (not (lcp::find-cpp-enum "::NonExistent"))))))

(deftest "util"
  (subtest "fnv1a64"
    (is (lcp::fnv1a64-hash-string "query::plan::LogicalOperator")
        #xCF6E3316FE845113)
    (is (lcp::fnv1a64-hash-string "SomeString") #x1730D3E779304E6C)
    (is (lcp::fnv1a64-hash-string "SomeStrink") #x1730D7E779305538)))

(defun clang-format (cpp-string)
  (with-input-from-string (s cpp-string)
    (string-left-trim
     '(#\Newline)
     (uiop:run-program "clang-format -style=file" :input s :output '(:string :stripped t)))))

(defmacro is-generated (got expected)
  `(is (let ((lcp::*cpp-gensym-counter* 0))
         (clang-format ,got))
       (clang-format ,expected) :test #'string=))

(defun undefine-cpp-types ()
  (setf lcp::*cpp-classes* nil)
  (setf lcp::*cpp-enums* nil))

(deftest "slk"
  (subtest "function declarations"
    (undefine-cpp-types)
    (let ((test-struct (lcp:define-struct test-struct ()
                         ())))
      (is-generated (lcp.slk:save-function-declaration-for-class test-struct)
                    "void Save(const TestStruct &self, slk::Builder *builder)")
      (is-generated (lcp.slk:load-function-declaration-for-class test-struct)
                    "void Load(TestStruct *self, slk::Reader *reader)"))
    (undefine-cpp-types)
    (let ((derived (lcp:define-class derived (base)
                     ())))
      (is-generated (lcp.slk:save-function-declaration-for-class derived)
                    "void Save(const Derived &self, slk::Builder *builder)")
      (is-generated (lcp.slk:load-function-declaration-for-class derived)
                    "void Load(Derived *self, slk::Reader *reader)"))
    (undefine-cpp-types)
    (let ((test-struct (lcp:define-struct test-struct ()
                         ()
                         (:serialize (:slk :save-args '((extra-arg "SaveArgType"))
                                           :load-args '((extra-arg "LoadArgType")))))))
      (is-generated (lcp.slk:save-function-declaration-for-class test-struct)
                    "void Save(const TestStruct &self, slk::Builder *builder, SaveArgType extra_arg)")
      (is-generated (lcp.slk:load-function-declaration-for-class test-struct)
                    "void Load(TestStruct *self, slk::Reader *reader, LoadArgType extra_arg)"))
    (undefine-cpp-types)
    (let ((base-class (lcp:define-struct base ()
                        ()
                        (:serialize (:slk :save-args '((extra-arg "SaveArgType"))
                                          :load-args '((extra-arg "LoadArgType"))))))
          (derived-class (lcp:define-struct derived (base)
                           ())))
      (declare (ignore base-class))
      (is-generated (lcp.slk:save-function-declaration-for-class derived-class)
                    "void Save(const Derived &self, slk::Builder *builder, SaveArgType extra_arg)")
      (is-generated (lcp.slk:load-function-declaration-for-class derived-class)
                    "void Load(Derived *self, slk::Reader *reader, LoadArgType extra_arg)")
      (is-generated (lcp.slk:construct-and-load-function-declaration-for-class derived-class)
                    "void ConstructAndLoad(std::unique_ptr<Derived> *self, slk::Reader *reader, LoadArgType extra_arg)"))
    (undefine-cpp-types)
    (let ((my-enum (lcp:define-enum my-enum
                       (first-value second-value))))
      (is-generated (lcp.slk:save-function-declaration-for-enum my-enum)
                    "void Save(const MyEnum &self, slk::Builder *builder)")
      (is-generated (lcp.slk:load-function-declaration-for-enum my-enum)
                    "void Load(MyEnum *self, slk::Reader *reader)"))
    (undefine-cpp-types)
    ;; Unsupported multiple inheritance
    (is-error (lcp.slk:save-function-declaration-for-class
               (lcp:define-class derived (fst-base snd-base)
                 ()))
              'lcp.slk:slk-error)
    (undefine-cpp-types)
    ;; Ignoring multiple inheritance
    (is-generated (lcp.slk:save-function-declaration-for-class
                   (lcp:define-class derived (fst-base snd-base)
                     ()
                     (:serialize (:slk :ignore-other-base-classes t))))
                  "void Save(const Derived &self, slk::Builder *builder)")
    (undefine-cpp-types)
    ;; Unsupported class templates
    (is-error (lcp.slk:save-function-declaration-for-class
               (lcp:define-class (derived t-param) (base)
                 ()))
              'lcp.slk:slk-error)
    (undefine-cpp-types)
    (is-error (lcp.slk:save-function-declaration-for-class
               (lcp:define-struct (test-struct fst-param snd-param) ()
                 ()))
              'lcp.slk:slk-error)
    (undefine-cpp-types))

  (subtest "enum serialization"
    (undefine-cpp-types)
    (let ((my-enum (lcp:define-enum my-enum
                       (first-value second-value))))
      (is-generated (lcp.slk:save-function-definition-for-enum my-enum)
                    "void Save(const MyEnum &self, slk::Builder *builder) {
                       uint8_t enum_value;
                       switch (self) {
                         case MyEnum::FIRST_VALUE: enum_value = 0; break;
                         case MyEnum::SECOND_VALUE: enum_value = 1; break;
                       }
                       slk::Save(enum_value, builder);
                     }")
      (is-generated (lcp.slk:load-function-definition-for-enum my-enum)
                    "void Load(MyEnum *self, slk::Reader *reader) {
                       uint8_t enum_value;
                       slk::Load(&enum_value, reader);
                       switch (enum_value) {
                         case static_cast<uint8_t>(0): *self = MyEnum::FIRST_VALUE; break;
                         case static_cast<uint8_t>(1): *self = MyEnum::SECOND_VALUE; break;
                         default: throw slk::SlkDecodeException(\"Trying to load unknown enum value!\");
                       }
                     }")))

  (subtest "plain class serialization"
    (undefine-cpp-types)
    (let ((test-struct (lcp:define-struct test-struct ()
                         ((int-member :int64_t)
                          (vec-member "std::vector<SomeType>")))))
      (is-generated (lcp.slk:save-function-definition-for-class test-struct)
                    "void Save(const TestStruct &self, slk::Builder *builder) {
                       slk::Save(self.int_member, builder);
                       slk::Save(self.vec_member, builder);
                     }")
      (is-generated (lcp.slk:load-function-definition-for-class test-struct)
                    "void Load (TestStruct *self, slk::Reader *reader) {
                       slk::Load(&self->int_member, reader);
                       slk::Load(&self->vec_member, reader);
                     }"))
    (undefine-cpp-types)
    (let ((test-struct (lcp:define-struct test-struct ()
                         ((skip-member :int64_t :dont-save t)))))
      (is-generated (lcp.slk:save-function-definition-for-class test-struct)
                    "void Save(const TestStruct &self, slk::Builder *builder) {}")
      (is-generated (lcp.slk:load-function-definition-for-class test-struct)
                    "void Load(TestStruct *self, slk::Reader *reader) {}"))
    (undefine-cpp-types)
    (let ((test-struct
           (lcp:define-struct test-struct ()
             ((custom-member "SomeType"
                             :slk-save (lambda (member-name)
                                         (check-type member-name string)
                                         (format nil "self.~A.CustomSave(builder);" member-name))
                             :slk-load (lambda (member-name)
                                         (check-type member-name string)
                                         (format nil "self->~A.CustomLoad(reader);" member-name)))))))
      (is-generated (lcp.slk:save-function-definition-for-class test-struct)
                    "void Save(const TestStruct &self, slk::Builder *builder) {
                       { self.custom_member.CustomSave(builder); }
                     }")
      (is-generated (lcp.slk:load-function-definition-for-class test-struct)
                    "void Load(TestStruct *self, slk::Reader *reader) {
                       { self->custom_member.CustomLoad(reader); }
                     }"))
    (undefine-cpp-types)
    (let ((raw-ptr-class (lcp:define-struct has-raw-ptr ()
                           ((raw-ptr "SomeType *"))))
          (shared-ptr-class (lcp:define-struct has-shared-ptr ()
                              ((shared-ptr "std::shared_ptr<SomeType>"))))
          (unique-ptr-class (lcp:define-struct has-unique-ptr()
                              ((unique-ptr "std::unique_ptr<SomeType>")))))
      (dolist (ptr-class (list raw-ptr-class shared-ptr-class unique-ptr-class))
        (is-error (lcp.slk:save-function-definition-for-class ptr-class)
                  'lcp.slk:slk-error)
        (is-error (lcp.slk:load-function-definition-for-class ptr-class)
                  'lcp.slk:slk-error)))
    (undefine-cpp-types)
    (flet ((custom-save (m)
             (declare (ignore m))
             "CustomSave();")
           (custom-load (m)
             (declare (ignore m))
             "CustomLoad();"))
      (let ((raw-ptr-class (lcp:define-struct has-raw-ptr ()
                             ((member "SomeType *"
                                      :slk-save #'custom-save
                                      :slk-load #'custom-load))))
            (shared-ptr-class (lcp:define-struct has-shared-ptr ()
                                ((member "std::shared_ptr<SomeType>"
                                         :slk-save #'custom-save
                                         :slk-load #'custom-load))))
            (unique-ptr-class (lcp:define-struct has-unique-ptr()
                                ((member "std::unique_ptr<SomeType>"
                                         :slk-save #'custom-save
                                         :slk-load #'custom-load)))))
        (dolist (ptr-class (list raw-ptr-class shared-ptr-class unique-ptr-class))
          (is-generated (lcp.slk:save-function-definition-for-class ptr-class)
                        (format nil "void Save(const ~A &self, slk::Builder *builder) { { CustomSave(); } }"
                                (lcp::cpp-type-decl ptr-class)))
          (is-generated (lcp.slk:load-function-definition-for-class ptr-class)
                        (format nil "void Load(~A *self, slk::Reader *reader) { { CustomLoad(); } }"
                                (lcp::cpp-type-decl ptr-class)))))))

  (subtest "class inheritance serialization"
    (undefine-cpp-types)
    ;; Unsupported multiple inheritance
    (is-error (lcp.slk:save-function-declaration-for-class
               (lcp:define-struct derived (fst-base snd-base)
                 ()))
              'lcp.slk:slk-error)

    (undefine-cpp-types)
    ;; We will test single inheritance and ignored multiple inheritance, both
    ;; should generate the same code that follows.
    (let ((base-save-code
           "void Save(const Base &self, slk::Builder *builder) {
              if (const auto *derived_derived = utils::Downcast<const Derived>(&self)) {
                return slk::Save(*derived_derived, builder);
              }
              slk::Save(Base::kType.id, builder);
              slk::Save(self.base_member, builder);
            }")
          (base-construct-code
           "void ConstructAndLoad(std::unique_ptr<Base> *self, slk::Reader *reader) {
              uint64_t type_id;
              slk::Load(&type_id, reader);
              if (Base::kType.id == type_id) {
                auto base_instance = std::make_unique<Base>();
                slk::Load(base_instance.get(), reader);
                *self = std::move(base_instance);
                return;
              }
              if (Derived::kType.id == type_id) {
                auto derived_instance = std::make_unique<Derived>();
                slk::Load(derived_instance.get(), reader);
                *self = std::move(derived_instance);
                return;
              }
              throw slk::SlkDecodeException(\"Trying to load unknown derived type!\");
            }")
          (base-load-code
           "void Load(Base *self, slk::Reader *reader) {
              if (self->GetTypeInfo() != Base::kType)
                throw slk::SlkDecodeException(\"Trying to load incorrect derived type!\");
              slk::Load(&self->base_member, reader);
            }")
          (derived-save-code
           "void Save(const Derived &self, slk::Builder *builder) {
              slk::Save(Derived::kType.id, builder);
              // Save parent Base
              { slk::Save(self.base_member, builder); }
              slk::Save(self.derived_member, builder);
            }")
          (derived-construct-code
           "void ConstructAndLoad(std::unique_ptr<Derived> *self, slk::Reader *reader) {
              uint64_t type_id;
              slk::Load(&type_id, reader);
              if (Derived::kType.id == type_id) {
                auto derived_instance = std::make_unique<Derived>();
                slk::Load(derived_instance.get(), reader);
                *self = std::move(derived_instance);
                return;
              }
              throw slk::SlkDecodeException(\"Trying to load unknown derived type!\");
            }")
          (derived-load-code
           "void Load(Derived *self, slk::Reader *reader) {
              // Load parent Base
              { slk::Load(&self->base_member, reader); }
              slk::Load(&self->derived_member, reader);
            }"))
      ;; Single inheritance
      (let ((base-class (lcp:define-struct base ()
                          ((base-member :bool))))
            (derived-class (lcp:define-struct derived (base)
                             ((derived-member :int64_t)))))
        (is-generated (lcp.slk:save-function-definition-for-class base-class)
                      base-save-code)
        (is-generated (lcp.slk:construct-and-load-function-definition-for-class base-class)
                      base-construct-code)
        (is-generated (lcp.slk:load-function-definition-for-class base-class)
                      base-load-code)
        (is-generated (lcp.slk:save-function-definition-for-class derived-class)
                      derived-save-code)
        (is-generated (lcp.slk:construct-and-load-function-definition-for-class derived-class)
                      derived-construct-code)
        (is-generated (lcp.slk:load-function-definition-for-class derived-class)
                      derived-load-code))
      (undefine-cpp-types)
      ;; Ignored multiple inheritance should be the same as single
      (let ((base-class (lcp:define-struct base ()
                          ((base-member :bool))))
            (derived-class (lcp:define-struct derived (base ignored-base)
                             ((derived-member :int64_t))
                             (:serialize (:slk :ignore-other-base-classes t)))))
        (is-generated (lcp.slk:save-function-definition-for-class base-class)
                      base-save-code)
        (is-generated (lcp.slk:construct-and-load-function-definition-for-class base-class)
                      base-construct-code)
        (is-generated (lcp.slk:load-function-definition-for-class base-class)
                      base-load-code)
        (is-generated (lcp.slk:save-function-definition-for-class derived-class)
                      derived-save-code)
        (is-generated (lcp.slk:construct-and-load-function-definition-for-class derived-class)
                      derived-construct-code)
        (is-generated (lcp.slk:load-function-definition-for-class derived-class)
                      derived-load-code)))

    (undefine-cpp-types)
    (let ((abstract-base-class (lcp:define-struct abstract-base ()
                                 ((base-member :bool))
                                 (:abstractp t)))
          (derived-class (lcp:define-struct derived (abstract-base)
                           ((derived-member :int64_t)))))
      (is-generated (lcp.slk:save-function-definition-for-class abstract-base-class)
                    "void Save(const AbstractBase &self, slk::Builder *builder) {
                       if (const auto *derived_derived = utils::Downcast<const Derived>(&self)) {
                         return slk::Save(*derived_derived, builder);
                       }
                       LOG(FATAL) << \"`AbstractBase` is marked as an abstract class!\";
                     }")
      (is-generated (lcp.slk:construct-and-load-function-definition-for-class abstract-base-class)
                    "void ConstructAndLoad(std::unique_ptr<AbstractBase> *self, slk::Reader *reader) {
                       uint64_t type_id;
                       slk::Load(&type_id, reader);
                       if (Derived::kType.id == type_id) {
                         auto derived_instance = std::make_unique<Derived>();
                         slk::Load(derived_instance.get(), reader);
                         *self = std::move(derived_instance);
                         return;
                       }
                       throw slk::SlkDecodeException(\"Trying to load unknown derived type!\");
                     }")
      (is-generated (lcp.slk:save-function-definition-for-class derived-class)
                    "void Save(const Derived &self, slk::Builder *builder) {
                       slk::Save(Derived::kType.id, builder);
                       // Save parent AbstractBase
                       { slk::Save(self.base_member, builder); }
                       slk::Save(self.derived_member, builder);
                     }")
      (is-generated (lcp.slk:construct-and-load-function-definition-for-class derived-class)
                    "void ConstructAndLoad(std::unique_ptr<Derived> *self, slk::Reader *reader) {
                       uint64_t type_id;
                       slk::Load(&type_id, reader);
                       if (Derived::kType.id == type_id) {
                         auto derived_instance = std::make_unique<Derived>();
                         slk::Load(derived_instance.get(), reader);
                         *self = std::move(derived_instance);
                         return;
                       }
                       throw slk::SlkDecodeException(\"Trying to load unknown derived type!\");
                     }")
      (is-generated (lcp.slk:load-function-definition-for-class derived-class)
                    "void Load(Derived *self, slk::Reader *reader) {
                       // Load parent AbstractBase
                       { slk::Load(&self->base_member, reader); }
                       slk::Load(&self->derived_member, reader);
                     }"))
    (undefine-cpp-types)
    (let ((base-class-template (lcp:define-struct (base t-param) ()
                                  ((base-member :bool))))
          (derived-class (lcp:define-struct derived (base)
                           ((derived-member :int64_t)))))
      (is-error (lcp.slk:save-function-definition-for-class base-class-template)
                'lcp.slk:slk-error)
      (is-error (lcp.slk:save-function-definition-for-class derived-class)
                'lcp.slk:slk-error))
    (undefine-cpp-types)
    (let ((base-class (lcp:define-struct base ()
                        ((base-member :bool))))
          (derived-class-template (lcp:define-struct (derived t-param) (base)
                                     ((derived-member :int64_t)))))
      (is-error (lcp.slk:save-function-definition-for-class base-class)
                'lcp.slk:slk-error)
      (is-error (lcp.slk:save-function-definition-for-class derived-class-template)
                'lcp.slk:slk-error))

    (undefine-cpp-types)
    (let ((class (lcp:define-struct derived ("UnknownBase")
                   ((member :bool)))))
      (is-error (lcp.slk:save-function-definition-for-class class)
                'lcp.slk:slk-error)
      (is-error (lcp.slk:load-function-definition-for-class class)
                'lcp.slk:slk-error))
    (undefine-cpp-types)
    (let ((class (lcp:define-struct derived ("UnknownBase")
                   ((member :bool))
                   (:serialize (:slk :base t)))))
      (is-generated (lcp.slk:save-function-definition-for-class class)
                    "void Save(const Derived &self, slk::Builder *builder) { slk::Save(self.member, builder); }")
      (is-generated (lcp.slk:load-function-definition-for-class class)
                    "void Load(Derived *self, slk::Reader *reader) { slk::Load(&self->member, reader); }"))

    (undefine-cpp-types)
    (let ((base-class (lcp:define-struct base ()
                        ()
                        (:abstractp t)
                        (:serialize (:slk :save-args '((extra-arg "SaveArg"))
                                          :load-args '((extra-arg "LoadArg"))))))
          (derived-class (lcp:define-struct derived (base)
                           ())))
      (declare (ignore derived-class))
      (is-generated (lcp.slk:save-function-definition-for-class base-class)
                    "void Save(const Base &self, slk::Builder *builder, SaveArg extra_arg) {
                       if (const auto *derived_derived = utils::Downcast<const Derived>(&self)) {
                         return slk::Save(*derived_derived, builder, extra_arg);
                       }
                       LOG(FATAL) << \"`Base` is marked as an abstract class!\";
                     }")
      (is-generated (lcp.slk:construct-and-load-function-definition-for-class base-class)
                    "void ConstructAndLoad(std::unique_ptr<Base> *self, slk::Reader *reader, LoadArg extra_arg) {
                       uint64_t type_id;
                       slk::Load(&type_id, reader);
                       if (Derived::kType.id == type_id) {
                         auto derived_instance = std::make_unique<Derived>();
                         slk::Load(derived_instance.get(), reader, extra_arg);
                         *self = std::move(derived_instance);
                         return;
                       }
                       throw slk::SlkDecodeException(\"Trying to load unknown derived type!\");
                     }")))

  (subtest "non-public members"
    (undefine-cpp-types)
    (is-error (lcp.slk:save-function-definition-for-class
               (lcp:define-class test-class ()
                 ((public-member :bool :scope :public)
                  (private-member :int64_t))))
              'lcp.slk:slk-error)
    (undefine-cpp-types)
    (is-error (lcp.slk:save-function-definition-for-class
               (lcp:define-struct test-struct ()
                 ((protected-member :int64_t :scope :protected)
                  (public-member :char))))
              'lcp.slk:slk-error)))

(deftest "clone"
  (macrolet ((single-member-test (member expected)
               (let ((class-sym (gensym)))
                 `(let ((,class-sym (lcp:define-class my-class ()
                                      (,member)
                                      (:clone))))
                    ,(etypecase expected
                       (string
                        `(is-generated (lcp.clone:clone-function-definition-for-class ,class-sym)
                                       (format nil "MyClass Clone() const {
                                                     MyClass object;
                                                     ~A
                                                     return object;
                                                   }"
                                               ,expected)))
                       (symbol
                        (assert (eq expected 'lcp.clone:clone-error))
                        `(is-error (lcp.clone:clone-function-definition-for-class ,class-sym)
                                   ',expected)))))))
    (subtest "no inheritance"
      (undefine-cpp-types)
      (let ((tree-class (lcp:define-class tree ()
                          ((value :int32_t)
                           (left "std::unique_ptr<Tree>")
                           (right "std::unique_ptr<Tree>"))
                          (:clone :return-type (lambda (typename)
                                                 (format nil "std::unique_ptr<~A>" typename))
                                  :init-object (lambda (var typename)
                                                 (format nil "auto ~A = std::make_unique<~A>();"
                                                         var typename)))))
            (forest-class (lcp:define-class forest ()
                            ((name "std::string")
                             (small-tree "std::unique_ptr<Tree>")
                             (big-tree "std::unique_ptr<Tree>"))
                            (:clone))))
        (is-generated (lcp.clone:clone-function-definition-for-class tree-class)
                      "std::unique_ptr<Tree> Clone() const {
                       auto object = std::make_unique<Tree>();
                       object->value_ = value_;
                       object->left_ = left_ ? left_->Clone() : nullptr;
                       object->right_ = right_ ? right_->Clone() : nullptr;
                       return object;
                     }")
        (is-generated (lcp.clone:clone-function-definition-for-class forest-class)
                      "Forest Clone() const {
                       Forest object;
                       object.name_ = name_;
                       object.small_tree_ = small_tree_ ? small_tree_->Clone() : nullptr;
                       object.big_tree_ = big_tree_ ? big_tree_->Clone() : nullptr;
                       return object;
                     }")))
    (subtest "single inheritance"
      (undefine-cpp-types)
      ;; Simple case
      (let ((base-class (lcp:define-class base ()
                          ((int-member :int32_t)
                           (string-member "std::string"))
                          (:clone)))
            (child-class (lcp:define-class child (base)
                           ((another-int-member :int64_t))
                           (:clone))))
        (is-generated (lcp.clone:clone-function-definition-for-class base-class)
                      "virtual std::unique_ptr<Base> Clone() const {
                       auto object = std::make_unique<Base>();
                       object->int_member_ = int_member_;
                       object->string_member_ = string_member_;
                       return object;
                     }")
        (is-generated (lcp.clone:clone-function-definition-for-class child-class)
                      "std::unique_ptr<Base> Clone() const override {
                       auto object = std::make_unique<Child>();
                       object->int_member_ = int_member_;
                       object->string_member_ = string_member_;
                       object->another_int_member_ = another_int_member_;
                       return object;
                     }"))
      (undefine-cpp-types)
      ;; Abstract base class
      (let ((base-class (lcp:define-class base ()
                          ((int-member :int32_t)
                           (string-member "std::string"))
                          (:abstractp t)
                          (:clone)))
            (child-class (lcp:define-class child (base)
                           ((another-int-member :int64_t))
                           (:clone))))
        (is-generated (lcp.clone:clone-function-definition-for-class base-class)
                      "virtual std::unique_ptr<Base> Clone() const = 0;")
        (is-generated (lcp.clone:clone-function-definition-for-class child-class)
                      "std::unique_ptr<Base> Clone() const override {
                       auto object = std::make_unique<Child>();
                       object->int_member_ = int_member_;
                       object->string_member_ = string_member_;
                       object->another_int_member_ = another_int_member_;
                       return object;
                     }"))
      (undefine-cpp-types)
      ;; :return-type and :init-object propagation
      (let ((base-class (lcp:define-class base ()
                          ((int-member :int32_t)
                           (string-member "std::string"))
                          (:abstractp t)
                          (:clone :return-type (lambda (typename)
                                                 (format nil "~A*" typename))
                                  :init-object (lambda (var typename)
                                                 (format nil "~A* ~A = GlobalFactory::Create();"
                                                         typename var)))))
            (child-class (lcp:define-class child (base)
                           ((another-int-member :int64_t))
                           (:clone))))
        (is-generated (lcp.clone:clone-function-definition-for-class base-class)
                      "virtual Base *Clone() const = 0;")
        (is-generated (lcp.clone:clone-function-definition-for-class child-class)
                      "Child *Clone() const override {
                       Child *object = GlobalFactory::Create();
                       object->int_member_ = int_member_;
                       object->string_member_ = string_member_;
                       object->another_int_member_ = another_int_member_;
                       return object;
                     }"))
      (undefine-cpp-types)
      ;; inheritance with :ignore-other-base-classes and :base
      (let ((base-class (lcp:define-class base ("::utils::TotalOrdering")
                          ((int-member :int32_t)
                           (string-member "std::string"))
                          (:abstractp t)
                          (:clone :base t
                                  :return-type (lambda (typename)
                                                 (format nil "~A*" typename))
                                  :init-object (lambda (var typename)
                                                 (format nil "~A* ~A = GlobalFactory::Create();"
                                                         typename var)))))
            (child-class (lcp:define-class child (base "::utils::TotalOrdering" "::utils::TotalOrdering")
                           ((another-int-member :int64_t))
                           (:clone :ignore-other-base-classes t))))
        (is-generated (lcp.clone:clone-function-definition-for-class base-class)
                      "virtual Base *Clone() const = 0;")
        (is-generated (lcp.clone:clone-function-definition-for-class child-class)
                      "Child *Clone() const override {
                       Child *object = GlobalFactory::Create();
                       object->int_member_ = int_member_;
                       object->string_member_ = string_member_;
                       object->another_int_member_ = another_int_member_;
                       return object;
                     }")))
    (subtest "extra args"
      (undefine-cpp-types)
      ;; extra arguments are always passed when calling `Clone` function
      (let ((expression-class (lcp:define-class expression ()
                                ((lhs "Expression *")
                                 (rhs "Expression *"))
                                (:abstractp t)
                                (:clone :return-type (lambda (typename)
                                                       (format nil "~A*" typename))
                                        :init-object (lambda (var typename)
                                                       (format nil "~A* ~A = storage->Create<~A>();"
                                                               typename var typename))
                                        :args '((storage "ExpressionStorage *")))))
            (and-class (lcp:define-class and (expression)
                         ()
                         (:clone)))
            (or-class (lcp:define-class or (expression)
                        ()
                        (:clone)))
            (filter-class (lcp:define-class filter ()
                            ((expressions "std::vector<Expression *>"))
                            (:clone :args '((exp-storage "ExpressionStorage *"))))))
        (is-generated (lcp.clone:clone-function-definition-for-class expression-class)
                      "virtual Expression *Clone(ExpressionStorage *storage) const = 0;")
        (is-generated (lcp.clone:clone-function-definition-for-class and-class)
                      "And *Clone(ExpressionStorage *storage) const override {
                       And *object = storage->Create<And>();
                       object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
                       object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
                       return object;
                     }")
        (is-generated (lcp.clone:clone-function-definition-for-class or-class)
                      "Or *Clone(ExpressionStorage *storage) const override {
                       Or *object = storage->Create<Or>();
                       object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
                       object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
                       return object;
                     }")
        (is-generated (lcp.clone:clone-function-definition-for-class filter-class)
                      "Filter Clone(ExpressionStorage *exp_storage) const {
                       Filter object;
                       object.expressions_.resize(expressions_.size());
                       for (auto i0 = 0; i0 < expressions_.size(); ++i0) {
                         object.expressions_[i0] =
                             expressions_[i0] ? expressions_[i0]->Clone(exp_storage) : nullptr;
                       }
                       return object;
                     }")))
    (subtest "unsupported"
      ;; multiple inheritance
      (undefine-cpp-types)
      (lcp:define-class first-base ()
        ((int-member :int32_t))
        (:clone))
      (lcp:define-class second-base ()
        ((private-member :int32_t :scope :private))
        (:clone))
      (let ((child-class (lcp:define-class child (first-base second-base)
                           ((name "std::string"))
                           (:clone))))
        (is-error (lcp.clone:clone-function-definition-for-class child-class)
                  'lcp.clone:clone-error))
      ;; Class templates
      (undefine-cpp-types)
      (let ((container-class (lcp:define-class (my-container t-element) ()
                               ((data "TElement *")
                                (size "size_t")))))
        (is-error (lcp.clone:clone-function-definition-for-class container-class)
                  'lcp.clone:clone-error)))
    (subtest "custom clone"
      (undefine-cpp-types)
      (let ((my-class (lcp:define-class my-class ()
                        ((callback "std::function<void(int, int)>" :clone :copy)
                         (click-counter :int32_t :clone nil)
                         (widget "Widget"
                                 :clone (lambda (source dest)
                                          #>cpp
                                          ${dest} = WidgetFactory::Create(${source}.type());
                                          cpp<#)))
                        (:clone))))
        (is-generated (lcp.clone:clone-function-definition-for-class my-class)
                      "MyClass Clone() const {
                       MyClass object;
                       object.callback_ = callback_;
                       object.widget_ = WidgetFactory::Create(widget_.type());
                       return object;
                     }")))
    (subtest "types"
      (undefine-cpp-types)
      (lcp:define-class klondike ()
        ()
        (:clone))
      (subtest "vector"
        (single-member-test (member "std::vector<int32_t>")
                            "object.member_ = member_;")
        (single-member-test (member "std::vector<std::vector<int32_t>>")
                            "object.member_ = member_;")
        (single-member-test (member "std::vector<Klondike>")
                            "object.member_.resize(member_.size());
                             for (auto i0 = 0; i0 < member_.size(); ++i0) {
                               object.member_[i0] = member_[i0].Clone();
                             }")
        (single-member-test (member "std::vector<std::vector<Klondike>>")
                            "object.member_.resize(member_.size());
                             for (auto i0 = 0; i0 < member_.size(); ++i0) {
                               object.member_[i0].resize(member_[i0].size());
                               for (auto i1 = 0; i1 < member_[i0].size(); ++i1) {
                                 object.member_[i0][i1] = member_[i0][i1].Clone();
                               }
                             }"))
      (subtest "optional"
        (single-member-test (member "std::optional<int32_t>")
                            "object.member_ = member_;")
        (single-member-test (member "std::optional<Klondike>")
                            "if (member_) {
                               Klondike value0;
                               value0 = (*member_).Clone();
                               object.member_.emplace(std::move(value0));
                             } else {
                               object.member_ = std::nullopt;
                             }"))
      (subtest "unordered_map"
        (single-member-test (member "std::unordered_map<int32_t, std::string>")
                            "object.member_ = member_;")
        (single-member-test (member "std::unordered_map<int32_t, std::unordered_map<int32_t, std::string>>")
                            "object.member_ = member_;")
        (single-member-test (member "std::unordered_map<int32_t, Klondike>")
                            "for (const auto &kv0 : member_) {
                               std::pair<int32_t, Klondike> entry1;
                               {
                                 int32_t first2;
                                 first2 = kv0.first;
                                 Klondike second3;
                                 second3 = kv0.second.Clone();
                                 entry1 = std::make_pair(std::move(first2), std::move(second3));
                               }

                               object.member_.emplace(std::move(entry1));
                             }")
        (single-member-test (member "std::unordered_map<int32_t, std::unordered_map<int32_t, Klondike>>")
                            "for (const auto &kv0 : member_) {
                               std::pair<int32_t, std::unordered_map<int32_t, Klondike>> entry1;
                               {
                                 int32_t first2;
                                 first2 = kv0.first;
                                 std::unordered_map<int32_t, Klondike> second3;
                                 for (const auto &kv4 : kv0.second) {
                                   std::pair<int32_t, Klondike> entry5;
                                   {
                                     int32_t first6;
                                     first6 = kv4.first;
                                     Klondike second7;
                                     second7 = kv4.second.Clone();
                                     entry5 = std::make_pair(std::move(first6), std::move(second7));
                                   }

                                   second3.emplace(std::move(entry5));
                                 }
                                 entry1 = std::make_pair(std::move(first2), std::move(second3));

                               }

                               object.member_.emplace(std::move(entry1));
                             }")
        (single-member-test (member "std::unordered_map<Klondike, Klondike>")
                            "for (const auto &kv0 : member_) {
                               std::pair<Klondike, Klondike> entry1;
                               {
                                 Klondike first2;
                                 first2 = kv0.first.Clone();
                                 Klondike second3;
                                 second3 = kv0.second.Clone();
                                 entry1 = std::make_pair(std::move(first2), std::move(second3));
                               }

                               object.member_.emplace(std::move(entry1));
                             }"))
      (subtest "pair"
        (single-member-test (member "std::pair<int32_t, int32_t>")
                            "object.member_ = member_;")
        (single-member-test (member "std::pair<int32_t, Klondike>")
                            "{
                               int32_t first0;
                               first0 = member_.first;
                               Klondike second1;
                               second1 = member_.second.Clone();
                               object.member_ = std::make_pair(std::move(first0), std::move(second1));
                             }")
        (single-member-test (member "std::pair<Klondike, int32_t>")
                            "{
                               Klondike first0;
                               first0 = member_.first.Clone();
                               int32_t second1;
                               second1 = member_.second;
                               object.member_ = std::make_pair(std::move(first0), std::move(second1));
                             }")
        (single-member-test (member "std::pair<Klondike, Klondike>")
                            "{
                               Klondike first0;
                               first0 = member_.first.Clone();
                               Klondike second1;
                               second1 = member_.second.Clone();
                               object.member_ = std::make_pair(std::move(first0), std::move(second1));
                             }")
        (single-member-test (member "std::pair<std::string, std::pair<int32_t, Klondike>>")
                            "{
                               std::string first0;
                               first0 = member_.first;
                               std::pair<int32_t, Klondike> second1;
                               {
                                 int32_t first2;
                                 first2 = member_.second.first;
                                 Klondike second3;
                                 second3 = member_.second.second.Clone();
                                 second1 = std::make_pair(std::move(first2), std::move(second3));
                               }

                               object.member_ = std::make_pair(std::move(first0), std::move(second1));
                             }"))
      (subtest "pointers"
        (single-member-test (member "Klondike *")
                            "object.member_ = member_ ? member_->Clone() : nullptr;")
        (single-member-test (member "std::unique_ptr<Klondike>")
                            "object.member_ = member_ ? member_->Clone() : nullptr;")
        (single-member-test (member "std::shared_ptr<Klondike>")
                            "object.member_ = member_ ? member_->Clone() : nullptr;"))
      (subtest "enum"
        (lcp:define-enum enum (val1 val2 val3))
        (single-member-test (member "Enum")
                            "object.member_ = member_;"))
      (subtest "builtin c++ types"
        (single-member-test (member :int32_t)
                            "object.member_ = member_;")
        (single-member-test (member :char)
                            "object.member_ = member_;")))
    (subtest "class copying"
      (undefine-cpp-types)
      (lcp:define-class non-copyable-class-1 ()
        ((counter "int32_t *")))
      (lcp:define-class cloneable-class ()
        ((uid :int32_t))
        (:clone))
      (lcp:define-class copyable-class-1 ()
        ((counter "int32_t *" :clone :copy)))
      (lcp:define-class copyable-class-2 ()
        ((member :int32_t)))
      (single-member-test (member "NonCopyableClass1")
                          lcp.clone:clone-error)
      (single-member-test (member "CloneableClass")
                          "object.member_ = member_.Clone();")
      (single-member-test (member "CopyableClass1")
                          "object.member_ = member_;")
      (single-member-test (member "CopyableClass2")
                          "object.member_ = member_;")
      (single-member-test (member "UnknownClass")
                          "object.member_ = member_;")
      (undefine-cpp-types))))
