(defpackage #:lcp-test
  (:use #:cl #:prove))

(in-package #:lcp-test)

(defun same-type-test (a b)
  "Test whether A and B are the same C++ type under LCP::CPP-TYPE=."
  (is a b :test #'lcp::cpp-type=))

(defun parse-test (type-decl cpp-type)
  "Test whether TYPE-DECL parses as the C++ type designated by CPP-TYPE."
  (is (lcp::parse-cpp-type-declaration type-decl) cpp-type
      :test #'lcp::cpp-type=))

(defun decl-test (type-decl cpp-type &key (type-params t) (namespace t))
  "Test whether the C++ type designated by CPP-TYPE prints as TYPE-DECL."
  (is (lcp::cpp-type-decl cpp-type
                          :type-params type-params
                          :namespace namespace)
      type-decl))

(defun different-parse-test (type-decl1 type-decl2)
  (isnt (lcp::parse-cpp-type-declaration type-decl1)
        (lcp::parse-cpp-type-declaration type-decl2)
        :test #'lcp::cpp-type=))

(plan nil)

(deftest "supported"
  (subtest "designators"
    (mapc (lambda (sym)
            (let ((type (lcp::make-cpp-primitive-type sym)))
              (same-type-test sym type)
              (same-type-test (string-downcase sym) type)
              (same-type-test (string-upcase sym) type)
              (same-type-test (string-capitalize sym) type)
              (same-type-test (intern (string sym)) type)
              (same-type-test (intern (string-downcase sym)) type)
              (same-type-test (intern (string-upcase sym)) type)
              (same-type-test (intern (string-capitalize sym)) type)
              (same-type-test (lcp::make-cpp-primitive-type sym)
                              type)))
          lcp::+cpp-primitive-type-keywords+)
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

    (parse-test "::std::pair<my_space::MyClass<std::function<void(int, bool)>, double>, char>"
                (lcp::make-cpp-type
                 "pair"
                 :namespace'("" "std")
                 :type-args
                 `(,(lcp::make-cpp-type
                     "MyClass"
                     :namespace '("my_space")
                     :type-args
                     `(,(lcp::make-cpp-type
                         "function"
                         :namespace '("std")
                         :type-args '("void(int, bool)"))
                       :double))
                    :char)))

    (parse-test "::my_namespace::EnclosingClass::Thing"
                (lcp::make-cpp-type "Thing"
                                    :namespace '("" "my_namespace")
                                    :enclosing-class "EnclosingClass")))

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
               :type-params nil))

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

(deftest "unsupported"
  (subtest "cv-qualifiers"
    (different-parse-test "const char" "char const")
    (different-parse-test "volatile char" "char volatile")
    (different-parse-test "const volatile char" "char const volatile")
    (different-parse-test "const char *" "char const *")
    (different-parse-test "volatile char *" "char volatile *"))

  (subtest "arrays"
    (different-parse-test "char (*)[]" "char (*) []")
    (different-parse-test "char (*)[4]" "char (*) [4]")))

(deftest "fnv-hash"
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

(defun is-generated (got expected)
  (is (clang-format got) (clang-format expected) :test #'string=))

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
    ;; Unsupported template classes
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
              if (const auto *derived_derived = dynamic_cast<const Derived *>(&self)) {
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
              // CHECK(self->GetTypeInfo() == Base::kType);
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
                       if (const auto *derived_derived = dynamic_cast<const Derived *>(&self)) {
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
    (let ((base-templated-class (lcp:define-struct (base t-param) ()
                                  ((base-member :bool))))
          (derived-class (lcp:define-struct derived (base)
                           ((derived-member :int64_t)))))
      (is-error (lcp.slk:save-function-definition-for-class base-templated-class)
                'lcp.slk:slk-error)
      (is-error (lcp.slk:save-function-definition-for-class derived-class)
                'lcp.slk:slk-error))
    (undefine-cpp-types)
    (let ((base-class (lcp:define-struct base ()
                        ((base-member :bool))))
          (derived-templated-class (lcp:define-struct (derived t-param) (base)
                                     ((derived-member :int64_t)))))
      (is-error (lcp.slk:save-function-definition-for-class base-class)
                'lcp.slk:slk-error)
      (is-error (lcp.slk:save-function-definition-for-class derived-templated-class)
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
                       if (const auto *derived_derived = dynamic_cast<const Derived *>(&self)) {
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
