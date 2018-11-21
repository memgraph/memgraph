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

(defun decl-test (type-decl type &key (type-params t) (namespace t))
  "Test whether the C++ type designated by TYPE prints as TYPE-DECL."
  (is (lcp::cpp-type-decl type
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
                "pair" :type-params '("TIntegral1 TIntegral2"))
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
    (is-generated (lcp.slk:save-function-declaration-for-class
                   (lcp:define-struct test-struct ()
                     ()))
                  "void Save(const TestStruct &self, slk::Builder *builder)")
    (undefine-cpp-types)
    (is-generated (lcp.slk:save-function-declaration-for-class
                   (lcp:define-class derived (base)
                     ()))
                  "void Save(const Derived &self, slk::Builder *builder)")
    (undefine-cpp-types)
    (let ((my-enum (lcp:define-enum my-enum
                       (first-value second-value))))
      (is-generated (lcp.slk:save-function-declaration-for-enum my-enum)
                    "void Save(const MyEnum &self, slk::Builder *builder)")
      (is-generated (lcp.slk:load-function-declaration-for-enum my-enum)
                    "void Load(MyEnum *self, slk::Reader *reader)"))
    (undefine-cpp-types)
    (is-error (lcp.slk:save-function-declaration-for-class
               (lcp:define-class derived (fst-base snd-base)
                 ()))
              'lcp.slk:slk-error)
    (undefine-cpp-types)
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

  (subtest "save definitions"
    (undefine-cpp-types)
    (is-generated (lcp.slk:save-function-definition-for-class
                   (lcp:define-struct test-struct ()
                     ((int-member :int64_t)
                      (vec-member "std::vector<SomeType>"))))
                  "void Save(const TestStruct &self, slk::Builder *builder) {
                     slk::Save(self.int_member, builder);
                     slk::Save(self.vec_member, builder);
                   }")
    (undefine-cpp-types)
    (is-generated (lcp.slk:save-function-definition-for-class
                   (lcp:define-struct test-struct ()
                     ((skip-member :int64_t :dont-save t))))
                  "void Save(const TestStruct &self, slk::Builder *builder) {}")
    (undefine-cpp-types)
    (is-generated (lcp.slk:save-function-definition-for-class
                   (lcp:define-struct test-struct ()
                     ((custom-member "SomeType"
                                     :slk-save (lambda (member-name)
                                                 (check-type member-name string)
                                                 (format nil "self.~A.CustomSave(builder);" member-name))))))
                  "void Save(const TestStruct &self, slk::Builder *builder) {
                     self.custom_member.CustomSave(builder);
                   }")
    (undefine-cpp-types)
    (is-generated (lcp.slk:save-function-definition-for-enum
                   (lcp:define-enum test-enum
                       (first-value second-value)))
                  "void Save(const TestEnum &self, slk::Builder *builder) {
                     uint8_t enum_value;
                     switch (self) {
                     case TestEnum::FIRST_VALUE: enum_value = 0; break;
                     case TestEnum::SECOND_VALUE: enum_value = 1; break;
                     }
                     slk::Save(enum_value, builder);
                   }")
    (undefine-cpp-types)

    (subtest "inheritance"
      (undefine-cpp-types)
      (is-error (lcp.slk:save-function-declaration-for-class
                 (lcp:define-struct derived (fst-base snd-base)
                   ()))
                'lcp.slk:slk-error)
      (undefine-cpp-types)
      (let ((base-class (lcp:define-struct base ()
                          ((base-member :bool))))
            (derived-class (lcp:define-struct derived (base)
                             ((derived-member :int64_t)))))
        (is-generated (lcp.slk:save-function-definition-for-class base-class)
                      "void Save(const Base &self, slk::Builder *builder) {
                         if (const auto &derived_derived = dynamic_cast<const Derived &>(self)) {
                           return slk::Save(derived_derived, builder);
                         }
                         slk::Save(self.base_member, builder);
                       }")
        (is-generated (lcp.slk:save-function-definition-for-class derived-class)
                      "void Save(const Derived &self, slk::Builder *builder) {
                         // Save parent Base
                         { slk::Save(self.base_member, builder); }
                         slk::Save(self.derived_member, builder);
                       }"))
      (undefine-cpp-types)
      (let ((abstract-base-class (lcp:define-struct abstract-base ()
                                   ((base-member :bool))
                                   (:abstractp t)))
            (derived-class (lcp:define-struct derived (abstract-base)
                             ((derived-member :int64_t)))))
        (is-generated (lcp.slk:save-function-definition-for-class abstract-base-class)
                      "void Save(const AbstractBase &self, slk::Builder *builder) {
                         if (const auto &derived_derived = dynamic_cast<const Derived &>(self)) {
                           return slk::Save(derived_derived, builder);
                         }
                         LOG(FATAL) << \"`AbstractBase` is marked as an abstract class!\";
                       }")
        (is-generated (lcp.slk:save-function-definition-for-class derived-class)
                      "void Save(const Derived &self, slk::Builder *builder) {
                         // Save parent AbstractBase
                         { slk::Save(self.base_member, builder); }
                         slk::Save(self.derived_member, builder);
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
                  'lcp.slk:slk-error))))

  (subtest "load definitions"
    (undefine-cpp-types)
    (is-generated (lcp.slk:load-function-definition-for-enum
                   (lcp:define-enum my-enum
                       (first-value second-value)))
                  "void Load(MyEnum *self, slk::Reader *reader) {
                     uint8_t enum_value;
                     slk::Load(&enum_value, reader);
                     switch (enum_value) {
                       case static_cast<uint8_t>(0): *self = MyEnum::FIRST_VALUE; break;
                       case static_cast<uint8_t>(1): *self = MyEnum::SECOND_VALUE; break;
                       default: LOG(FATAL) << \"Trying to load unknown enum value!\";
                     }
                   }"))

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
