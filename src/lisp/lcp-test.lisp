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
