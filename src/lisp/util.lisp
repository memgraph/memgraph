(in-package #:lcp)

(defun fnv1a64-hash-string (string)
  "Produce (UNSIGNED-BYTE 64) hash of the given STRING using FNV-1a algorithm.
See https://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash."
  (check-type string string)
  (let ((hash 14695981039346656037) ; Offset basis
        (prime 1099511628211))
    (declare (type (unsigned-byte 64) hash prime))
    (loop :for c :across string :do
      (setf hash (mod (* (boole boole-xor hash (char-code c)) prime)
                      ;; Fit to 64bit
                      (expt 2 64))))
    hash))

(defun count-newlines (stream &key stop-position)
  (loop :for pos := (file-position stream)
        :and char := (read-char stream nil nil)
        :until (or (not char) (and stop-position (> pos stop-position)))
        :when (char= #\Newline char) count it))

(defun prefix-of-p (prefix seq &key (test #'eql))
  "Test whether the sequence PREFIX is a prefix of the sequence SEQ. The
elements are compared using the 2-element test function TEST. An empty sequence
is considered a prefix of every sequence."
  (let ((len1 (length prefix))
        (len2 (length seq)))
    (and (<= len1 len2)
         (not (mismatch prefix seq :end2 len1 :test test)))))

(defun minimize (sequence &key (test #'<) key)
  "Find the minimum element within the sequence SEQUENCE.

The minimization is done according to the 2-argument comparison function TEST
which acts as \"strictly less\". If the result of (funcall test a b) is t, then
A is considered to be strictly less than B.

If KEY is provided, it should be a 1-argument function. When performing a
comparison between 2 elements, the function is applied to each element and the
results are used in place of the original elements."
  (reduce
   (lambda (a b)
     (let ((ka a)
           (kb b))
       (when key
         (setf ka (funcall key a)
               kb (funcall key b)))
       (if (funcall test kb ka) b a)))
   sequence))

(defun position-of-closing-delimiter (str open-char close-char
                                      &key (start 0) end)
  "Given a pair of opening and closing delimiters OPEN-CHAR and CLOSE-CHAR, find
within the string STR the position of the closing delimiter that corresponds to
the first occurrence of the opening delimiter. The delimiters may be nested to
an arbitrary depth (and handling such cases is the point of this function).

Return the position of the found closing delimiter or NIL if one wasn't found."
  (let ((len (length str))
        (open-char-pos
          (position open-char str :start start :end end)))
    (when open-char-pos
      (loop :with count := 1
            :for i :from (1+ open-char-pos) :below (or end len) :do
              (cond
                ((char= (aref str i) open-char)
                 (incf count))
                ((char= (aref str i) close-char)
                 (decf count)))
              (when (zerop count)
                (return i))))))

(defun assoc-body (item alist &key (key #'identity) (test #'eql))
  "Return the body (cdr) of the first association with the key ITEM, but error
if the body is empty. If the association doesn't exist, return NIL."
  (let ((acons (assoc item alist :key key :test test)))
    (and acons (or (cdr acons) (error "~s has no body" acons)))))

(defun assoc-body-all (item alist &key (key #'identity) (test #'eql))
  "Return all of the bodies (cdrs) of the associations with the key ITEM, but
error if any of the bodies is empty. If no associations exist, return NIL."
  (loop :for acons :in alist
        :when (funcall test (funcall key (car acons)) item)
          :collect (or (cdr acons) (error "~s has no body" acons))))

(defun assoc-second (item alist &key (key #'identity) (test #'eql))
  "Return the second element (cadr) of the first association with the key ITEM,
but error if the association's body is not a 1-element list. If the association
doesn't exist, return NIL."
  (let ((acons (assoc item alist :key key :test test)))
    (when acons
      (unless (= (length acons) 2)
        (error "~s is not a pair" acons))
      (second acons))))

(defun concat (lists)
  "Concatenate all of the lists in LISTS."
  (loop :for list :in lists :append list))

(defmacro muffle-warnings (&body body)
  "Execute BODY in a dynamic context where a handler for conditions of type
WARNING has been established. The handler muffles the warning by calling
MUFFLE-WARNING."
  `(handler-bind ((warning #'muffle-warning))
     ,@body))
