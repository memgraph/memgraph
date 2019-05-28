(in-package #:lcp)

(defstruct raw-cpp
  "Represents a raw character string of C++ code."
  (string "" :type string :read-only t))

(defvar +whitespace-chars+
  '(#\Newline #\Space #\Return #\Linefeed #\Tab #\Page))

(defun read-expecting (string &optional (stream *standard-input*)
                                (eof-error-p t) recursivep)
  "Tries to read from STREAM a character sequence that matches a possibly empty
STRING and reports whether it was successful.

If EOF-ERROR-P is T and EOF is reached before all of the characters are matched,
an END-OF-FILE error is signaled.

Otherwise, returns 2 values, SUCCESSP and COUNT. SUCCESSP is a boolean denoting
whether it was able to match all of the characters. COUNT is the number of
characters that were read from STREAM, i.e. successfully matched.

If STRING is empty then it is automatically treated as a successful
match (returning T and 0), whether or not STREAM has reached EOF.

RECURSIVEP should be treated as in standard reader functions such as READ."
  (loop :for count :from 0
        :for c1 :across string
        :for c2 := (peek-char nil stream eof-error-p nil recursivep)
        :while (and c2 (char= c1 c2))
        :do (read-char stream t nil recursivep)
        :finally (return (values (= count (length string)) count))))

(defun |#>-reader| (stream sub-char numarg)
  "Reads the #>cpp ... cpp<# block into an instance of RAW-CPP.

The block supports string interpolation of variables by using the syntax similar
to shell interpolation. For example, ${variable} will interpolate (be replaced
with) the value of VARIABLE."
  (declare (ignore sub-char numarg))
  (let ((output
          (make-array 0 :element-type 'character :adjustable t :fill-pointer 0))
        (begin-cpp "cpp")
        (end-cpp "cpp<#")
        (interpolated-args nil))
    (unless (read-expecting begin-cpp stream nil t)
      (error "Expected a C++ block to start with \"#>cpp\""))
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
                           (set-macro-character #\} (get-macro-character #\)))
                           (read-delimited-list #\} stream t))))
               (when (and (not *read-suppress*)
                          (or (null form)
                              (not (symbolp (car form)))
                              (cdr form)))
                 (error "Expected a variable inside ${...}, got ~S" form))
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
        (end-of-file ()
          (error "Missing a closing \"cpp<#\" delimiter for a C++ block"))))
    (let ((trimmed-string
            (string-trim +whitespace-chars+
                         (subseq output
                                 0 (- (length output) (length end-cpp))))))
      `(make-raw-cpp
        :string ,(if interpolated-args
                     `(format nil ,trimmed-string ,@(reverse interpolated-args))
                     trimmed-string)))))

(named-readtables:defreadtable lcp-syntax
  (:merge :standard)
  (:dispatch-macro-char #\# #\> #'|#>-reader|))
