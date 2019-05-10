(defpackage #:lcp.debug
  (:use #:cl #:lcp)
  (:export #:lcp-debugger-hook))

(in-package #:lcp.debug)

(defparameter *swank-port* 4010
  "A port on which the Swank server should be started.")

(defun swank-started-p ()
  "Test whether at least one Swank server was started."
  (and swank::*servers* t))

(defun swank-has-connections-p ()
  "Test whether at least one SLIME connection is still alive."
  (and swank::*connections* t))

(defun invoke-slime-debugger (condition)
  "Invoke the SLIME debugger if at least one SLIME connection is active.
Otherwise invoke the standard debugger."
  (let ((*debugger-hook* #'swank:swank-debugger-hook))
    (invoke-debugger condition)))

(defun lcp-debugger-hook (condition hook)
  "This debugger hook will start a Swank server in order to facilitate the
debugging of Lisp images which were not started with Swank running.

Upon first invocation of this hook, a Swank server will be started on the port
equal to the value of *SWANK-PORT*. Then, the restart SLIME-DEBUGGER will be
established and the standard debugger will be invoked.

Before invoking the SLIME-DEBUGGER restart the user should connect to the
started Swank server using SLIME. If the restart is invoked and no connections
exist, the standard debugger is invoked.

If this hook is called and Swank connections already exist, the SLIME debugger
is immediately entered without establishing any restarts."
  (declare (ignore hook))
  (if (swank-has-connections-p)
      (invoke-slime-debugger condition)
      (progn
        (unless (swank-started-p)
          (swank:create-server :port *swank-port* :dont-close t))
        (restart-case (invoke-debugger condition)
          (slime-debugger ()
            :report (lambda (stream)
                      (format stream "SLIME debugger (port ~S)" *swank-port*))
            (invoke-slime-debugger condition))))))
