(defun require-package (package &optional min-version no-refresh)
  "Install PACKAGE with optional MIN-VERSION.
  If NO-REFRESH is non NIL, the available package list will not be refreshed."
  (if (package-installed-p package min-version)
      t
    (if (or (assoc package package-archive-contents) no-refresh)
        (package-install package)
      (package-refresh-contents)
      (package-install package))))

(require-package 'mmm-mode)
(require 'mmm-mode)

(setq mmm-global-mode 'maybe)

(define-derived-mode lcp-mode lisp-mode "LCP" "Mode for editing LCP files at Memgraph."
  (setq mmm-submode-decoration-level 0))

(mmm-add-classes '((lcp-c++ :submode c-mode ; c++-mode is buggy
                            :front "#>cpp" :back "cpp<#")))
(mmm-add-mode-ext-class 'lcp-mode nil 'lcp-c++)

(add-to-list 'auto-mode-alist '("\\.lcp\\'" . lcp-mode))

(provide 'lcp-mode)
