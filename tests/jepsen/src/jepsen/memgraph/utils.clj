(ns jepsen.memgraph.utils)

;; Jepsen related utils.
; TODO: (andi) Abstract away to a common function.
(defn get-instance-url
  "An URL for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))
