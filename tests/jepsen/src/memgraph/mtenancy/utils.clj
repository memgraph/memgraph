(ns memgraph.mtenancy.utils)

(defn data-instance?
  "Is node data instances?"
  [node]
  (some #(= % node) #{"n1" "n2"}))

(defn coord-instance?
  "Is node coordinator instances?"
  [node]
  (some #(= % node) #{"n3" "n4" "n5"}))
