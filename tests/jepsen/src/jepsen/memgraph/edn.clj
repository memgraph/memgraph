(ns jepsen.memgraph.edn
  (:require [clojure.edn :as edn]))

(defn load-configuration
  "Load a configuration file."
  [path]
  (-> path slurp edn/read-string))
