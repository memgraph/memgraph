(ns jepsen.memgraph.utils
  (:require [clojure.string :as str]))

(defn get-instance-url
  "Get Bolt server address for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

(defn expected-expection?
  "Check if the exception is expected."
  [exception-message expected-message]
  (str/includes? exception-message expected-message))

(defn rethrow-if-unexpected
  [exception expected-message]
  (when-not (expected-expection? (str exception) expected-message)
    (throw (Exception. (str "Invalid exception happened: " exception)))))
