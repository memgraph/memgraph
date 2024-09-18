(ns memgraph.utils
  (:require
   [neo4j-clj.core :as dbclient]
   [clojure.string :as string]
   [clojure.tools.logging :refer [info]])
  (:import (java.net URI)))

(defn bolt-url
  "Get Bolt server address for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

(defn open-bolt
  "Open Bolt connection to the node. All instances use port 7687, so it is hardcoded."
  [node]
  (dbclient/connect (URI. (bolt-url node 7687)) "" ""))

(defn random-nonempty-subset
  "Return a random nonempty subset of the input collection. Relies on the fact that first 3 instances from the collection are data instances
  and last 3 are coordinators. It kills a random subset of data instances and with 50% probability 1 coordinator."
  [coll]
  (let [data-instances (take 3 coll)
        coords (take-last 3 coll)
        data-instances-to-kill (rand-int (+ 1 (count data-instances)))
        chosen-data-instances (take data-instances-to-kill (shuffle data-instances))
        coords-to-kill (rand-int (+ 1 (count coords)))
        chosen-coords (take coords-to-kill (shuffle coords))
        chosen-instances (concat chosen-data-instances chosen-coords)]
    (info "Chosen instances" chosen-instances)
    chosen-instances))

; neo4j-clj related utils.
(defmacro with-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separete transaction."
  [connection session & body]
  `(with-open [~session (dbclient/get-session ~connection)]
     ~@body))

(defn op
  "Construct a nemesis op"
  [f]
  {:type :info :f f})

(defn node-is-down
  "Log that a node is down"
  [node]
  (str "Node " node " is down"))

(defn query-forbidden-on-main?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "query forbidden on the main"))

(defn query-forbidden-on-replica?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "query forbidden on the replica"))

(defn sync-replica-down?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction."))

(defn conflicting-txns?
  "Conflicting transactions error message is allowed."
  [e]
  (string/includes? (str e) "Cannot resolve conflicting transactions."))

(defn process-service-unavailable-exc
  "Return a map as the result of ServiceUnavailableException."
  [op node]
  (assoc op :type :info :value (node-is-down node)))

(defn analyze-bank-data-reads
  "Checks whether balances always sum to the correctnumber"
  [ok-data-reads account-num starting-balance]
  (->> ok-data-reads
       (map #(-> % :value))  ; Extract the :value from each map
       (filter #(= (count (:accounts %)) account-num))  ; Check the number of accounts
       (map (fn [value]  ; Process each value
              (let [balances (map :balance (:accounts value))  ; Get the balances from the accounts
                    expected-total (* account-num starting-balance)]  ; Calculate the expected total balance
                (cond
                  (and (not-empty balances)  ; Ensure balances are not empty
                       (not= expected-total (reduce + balances)))  ; Check if the total balance is incorrect
                  {:type :wrong-total
                   :expected expected-total
                   :found (reduce + balances)
                   :value value}

                  (some neg? balances)  ; Check for negative balances
                  {:type :negative-value
                   :found balances
                   :op value}))))
       (filter identity)  ; Remove nil entries
       (into [])))

(defn analyze-empty-data-nodes
  "Checks whether there is any node that has empty reads."
  [ok-data-reads]
  (let [all-nodes (->> ok-data-reads
                       (map #(-> % :value :node))
                       (reduce conj #{}))]
    (->> all-nodes
         (filter (fn [node]
                   (every?
                    empty?
                    (->> ok-data-reads
                         (map :value)
                         (filter #(= node (:node %)))
                         (map :accounts)))))
         (filter identity)
         (into []))))
