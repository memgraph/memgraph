(ns jepsen.memgraph.utils
  (:require
   [neo4j-clj.core :as dbclient]
   [clojure.string :as string]
   [clojure.tools.logging :refer [info]]
   [jepsen.generator :as gen])
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
        kill-coord? (< (rand) 0.5)]

    (if kill-coord?
      (let [chosen-coord (first (shuffle coords))
            chosen-instances (conj chosen-data-instances chosen-coord)]
        (info "Chosen instances" chosen-instances)
        chosen-instances)
      (do
        (info "Chosen instances" chosen-data-instances)
        chosen-data-instances))))

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

(defn process-service-unavilable-exc
  "Return a map as the result of ServiceUnavailableException."
  [op node]
  (assoc op :type :info :value (node-is-down node)))

(defn read-balances
  "Read the current state of all accounts"
  [_ _]
  {:type :invoke, :f :read-balances, :value nil})

(def account-num
  "Number of accounts to be created. Random number in [5, 10]" (+ 5 (rand-int 6)))

(def starting-balance
  "Starting balance of each account" (rand-nth [400 450 500 550 600 650]))

(def max-transfer-amount
  "Maximum amount of money that can be transferred in one transaction. Random number in [20, 30]"
  (+ 20 (rand-int 11)))

(defn transfer
  "Transfer money from one account to another by some amount"
  [_ _]
  {:type :invoke
   :f :transfer
   :value {:from   (rand-int account-num)
           :to     (rand-int account-num)
           :amount (+ 1 (rand-int max-transfer-amount))}})

(def valid-transfer
  "Filter only valid transfers (where :from and :to are different)"
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

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
