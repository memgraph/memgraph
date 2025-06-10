(ns memgraph.utils
  (:require
   [neo4j-clj.core :as dbclient]
   [clojure.string :as string])
  (:import (java.net URI)
           (java.time LocalTime)
           (java.time.format DateTimeFormatter)
           (org.neo4j.driver Driver)))

(defn current-local-time-formatted
  "Get current time in HH:mm:ss.SSS"
  []
  (let [formatter (DateTimeFormatter/ofPattern "HH:mm:ss.SSS")]
    (.format (LocalTime/now) formatter)))

(defn bolt-url
  "Get Bolt server address for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

(defn bolt-routing-url
  "Get URL for connecting using bolt+routing connection."
  [node port]
  (str "neo4j://" node ":" port))

(defn open-bolt
  "Open Bolt connection to the node. All instances use port 7687, so it is hardcoded."
  [node]
  (dbclient/connect (URI. (bolt-url node 7687)) "" ""))

; TODO: (andi) Add authentication
(defn open-bolt-routing
  "Open bolt+routing connection to the node."
  [node]
  (dbclient/connect (URI. (bolt-routing-url node 7687))))

(defn random-nonempty-subset
  "Return a random nonempty subset of the input collection. Relies on the fact that first 3 instances from the collection are data instances
  and last 3 are coordinators. It kills a random subset of data instances and coordinators."
  [coll]
  (let [data-instances (take 3 coll)
        coords (take-last 3 coll)
        data-instances-to-kill (rand-int (+ 1 (count data-instances)))
        chosen-data-instances (take data-instances-to-kill (shuffle data-instances))
        coords-to-kill (rand-int (+ 1 (count coords)))
        chosen-coords (take coords-to-kill (shuffle coords))
        chosen-instances (concat chosen-data-instances chosen-coords)]
    chosen-instances))

;;; "SessionParameters{bookmarks=null, defaultAccessMode=WRITE, database='mydb', fetchSize=null, impersonatedUser=null, bookmarkManager=null}"
(defn db-session-config
  [db]
  (-> (org.neo4j.driver.SessionConfig/builder)
      (.withDatabase db)
      (.build)))

(defn get-session-with-config [^Driver connection config]
  (.session (:db connection) config))

; neo4j-clj related utils.
(defmacro with-db-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separete transaction."
  [connection config session & body]
  `(with-open [~session (get-session-with-config ~connection ~config)]
     ~@body))

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

(defn not-main-anymore?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "Cannot commit because instance is not main anymore"))

(defn node-is-down
  "Log that a node is down"
  [node]
  (str "Node " node " is down"))

(defn cannot-get-shared-access?
  "Cannot get shared access to the storage."
  [e]
  (string/includes? (str e) "Cannot get shared access storage"))

(defn unique-constraint-violated?
  "Unique constraint was violated."
  [e]
  (string/includes? (str e) "Unable to commit due to unique constraint violation"))

(defn txn-timeout?
  "Txn timeout has occurred."
  [e]
  (string/includes? (str e) "Transaction was asked to abort because of transaction timeout."
 )

(defn server-no-longer-available
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "no longer available"))

(defn no-write-server
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "Failed to obtain connection towards WRITE server"))

(defn query-forbidden-on-main?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "Write queries are forbidden on the main"))

(defn query-forbidden-on-replica?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "Write queries are forbidden on the replica"))

(defn sync-replica-down?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction."))

(defn main-became-replica?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "Cannot commit because instance is not main anymore."))

(defn main-unwriteable?
  "Accepts exception e as argument."
  [e]
  (string/includes? (str e) "Write queries currently forbidden on the main instance."))

(defn conflicting-txns?
  "Conflicting transactions error message is allowed."
  [e]
  (string/includes? (str e) "Cannot resolve conflicting transactions."))

(defn concurrent-system-queries?
  "Concurrent system queries error message is allowed on some queries."
  [e]
  (string/includes? (str e) "Multiple concurrent system queries are not supported."))

(defn not-leader?
  "Not a leader error message is allowed when doing a cluster setup."
  [e]
  (string/includes? (str e) "not a leader"))

(defn adding-coordinator-failed?
  "If concurrently trying to add coordinators, that could cause issues."
  [e]
  (string/includes? (str e) "Failed to accept request to add server"))

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
