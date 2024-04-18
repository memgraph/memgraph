(ns jepsen.memgraph.bank
  "Bank account test on Memgraph.
  The test should do random transfers on
  the main instance while randomly reading
  the total sum of the all nodes which
  should be consistent."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as string]
            [clojure.core.reducers :as r]
            [jepsen
             [store :as store]
             [checker :as checker]
             [generator :as gen]
             [history :as h]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.checker.perf :as perf]
            [jepsen.memgraph.client :as client]
            [jepsen.memgraph.utils :as utils]))

(def account-num
  "Number of accounts to be created"
  5)

(def starting-balance
  "Starting balance of each account"
  400)

(def max-transfer-amount
  20)

; Implicit 1st parameter you need to send is txn. 2nd is id. 3rd balance
(dbclient/defquery create-account
  "CREATE (n:Account {id: $id, balance: $balance});")

; Implicit 1st parameter you need to send is txn.
(dbclient/defquery get-all-accounts
  "MATCH (n:Account) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id.
(dbclient/defquery get-account
  "MATCH (n:Account {id: $id}) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3d is amount.
(dbclient/defquery update-balance
  "MATCH (n:Account {id: $id})
   SET n.balance = n.balance + $amount
   RETURN n")

(defn transfer-money
  "Transfer money from one account to another by some amount
  if the account you're transfering money from has enough
  money."
  [conn from to amount]
  (dbclient/with-transaction conn tx
    (when (-> (get-account tx {:id from}) first :n :balance (>= amount))
      (update-balance tx {:id from :amount (- amount)})
      (update-balance tx {:id to :amount amount}))))

(client/replication-client Client []
                           ; Open connection to the node. Setup each node.
                           (open! [this test node]
                                  (client/replication-open-connection this node nodes-config))
                           ; On main detach-delete-all and create accounts.
                           (setup! [this test]
                                   (when (= replication-role :main)
                                     (utils/with-session conn session
                                       (do
                                         (client/detach-delete-all session)
                                         (dotimes [i account-num]
                                           (info "Creating account:" i)
                                           (create-account session {:id i :balance starting-balance}))))))
                           (invoke! [this test op]
                                    (client/replication-invoke-case (:f op)
                                                                    ; Create a map with the following structure: {:type :ok :value {:accounts [account1 account2 ...] :node node}}
                                                                    ; Read always succeeds and returns all accounts.
                                                                    ; Node is a variable, not an argument to the function. It indicated current node on which action :read is being executed.
                                                                    :read (utils/with-session conn session
                                                                            (let [accounts (->> (get-all-accounts session) (map :n) (reduce conj []))
                                                                                  total (reduce + (map :balance accounts))]
                                                                              (assoc op
                                                                                     :type :ok
                                                                                     :value {:accounts accounts
                                                                                             :node node
                                                                                             :total total
                                                                                             :correct (= total (* account-num starting-balance))})))

; Transfer money from one account to another. Only executed on main.
                                                                    ; If the transferring succeeds, return :ok, otherwise return :fail.
                                                                    ; Transfer will fail if the account doesn't exist or if the account doesn't have enough or if update-balance
                                                                    ; doesn't return anything.
                                                                    ; Allow the exception due to down sync replica.
                                                                    :transfer (if (= replication-role :main)
                                                                                (try
                                                                                  (let [transfer-info (:value op)]
                                                                                    (assoc op
                                                                                           :type (if
                                                                                                  (transfer-money
                                                                                                   conn
                                                                                                   (:from transfer-info)
                                                                                                   (:to transfer-info)
                                                                                                   (:amount transfer-info))
                                                                                                   :ok
                                                                                                   :fail)))
                                                                                  (catch Exception e
                                                                                    (if (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction.")
                                                                                      (assoc op :type :ok :info (str e)); Exception due to down sync replica is accepted/expected
                                                                                      (assoc op :type :fail :info (str e)))))
                                                                                (assoc op :type :fail :info "Not main node."))))
                           ; On teardown! only main will detach-delete-all.
                           (teardown! [this test]
                                      (when (= replication-role :main)
                                        (utils/with-session conn session
                                          (try
                                            ; Can fail for various reasons, not important at this point.
                                            (client/detach-delete-all session)
                                            (catch Exception _)))))
                           ; Close connection to the node.
                           (close! [_ est]
                                   (dbclient/disconnect conn)))

(defn read-balances
  "Read the current state of all accounts"
  [_ _]
  {:type :invoke, :f :read, :value nil})

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

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total
  Each node should have at least one read that returned all accounts.
  We allow the reads to be empty because the replica can connect to
  main at some later point, until that point the replica is empty."
  []
  (reify checker/Checker
    (check [_ _ history _]
      (let [ok-reads  (->> history
                           (filter #(= :ok (:type %)))
                           (filter #(= :read (:f %))))
            bad-reads (->> ok-reads
                           (map #(->> % :value :accounts))
                           (filter #(= (count %) 5))
                           (map (fn [op]
                                  (let [balances       (map :balance op)
                                        expected-total (* account-num starting-balance)]
                                    (cond (and
                                           (not-empty balances)
                                           (not=
                                            expected-total
                                            (reduce + balances)))
                                          {:type :wrong-total
                                           :expected expected-total
                                           :found (reduce + balances)
                                           :op op}

                                          (some neg? balances)
                                          {:type :negative-value
                                           :found balances
                                           :op op}))))
                           (filter identity)
                           (into []))
            empty-nodes (let [all-nodes (->> ok-reads
                                             (map #(-> % :value :node))
                                             (reduce conj #{}))]
                          (->> all-nodes
                               (filter (fn [node]
                                         (every?
                                          empty?
                                          (->> ok-reads
                                               (map :value)
                                               (filter #(= node (:node %)))
                                               (map :accounts)))))
                               (filter identity)
                               (into [])))]
        {:valid? (and
                  (empty? bad-reads)
                  (empty? empty-nodes))
         :empty-nodes empty-nodes
         :bad-reads bad-reads}))))

(defn ok-reads
  "Filters a history to just OK reads. Returns nil if there are none."
  [history]
  (let [h (filter #(and (h/ok? %)
                        (= :read (:f %)))
                  history)]
    (when (seq h)
      (vec h))))

(defn by-node
  "Groups operations by node."
  [test history]
  (let [nodes (:nodes test)
        n     (count nodes)]
    (->> history
         (r/filter (comp number? :process))
         (group-by (fn [op]
                     (let [p (:process op)]
                       (nth nodes (mod p n))))))))

(defn points
  "Turns a history into a seqeunce of [time total-of-accounts] points."
  [history]
  (mapv (fn [op]
          [(util/nanos->secs (:time op))
           (:total (:value op))])
        history))

(defn plotter
  "Renders a graph of balances over time"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (when-let [reads (ok-reads history)]
        (let [totals (->> reads
                          (by-node test)
                          (util/map-vals points))
              colors (perf/qs->colors (keys totals))
              path (.getCanonicalPath
                    (store/path! test (:subdirectory opts) "bank.png"))
              preamble (concat (perf/preamble path)
                               [['set 'title (str (:name test) " bank")]
                                '[set ylabel "Total of all accounts"]])
              series (for [[node data] totals]
                       {:title      node
                        :with       :points
                        :pointtype  2
                        :linetype   (colors node)
                        :data       data})]
          (-> {:preamble  preamble
               :series    series}
              (perf/with-range)
              (perf/with-nemeses history (:nemeses (:plot test)))
              perf/plot!)
          {:valid? true})))))

(defn workload
  "Basic test workload"
  [opts]
  {:client    (Client. nil nil nil (:nodes-config opts))
   :checker   (checker/compose
               {:bank     (bank-checker)
                :timeline (timeline/html)
                :plot     (plotter)})
   :generator (client/replication-gen (gen/mix [read-balances valid-transfer]))
   :final-generator {:clients (gen/once read-balances) :recovery-time 20}})
