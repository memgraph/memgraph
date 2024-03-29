(ns jepsen.memgraph.ha
  "Bank account test on Memgraph.
  The test should do random transfers on
  the main instance while randomly reading
  the total sum of the all nodes which
  should be consistent."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as string]
            [jepsen
             [checker :as checker]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.haclient :as haclient]))

(def account-num
  "Number of accounts to be created"
  5)

(def starting-balance
  "Starting balance of each account"
  400)

(def max-transfer-amount
  20)

(dbclient/defquery create-account
  "CREATE (n:Account {id: $id, balance: $balance});")

(dbclient/defquery get-all-accounts
  "MATCH (n:Account) RETURN n;")

(dbclient/defquery get-account
  "MATCH (n:Account {id: $id}) RETURN n;")

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
      (do
        (update-balance tx {:id from :amount (- amount)})
        (update-balance tx {:id to :amount amount})))))

(defmacro replication-invoke-case
  "Call the case method on the op using the defined cases
  while a handler for :register case is added."
  [f & cases]
  (concat (list 'case f
                :register '(if (= replication-role :main)
                             (do
                               (doseq [n (filter #(= (:replication-role (val %))
                                                     :replica)
                                                 node-config)]
                                 (try
                                   (haclient/with-session conn session
                                     ((haclient/create-register-replica-query
                                       (first n)
                                       (second n)) session))
                                   (catch Exception e)))
                               (assoc op :type :ok))
                             (assoc op :type :fail)))
          cases))

(haclient/replication-client HAClient []
                             (open! [this test node]
                                    (info "Opening connection to node: node-config: " node node-config)
                                    (haclient/replication-open-connection this node node-config))
                             (setup! [this test])
                             (invoke! [this test op])
                             (teardown! [this test])
                             (close! [_ est]
                                     (dbclient/disconnect conn)))

(defn read-balances
  "Read the current state of all accounts"
  [test process]
  {:type :invoke, :f :read, :value nil})

(defn transfer
  "Transfer money"
  [test process]
  {:type :invoke :f :transfer :value {:from   (rand-int account-num)
                                      :to     (rand-int account-num)
                                      :amount (rand-int max-transfer-amount)}})

(def valid-transfer
  "Filter only valid transfers (where :from and :to are different)"
  (gen/filter (fn [op] (not= (-> op :value :from) (-> op :value :to))) transfer))

; TODO: (andi) Can all of this be abstracted under bank_utils for example?

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total
  Each node should have at least one read that returned all accounts.
  We allow the reads to be empty because the replica can connect to
  main at some later point, until that point the replica is empty."
  []
  (reify checker/Checker
    (check [this test history opts]
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

(defn test-setup
  "Basic test workload"
  [opts]
  (info "Node config: " (:node-config opts))
  {:client    (HAClient. nil nil nil (:node-config opts))
   ; :checker   (checker/compose
   ;             {:bank     (bank-checker)
   ;              :timeline (timeline/html)})
   ; :generator (haclient/replication-gen (gen/mix [read-balances valid-transfer]))
   ; :final-generator {:gen (gen/once read-balances) :recovery-time 20}})
   })
