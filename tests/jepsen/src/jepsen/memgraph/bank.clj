(ns jepsen.memgraph.bank
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
            [jepsen.memgraph.client :as c]))

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

(c/replication-client Client []
                      (open! [this test node]
                             (c/replication-open-connection this node node-config))
                      (setup! [this test]
                              (when (= replication-role :main)
                                (c/with-session conn session
                                  (do
                                    (c/detach-delete-all session)
                                    (dotimes [i account-num]
                                      (info "Creating account:" i)
                                      (create-account session {:id i :balance starting-balance}))))))
                      (invoke! [this test op]
                               (c/replication-invoke-case (:f op)
                                                          :read (c/with-session conn session
                                                                  (assoc op
                                                                         :type :ok
                                                                         :value {:accounts (->> (get-all-accounts session) (map :n) (reduce conj []))
                                                                                 :node node}))
                                                          :transfer (if (= replication-role :main)
                                                                      (try
                                                                        (let [value (:value op)]
                                                                          (assoc op
                                                                                 :type (if
                                                                                        (transfer-money
                                                                                         conn
                                                                                         (:from value)
                                                                                         (:to value)
                                                                                         (:amount value))
                                                                                         :ok
                                                                                         :fail)))
                                                                        (catch Exception e
                                                                          (if (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction.")
                                                                            (assoc op :type :ok :info (str e)); Exception due to down sync replica is accepted/expected
                                                                            (assoc op :type :fail :info (str e)))))
                                                                      (assoc op :type :fail))))
                      (teardown! [this test]
                                 (when (= replication-role :main)
                                   (c/with-session conn session
                                     (try
                                       (c/detach-delete-all session)
                                       (catch Exception e
                                         (if-not (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction.")
                                           (throw (Exception. (str "Invalid exception when deleting all nodes: " e)))); Exception due to down sync replica is accepted/expected
                                         )))))
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

(defn workload
  "Basic test workload"
  [opts]
  {:client    (Client. nil nil nil (:node-config opts))
   :checker   (checker/compose
               {:bank     (bank-checker)
                :timeline (timeline/html)})
   :generator (c/replication-gen (gen/mix [read-balances valid-transfer]))
   :final-generator {:gen (gen/once read-balances) :recovery-time 20}})
