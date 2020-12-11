(ns jepsen.memgraph.bank
  "Bank account test on Memgraph.
  The test should do random transfers on
  the main instance while randomly reading
  the total sum of the all nodes which
  should be consistent."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.math.combinatorics :as comb]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.client :as c]))

(def account-num
  "Number of accounts to be created"
  5)

(def starting-balance
  "Starting balance of each account"
  100)

(def max-transfer-amount
  50)

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


(defrecord Client [conn replication-role node-config]
  client/Client
  (open! [this test node]
    (assoc this :replication-role (:replication-role (get node-config node))
                :conn (c/open node)))
  (setup! [this test]
    (c/with-session conn session
      (c/detach-delete-all session)
      (when (= replication-role :main)
        (dotimes [i account-num]
          (info "Creating account:" i)
          (create-account session {:id i :balance starting-balance})))))
  (invoke! [this test op]
    (case (:f op)
      :read (do
              (if (= replication-role :main)
                (c/with-session conn session
                  (assoc op :type :ok :value (->> (get-all-accounts session) (map :n) (reduce conj []))))
                (assoc op :type :fail)))
      :transfer (do
                  (if (= replication-role :main)
                    (try
                      (let [value (:value op)]
                        (assoc op
                               :type (if
                                       (transfer-money conn (:from value) (:to value) (:amount value))
                                       :ok
                                       :fail)))
                      (catch Exception e
                        ; Transaction can fail on serialization errors
                        (assoc op :type :fail :info (str e))))
                    (assoc op :type :fail)))))
  (teardown! [this test]
    (c/with-session conn session
      (c/detach-delete-all session)))
  (close! [_ est]
    (dbclient/disconnect conn)))

(defn read-balances
  "Read the current state of all accounts"
  [test process]
  {:type :invoke, :f :read, :value nil})

(defn transfer
  "Transfer money"
  [test process]
  {:type :invoke :f :transfer :value {:from (rand-int account-num)
                                      :to   (rand-int account-num)
                                      :amount (rand-int max-transfer-amount)}})

(def valid-transfer
  "Filter only valid transfers (where :from and :to are different)"
  (gen/filter (fn [op] (not= (-> op :value :from) (-> op :value :to))) transfer))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [bad-reads (->> history
                           (filter #(= :ok (:type %)))
                           (filter #(= :read (:f %)))
                           (map (fn [op]
                                  (let [balances       (map :balance (:value op))
                                        expected-total (* account-num starting-balance)]
                                    (cond (not= expected-total
                                                (reduce + balances))
                                          {:type :wrong-total
                                           :expected expected-total
                                           :found (reduce + balances)
                                           :op op}

                                          (some neg? balances)
                                          {:type :negative-value
                                           :found balances
                                           :op op}
                                          ))))
                           (filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn workload
  "Basic test workload"
  [opts]
  {:client (Client. nil nil (:node-config opts))
   :checker (checker/compose
              {:bank     (bank-checker)
               :timeline (timeline/html)})
   :generator (gen/mix [read-balances valid-transfer])})
