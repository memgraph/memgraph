(ns jepsen.memgraph.memgraph-test
  (:require [clojure.test :refer :all]
            [jepsen.memgraph.utils :as utils]
            [jepsen.memgraph.bank :as bank]
            [jepsen.memgraph.haclient :as haclient]
            [jepsen.memgraph.haempty :as haempty]
            [jepsen.memgraph.client :as client]
            [jepsen.memgraph.large :as large]))

(deftest get-instance-url
  (testing "Get instance URL."
    (is (= (utils/get-instance-url "node" 7687) "bolt://node:7687"))))

(deftest random-non-empty-subset
  (testing "Random, non-empty subset func from utils."
    (let [in ["n1" "n2" "n3" "n4" "n5" "n5"]
          out (utils/random-nonempty-subset in)
          n4-occurences (count (filter #(= % "n4") out))
          n5-occurences (count (filter #(= % "n5") out))
          n6-occurences (count (filter #(= % "n6") out))
          coord-total-occurences (+ n4-occurences n5-occurences n6-occurences)]

      (is (=
           (count out)
           (count (distinct out))))

      (is (< n4-occurences 2))
      (is (< n5-occurences 2))
      (is (< n6-occurences 2))
      (is (< coord-total-occurences 2))
      (is (< (count out) 5)))))

(deftest op
  (testing "Test utils/op"
    (is (= (utils/op "rnd") {:type :info :f "rnd"}))))

(deftest large-test-setup
  (testing "Test that large test is configured correctly with the number of nodes."
    (is (= large/node-num 5000)))

  (testing "Test add operation of large test."
    (is (= (large/add-nodes :1 :2) {:type :invoke :f :add :value nil})))

  (testing "Test read operation of large test."
    (is (= (large/read-nodes :1 :2) {:type :invoke :f :read :value nil}))))

(deftest register-repl-instances
  (testing "Test registration replication instances op"
    (is (= (haclient/register-replication-instances :1 :2) {:type :invoke :f :register :value nil}))))

(deftest bank-test-setup
  (testing "Test that bank test is configured correctly with the number of accounts."
    (is (= bank/account-num 5)))
  (testing "Test that bank test is configured correctly with the starting balance."
    (is (= bank/starting-balance 400)))
  (testing "Test that bank test is configured correctly with the max transfer amount."
    (is (= bank/max-transfer-amount 20)))
  (testing "Test that bank test read balances operation is correctly configured."
    (is (= (bank/read-balances :1 :2) {:type :invoke, :f :read, :value nil})))
  (testing "Test bank test transfer method."
    (let [transfer-res (bank/transfer :1 :2)
          type (:type transfer-res)
          f (:f transfer-res)
          value (:value transfer-res)
          from (:from value)
          to (:to value)
          amount (:amount value)]
      (is (= :invoke type))
      (is (= :transfer f))
      (is (< from bank/account-num))
      (is (< to bank/account-num))
      (is (<= amount bank/max-transfer-amount)))))

(deftest test-client
  (testing "Replication mode string"
    (is (= "ASYNC" (client/replication-mode-str {:replication-mode :async}))))
  (testing "Register replicas"
    (is (= (client/register-replicas :1 :2) {:type :invoke, :f :register, :value nil}))))

(deftest haempty-test
  (testing "HA empty test read operation."
    (is (= (haempty/reads :1 :2) {:type :invoke, :f :read, :value nil})))
  (testing "Single read to roles."
    (let [instances (list {:role "coordinator" :health "up"} {:role "replica" :health "up"} {:role "main" :health "up"})]

      (is (= (haempty/single-read-to-roles instances) (list "coordinator" "replica" "main")))))

  (testing "Single read to role and health."
    (let [instances (list {:id 1 :role "coordinator" :health "up"} {:id 2 :role "replica" :health "up"} {:id 3 :role "main" :health "up"})]

      (is (= (haempty/single-read-to-role-and-health instances) (list {:role "coordinator" :health "up"} {:role "replica" :health "up"} {:role "main" :health "up"})))))

  (testing "Get coordinators."
    (let [instances (list "coordinator" "coordinator" "main")]

      (is (= (haempty/get-coordinators instances) (list "coordinator" "coordinator")))))

  (testing "Less than 3 coordinators."
    (let [instances (list "coordinator" "coordinator" "main")]

      (is (= (haempty/less-than-three-coordinators instances) true))))

  (testing "Get mains."
    (let [instances (list "coordinator" "coordinator" "main")]

      (is (= (haempty/get-mains instances) (list "main")))))

  (testing "More than 1 main"
    (let [instances (list "main" "coordinator" "main")]

      (is (= (haempty/more-than-one-main instances) true)))))
