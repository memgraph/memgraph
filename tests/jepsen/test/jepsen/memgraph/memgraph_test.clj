(ns jepsen.memgraph.memgraph-test
  (:require [clojure.test :refer :all]
            [jepsen.memgraph.utils :as utils]
            [jepsen.memgraph.habank :as habank]
            [jepsen.memgraph.client :as client]
            [jepsen.memgraph.large :as large]))

(deftest get-instance-url
  (testing "Get instance URL."
    (is (= (utils/bolt-url "node" 7687) "bolt://node:7687"))))

(deftest random-non-empty-subset
  (testing "Random, non-empty subset func from utils."
    (let [in ["n1" "n2" "n3" "n4" "n5" "n6"]
          out (utils/random-nonempty-subset in)]

      (is (=
           (count out)
           (count (distinct out))))

      (is (<= (count out) 6)))))

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

(deftest initialize-instances
  (testing "Test registration replication instances op"
    (is (= (habank/setup-cluster :1 :2) {:type :invoke :f :setup-cluster :value nil}))))

(deftest bank-test-setup
  (testing "Test bank test transfer method."
    (let [transfer-res (utils/transfer :1 :2)
          type (:type transfer-res)
          f (:f transfer-res)
          value (:value transfer-res)
          from (:from value)
          to (:to value)
          amount (:amount value)]
      (is (= :invoke type))
      (is (= :transfer f))
      (is (< from utils/account-num))
      (is (< to utils/account-num))
      (is (<= amount utils/max-transfer-amount)))))

(deftest test-client
  (testing "Replication mode string"
    (is (= "ASYNC" (client/replication-mode-str {:replication-mode :async}))))
  (testing "Register replicas"
    (is (= (client/register-replicas :1 :2) {:type :invoke, :f :register, :value nil}))))

(deftest habank-test
  (testing "HA empty test read operation."
    (is (= (utils/read-balances :1 :2) {:type :invoke, :f :read-balances, :value nil})))
  (testing "Single read to roles."
    (let [instances (list {:role "leader" :health "up"} {:role "replica" :health "up"} {:role "main" :health "up"})]

      (is (= (habank/single-read-to-roles instances) (list "leader" "replica" "main")))))

  (testing "Single read to role and health."
    (let [instances (list {:id 1 :role "follower" :health "up"} {:id 2 :role "replica" :health "up"} {:id 3 :role "main" :health "up"})]

      (is (= (habank/single-read-to-role-and-health instances) (list {:role "follower" :health "up"} {:role "replica" :health "up"} {:role "main" :health "up"})))))

  (testing "Get coordinators."
    (let [instances (list "leader" "follower" "main")]

      (is (= (habank/get-coordinators instances) (list "leader" "follower")))))

  (testing "Less than 3 coordinators."
    (let [instances (list "leader" "follower" "main")]

      (is (= (habank/less-than-three-coordinators instances) true))))

  (testing "Get mains."
    (let [instances (list "leader" "follower" "main")]

      (is (= (habank/get-mains instances) (list "main")))))

  (testing "More than 1 main"
    (let [instances (list "main" "follower" "main")]

      (is (= (habank/more-than-one-main instances) true)))))
