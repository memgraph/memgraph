(ns memgraph.memgraph-test
  (:require [clojure.test :refer :all]
            [memgraph.high-availability.bank.test :as habank]
            [memgraph
             [utils :as utils]
             [query :as query]]))

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

(deftest test-client
  (testing "Replication mode string"
    (is (= "ASYNC" (query/replication-mode-str {:replication-mode :async})))))

(deftest habank-test
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
