(ns memgraph.memgraph-test
  (:require [clojure.test :refer :all]
            [memgraph.high-availability.bank.test :as habank]
            [memgraph.high-availability.create.test :as hacreate]
            [memgraph
             [utils :as utils]
             [query :as query]]))

(deftest hacreate-test
  (testing "Hamming1"
    (is (= (hacreate/hamming-sim [1 2 3] [1 2 3]) 1)))

  (testing "Hamming2"
    (is (= (hacreate/hamming-sim [1 2 3] [1 2]) 2/3)))

  (testing "Hamming3"
    (is (= (hacreate/hamming-sim [1 2 3] [2 3]) 0)))

  (testing "Hamming4"
    (is (= (hacreate/hamming-sim (range 1 11) (range 1 21)) 1/2)))

  (testing "Hamming5"
    (is (= (hacreate/hamming-sim (range 1 11) (range 2 16)) 0)))

  (testing "Jaccard1"
    (is (= (hacreate/jaccard-sim [1 2 3] [1 2 3]) 1)))

  (testing "Jaccard2"
    (is (= (hacreate/jaccard-sim [1 2 3] [1 2]) 2/3)))

  (testing "Jaccard3"
    (is (= (hacreate/jaccard-sim [1 2 3] [2 3]) 2/3)))

  (testing "Jaccard4"
    (is (= (hacreate/jaccard-sim (range 1 11) (range 1 16)) 2/3)))

  (testing "sequence->intervals1"
    (let [my-seq [1 2 3 4 5 8 9 10]]
      (is (= (hacreate/sequence->intervals my-seq) [[1 5] [8 10]]))))

  (testing "sequence->intervals2"
    (let [my-seq [1 2 3 4 5 8 9 10 12]]
      (is (= (hacreate/sequence->intervals my-seq) [[1 5] [8 10] [12 12]]))))

  (testing "sequence->intervals3"
    (let [my-seq (apply vector (concat (range 1 5001) (range 10001 15001)))]
      (is (= (hacreate/sequence->intervals my-seq) [[1 5000] [10001 15000]]))))

  (testing "sequence->intervals4"
    (let [my-seq (apply vector (concat (range 5001 10001) (range 50001 55001) (range 110001 115001)))]
      (is (= (hacreate/sequence->intervals my-seq) [[5001 10000] [50001 55000] [110001 115000]]))))

  (testing "sequence->intervals5"
    (let [my-seq []]
      (is (= (hacreate/sequence->intervals my-seq) []))))

  (testing "sequence->intervals6"
    (let [my-seq [1 2 3 4 5 6 7 8 9 10]]
      (is (= (hacreate/sequence->intervals my-seq) [[1 10]]))))

  (testing "duplicates1"
    (let [my-seq [1 1 2 2 3 3]]

      (is (= (hacreate/duplicates my-seq) [1 2 3]))))

  (testing "duplicates2"
    (let [my-seq '(1 2 2 3 3 4 4 4 10 12 100 100 76541 76541)]

      (is (= (hacreate/duplicates my-seq) [2 3 4 100 76541]))))

  (testing "duplicated_intervals1"
    (let [my-seq '(1 2 2 3 3 4 4 4 10 12 100 100 76541 76541)]

      (is (= (hacreate/sequence->intervals (hacreate/duplicates my-seq)) [[2 4] [100 100] [76541 76541]]))))

  (testing "duplicated_intervals2"
    (let [my-seq '(1 2 3 4 5 6 7 8 9 10)]

      (is (= (hacreate/sequence->intervals (hacreate/duplicates my-seq)) []))))

  (testing "mono-increasing-seq-empty"
    (let [my-seq []]

      (is (= (hacreate/seq->monotonically-incr-seq my-seq) []))))

  (testing "mono-increasing-seq1"
    (let [my-seq [1 2 3]]

      (is (= (hacreate/seq->monotonically-incr-seq my-seq) [1 2 3]))))

  (testing "mono-increasing-seq2"
    (let [my-seq [1 2 1]]

      (is (= (hacreate/seq->monotonically-incr-seq my-seq) [1 2]))))

  (testing "mono-increasing-seq3"
    (let [my-seq [1 2 2]]

      (is (= (hacreate/seq->monotonically-incr-seq my-seq) [1 2]))))

  (testing "mono-increasing-seq4"
    (let [my-seq [1 2 4 4 2 2 7 9 10]]

      (is (= (hacreate/seq->monotonically-incr-seq my-seq) [1 2 4 7 9 10]))))

  (testing "is-mono-increasing-seq?1"
    (let [my-seq []]

      (is (= (hacreate/is-mono-increasing-seq? my-seq) true))))

  (testing "is-mono-increasing-seq?2"
    (let [my-seq [1]]

      (is (= (hacreate/is-mono-increasing-seq? my-seq) true))))

  (testing "is-mono-increasing-seq?3"
    (let [my-seq [1 5 5]]

      (is (= (hacreate/is-mono-increasing-seq? my-seq) false))))

  (testing "is-mono-increasing-seq?4"
    (let [my-seq [1 5 6 7 9 11]]

      (is (= (hacreate/is-mono-increasing-seq? my-seq) true))))

  (testing "is-mono-increasing-seq?4"
    (let [my-seq [1 5 5 7 8 9 9 10 10]]

      (is (= (hacreate/is-mono-increasing-seq? my-seq) false))))

  (testing "missing-intervals1"
    (let [my-seq []]

      (is (= (hacreate/missing-intervals my-seq) []))))

  (testing "missing-intervals2"
    (let [my-seq [1 2 3 4 5]]

      (is (= (hacreate/missing-intervals my-seq) []))))

  (testing "missing-intervals3"
    (let [my-seq [1 2 3 5 6]]

      (is (= (hacreate/missing-intervals my-seq) [[4 4]]))))

  (testing "missing-intervals4"
    (let [my-seq [1 2 3 9 10 11 15 16 17 20 21 22]]

      (is (= (hacreate/missing-intervals my-seq) [[4 8] [12 14] [18 19]]))))

  (testing "missing-intervals5"
    (let [my-seq (apply vector (concat (range 5001 10001) (range 50001 55001) (range 110001 115001)))]

      (is (= (hacreate/missing-intervals my-seq) [[10001 50000] [55001 110000]]))))

  (testing "cum-probs1"
    (let [my-probs [0.1 0.3 0.6]]

      (is (= (hacreate/cum-probs my-probs) [0.1 0.4 1.0]))))

  (testing "cum-probs2"
    (let [my-probs [0.5 0.5]]

      (is (= (hacreate/cum-probs my-probs) [0.5 1.0]))))

  (testing "cum-probs3"
    (let [my-probs [0.25 0.25 0.25 0.25]]

      (is (= (hacreate/cum-probs my-probs) [0.25 0.5 0.75 1.0]))))

  (testing "cum-probs4"
    (let [my-probs [1.0]]

      (is (= (hacreate/cum-probs my-probs) [1.0]))))

  (testing "cum-probs5"
    (let [my-probs []]

      (is (= (hacreate/cum-probs my-probs) [0]))))

  (testing "get-competent-idx1"
    (let [cum-probs [0.1 0.4 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.05) 0))))

  (testing "get-competent-idx2"
    (let [cum-probs [0.1 0.4 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.1) 1))))

  (testing "get-competent-idx3"
    (let [cum-probs [0.1 0.4 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.3) 1))))

  (testing "get-competent-idx4"
    (let [cum-probs [0.1 0.4 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.4) 2))))

  (testing "get-competent-idx5"
    (let [cum-probs [0.1 0.4 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.8) 2))))

  (testing "get-competent-idx6"
    (let [cum-probs [0.1 0.4 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.99) 2))))

  (testing "get-competent-idx7"
    (let [cum-probs [0.5 1.0]]

      (is (= (hacreate/get-competent-idx cum-probs 0.2) 0)))))

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
