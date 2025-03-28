(ns memgraph.high-availability.create.test
  "Create test for HA."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.core :as c]
            [clojure.set :as set]
            [clojure.string :as string]
            [jepsen
             [checker :as checker]
             [generator :as gen]
             [client :as jclient]]
            [jepsen.checker.timeline :as timeline]
            [memgraph.high-availability.utils :as hautils]
            [memgraph.high-availability.create.nemesis :as nemesis]
            [memgraph.utils :as utils]
            [memgraph.query :as mgquery]))

(defn is-main?
  "Tests if data instance is main. Returns bool true/false, catches all exceptions."
  [bolt-conn]
  (try
    (utils/with-session bolt-conn session
      (let [role-map (first (reduce conj [] (mgquery/show-replication-role session)))
            role-vec (vec (apply concat role-map))
            role (last role-vec)]
        (info "Role:" role)
        (info "is-main?" (= role "main"))
        (= role "main")))
    (catch Exception _
      false)))

(def registered-replication-instances? (atom false))
(def added-coordinator-instances? (atom false))
(def main-set? (atom false))

(def batch-size 5000)

(def delay-requests-sec 5)
(defn hamming-sim
  "Calculates Hamming distance between two sequences. Used as a consistency measure when the order is important."
  [seq1 seq2]
  (let [seq1-size (count seq1)
        seq2-size (count seq2)
        max-size (max seq1-size seq2-size)
        size-diff (abs (- seq2-size seq1-size))
        elemwise-diff (reduce +
                              (map (fn [elem1 elem2]
                                     (if (= elem1 elem2) 0 1))

                                   seq1 seq2))
        sim (- 1 (/ (+ elemwise-diff size-diff) max-size))]
    sim))

(defn jaccard-sim
  "Calculates Jaccard similarity between two input sequences."
  [seq1 seq2]
  (let [seq1-set (set seq1)
        seq2-set (set seq2)
        jaccard-int (set/intersection seq1-set seq2-set)
        jaccard-un (set/union seq1-set seq2-set)
        sim (/ (count jaccard-int) (count jaccard-un))]
    sim))

(defn duplicates
  "Function returns all duplicated values from the sequence seq. The input seq doesn't need to be sorted."
  [seq]
  (let [freqs (frequencies seq)]
    (->> freqs
         (filter #(> (val %) 1))
         (map key)
         (apply sorted-set)
         (apply vector))))

(defn seq->monotonically-incr-seq
  "Converts a vector into a monotonically increasing sequence."
  [coll]
  (assert (vector? coll), "Input must be a vector.")
  (if (empty? coll)
    []
    (reduce (fn [res elem]
              (if (or (empty? res) (> elem (peek res)))
                (conj res elem)
                res))

            [] coll)))

(defn is-mono-increasing-seq?
  "Checks if the vector coll is a monotonically increasing sequence. Stops recursion as soon as 1st element not satisfying result is found.
  Duplicates aren't allowed, here we use a notion of strictly monotonically increasing sequence.
  "
  [coll]
  (loop [prev-elem (first coll)
         remaining (rest coll)]
    (cond
      (empty? remaining) true
      (<= (first remaining) prev-elem) false
      :else (recur (first remaining) (rest remaining)))))

(defn missing-intervals
  "Finds missing numbers from monotonically increasing vector and reports them as intervals."
  [coll]
  (assert (vector? coll) "Input must be a vector.")
  (assert (is-mono-increasing-seq? coll) "The input must be monotonically increasing sequence.")
  (if (empty? coll)
    []
    (let [coll-size (count coll) ; O(1)
          head-coll (subvec coll 0 (dec coll-size)) ; O(1), no new structure is being created.
          shifted-coll (rest coll) ; returns a sequence. O(1), leverages laziness.
          indices (range 0 (dec coll-size)) ; lazy sequence of numbers
          split-indices-with-nil (map (fn [index orig shifted]
                                        (when (not= (inc orig) shifted)
                                          index)) indices head-coll shifted-coll) ; also lazy sequence. O(1) at this point.
          start-intervals (filter some? split-indices-with-nil) ; O(1) at this point.
          end-intervals (map inc start-intervals) ; could start materializing
          coll-intervals (map (fn [start end] [(inc (nth coll start)) (dec (nth coll end))]) start-intervals end-intervals)]

      coll-intervals)))

(defn sequence->intervals
  "Compresses a vector into intervals. Each interval is represented by a vector where the 1st element represents
  starting idx and the 2nd element represent last index from the interval. Interval is included from both sides. In total runs in O(n).
  One interval represents a range of monotonically increasing ids. The input collection must not contain duplicates and we assert this by checking
  that the input collection is strictly monotonically increasing sequence.
  "
  [coll]
  (assert (vector? coll) "Input must be a vector.")
  (assert (is-mono-increasing-seq? coll) "The input must be monotonically increasing sequence.")
  (if (empty? coll)
    []
    (let [coll-size (count coll) ; O(1)
          head-coll (subvec coll 0 (dec coll-size)) ; O(1), no new structure is being created.
          shifted-coll (rest coll) ; returns a sequence. O(1), leverages laziness.
          indices (range 0 (dec coll-size)) ; lazy sequence of numbers
          split-indices-with-nil (map (fn [index orig shifted]
                                        (when (not= (inc orig) shifted)
                                          index)) indices head-coll shifted-coll) ; also lazy sequence. O(1) at this point.
          end-intervals (filter some? split-indices-with-nil) ; O(1) at this point.
          start-intervals (conj (map inc end-intervals) 0) ; could start materializing
          end-intervals (c/concat end-intervals [(dec coll-size)]); could start materializing
          coll-intervals (map (fn [start end] [(nth coll start) (nth coll end)]) start-intervals end-intervals)]

      coll-intervals)))

(defn get-expected-indices
  "Returns the range of all indices that should've been inserted."
  [max-id]
  (range 1 (inc max-id)))

(defn random-coord
  "Get random leader."
  [nodes]
  (nth nodes (+ 3 (rand-int 3)))) ; Assumes that first 3 instances are data instances and last 3 are coordinators.

(defn random-data-instance
  "Get random data instance."
  [nodes]
  (nth nodes (rand-int 3)))

(defn register-replication-instances
  "Register replication instances."
  [session nodes-config]
  (doseq [repl-config (filter #(contains? (val %) :replication-port)
                              nodes-config)]
    (try
      ((mgquery/register-replication-instance
        (first repl-config)
        (second repl-config)) session)
      (info "Registered replication instance:" (first repl-config))
      (catch Exception e
        (if (string/includes? (str e) "name already exists") ; It means already registered
          (info "Replication instance" (first repl-config) "already registered, continuing to register other replication instances.")
          (throw e))))))

(defn add-coordinator-instances
  "Add coordinator instances."
  [session _myself nodes-config]
  (doseq [coord-config (->> nodes-config
                            (filter #(contains? (val %) :coordinator-id)))]
    (try
      ((mgquery/add-coordinator-instance
        (first coord-config) (second coord-config)) session)
      (info "Added coordinator:" (first coord-config))
      (catch Exception e
        (if (string/includes? (str e) "id already exists")
          (info "Coordinator instance" (first coord-config) "already exists, continuing to add other coordinator instances.")
          (throw e))))))

(defn set-instance-to-main
  "Set instance to main."
  [session first-main]
  ((mgquery/set-instance-to-main first-main) session)
  (info "Set instance" first-main "to main."))

(defn mg-get-nodes
  "Get all nodes as part of the txn."
  [txn]
  (mgquery/collect-ids txn))

(defrecord Client [nodes-config first-leader first-main license organization]
  jclient/Client
  ; Open Bolt connection to all nodes.
  (open! [this _test node]
    (info "Opening bolt connection to node..." node)
    (let [bolt-conn (utils/open-bolt node)
          node-config (get nodes-config node)]
      (assoc this
             :bolt-conn bolt-conn
             :node-config node-config
             :node node)))

; Use Bolt connection to set enterprise.license and organization.name.
  (setup! [this _test]
    (try
      (utils/with-session (:bolt-conn this) session
        ((mgquery/set-db-setting "enterprise.license" license) session)
        ((mgquery/set-db-setting "organization.name" organization) session))
      (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
        (info (utils/node-is-down (:node this))))))

  (invoke! [this _test op]
    (let [bolt-conn (:bolt-conn this)
          node (:node this)]
      (case (:f op)
        :get-nodes (if (hautils/data-instance? node)
                     (try
                       (utils/with-session bolt-conn session
                         (let [indices (->> (mg-get-nodes session) (map :id) (reduce conj []))]
                           (assoc op :type :ok :value {:indices indices :node node})))

; There shouldn't be any other exception since nemesis will heal all nodes as part of its final generator.
                       (catch Exception e
                         (assoc op :type :fail :value (str e))))
                     (assoc op :type :info :value "Not data instance."))

        :create-unique-constraint (if (and (hautils/data-instance? node) (is-main? bolt-conn))
                                    (try
                                      (utils/with-session bolt-conn session
                                        (mgquery/create-unique-constraint session)

                                        (assoc op :type :ok :value {:str "Created unique constraint"}))

                                      (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                        (utils/process-service-unavailable-exc op node))

                                      (catch org.neo4j.driver.exceptions.ClientException e
                                        (cond
                                          (utils/concurrent-system-queries? e)
                                          (assoc op :type :info :value {:str "Concurrent system queries are not allowed"})

                                          :else
                                          (assoc op :type :fail :value (str e))))

                                      (catch Exception e
                                        (assoc op :type :fail :value (str e))))

                                    (assoc op :type :info :value "Not main data instance."))

        :add-nodes (if (hautils/coord-instance? node)
                     (try
                       (utils/with-session bolt-conn session
                         (let [instances (reduce conj [] (mgquery/get-all-instances session))
                               current-leader (hautils/get-current-leader instances)]
                           (if (= current-leader node)
                             (let [bolt-routing-conn (utils/open-bolt-routing node)
                                   max-idx (atom nil)]
                               (try
                                 (utils/with-session bolt-routing-conn session
                                   (let [local-idx (->> (mgquery/add-nodes session {:batchSize batch-size})
                                                        (map :id)
                                                        (reduce conj [])
                                                        first)]
                                     (reset! max-idx local-idx)
                                     (dbclient/disconnect bolt-routing-conn)
                                     (assoc op :type :ok :value {:str "Nodes created" :max-idx @max-idx})))
                                 (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                   (dbclient/disconnect bolt-routing-conn)
                                   (utils/process-service-unavailable-exc op node))
                                 (catch Exception e
                                   (dbclient/disconnect bolt-routing-conn)
                                   (cond
                                     (utils/server-no-longer-available e)
                                     (assoc op :type :info :value {:str "Server no longer available."})

                                     (utils/no-write-server e)
                                     (assoc op :type :info :value {:str "Failed to obtain connection towards write server."})

                                     (utils/sync-replica-down? e)
                                     (assoc op :type :ok :value {:str "Nodes created. SYNC replica is down." :max-idx @max-idx})

                                     (utils/main-became-replica? e)
                                     (assoc op :type :ok :value {:str "Cannot commit because instance is not main anymore."})

                                     (utils/main-unwriteable? e)
                                     (assoc op :type :ok :value {:str "Cannot commit because main is currently non-writeable."})

                                     (utils/txn-asked-to-abort? e)
                                     (assoc op :type :ok :value {:str "Txn was asked to abort"})

                                     (utils/unique-constraint-violated? e)
                                     (assoc op :type :ok :value {:str "Unique constraint was violated."})

                                     (or (utils/query-forbidden-on-replica? e)
                                         (utils/query-forbidden-on-main? e))
                                     (assoc op :type :info :value (str e))
                                     :else
                                     (assoc op :type :fail :value (str e))))))
                             (assoc op :type :info :value "This coordinator is not the current leader."))))
                       (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                         (utils/process-service-unavailable-exc op node))
                       (catch Exception e
                         (assoc op :type :fail :value (str e))))
                     (assoc op :type :info :value "Not coordinator instance."))

; Show instances should be run only on coordinators/
        :show-instances-read (if (hautils/coord-instance? node)
                               (try
                                 (utils/with-session bolt-conn session ; Use bolt connection for running show instances.
                                   (let [instances (reduce conj [] (mgquery/get-all-instances session))]
                                     (assoc op
                                            :type :ok
                                            :value {:instances instances :node node :time (utils/current-local-time-formatted)})))
                                 (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                   (utils/process-service-unavailable-exc op node))
                                 (catch Exception e
                                   (cond
                                     (utils/txn-asked-to-abort? e)
                                     (assoc op :type :ok :value {:str "Txn was asked to abort"})

                                     :else
                                     (assoc op :type :fail :value (str e)))))

                               (assoc op :type :info :value "Not coordinator"))
        :setup-cluster
        ; If nothing was done before, registration will be done on the 1st leader and all good.
        ; If leader didn't change but registration was done, we won't even try to register -> all good again.
        ; If leader changes, registration should already be done or not a leader will be printed.
        (if (= first-leader node)

          (try
            (utils/with-session bolt-conn session
              (when (not @registered-replication-instances?)
                (register-replication-instances session nodes-config)
                (reset! registered-replication-instances? true))

              (when (not @added-coordinator-instances?)
                (add-coordinator-instances session node nodes-config)
                (reset! added-coordinator-instances? true))

              (when (not @main-set?)
                (set-instance-to-main session first-main)
                (reset! main-set? true))

              (assoc op :type :ok)) ; NOTE: This doesn't necessarily mean all instances were successfully registered.

            (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
              (info "Registering instances failed because node" node "is down.")
              (utils/process-service-unavailable-exc op node))
            (catch Exception e
              (if (string/includes? (str e) "not a leader")
                (assoc op :type :info :value "Not a leader")
                (assoc op :type :fail :value (str e)))))

          (assoc op :type :info :value "Not first leader")))))

  (teardown! [_this _test])
  (close! [this _test]
    (dbclient/disconnect (:bolt-conn this))))

(defn single-read-to-roles
  "Convert single read to roles. Single read is a list of instances."
  [single-read]
  (map :role single-read))

(defn get-coordinators
  "From list of roles, returns those which are coordinators."
  [roles]
  (let [leader-followers #{"leader"
                           "follower"}]

    (filter leader-followers roles)))

(defn get-mains
  "From list of roles, returns those which are main."
  [roles]
  (filter #(= "main" %) roles))

(defn less-than-three-coordinators
  "Check if there aren't exactly 3 coordinators in single read where single-read is a read list of instances. Single-read here is already processed list of roles."
  [roles]
  (< (count (get-coordinators roles)) 3))

(defn more-than-one-main
  "Check if there is more than one main in single read where single-read is a read list of instances. Single-read here is already processed list of roles."
  [roles]
  (> (count (get-mains roles)) 1))

(defn checker
  "Checker."
  []
  (reify checker/Checker
    (check [_checker _test history _opts]
      ; si prefix stands for show-instances
      (let [ok-get-nodes (->> history
                              (filter #(= :ok (:type %)))
                              (filter #(= :get-nodes (:f %)))
                              (map :value))

            n1-ids (->> ok-get-nodes
                        (filter #(= "n1" (:node %)))
                        first
                        (:indices))

            n1-mono-increasing-ids (seq->monotonically-incr-seq n1-ids)

            n1-missing-intervals (missing-intervals n1-mono-increasing-ids)

            n1-max-idx (apply max n1-ids)

            n1-duplicates (duplicates n1-ids)

            n1-duplicates-intervals (sequence->intervals n1-duplicates)

            ; We assume already here that n1-max-idx will be = n2-max-idx = n3-max-idx so expected-ids is the same for n1, n2 and n3.
            ; We should give instances enough time at the end to consolidate their results.
            ; If this is not the case the result won't be valid (valid? will be false because of the check n1-max-idx=n2-max-idx=n3-max-idx).
            expected-ids (get-expected-indices n1-max-idx)

            n1-hamming-consistency (hamming-sim expected-ids n1-ids)

            n1-jaccard-consistency (jaccard-sim expected-ids n1-ids)

            n2-ids (->> ok-get-nodes
                        (filter #(= "n2" (:node %)))
                        first
                        (:indices))

            n2-mono-increasing-ids (seq->monotonically-incr-seq n2-ids)

            n2-missing-intervals (missing-intervals n2-mono-increasing-ids)

            n2-max-idx (apply max n2-ids)

            n2-duplicates (duplicates n2-ids)

            n2-duplicates-intervals (sequence->intervals n2-duplicates)

            n2-hamming-consistency (hamming-sim expected-ids n2-ids)

            n2-jaccard-consistency (jaccard-sim expected-ids n2-ids)

            n3-ids (->> ok-get-nodes
                        (filter #(= "n3" (:node %)))
                        first
                        (:indices))

            n3-mono-increasing-ids (seq->monotonically-incr-seq n3-ids)

            n3-missing-intervals (missing-intervals n3-mono-increasing-ids)

            n3-max-idx (apply max n3-ids)

            n3-duplicates (duplicates n3-ids)

            n3-duplicates-intervals (sequence->intervals n3-duplicates)

            n3-hamming-consistency (hamming-sim expected-ids n3-ids)

            n3-jaccard-consistency (jaccard-sim expected-ids n3-ids)

            failed-setup-cluster (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :setup-cluster (:f %)))
                                      (map :value))

            failed-show-instances (->> history
                                       (filter #(= :fail (:type %)))
                                       (filter #(= :show-instances-read (:f %)))
                                       (map :value))

            failed-add-nodes (->> history
                                  (filter #(= :fail (:type %)))
                                  (filter #(= :add-nodes (:f %)))
                                  (map :value))

            failed-get-nodes (->> history
                                  (filter #(= :fail (:type %)))
                                  (filter #(= :get-nodes (:f %)))
                                  (map :value))

            si-reads  (->> history
                           (filter #(= :ok (:type %)))
                           (filter #(= :show-instances-read (:f %)))
                           (map :value))

            partial-instances (->> si-reads
                                   (filter #(not= 6 (count (:instances %)))))
            ; All reads grouped by node {node->instances}
            coord->instances (->> si-reads
                                  (group-by :node)
                                  (map (fn [[node values]] [node (map :instances values)]))
                                  (into {}))
            coord->roles (->> coord->instances
                              (map (fn [[node reads]]
                                     [node (map single-read-to-roles reads)]))
                              (into {}))
            partial-coordinators (->> coord->roles
                                      (filter (fn [[_ reads]] (some less-than-three-coordinators reads)))
                                      (keys))
            more-than-one-main (->> coord->roles
                                    (filter (fn [[_ reads]] (some more-than-one-main reads)))
                                    (keys))
            coordinators (set (keys coord->instances))

            initial-result {:valid? (and
                                     (= coordinators #{"n4" "n5" "n6"})
                                     (empty? partial-coordinators)
                                     (empty? more-than-one-main)
                                     (empty? partial-instances)
                                     (empty? failed-setup-cluster)
                                     (empty? failed-show-instances)
                                     (empty? n1-duplicates)
                                     (empty? n2-duplicates)
                                     (empty? n3-duplicates)
                                     (empty? n1-missing-intervals)
                                     (empty? n2-missing-intervals)
                                     (empty? n3-missing-intervals)
                                     (= n1-jaccard-consistency n2-jaccard-consistency n3-jaccard-consistency 1)
                                     (= n1-hamming-consistency n2-hamming-consistency n3-hamming-consistency 1)
                                     (= n1-max-idx n2-max-idx n3-max-idx))
                            :empty-partial-coordinators? (empty? partial-coordinators) ; coordinators which have missing coordinators in their reads
                            :empty-more-than-one-main-nodes? (empty? more-than-one-main) ; nodes on which more-than-one-main was detected
                            :correct-coordinators? (= coordinators #{"n4" "n5" "n6"})
                            :empty-n1-duplicates? (empty? n1-duplicates)
                            :n1-hamming-consistency (float n1-hamming-consistency)
                            :n1-jaccard-consistency (float n1-jaccard-consistency)
                            :n1-max-idx n1-max-idx
                            :empty-n1-missing-intervals? (empty? n1-missing-intervals)
                            :empty-n2-duplicates? (empty? n2-duplicates)
                            :n2-hamming-consistency (float n2-hamming-consistency)
                            :n2-jaccard-consistency (float n2-jaccard-consistency)
                            :n2-max-idx n2-max-idx
                            :empty-n2-missing-intervals? (empty? n2-missing-intervals)
                            :empty-n3-duplicates? (empty? n3-duplicates)
                            :n3-hamming-consistency (float n3-hamming-consistency)
                            :n3-jaccard-consistency (float n3-jaccard-consistency)
                            :n3-max-idx n3-max-idx
                            :empty-n3-missing-intervals? (empty? n3-missing-intervals)
                            :empty-failed-setup-cluster? (empty? failed-setup-cluster) ; There shouldn't be any failed setup cluster operations.
                            :empty-failed-show-instances? (empty? failed-show-instances) ; There shouldn't be any failed show instances operations.
                            :empty-failed-add-nodes? (empty? failed-add-nodes) ; There shouldn't be any failed add-nodes operations.
                            :empty-failed-get-nodes? (empty? failed-get-nodes) ; There shouldn't be any failed get-nodes operations.
                            :empty-partial-instances? (empty? partial-instances)}

            updates [{:key :coordinators :condition (not (:correct-coordinators? initial-result)) :value coordinators}
                     {:key :partial-instances :condition (not (:empty-partial-instances? initial-result)) :value partial-instances}
                     {:key :n1-duplicates-intervals :condition (false? (:empty-n1-duplicates? initial-result)) :value n1-duplicates-intervals}
                     {:key :n1-missing-intervals :condition (false? (:empty-n1-missing-intervals? initial-result)) :value n1-missing-intervals}
                     {:key :n2-duplicates-intervals :condition (false? (:empty-n2-duplicates? initial-result)) :value n2-duplicates-intervals}
                     {:key :n2-missing-intervals :condition (false? (:empty-n2-missing-intervals? initial-result)) :value n2-missing-intervals}
                     {:key :n3-duplicates-intervals :condition (false? (:empty-n3-duplicates? initial-result)) :value n3-duplicates-intervals}
                     {:key :n3-missing-intervals :condition (false? (:empty-n3-missing-intervals? initial-result)) :value n3-missing-intervals}
                     {:key :failed-setup-cluster :condition (not (:empty-failed-setup-cluster? initial-result)) :value failed-setup-cluster}
                     {:key :failed-add-nodes :condition (not (:empty-failed-add-nodes? initial-result)) :value failed-add-nodes}
                     {:key :failed-get-nodes :condition (not (:empty-failed-get-nodes? initial-result)) :value failed-get-nodes}
                     {:key :failed-show-instances :condition (not (:empty-failed-show-instances? initial-result)) :value failed-show-instances}]]

        (reduce (fn [result update]
                  (if (:condition update)
                    (assoc result (:key update) (:value update))
                    result))
                initial-result
                updates)))))

(defn create-unique-constraint
  "Invoke show-instances-read op."
  [_ _]
  {:type :invoke, :f :create-unique-constraint, :value nil})

(defn show-instances-reads
  "Invoke show-instances-read op."
  [_ _]
  {:type :invoke, :f :show-instances-read, :value nil})

(defn setup-cluster
  "Invoke setup-cluster operation."
  [_ _]
  {:type :invoke :f :setup-cluster :value nil})

(defn add-nodes
  "Invoke add-nodes."
  [_ _]
  {:type :invoke :f :add-nodes :value nil})

(defn get-nodes
  "Invoke get-nodes op."
  [_ _]
  {:type :invoke :f :get-nodes :value nil})

(defn client-generator
  "Client generator."
  []
  (gen/each-thread
   (gen/phases
    (gen/once setup-cluster)
    (gen/sleep 5)
    (gen/once create-unique-constraint)
    (gen/delay delay-requests-sec
               (gen/mix [show-instances-reads add-nodes])))))

(defn workload
  "Basic HA workload."
  [opts]
  (let [nodes-config (:nodes-config opts)
        db (:db opts)
        first-leader (random-coord (keys nodes-config))
        first-main (random-data-instance (keys nodes-config))
        organization (:organization opts)
        license (:license opts)
        recovery-time (:recovery-time opts)
        nemesis-start-sleep (:nemesis-start-sleep opts)]
    {:client    (Client. nodes-config first-leader first-main license organization)
     :checker   (checker/compose
                 {:hacreate     (checker)
                  :timeline (timeline/html)})
     :generator (client-generator)
     :final-generator {:clients (gen/each-thread (gen/once get-nodes)) :recovery-time recovery-time}
     :nemesis-config (nemesis/create db nodes-config nemesis-start-sleep)}))
