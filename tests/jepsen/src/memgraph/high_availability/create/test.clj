(ns memgraph.high-availability.create.test
  "Create test for HA."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.core :as c]
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

(def registered-replication-instances? (atom false))
(def added-coordinator-instances? (atom false))
(def main-set? (atom false))
(def batches-inserted (atom 0))

(def batch-size 5000)

(defn hamming-sim
  "Calculates hamming distance between two sequences."
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

(defn batch-start-idx
  "Calculates start index for the new batch. E.g 1, 1001, 2001..."
  []
  (+ 1 (* (deref batches-inserted) batch-size)))

(defn get-expected-number-indices
  "Calculates the number of vertices that should've been inserted."
  []
  (* (deref batches-inserted) batch-size))

(defn get-expected-indices
  "Returns the range of all indices that should've been inserted."
  []
  (range 1 (+ 1 (* (deref batches-inserted) batch-size))))

(defn batch-end-idx
  "Calculates end index for the new batch. End index will not be included. E.g 1001, 2001"
  [batch-start-idx]
  (+ batch-start-idx (- batch-size 1)))

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
  [session myself nodes-config]
  (doseq [coord-config (->> nodes-config
                            (filter #(not= (key %) myself)) ; Don't register itself
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

(defn mg-add-nodes
  "Add nodes as part of the txn."
  [start-idx end-idx txn]
  ((mgquery/add-nodes start-idx end-idx) txn))

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
                       (dbclient/with-transaction bolt-conn txn
                         (let [indices (->> (mg-get-nodes txn) (map :id) (reduce conj []))]
                           (assoc op :type :ok :value {:indices indices :node node})))
                        ; There shouldn't be any other exception since nemesis will heal all nodes as part of its final generator.
                       (catch Exception e
                         (assoc op :type :fail :value (str e))))
                     (assoc op :type :info :value "Not data instance."))

        :add-nodes (if (hautils/data-instance? node)
                     (try
                       (dbclient/with-transaction bolt-conn txn
                         (let [start-idx (batch-start-idx)
                               end-idx (batch-end-idx start-idx)]
                           (mg-add-nodes start-idx end-idx txn)
                           (swap! batches-inserted + 1)
                           (assoc op :type :ok :value (str "Nodes with indices [" start-idx "," end-idx "] created."))))

                       (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                         (utils/process-service-unavailable-exc op node))

                       (catch Exception e
                         (if (or
                              (utils/query-forbidden-on-replica? e)
                              (utils/query-forbidden-on-main? e)
                              (utils/sync-replica-down? e))
                           (assoc op :type :info :value (str e))
                           (assoc op :type :fail :value (str e)))))

                     (assoc op :type :info :value "Not data instance."))

; Show instances should be run only on coordinators/
        :show-instances-read (if (hautils/coord-instance? node)
                               (try
                                 (utils/with-session bolt-conn session ; Use bolt connection for running show instances.
                                   (let [instances (reduce conj [] (mgquery/get-all-instances session))]
                                     (assoc op
                                            :type :ok
                                            :value {:instances instances :node node})))
                                 (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                   (utils/process-service-unavailable-exc op node))
                                 (catch Exception e
                                   (assoc op :type :fail :value (str e))))
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

(defn coll-has-duplicates
  "Check if the collection has duplicates by comparing the size of set and seq."
  [coll]
  (not= (count coll) (count (set coll))))

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
            expected-ids-number (get-expected-number-indices)

            expected-ids (get-expected-indices)

            n1-ids (->> ok-get-nodes
                        (filter #(= "n1" (:node %)))
                        first
                        (:indices))

            n1-consistency (hamming-sim expected-ids n1-ids)

            n2-ids (->> ok-get-nodes
                        (filter #(= "n2" (:node %)))
                        first
                        (:indices))

            n2-consistency (hamming-sim expected-ids n2-ids)

            n3-ids (->> ok-get-nodes
                        (filter #(= "n3" (:node %)))
                        first
                        (:indices))

            n3-consistency (hamming-sim expected-ids n3-ids)

            failed-setup-cluster (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :setup-cluster (:f %)))
                                      (map :value))
            failed-show-instances (->> history
                                       (filter #(= :fail (:type %)))
                                       (filter #(= :show-instances-read (:f %)))
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
                                     (= n1-consistency 1)
                                     (= n2-consistency 1)
                                     (= n3-consistency 1))
                            :empty-partial-coordinators? (empty? partial-coordinators) ; coordinators which have missing coordinators in their reads
                            :empty-more-than-one-main-nodes? (empty? more-than-one-main) ; nodes on which more-than-one-main was detected
                            :correct-coordinators? (= coordinators #{"n4" "n5" "n6"})
                            :n1-no-duplicated-ids? (false? (coll-has-duplicates n1-ids))
                            :n1-consistency (float n1-consistency)
                            :n2-no-duplicated-ids? (false? (coll-has-duplicates n2-ids))
                            :n2-consistency (float n2-consistency)
                            :n3-no-duplicated-ids? (false? (coll-has-duplicates n3-ids))
                            :n3-consistency (float n3-consistency)
                            :total-indices expected-ids-number
                            :empty-failed-setup-cluster? (empty? failed-setup-cluster) ; There shouldn't be any failed setup cluster operations.
                            :empty-failed-show-instances? (empty? failed-show-instances) ; There shouldn't be any failed show instances operations.
                            :empty-partial-instances? (empty? partial-instances)}

            updates [{:key :coordinators :condition (not (:correct-coordinators? initial-result)) :value coordinators}
                     {:key :partial-instances :condition (not (:empty-partial-instances? initial-result)) :value partial-instances}
                     {:key :n1-ids :condition (or (not= 1 n1-consistency) (false? (:n1-no-duplicated-ids? initial-result))) :value n1-ids}
                     {:key :n2-ids :condition (or (not= 1 n2-consistency) (false? (:n2-no-duplicated-ids? initial-result))) :value n2-ids}
                     {:key :n3-ids :condition (or (not= 1 n3-consistency) (false? (:n3-no-duplicated-ids? initial-result))) :value n3-ids}
                     {:key :failed-setup-cluster :condition (not (:empty-failed-setup-cluster? initial-result)) :value failed-setup-cluster}
                     {:key :failed-show-instances :condition (not (:empty-failed-show-instances? initial-result)) :value failed-show-instances}]]

        (reduce (fn [result update]
                  (if (:condition update)
                    (assoc result (:key update) (:value update))
                    result))
                initial-result
                updates)))))

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
    (gen/delay 2
               (gen/mix [show-instances-reads add-nodes])))))

(defn workload
  "Basic HA workload."
  [opts]
  (let [nodes-config (:nodes-config opts)
        first-leader (random-coord (keys nodes-config))
        first-main (random-data-instance (keys nodes-config))
        organization (:organization opts)
        license (:license opts)]
    {:client    (Client. nodes-config first-leader first-main license organization)
     :checker   (checker/compose
                 {:hacreate     (checker)
                  :timeline (timeline/html)})
     :generator (client-generator)
     :final-generator {:clients (gen/each-thread (gen/once get-nodes)) :recovery-time 30}
     :nemesis-config (nemesis/create nodes-config)}))
