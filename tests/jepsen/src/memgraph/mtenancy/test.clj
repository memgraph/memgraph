(ns memgraph.mtenancy.test
  "Test for multitenancy with HA."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.core :as c]
            [clojure.string :as string]
            [jepsen
             [checker :as checker]
             [generator :as gen]
             [client :as jclient]]
            [jepsen.checker.timeline :as timeline]
            [memgraph.mtenancy.utils :as mutils]
            [memgraph.mtenancy.nemesis :as nemesis]
            [memgraph.utils :as utils]
            [memgraph.query :as mgquery]))

(def registered-replication-instances? (atom false))
(def added-coordinator-instances? (atom false))
(def main-set? (atom false))

(def pokec-medium-expected-num-nodes 100000)
(def pokec-medium-expected-num-edges 1768515) ; one-directional edges

(defn random-coord
  "Get random leader."
  [nodes]
  (nth nodes (+ 2 (rand-int 3)))) ; Runs under assumption that first 2 instances are data instances and last 3 are coordinators.

(defn random-data-instance
  "Get random data instance."
  [nodes]
  (nth nodes (rand-int 2))) ; Runs under assumption that first 2 instances are data instances

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

(defrecord Client [nodes-config first-leader first-main license organization num-tenants]
  jclient/Client
  ; Open Bolt connection to all nodes.
  (open! [this _test node]
    (info "Opening bolt connection to node..." node)
    (let [bolt-conn (utils/open-bolt node)
          node-config (get nodes-config node)]
      (assoc this
             :bolt-conn bolt-conn
             :node-config node-config
             :node node
             :num-tenants num-tenants)))

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
        :get-num-nodes (if (mutils/data-instance? node)
                         (try
                           (let
                            [num-nodes
                             (reduce (fn [acc-nodes db]
                                       (let [session-config (utils/db-session-config db)]
                                         (utils/with-db-session bolt-conn session-config session
                                           (let [db-num-nodes (->> (mgquery/get-num-nodes session) first :c)]
                                             (conj acc-nodes db-num-nodes)))))

                                     [] (mutils/get-all-dbs num-tenants))]

                             (assoc op :type :ok :value {:num-nodes num-nodes :node node}))
                        ; There shouldn't be any other exception since nemesis will heal all nodes as part of its final generator.
                           (catch Exception e
                             (assoc op :type :fail :value (str e))))
                         (assoc op :type :info :value "Not data instance."))

        :get-num-edges (if (mutils/data-instance? node)
                         (try
                           (let
                            [num-edges
                             (reduce (fn [acc-edges db]
                                       (let [session-config (utils/db-session-config db)]
                                         (utils/with-db-session bolt-conn session-config session
                                           (let [db-num-edges (->> (mgquery/get-num-edges session) first :c)]
                                             (conj acc-edges db-num-edges)))))

                                     [] (mutils/get-all-dbs num-tenants))]

                             (assoc op :type :ok :value {:num-edges num-edges :node node}))

; There shouldn't be any other exception since nemesis will heal all nodes as part of its final generator.
                           (catch Exception e
                             (assoc op :type :fail :value (str e))))
                         (assoc op :type :info :value "Not data instance."))

; Show instances should be run only on coordinators/
        :show-instances-read (if (mutils/coord-instance? node)
                               (try
                                 (utils/with-session bolt-conn session ; Use bolt connection for running show instances.
                                   (let [instances (reduce conj [] (mgquery/get-all-instances session))]
                                     (assoc op
                                            :type :ok
                                            :value {:instances instances :node node :time (utils/current-local-time-formatted)})))
                                 (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                   (utils/process-service-unavailable-exc op node))
                                 (catch Exception e
                                   (assoc op :type :fail :value (str e))))
                               (assoc op :type :info :value "Not coordinator"))

        :update-nodes (if (and (mutils/data-instance? node) (is-main? bolt-conn))
                        (try
                          (let [session-config (utils/db-session-config (mutils/get-random-db num-tenants))
                                random-start-node (rand-int pokec-medium-expected-num-nodes)]
                            (utils/with-db-session bolt-conn session-config session
                              (mgquery/update-pokec-nodes session {:param random-start-node}))

                            (assoc op :type :ok :value {:str "Updated nodes"}))

                          (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                            (utils/process-service-unavailable-exc op node))

                          (catch org.neo4j.driver.exceptions.ClientException e
                            (cond
                              (utils/sync-replica-down? e)
                              (assoc op :type :ok :value {:str "Nodes updated. SYNC replica is down."})

                              (utils/main-became-replica? e)
                              (assoc op :type :ok :value {:str "Cannot commit because instance is not main anymore."})

                              (utils/main-unwriteable? e)
                              (assoc op :type :ok :value {:str "Cannot commit because main is currently non-writeable."})

                              (or (utils/query-forbidden-on-replica? e)
                                  (utils/query-forbidden-on-main? e))
                              (assoc op :type :info :value (str e))

                              :else
                              (assoc op :type :fail :value (str e))))

                          (catch Exception e
                            (assoc op :type :fail :value (str e))))

                        (assoc op :type :info :value "Not main data instance."))

        :create-ttl-edges (if (and (mutils/data-instance? node) (is-main? bolt-conn))
                            (try
                              (let [session-config (utils/db-session-config (mutils/get-random-db num-tenants))
                                    random-start-node (rand-int pokec-medium-expected-num-nodes)]
                                (utils/with-db-session bolt-conn session-config session
                                  (mgquery/create-ttl-edges session {:param random-start-node}))

                                (assoc op :type :ok :value {:str "Created TTL edges"}))

                              (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                (utils/process-service-unavailable-exc op node))

                              (catch org.neo4j.driver.exceptions.ClientException e
                                (cond
                                  (utils/sync-replica-down? e)
                                  (assoc op :type :ok :value {:str "TTL edges created. SYNC replica is down."})

                                  (utils/main-became-replica? e)
                                  (assoc op :type :info :value {:str "Cannot commit because instance is not main anymore."})

                                  (utils/main-unwriteable? e)
                                  (assoc op :type :info :value {:str "Cannot commit because main is currently non-writeable."})

                                  (utils/conflicting-txns? e)
                                  (assoc op :type :info :value {:str "Conflicting txns"})

                                  (or (utils/query-forbidden-on-replica? e)
                                      (utils/query-forbidden-on-main? e))
                                  (assoc op :type :info :value (str e))

                                  :else
                                  (assoc op :type :fail :value (str e))))

                              (catch Exception e
                                (assoc op :type :fail :value (str e))))

                            (assoc op :type :info :value "Not main data instance."))

        :delete-ttl-edges (if (and (mutils/data-instance? node) (is-main? bolt-conn))
                            (try
                              (let [session-config (utils/db-session-config (mutils/get-random-db num-tenants))]
                                (utils/with-db-session bolt-conn session-config session
                                  (mgquery/delete-ttl-edges session))

                                (assoc op :type :ok :value {:str "Deleted TTL edges"}))

                              (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                (utils/process-service-unavailable-exc op node))

                              (catch org.neo4j.driver.exceptions.ClientException e
                                (cond
                                  (utils/sync-replica-down? e)
                                  (assoc op :type :ok :value {:str "Edges deleted. SYNC replica is down."})

                                  (utils/main-became-replica? e)
                                  (assoc op :type :info :value {:str "Cannot commit because instance is not main anymore."})

                                  (utils/main-unwriteable? e)
                                  (assoc op :type :info :value {:str "Cannot commit because main is currently non-writeable."})

                                  (utils/conflicting-txns? e)
                                  (assoc op :type :info :value {:str "Conflicting txns"})

                                  (or (utils/query-forbidden-on-replica? e)
                                      (utils/query-forbidden-on-main? e))
                                  (assoc op :type :info :value (str e))

                                  :else
                                  (assoc op :type :fail :value (str e))))

                              (catch Exception e
                                (assoc op :type :fail :value (str e))))

                            (assoc op :type :info :value "Not main data instance."))

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
              (cond
                (utils/not-leader? e)
                (assoc op :type :info :value "Not a leader")

                (utils/adding-coordinator-failed? e)
                (assoc op :type :info :value "Failed to add coordinator")

                :else
                (assoc op :type :fail :value (str e)))))

          (assoc op :type :info :value "Not first leader"))

        :create-databases (if (and (mutils/data-instance? node) (is-main? bolt-conn))
                            (try
                              (utils/with-session bolt-conn session
                                (doseq [db (mutils/get-new-dbs num-tenants)]
                                  ((mgquery/create-database db) session))

                                (assoc op :type :ok :value {:str "Created databases" :num-tenants num-tenants}))

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

        :import-nodes (if (and (mutils/data-instance? node) (is-main? bolt-conn))
                        (try
                          (doseq [db (mutils/get-all-dbs num-tenants)]
                            (let [session-config (utils/db-session-config db)]
                              (utils/with-db-session bolt-conn session-config session
                                (mgquery/create-label-idx session)
                                (mgquery/create-label-property-idx session)
                                (mgquery/create-ttl-edge-idx session)
                                (mgquery/import-pokec-medium-nodes session))))

                          (assoc op :type :ok :value {:str "pokec_medium nodes imported"})

                          (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                            (utils/process-service-unavailable-exc op node))

                          (catch Exception e
                            (assoc op :type :fail :value (str e))))

                        (assoc op :type :info :value "Not main data instance."))

        :import-edges (if (and (mutils/data-instance? node) (is-main? bolt-conn))
                        (try
                          (doseq [db (mutils/get-all-dbs num-tenants)]
                            (let [session-config (utils/db-session-config db)]
                              (utils/with-db-session bolt-conn session-config session
                                (mgquery/import-pokec-medium-edges session))))

                          (assoc op :type :ok :value {:str "pokec_medium edges imported"})

                          (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                            (utils/process-service-unavailable-exc op node))

                          (catch org.neo4j.driver.exceptions.ClientException e
                            (cond
                              (utils/not-main-anymore? e)
                              (assoc op :type :info :value {:str "Not main anymore"})

                              :else
                              (assoc op :type :fail :value (str e))))

                          (catch org.neo4j.driver.exceptions.TransientException e
                            (cond
                              (utils/sync-replica-down? e)
                              (assoc op :type :ok :value {:str "Edges deleted. SYNC replica is down."})

                              (utils/conflicting-txns? e)
                              (assoc op :type :info :value {:str "Conflicting txns"})

                              :else
                              (assoc op :type :fail :value (str e))))

                          (catch Exception e
                            (assoc op :type :fail :value (str e))))

                        (assoc op :type :info :value "Not main data instance.")))))

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
      (let [ok-get-num-nodes (->> history
                                  (filter #(= :ok (:type %)))
                                  (filter #(= :get-num-nodes (:f %)))
                                  (map :value))

            ok-get-num-edges (->> history
                                  (filter #(= :ok (:type %)))
                                  (filter #(= :get-num-edges (:f %)))
                                  (map :value))

            n1-num-nodes (->> ok-get-num-nodes
                              (filter #(= "n1" (:node %)))
                              first
                              (:num-nodes))

            n1-num-edges (->> ok-get-num-edges
                              (filter #(= "n1" (:node %)))
                              first
                              (:num-edges))

            n2-num-nodes (->> ok-get-num-nodes
                              (filter #(= "n2" (:node %)))
                              first
                              (:num-nodes))

            n2-num-edges (->> ok-get-num-edges
                              (filter #(= "n2" (:node %)))
                              first
                              (:num-edges))

            failed-setup-cluster (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :setup-cluster (:f %)))
                                      (map :value))

            failed-create-databases (->> history
                                         (filter #(= :fail (:type %)))
                                         (filter #(= :create-databases (:f %)))
                                         (map :value))

            failed-import-nodes (->> history
                                     (filter #(= :fail (:type %)))
                                     (filter #(= :import-nodes (:f %)))
                                     (map :value))

            failed-import-edges (->> history
                                     (filter #(= :fail (:type %)))
                                     (filter #(= :import-edges (:f %)))
                                     (map :value))

            failed-show-instances (->> history
                                       (filter #(= :fail (:type %)))
                                       (filter #(= :show-instances-read (:f %)))
                                       (map :value))

            failed-update-nodes (->> history
                                     (filter #(= :fail (:type %)))
                                     (filter #(= :update-nodes (:f %)))
                                     (map :value))

            failed-create-ttl-edges (->> history
                                         (filter #(= :fail (:type %)))
                                         (filter #(= :create-ttl-edges (:f %)))
                                         (map :value))

            failed-delete-ttl-edges (->> history
                                         (filter #(= :fail (:type %)))
                                         (filter #(= :delete-ttl-edges (:f %)))
                                         (map :value))

            failed-get-num-nodes (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :get-num-nodes (:f %)))
                                      (map :value))

            failed-get-num-edges (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :get-num-edges (:f %)))
                                      (map :value))

            si-reads  (->> history
                           (filter #(= :ok (:type %)))
                           (filter #(= :show-instances-read (:f %)))
                           (map :value))

            partial-instances (->> si-reads
                                   (filter #(not= 5 (count (:instances %)))))
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
                                     (= coordinators #{"n3" "n4" "n5"})
                                     (empty? partial-coordinators)
                                     (empty? more-than-one-main)
                                     (empty? partial-instances)
                                     (empty? failed-setup-cluster)
                                     (empty? failed-create-databases)
                                     (empty? failed-import-nodes)
                                     (empty? failed-import-edges)
                                     (empty? failed-show-instances)
                                     (empty? failed-update-nodes)
                                     (empty? failed-create-ttl-edges)
                                     (empty? failed-delete-ttl-edges)
                                     (every? #(= % pokec-medium-expected-num-nodes) n1-num-nodes)
                                     (every? #(= % pokec-medium-expected-num-nodes) n2-num-nodes)
                                     (every? #(= % pokec-medium-expected-num-edges) n1-num-edges)
                                     (every? #(= % pokec-medium-expected-num-edges) n2-num-edges))
                            :empty-partial-coordinators? (empty? partial-coordinators) ; coordinators which have missing coordinators in their reads
                            :empty-more-than-one-main-nodes? (empty? more-than-one-main) ; nodes on which more-than-one-main was detected
                            :correct-coordinators? (= coordinators #{"n3" "n4" "n5"})
                            :n1-all-nodes? (every? #(= % pokec-medium-expected-num-nodes) n1-num-nodes)
                            :n1-all-edges? (every? #(= % pokec-medium-expected-num-edges) n1-num-edges)
                            :n2-all-nodes? (every? #(= % pokec-medium-expected-num-nodes) n2-num-nodes)
                            :n2-all-edges? (every? #(= % pokec-medium-expected-num-edges) n2-num-edges)
                            :empty-failed-setup-cluster? (empty? failed-setup-cluster) ; There shouldn't be any failed setup cluster operations.
                            :empty-failed-create-databases? (empty? failed-create-databases) ; There shouldn't be any failed create-databases operations.
                            :empty-failed-import-nodes? (empty? failed-import-nodes) ; There shouldn't be any failed import-nodes operations.
                            :empty-failed-import-edges? (empty? failed-import-edges) ; There shouldn't be any failed import-edges operations.
                            :empty-failed-show-instances? (empty? failed-show-instances) ; There shouldn't be any failed show instances operations.
                            :empty-failed-update-nodes? (empty? failed-update-nodes) ; There shouldn't be any failed update nodes operations.
                            :empty-failed-create-ttl-edges? (empty? failed-create-ttl-edges) ; There shouldn't be any failed create-ttl-edges operations.
                            :empty-failed-delete-ttl-edges? (empty? failed-delete-ttl-edges) ; There shouldn't be any failed delete-ttl-edges operations.
                            :empty-failed-get-num-nodes? (empty? failed-get-num-nodes) ; There shouldn't be any failed get-num-nodes operations.
                            :empty-failed-get-num-edges? (empty? failed-get-num-edges) ; There shouldn't be any failed get-num-edges operations.
                            :empty-partial-instances? (empty? partial-instances)}

            updates [{:key :coordinators :condition (not (:correct-coordinators? initial-result)) :value coordinators}
                     {:key :partial-instances :condition (not (:empty-partial-instances? initial-result)) :value partial-instances}
                     {:key :n1-not-all-nodes :condition (not (:n1-all-nodes? initial-result)) :value n1-num-nodes}
                     {:key :n1-not-all-edges :condition (not (:n1-all-edges? initial-result)) :value n1-num-edges}
                     {:key :n2-not-all-nodes :condition (not (:n2-all-nodes? initial-result)) :value n2-num-nodes}
                     {:key :n2-not-all-edges :condition (not (:n2-all-edges? initial-result)) :value n2-num-edges}
                     {:key :failed-setup-cluster :condition (not (:empty-failed-setup-cluster? initial-result)) :value failed-setup-cluster}
                     {:key :failed-create-databases :condition (not (:empty-failed-create-databases? initial-result)) :value failed-create-databases}
                     {:key :failed-import-nodes :condition (not (:empty-failed-import-nodes? initial-result)) :value failed-import-nodes}
                     {:key :failed-import-edges :condition (not (:empty-failed-import-edges? initial-result)) :value failed-import-edges}
                     {:key :failed-get-num-nodes :condition (not (:empty-failed-get-num-nodes? initial-result)) :value failed-get-num-nodes}
                     {:key :failed-get-num-edges :condition (not (:empty-failed-get-num-edges? initial-result)) :value failed-get-num-edges}
                     {:key :failed-update-nodes :condition (not (:empty-failed-update-nodes? initial-result)) :value failed-update-nodes}
                     {:key :failed-create-ttl-edges :condition (not (:empty-failed-create-ttl-edges? initial-result)) :value failed-create-ttl-edges}
                     {:key :failed-delete-ttl-edges :condition (not (:empty-failed-delete-ttl-edges? initial-result)) :value failed-delete-ttl-edges}
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

(defn update-nodes
  "Invoke update-nodes op."
  [_ _]
  {:type :invoke, :f :update-nodes, :value nil})

(defn create-ttl-edges
  "Invoke create-ttl-edges op."
  [_ _]
  {:type :invoke, :f :create-ttl-edges, :value nil})

(defn delete-ttl-edges
  "Invoke delete-ttl-edges op."
  [_ _]
  {:type :invoke, :f :delete-ttl-edges, :value nil})

(defn setup-cluster
  "Invoke setup-cluster operation."
  [_ _]
  {:type :invoke :f :setup-cluster :value nil})

(defn create-databases
  "Invoke create-databases operation."
  [_ _]
  {:type :invoke :f :create-databases :value nil})

(defn import-nodes
  "Invoke import-nodes operation."
  [_ _]
  {:type :invoke :f :import-nodes :value nil})

(defn import-edges
  "Invoke import-edges operation."
  [_ _]
  {:type :invoke :f :import-edges :value nil})

(defn get-num-nodes
  "Invoke get-num-nodes op."
  [_ _]
  {:type :invoke :f :get-num-nodes :value nil})

(defn get-num-edges
  "Invoke get-num-edges op."
  [_ _]
  {:type :invoke :f :get-num-edges :value nil})

(defn client-generator
  "Client generator."
  []
  (gen/each-thread
   (gen/phases
    (gen/once setup-cluster)
    (gen/sleep 2)
    (gen/once create-databases)
    (gen/once import-nodes)
    (gen/once import-edges)
    (gen/sleep 5)
    (gen/delay 2
               (gen/mix [show-instances-reads update-nodes create-ttl-edges delete-ttl-edges])))))

(defn final-client-generator
  "Final client generator."
  []
  (gen/each-thread
   (gen/phases
    (gen/once get-num-nodes)
    (gen/once get-num-edges))))

(defn workload
  "Basic HA workload."
  [opts]
  (let [nodes-config (:nodes-config opts)
        db (:db opts)
        first-leader (random-coord (keys nodes-config))
        first-main (random-data-instance (keys nodes-config))
        organization (:organization opts)
        license (:license opts)
        num-tenants (:num-tenants opts)
        recovery-time (:recovery-time opts)
        nemesis-start-sleep (:nemesis-start-sleep opts)]
    {:client    (Client. nodes-config first-leader first-main license organization num-tenants)
     :checker   (checker/compose
                 {:ha-mt     (checker)
                  :timeline (timeline/html)})
     :generator (client-generator)
     :final-generator {:clients (final-client-generator) :recovery-time recovery-time}
     :nemesis-config (nemesis/create db nodes-config nemesis-start-sleep)}))
