(ns jepsen.memgraph.haempty
  "High availability empty test. Test doesn't use any data, rather just tests management part of the cluster."
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [checker :as checker]
             [generator :as gen]
             [client :as client]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.haclient :as haclient]
            [jepsen.memgraph.utils :as utils]))

(dbclient/defquery get-all-instances
  "SHOW INSTANCES;")

(defrecord Client [nodes-config license organization]
  client/Client
  (open! [this _test node]
    (info "Opening connection to node" node)
    (let [connection (utils/open-bolt node)
          node-config (get nodes-config node)]
      (assoc this
             :conn connection
             :node-config node-config
             :node node)))
  (setup! [this _]
    (utils/with-session (:conn this) session
      ((haclient/set-db-setting "enterprise.license" license) session)
      ((haclient/set-db-setting "organization.name" organization) session)))

  (invoke! [this _test op]
    (let [node-config (:node-config this)]
      (case (:f op)
        :read (if (contains? node-config :coordinator-id)
                (try
                  (utils/with-session (:conn this) session
                    (let [instances (->> (get-all-instances session) (reduce conj []))]
                      (assoc op
                             :type :ok
                             :value {:instances instances :node (:node this)})))
                  (catch Exception e
                    (assoc op :type :fail :value e)))
                (assoc op :type :fail :value "Not coord"))

        :register (if (= (:node this) "n4") ; Node with coordinator-id = 1
                    (do
                      (doseq [repl-config (filter #(contains? (val %) :replication-port)
                                                  nodes-config)]
                        (try
                          (utils/with-session (:conn this) session
                            ((haclient/register-replication-instance
                              (first repl-config)
                              (second repl-config)) session))
                          (catch Exception e
                            (assoc op :type :fail :value e))))
                      (doseq [coord-config (->> nodes-config
                                                (filter #(not= (key %) "n4")) ; Don't register itself
                                                (filter #(contains? (val %) :coordinator-id)))]
                        (try
                          (utils/with-session (:conn this) session
                            ((haclient/add-coordinator-instance
                              (second coord-config)) session))
                          (catch Exception e
                            (assoc op :type :fail :value e))))
                      (let [rand-main (nth (keys nodes-config) (rand-int 3))] ; 3 because first 3 instances are replication instances in cluster.edn
                        (try
                          (utils/with-session (:conn this) session
                            ((haclient/set-instance-to-main rand-main) session))
                          (catch Exception e
                            (assoc op :type :fail :value e))))

                      (assoc op :type :ok))
                    (assoc op :type :fail :value "Trying to register on node != n4")))))

  (teardown! [_ _])
  (close! [this _]
    (dbclient/disconnect (:conn this))))

(defn reads
  "Create read action."
  [_ _]
  {:type :invoke, :f :read, :value nil})

(defn single-read-to-roles
  "Convert single read to roles. Single read is a list of instances."
  [single-read]
  (map :role single-read))

(defn single-read-to-role-and-health
  "Convert single read to role and health. Single read is a list of instances."
  [single-read]
  (map #(select-keys % [:health :role]) single-read))

(defn get-coordinators
  "From list of roles, returns those which are coordinator."
  [roles]
  (filter #(= "coordinator" %) roles))

(defn get-mains
  "From list of roles, returns those which are main."
  [roles]
  (filter #(= "main" %) roles))

; TODO: (andi) Rework bank and large clients to avoid using macros.

(defn less-than-three-coordinators
  "Check if there aren't exactly 3 coordinators in single read where single-read is a read list of instances. Single-read here is already processed list of roles."
  [roles]
  (< (count (get-coordinators roles)) 3))

(defn more-than-one-main
  "Check if there is more than one main in single read where single-read is a read list of instances. Single-read here is already processed list of roles."
  [roles]
  (> (count (get-mains roles)) 1))

(defn alive-instances-no-main
  "When all 3 data instances are alive, there should be exactly one main and 2 replicas."
  [roles-health]
  (let [mains (filter #(= "main" (:role %)) roles-health)
        replicas (filter #(= "replica" (:role %)) roles-health)
        all-data-instances-up (and
                               (every? #(= "up" (:health %)) mains)
                               (every? #(= "up" (:health %)) replicas))]

    (if all-data-instances-up
      (and (= 1 (count mains))
           (= 2 (count replicas)))
      true)))

(defn haempty-checker
  "HA empty checker"
  []
  (reify checker/Checker
    (check [_ _ history _]
      (let [reads  (->> history
                        (filter #(= :ok (:type %)))
                        (filter #(= :read (:f %)))
                        (map :value))
            ; Full reads all reads which returned 6 instances
            full-reads (->> reads
                            (filter #(= 6 (count (:instances %)))))
            ; All reads grouped by node
            coord->reads (->> reads
                              (group-by :node)
                              (map (fn [[node values]] [node (map :instances values)]))
                              (into {}))
            ; All full reads grouped by node
            coord->full-reads (->> full-reads
                                   (group-by :node)
                                   (map (fn [[node values]] [node (map :instances values)]))
                                   (into {}))
            coord->roles (->> coord->full-reads
                              (map (fn [[node reads]]
                                     [node (map single-read-to-roles reads)]))
                              (into {}))
            ; coords-missing-reads are coordinators who have full reads where not all coordinators are present
            coords-missing-reads (->> coord->roles
                                      (filter (fn [[_ reads]] (some less-than-three-coordinators reads)))
                                      (keys))
            ; Check from full reads if there is any where there was more than one main.
            more-than-one-main (->> coord->roles
                                    (filter (fn [[_ reads]] (some more-than-one-main reads)))
                                    (keys))
            ; Mapping from coordinator to reads containing only health and role.
            coord->roles-health (->> coord->full-reads
                                     (map (fn [[node reads]]
                                            [node (map single-read-to-role-and-health reads)]))
                                     (into {}))
            ; Check not-used, maybe will be added in the future.
            _ (->> coord->roles-health
                   (filter (fn [[_ reads]] (some alive-instances-no-main reads)))
                   (vals))
            ; Node is considered empty if all reads are empty -> probably a mistake in registration.
            empty-nodes (->> coord->reads
                             (filter (fn [[_ reads]]
                                       (every? empty? reads)))
                             (keys))
            coordinators (set (keys coord->reads)) ; Only coordinators should run SHOW INSTANCES
            ]

        {:valid? (and (empty? empty-nodes)
                      (= coordinators #{"n4" "n5" "n6"})
                      (empty? coords-missing-reads)
                      (empty? more-than-one-main))
         :empty-nodes empty-nodes ; nodes which have all reads empty
         :missing-coords-nodes coords-missing-reads ; coordinators which have missing coordinators in their reads
         :more-than-one-main-nodes more-than-one-main ; nodes on which more-than-one-main was detected
         :coordinators coordinators}))))

(defn workload
  "Basic HA workload."
  [opts]
  {:client    (Client. (:nodes-config opts) (:license opts) (:organization opts))
   :checker   (checker/compose
               {:haempty     (haempty-checker)
                :timeline (timeline/html)})
   :generator (haclient/ha-gen (gen/mix [reads]))
   :final-generator {:clients (gen/once reads) :recovery-time 20}})
