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
  (open! [this _ node]
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

  (invoke! [this _ op]
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

; TODO: (andi) Process single-read to status

(defn get-coordinators
  "From list of roles, returns those which are coordinator."
  [roles]
  (let [coordinators (->> roles
                          (filter #(= "coordinator" %)))]
    coordinators))

; TODO: (andi)
; get replicas
; get main

; TODO: (andi) Rework bank and large clients to avoid using macros.

(defn roles-incorrect
  "Check if there isn't exactly 3 coordinators in single read where single-read is read list of instances. Single-read here is already processed list of roles."
  [roles]
  (not= 3 (count (get-coordinators roles))))

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
            instances-by-node (->> reads
                                   (group-by :node)
                                   (map (fn [[node values]] [node (map :instances values)]))
                                   (into {}))
            ; All full reads grouped by node
            full-instances-by-node (->> full-reads
                                        (group-by :node)
                                        (map (fn [[node values]] [node (map :instances values)]))
                                        (into {}))
            ; Invalid full reads are those where not all coordinators are present
            ; TODO: (andi) Rename and rework, have coord->reads, coord->roles, coord->status.
            invalid-full-reads (->> full-instances-by-node
                                    (map (fn [[node reads]]
                                           [node (map single-read-to-roles reads)]))
                                    (filter (fn [[_ reads]] (some roles-incorrect reads)))
                                    (keys))
            ; Node is considered empty if all reads are empty -> probably a mistake in registration.
            empty-nodes (->> instances-by-node
                             (filter (fn [[_ reads]]
                                       (every? (fn [single-read] (empty? single-read)) reads)))
                             (keys))
            coordinators (set (keys instances-by-node)) ; Only coordinators should run SHOW INSTANCES
            empty-nodes-cond (empty? empty-nodes)
            invalid-full-reads-cond (empty? invalid-full-reads)
            coordinators-cond (= coordinators #{"n4" "n5" "n6"})]

        {:valid? (and empty-nodes-cond coordinators-cond invalid-full-reads-cond)
         :empty-nodes empty-nodes
         :invalid-reads (keys invalid-full-reads)
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
