(ns jepsen.memgraph.habank
  "TODO, fill"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as string]
            [jepsen
             [checker :as checker]
             [generator :as gen]
             [client :as client]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.haclient :as haclient]
            [jepsen.memgraph.client :as mgclient]
            [jepsen.memgraph.utils :as utils]
            [clojure.core :as c]))

(def registered-replication-instances? (atom false))
(def added-coordinator-instances? (atom false))
(def main-set? (atom false))

(defn data-instance?
  "Is node data instances?"
  [node]
  (some #(= % node) #{"n1" "n2" "n3"}))

(defn coord-instance?
  "Is node coordinator instances?"
  [node]
  (some #(= % node) #{"n4" "n5" "n6"}))

(defn random-coord
  "Get random leader."
  [nodes]
  (nth nodes (+ 3 (rand-int 3)))) ; Assumes that first 3 instances are data instances and last 3 are coordinators.

(defn random-data-instance
  "Get random data instance."
  [nodes]
  (nth nodes (rand-int 3)))

(defn accounts-exist?
  "Check if accounts are created."
  [conn]
  (utils/with-session conn session
    (let [accounts (->> (mgclient/get-all-accounts session) (map :n) (reduce conj []))]
      (not-empty accounts))))

(defn transfer-money
  "Transfer money from one account to another by some amount
  if the account you're transfering money from has enough
  money."
  [conn op from to amount]
  (dbclient/with-transaction conn tx
    (when (-> (mgclient/get-account tx {:id from}) first :n :balance (>= amount))
      (mgclient/update-balance tx {:id from :amount (- amount)})
      (mgclient/update-balance tx {:id to :amount amount})))
  (info "Transfered money from account" from "to account" to "with amount" amount)
  (assoc op :type :ok))

(defn register-replication-instances
  "Register replication instances."
  [session nodes-config]
  (doseq [repl-config (filter #(contains? (val %) :replication-port)
                              nodes-config)]
    (try
      ((haclient/register-replication-instance
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
      ((haclient/add-coordinator-instance
        (second coord-config)) session)
      (info "Added coordinator:" (first coord-config))
      (catch Exception e
        (if (string/includes? (str e) "id already exists")
          (info "Coordinator instance" (first coord-config) "already exists, continuing to add other coordinator instances.")
          (throw e))))))

(defn set-instance-to-main
  "Set instance to main."
  [session first-main]
  ((haclient/set-instance-to-main first-main) session)
  (info "Set instance" first-main "to main."))

(defn insert-data
  "Delete existing accounts and create new ones."
  [session op]
  (info "Deleting all accounts...")
  (mgclient/detach-delete-all session)
  (info "Creating accounts...")
  (dotimes [i utils/account-num]
    (mgclient/create-account session {:id i :balance utils/starting-balance})
    (info "Created account:" i))
  (assoc op :type :ok))

(defrecord Client [nodes-config first-leader first-main license organization]
  client/Client
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
        ((haclient/set-db-setting "enterprise.license" license) session)
        ((haclient/set-db-setting "organization.name" organization) session))
      (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
        (info (utils/node-is-down (:node this))))))

  (invoke! [this _test op]
    (let [bolt-conn (:bolt-conn this)
          node (:node this)]
      (case (:f op)
      ; Show instances should be run only on coordinator.
        :show-instances-read (if (coord-instance? node)
                               (try
                                 (utils/with-session bolt-conn session ; Use bolt connection for running show instances.
                                   (let [instances (->> (mgclient/get-all-instances session) (reduce conj []))]
                                     (assoc op
                                            :type :ok
                                            :value {:instances instances :node node})))
                                 (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                   (utils/process-service-unavilable-exc op node))
                                 (catch Exception e
                                   (assoc op :type :fail :value (str e))))
                               (assoc op :type :info :value "Not coord"))
      ; Reading balances should be done only on data instances -> use bolt connection.
        :read-balances (if (data-instance? node)
                         (try
                           (utils/with-session bolt-conn session
                             (let [accounts (->> (mgclient/get-all-accounts session) (map :n) (reduce conj []))
                                   total (reduce + (map :balance accounts))]
                               (assoc op
                                      :type :ok
                                      :value {:accounts accounts
                                              :node node
                                              :total total
                                              :correct (= total (* utils/account-num utils/starting-balance))})))
                           (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                             (utils/process-service-unavilable-exc op node))
                           (catch Exception e
                             (assoc op :type :fail :value (str e))))
                         (assoc op :type :info :value "Not data instance"))

        ; Transfer money from one account to another. Only executed on main.
        ; If the transferring succeeds, return :ok, otherwise return :fail.
        ; Allow the exception due to down sync replica.
        :transfer
        (if (data-instance? node)
          (let [transfer-info (:value op)]
            (try
              (if (accounts-exist? bolt-conn)
                (transfer-money
                 bolt-conn
                 op
                 (:from transfer-info)
                 (:to transfer-info)
                 (:amount transfer-info)) ; Returns op
                (assoc op :type :info :value "Transfer allowed only when accounts exist."))
              (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                (assoc op :type :info :value (str "One of the nodes [" (:from transfer-info) ", " (:to transfer-info) "] participating in transfer is down")))
              (catch Exception e
                (if (or
                     (utils/query-forbidden-on-replica? e)
                     (utils/query-forbidden-on-main? e)
                     (utils/sync-replica-down? e))
                  (assoc op :type :info :value (str e))
                  (assoc op :type :fail :value (str e))))))
          (assoc op :type :info :value "Not data instance"))

        :setup-cluster
        ; If nothing was done before, registration will be done on the 1st leader and all good.
        ; If leader didn't change but registration was done, we won't even try to register -> all good again.
        ; If leader changes, registration should already be done or not a leader will be printed.
        (if (= first-leader node)

          (utils/with-session bolt-conn session
            (try
              (when (not @registered-replication-instances?)
                (register-replication-instances session nodes-config)
                (reset! registered-replication-instances? true))

              (when (not @added-coordinator-instances?)
                (add-coordinator-instances session node nodes-config)
                (reset! added-coordinator-instances? true))

              (when (not @main-set?)
                (set-instance-to-main session first-main)
                (reset! main-set? true))

              (assoc op :type :ok) ; NOTE: This doesn't necessarily mean all instances were successfully registered.

              (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                (info "Registering instances failed because node" node "is down.")
                (utils/process-service-unavilable-exc op node))
              (catch Exception e
                (if (string/includes? (str e) "not a leader")
                  (assoc op :type :info :value "Not a leader")
                  (assoc op :type :fail :value (str e))))))

          (assoc op :type :info :value "Not coordinator"))

        :initialize-data
        (if (data-instance? node)

          (utils/with-session bolt-conn session
            (try
              (let [accounts (->> (mgclient/get-all-accounts session) (map :n) (reduce conj []))]
                (if (empty? accounts)
                  (insert-data session op) ; Return assoc op :type :ok
                  (assoc op :type :info :value "Accounts already exist.")))
              (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                (utils/process-service-unavilable-exc op node))
              (catch Exception e
                (if (or (utils/query-forbidden-on-replica? e)
                        (utils/query-forbidden-on-main? e))
                  (assoc op :type :info :value (str e))
                  (assoc op :type :fail :value (str e))))))

          (assoc op :type :info :value "Not data instance")))))

  (teardown! [_this _test])
  (close! [this _test]
    (dbclient/disconnect (:bolt-conn this))))

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

(defn habank-checker
  "High availability bank checker"
  []
  (reify checker/Checker
    (check [_checker _test history _opts]
      ; si prefix stands for show-instances
      (let [failed-setup-cluster (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :setup-cluster (:f %)))
                                      (map :value))
            failed-initialize-data (->> history
                                        (filter #(= :fail (:type %)))
                                        (filter #(= :initialize-data (:f %)))
                                        (map :value))
            failed-show-instances (->> history
                                       (filter #(= :fail (:type %)))
                                       (filter #(= :show-instances-read (:f %)))
                                       (map :value))
            failed-read-balances (->> history
                                      (filter #(= :fail (:type %)))
                                      (filter #(= :read-balances (:f %)))
                                      (map :value))
            si-reads  (->> history
                           (filter #(= :ok (:type %)))
                           (filter #(= :show-instances-read (:f %)))
                           (map :value))
            ; Full reads all reads which returned 6 instances
            full-si-reads (->> si-reads
                               (filter #(= 6 (count (:instances %)))))
            ; All reads grouped by node
            coord->reads (->> si-reads
                              (group-by :node)
                              (map (fn [[node values]] [node (map :instances values)]))
                              (into {}))
            ; All full reads grouped by node
            coord->full-reads (->> full-si-reads
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
            empty-si-nodes (->> coord->reads
                                (filter (fn [[_ reads]]
                                          (every? empty? reads)))
                                (keys))
            coordinators (set (keys coord->reads)) ; Only coordinators should run SHOW INSTANCES
            ok-data-reads  (->> history
                                (filter #(= :ok (:type %)))
                                (filter #(= :read-balances (:f %))))
            bad-data-reads (->> ok-data-reads
                                (map #(->> % :value))
                                (filter #(= (count (:accounts %)) utils/account-num))
                                (map (fn [value]
                                       (let [balances  (map :balance (:accounts value))
                                             expected-total (* utils/account-num utils/starting-balance)]
                                         (cond (and
                                                (not-empty balances)
                                                (not=
                                                 expected-total
                                                 (reduce + balances)))
                                               {:type :wrong-total
                                                :expected expected-total
                                                :found (reduce + balances)
                                                :value value}

                                               (some neg? balances)
                                               {:type :negative-value
                                                :found balances
                                                :op value}))))
                                (filter identity)
                                (into []))
            empty-data-nodes (let [all-nodes (->> ok-data-reads
                                                  (map #(-> % :value :node))
                                                  (reduce conj #{}))]
                               (->> all-nodes
                                    (filter (fn [node]
                                              (every?
                                               empty?
                                               (->> ok-data-reads
                                                    (map :value)
                                                    (filter #(= node (:node %)))
                                                    (map :accounts)))))
                                    (filter identity)
                                    (into [])))

            result {:valid? (and (empty? empty-si-nodes)
                                 (= coordinators #{"n4" "n5" "n6"})
                                 (empty? coords-missing-reads)
                                 (empty? more-than-one-main)
                                 (empty? bad-data-reads)
                                 (empty? empty-data-nodes)
                                 (seq? full-si-reads) ; not-empty idiom
                                 (empty? failed-setup-cluster)
                                 (empty? failed-initialize-data)
                                 (empty? failed-show-instances)
                                 (empty? failed-read-balances))
                    :empty-si-nodes? (empty? empty-si-nodes) ; nodes which have all reads empty
                    :empty-coords-missing-reads? (empty? coords-missing-reads) ; coordinators which have missing coordinators in their reads
                    :empty-more-than-one-main-nodes? (empty? more-than-one-main) ; nodes on which more-than-one-main was detected
                    :correct-coordinators? (= coordinators #{"n4" "n5" "n6"})
                    :empty-bad-data-reads? (empty? bad-data-reads)
                    :empty-failed-setup-cluster? (empty? failed-setup-cluster)
                    :empty-failed-initialize-data? (empty? failed-initialize-data)
                    :empty-failed-show-instances? (empty? failed-show-instances)
                    :empty-failed-read-balances? (empty? failed-read-balances)
                    :full-si-reads-exist? (seq? full-si-reads)
                    :empty-data-nodes? (empty? empty-data-nodes)}]

        (when (not (:correct-coordinators? result))
          (assoc result :coordinators coordinators))

        (when (not (:empty-si-nodes? result))
          (assoc result :empty-si-nodes empty-si-nodes))

        (when (not (:empty-data-nodes? result))
          (assoc result :empty-data-nodes empty-data-nodes))

        (when (not (:empty-failed-setup-cluster? result))
          (assoc result :failed-setup-cluster failed-setup-cluster))

        (when (not (:empty-failed-initialize-data? result))
          (assoc result :failed-initialize-data failed-initialize-data))

        (when (not (:empty-failed-show-instances? result))
          (assoc result :failed-show-instances failed-show-instances))

        (when (not (:empty-failed-read-balances? result))
          (assoc result :failed-read-balances failed-read-balances))

        result))))

(defn show-instances-reads
  "Create read action."
  [_ _]
  {:type :invoke, :f :show-instances-read, :value nil})

(defn setup-cluster
  "Setup cluster operation."
  [_ _]
  {:type :invoke :f :setup-cluster :value nil})

(defn initialize-data
  "Initialize data operation."
  [_ _]
  {:type :invoke :f :initialize-data :value nil})

(defn ha-gen
  "Generator which should be used for HA tests
  as it adds register replication instance invoke."
  [generator]
  (gen/each-thread (gen/phases (cycle [(gen/time-limit 5 generator)]))))

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
                 {:habank     (habank-checker)
                  :timeline (timeline/html)})
     :generator (ha-gen (gen/mix [setup-cluster initialize-data show-instances-reads utils/read-balances utils/valid-transfer]))
     :final-generator {:clients (gen/once show-instances-reads) :recovery-time 20}}))
