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
            [jepsen.memgraph.utils :as utils]))

(def account-num
  "Number of accounts to be created"
  5)

(def starting-balance
  "Starting balance of each account"
  400)

(def max-transfer-amount
  20)

; Implicit 1st parameter you need to send is txn. 2nd is id. 3rd balance
(dbclient/defquery create-account
  "CREATE (n:Account {id: $id, balance: $balance});")

; Implicit 1st parameter you need to send is txn.
(dbclient/defquery get-all-accounts
  "MATCH (n:Account) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id.
(dbclient/defquery get-account
  "MATCH (n:Account {id: $id}) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3d is amount.
(dbclient/defquery update-balance
  "MATCH (n:Account {id: $id})
   SET n.balance = n.balance + $amount
   RETURN n")

(dbclient/defquery get-all-instances
  "SHOW INSTANCES;")

(dbclient/defquery show-repl-role
  "SHOW REPLICATION ROLE")

(defn data-instance-is-main?
  "Find current main. This function shouldn't be executed on coordinator instances because you can easily check if the instance is coordinator."
  [bolt-conn]
  (utils/with-session bolt-conn session
    (let [result (show-repl-role session)
          role (val (first (seq (first result))))
          main? (= "main" (str role))]
      main?)))

(defn data-instance?
  "Is node data instances?"
  [node]
  (some #(= % node) #{"n1" "n2" "n3"}))

(defn is-main?
  "Is node main?"
  [node bolt-conn]
  (and (data-instance? node) (data-instance-is-main? bolt-conn)))

(defn coord-instance?
  "Is node coordinator instances?"
  [node]
  (some #(= % node) #{"n4" "n5" "n6"}))

(defn accounts-not-created?
  "Check if accounts are not created."
  [conn]
  (utils/with-session conn session
    (let [accounts (->> (get-all-accounts session) (map :n) (reduce conj []))]
      (empty? accounts))))

(defn transfer-money
  "Transfer money from one account to another by some amount
  if the account you're transfering money from has enough
  money."
  [conn from to amount]
  (dbclient/with-transaction conn tx
    (when (-> (get-account tx {:id from}) first :n :balance (>= amount))
      (update-balance tx {:id from :amount (- amount)})
      (update-balance tx {:id to :amount amount})))
  (info "Transfered money from account" from "to account" to "with amount" amount))

(defrecord Client [nodes-config license organization]
  client/Client
  ; Open Bolt connection to all nodes and Bolt+routing to coordinators.
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
    (utils/with-session (:bolt-conn this) session
      ((haclient/set-db-setting "enterprise.license" license) session)
      ((haclient/set-db-setting "organization.name" organization) session)))

  (invoke! [this _test op]
    (case (:f op)
      ; Show instances should be run only on coordinator.
      :show-instances-read (if (coord-instance? (:node this))
                             (try
                               (utils/with-session (:bolt-conn this) session ; Use bolt connection for running show instances.
                                 (let [instances (->> (get-all-instances session) (reduce conj []))]
                                   (assoc op
                                          :type :ok
                                          :value {:instances instances :node (:node this)})))
                               (catch Exception e
                                 (assoc op :type :fail :value e)))
                             (assoc op :type :fail :value "Not coord"))
      ; Reading balances should be done only on data instances -> use bolt connection.
      :read-balances (if (data-instance? (:node this))
                       (utils/with-session (:bolt-conn this) session
                         (let [accounts (->> (get-all-accounts session) (map :n) (reduce conj []))
                               total (reduce + (map :balance accounts))]
                           (assoc op
                                  :type :ok
                                  :value {:accounts accounts
                                          :node (:node this)
                                          :total total
                                          :correct (= total (* account-num starting-balance))})))
                       (assoc op :type :fail :value "Not data instance"))

        ; Transfer money from one account to another. Only executed on main.
        ; If the transferring succeeds, return :ok, otherwise return :fail.
        ; Transfer will fail if the account doesn't exist or if the account doesn't have enough or if update-balance
        ; doesn't return anything.
        ; Allow the exception due to down sync replica.
      :transfer (if (is-main? (:node this) (:bolt-conn this))
                  (try
                    (let [transfer-info (:value op)]
                      (transfer-money
                       (:bolt-conn this)
                       (:from transfer-info)
                       (:to transfer-info)
                       (:amount transfer-info)))
                    (assoc op :type :ok)
                    (catch Exception e
                      (if (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction.")
                        (assoc op :type :ok :value (str e)); Exception due to down sync replica is accepted/expected
                        (assoc op :type :fail :value (str e)))))
                  (assoc op :type :fail :value "Not main node."))

      :register (if (= (:node this) "n4") ; Node with coordinator-id = 1
                  (do
                    (doseq [repl-config (filter #(contains? (val %) :replication-port)
                                                nodes-config)]
                      (try
                        (utils/with-session (:bolt-conn this) session ; Use bolt connection for registering replication instances.
                          ((haclient/register-replication-instance
                            (first repl-config)
                            (second repl-config)) session))
                        (catch Exception e
                          (assoc op :type :fail :value e))))
                    (doseq [coord-config (->> nodes-config
                                              (filter #(not= (key %) "n4")) ; Don't register itself
                                              (filter #(contains? (val %) :coordinator-id)))]
                      (try
                        (utils/with-session (:bolt-conn this) session ; Use bolt connection for registering coordinator instances.
                          ((haclient/add-coordinator-instance
                            (second coord-config)) session))
                        (catch Exception e
                          (assoc op :type :fail :value e))))
                    (let [rand-main (nth (keys nodes-config) (rand-int 3))] ; 3 because first 3 instances are replication instances in cluster.edn
                      (try
                        (utils/with-session (:bolt-conn this) session ; Use bolt connection for setting instance to main.
                          ((haclient/set-instance-to-main rand-main) session))
                        (catch Exception e
                          (assoc op :type :fail :value (str e)))))

                    (assoc op :type :ok))

                  (when (and (data-instance? (:node this)) (data-instance-is-main? (:bolt-conn this)) (accounts-not-created? (:bolt-conn this)))
                    (utils/with-session (:bolt-conn this) session
                      (info "Detaching and deleting all accounts...")
                      (mgclient/detach-delete-all session)
                      (info "Creating accounts...")
                      (dotimes [i account-num]
                        (create-account session {:id i :balance starting-balance})
                        (info "Created account:" i)))))))

  (teardown! [_this _test])
  (close! [this _test]
    (dbclient/disconnect (:bolt-conn this))))

(defn show-instances-reads
  "Create read action."
  [_ _]
  {:type :invoke, :f :show-instances-read, :value nil})

(defn read-balances
  "Read the current state of all accounts"
  [_ _]
  {:type :invoke, :f :read-balances, :value nil})

(defn transfer
  "Transfer money from one account to another by some amount"
  [_ _]
  {:type :invoke
   :f :transfer
   :value {:from   (rand-int account-num)
           :to     (rand-int account-num)
           :amount (+ 1 (rand-int max-transfer-amount))}})

(def valid-transfer
  "Filter only valid transfers (where :from and :to are different)"
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

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
    (check [_ _ history _]
      (let [reads  (->> history
                        (filter #(= :ok (:type %)))
                        (filter #(= :show-instances-read (:f %)))
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
               {:habank     (habank-checker)
                :timeline (timeline/html)})
   :generator (haclient/ha-gen (gen/mix [show-instances-reads read-balances valid-transfer]))
   :final-generator {:clients (gen/once show-instances-reads) :recovery-time 20}})
