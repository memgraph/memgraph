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
    (let [accounts (->> (get-all-accounts session) (map :n) (reduce conj []))]
      (not-empty accounts))))

(defn main-to-initialize?
  "Check if main needs to be initialized. Accepts the name of the node and its bolt connection."
  [bolt-conn node first-main]
  (try
    (and (= node first-main)
         (is-main? node bolt-conn)
         (not (accounts-exist? bolt-conn)))
    (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
      (do
        (info "Node" node "is down.")
        false))
    (catch Exception e
      (do
        (info "Node" node "failed because" (str e))
        false))))

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

(defn register-repl-instances
  "Register replication instances."
  [bolt-conn node nodes-config]
  (doseq [repl-config (filter #(contains? (val %) :replication-port)
                              nodes-config)]
    (try
      (utils/with-session bolt-conn session ; Use bolt connection for registering replication instances.
        ((haclient/register-replication-instance
          (first repl-config)
          (second repl-config)) session))
      (info "Registered replication instance:" (first repl-config))
      (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
        (info "Registering instance" (first repl-config) "failed because node" node "is down."))
      (catch Exception e
        (info "Registering instance" (first repl-config) "failed because" (str e))))))

(defn add-coordinator-instances
  "Register coordinator instances."
  [bolt-conn node first-leader nodes-config]
  (doseq [coord-config (->> nodes-config
                            (filter #(not= (key %) first-leader)) ; Don't register itself
                            (filter #(contains? (val %) :coordinator-id)))]
    (try
      (utils/with-session bolt-conn session ; Use bolt connection for registering coordinator instances.
        ((haclient/add-coordinator-instance
          (second coord-config)) session))
      (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
        (info "Adding coordinator" (first coord-config) "failed because node" node "is down."))
      (catch Exception e
        (info "Adding coordinator" (first coord-config) "failed because" (str e))))))

(defn set-instance-to-main
  "Set instance to main."
  [bolt-conn node first-main]
  (try
    (utils/with-session bolt-conn session ; Use bolt connection for setting instance to main.
      ((haclient/set-instance-to-main first-main) session))
    (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
      (info "Setting instance" first-main "to main failed because node" node "is down."))
    (catch Exception e
      (info "Setting instance" first-main "to main failed because" (str e)))))

(defn initialize-main
  "Delete existing accounts and create new ones."
  [bolt-conn node]
  (try
    (utils/with-session bolt-conn session
      (info "Deleting all accounts...")
      (mgclient/detach-delete-all session)
      (info "Creating accounts...")
      (dotimes [i account-num]
        (create-account session {:id i :balance starting-balance})
        (info "Created account:" i)))
    (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
      (info "Initializing main failed because node" node "is down."))
    (catch Exception e
      (info "Initializing main failed because" (str e)))))

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
        (info "Node" (:node this) "is down."))))

  (invoke! [this _test op]
    (let [bolt-conn (:bolt-conn this)
          node (:node this)]
      (case (:f op)
      ; Show instances should be run only on coordinator.
        :show-instances-read (if (coord-instance? node)
                               (try
                                 (utils/with-session bolt-conn session ; Use bolt connection for running show instances.
                                   (let [instances (->> (get-all-instances session) (reduce conj []))]
                                     (assoc op
                                            :type :ok
                                            :value {:instances instances :node node})))
                                 (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                                   ; Even whole assoc can be returned -> so we don't forget :ok
                                   (assoc op :type :info :value (str "Node " node " is down"))) ; TODO: (abstract this message into a function)
                                 (catch Exception e
                                   (assoc op :type :fail :value (str e))))
                               (assoc op :type :info :value "Not coord"))
      ; Reading balances should be done only on data instances -> use bolt connection.
        :read-balances (if (data-instance? node)
                         (try
                           (utils/with-session bolt-conn session
                             (let [accounts (->> (get-all-accounts session) (map :n) (reduce conj []))
                                   total (reduce + (map :balance accounts))]
                               (assoc op
                                      :type :ok
                                      :value {:accounts accounts
                                              :node node
                                              :total total
                                              :correct (= total (* account-num starting-balance))})))
                           (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
                             (assoc op :type :info :value (str "Node " node " is down")))
                           (catch Exception e
                             (assoc op :type :fail :value (str e))))
                         (assoc op :type :info :value "Not data instance"))

        ; Transfer money from one account to another. Only executed on main.
        ; If the transferring succeeds, return :ok, otherwise return :fail.
        ; Allow the exception due to down sync replica.
        :transfer
        (let [transfer-info (:value op)]
          (try
            (if (and (is-main? node bolt-conn) (accounts-exist? bolt-conn))
              (do
                (transfer-money
                 bolt-conn
                 (:from transfer-info)
                 (:to transfer-info)
                 (:amount transfer-info))
                (assoc op :type :ok))
              (assoc op :type :info :value "Transfer allowed only on initialized main."))
            (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
              (assoc op :type :info :value (str "One of the nodes [" (:from transfer-info) ", " (:to transfer-info) "] participating in transfer is down")))
            (catch Exception e
              (if (or
                   (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction.")
                   (string/includes? (str e) "Write query forbidden on the main! Coordinator needs to enable writing on main by sending RPC message."))
                (assoc op :type :info :value (str e)); Exception due to down sync replica is accepted/expected
                (assoc op :type :fail :value (str e))))))

        :initialize
        ; If nothing was done before, registration will be done on the 1st leader and all good.
        ; If leader didn't change but registration was done, all this will fail but not critically.
        ; If leader changes, registration will fail because of `not leader` message and again all good.
        (if (= node first-leader)
          (do
            (register-repl-instances bolt-conn node nodes-config)
            (add-coordinator-instances bolt-conn node first-leader nodes-config)
            (set-instance-to-main bolt-conn node first-main)
            (assoc op :type :ok)) ; NOTE: This doesn't mean all instances were successfully registered.
          (if (main-to-initialize? bolt-conn node first-main)
            (do
              (initialize-main bolt-conn node) ; NOTE: This doesn't mean that data is all set up 100%.
              (assoc op :type :ok))
            (assoc op :type :info :value "Not registering on the 1st leader"))))))

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
    (check [_checker _test history _opts]
      ; si prefix stands for show-instances
      (let [failed-initalizations (->> history
                                       (filter #(= :fail (:type %)))
                                       (filter #(= :initialize (:f %)))
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
                                (filter #(= (count (:accounts %)) 5))
                                (map (fn [value]
                                       (let [balances  (map :balance (:accounts value))
                                             expected-total (* account-num starting-balance)]
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
                                    (into [])))]

        {:valid? (and (empty? empty-si-nodes)
                      (= coordinators #{"n4" "n5" "n6"})
                      (empty? coords-missing-reads)
                      (empty? more-than-one-main)
                      (empty? bad-data-reads)
                      (empty? empty-data-nodes)
                      (empty? failed-initalizations)
                      (empty? failed-show-instances)
                      (empty? failed-read-balances))
         :empty-si-nodes empty-si-nodes ; nodes which have all reads empty
         :coords-missing-reads coords-missing-reads ; coordinators which have missing coordinators in their reads
         :more-than-one-main-nodes more-than-one-main ; nodes on which more-than-one-main was detected
         :coordinators coordinators
         :bad-data-reads bad-data-reads
         :failed-initalizations failed-initalizations
         :failed-show-instances failed-show-instances
         :failed-read-balances failed-read-balances
         :empty-data-nodes empty-data-nodes}))))

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
     :generator (haclient/ha-gen (gen/mix [show-instances-reads read-balances valid-transfer]))
     :final-generator {:clients (gen/once show-instances-reads) :recovery-time 20}}))
