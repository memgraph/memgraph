(ns jepsen.memgraph.basic
  "Basic Memgraph test"
  (:require [neo4j-clj.core :as dbclient]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.memgraph.client :as c]))

;; Operations specific to the tested system (Bolt and Cypher).

(dbclient/defquery create-node
  "CREATE (n:Node {id: $id, value: $value});")
(dbclient/defquery get-node
  "MATCH (n:Node {id: $id}) RETURN n;")
(dbclient/defquery get-all-nodes
  "MATCH (n:Node) RETURN n;")
(dbclient/defquery update-node
  "MATCH (n:Node {id: $id}) SET n.value = $value;")

(defn compare-and-set-node
  [conn o n]
  (dbclient/with-transaction conn tx
    (if (= (-> (get-node tx {:id "0"}) first :n :value) (str o))
      (update-node tx {:id "0" :value n})
      (throw+ "Unable to alter something that does NOT exist."))))
(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

;; Client specific to the tested system (Bolt and Cypher).
;; TODO (gitbuda): Write an impl targeting Memgraph replicated cluster.
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))
  (setup! [this test]
    (c/with-session conn session
      (create-node session {:id "0" :value 0})))
  (invoke! [this test op]
    (case (:f op)
      :read (assoc op :type :ok
                   :value (c/with-session conn session
                            (-> (get-node session {:id "0"}) first :n :value)))
      :write (do (c/with-session conn session
                   (update-node session {:id "0" :value (:value op)}))
                 (assoc op :type :ok))
      :cas (try+
            (let [[o n] (:value op)]
              (assoc op :type (if (compare-and-set-node conn o n) :ok :fail)))
            (catch Object _
              (assoc op :type :fail, :error :not-found)))))
  (teardown! [this test]
    (c/with-session conn session
      (try
        (c/detach-delete-all session)
        (catch Exception e
          ; Deletion can give exception if a sync replica is down, that's expected
          (assoc :type :fail :info (str e))))
      (detach-delete-all session)))
  (close! [_ est]
    (dbclient/disconnect conn)))

;; Abstract operations executed against the tested system.
(defn r [_ _]   {:type :invoke, :f :read,  :value nil})
(defn w [_ _]   {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Basic test workload"
  [opts]
  {:client (Client. nil)
   :checker (checker/compose
              {:linear (checker/linearizable
                {:model (model/cas-register 0)
                 :algorithm :linear})
               :timeline (timeline/html)})
   :generator (gen/mix [r w cas])
   :final-generator (gen/once r)})
