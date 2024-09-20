(ns memgraph.support
  (:require
   [clojure.tools.logging :refer [info]]
   [jepsen [db :as db]
    [control :as c]
    [util :as util :refer [meh]]]
   [jepsen.control.util :as cu]
   [jepsen.os.debian :as debian]))

;; Memgraph database config and setup.
(def mgdir  "/opt/memgraph")
(def mgdata (str mgdir "/mg_data"))
(def mglog  (str mgdir "/memgraph.log"))
(def mgpid  (str mgdir "/memgraph.pid"))
(def sync-after-n-txn (atom 100000))

(defn start-node!
  [test _]
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "TRACE"
   :--also-log-to-stderr
   :--data-recovery-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--data-recovery-on-startup
   :--replication-restore-state-on-startup
   :--storage-wal-file-flush-every-n-tx @sync-after-n-txn
   :--telemetry-enabled false
   :--storage-properties-on-edges))

(defn start-coordinator-node!
  [test node node-config]
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "TRACE"
   :--experimental-enabled "high-availability"
   :--also-log-to-stderr
   :--data-recovery-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--replication-restore-state-on-startup
   :--storage-properties-on-edges
   :--telemetry-enabled false
   :--coordinator-id (get node-config :coordinator-id)
   :--coordinator-port (get node-config :coordinator-port)
   :--coordinator-hostname node
   :--management-port (get node-config :management-port)))

(defn start-data-node!
  [test node-config]
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "TRACE"
   :--experimental-enabled "high-availability"
   :--also-log-to-stderr
   :--data-recovery-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--replication-restore-state-on-startup
   :--data-recovery-on-startup
   :--storage-properties-on-edges
   :--telemetry-enabled false
   :--management-port (get node-config :management-port)))

(defn start-memgraph-node!
  "Start Memgraph node. Can start HA and normal node."
  [test node nodes-config]
  (let [node-config (get nodes-config node)]
    (info "Starting Memgraph node" node-config)
    (if (:coordinator-id node-config)
      (start-coordinator-node! test node node-config)
      (if (:management-port node-config)
        (start-data-node! test node-config)
        (start-node! test node)))))

(defn stop-node!
  [test node]
  (info "Stopping Memgraph node" node)
  (cu/stop-daemon! (:local-binary test) mgpid))

(defn db
  "Manage Memgraph DB on each node."
  [opts]
  (reify db/DB ; Construct a new object satisfying the Jepsen's DB protocol.
    (setup! [_ test node] ; Each DB must support setup! method.
      (let [local-binary (:local-binary opts)
            nodes-config (:nodes-config opts)
            flush-after-n-txn (:sync-after-n-txn opts)]
        (reset! sync-after-n-txn flush-after-n-txn)
        (c/su
         (c/exec :apt-get :update)
         (debian/install ['python3 'python3-dev]))
        (c/su (meh (c/exec :killall :memgraph)))
        (try (c/exec :command :-v local-binary)
             (catch Exception _
               (throw (Exception. (str local-binary " is not there.")))))
        (start-memgraph-node! test node nodes-config)
        (info "Memgraph instance started")
        (Thread/sleep 5000))) ;; TODO(gitbuda): The sleep after Jepsen starting Memgraph is for sure questionable.
    (teardown! [_ test node] ; Each DB must support teardown! method.
      (info node "Tearing down Memgraph")
      (stop-node! test _)
      (c/su
       (c/exec :rm :-rf mgdata)
       (c/exec :rm :-rf mglog)))
    db/LogFiles
    (log-files [_ _ _] [mglog])))
