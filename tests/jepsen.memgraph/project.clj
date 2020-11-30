(defproject jepsen.memgraph "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Memgraph"
  :url "https://memgraph.com/"
  :license {:name "Memgraph Enterprise"
            :url "https://github.com/memgraph/memgraph/blob/master/release/LICENSE_ENTERPRISE.md"}
  :main jepsen.memgraph
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.1-SNAPSHOT"]
                 [verschlimmbesserung "0.1.3"]]
  :repl-options {:init-ns jepsen.memgraph})
