(defproject jepsen.memgraph "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Memgraph"
  :url "https://memgraph.com/"
  :license {:name "Memgraph Enterprise"
            :url "https://github.com/memgraph/memgraph/blob/master/release/LICENSE_ENTERPRISE.md"}
  :main jepsen.memgraph.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.4-SNAPSHOT"]
                 [gorillalabs/neo4j-clj "4.1.0"]]
  :profiles {:test {:dependencies [#_[org.neo4j.test/neo4j-harness "4.1.0"]]}}
  :repl-options {:init-ns jepsen.memgraph.core})
