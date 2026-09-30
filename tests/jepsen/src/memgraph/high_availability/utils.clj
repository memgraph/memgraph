(ns memgraph.high-availability.utils
  (:require
  [clojure.string :as str]
))

(defn data-instance?
  "Is node data instances?"
  [node]
  (some #(= % node) #{"n1" "n2" "n3"}))

(defn coord-instance?
  "Is node coordinator instances?"
  [node]
  (some #(= % node) #{"n4" "n5" "n6"}))

(defn leader?
  [instance]
  (= (:role instance) "leader"))

(defn extract-coord-name
  "We could use any server. We cannot just use name because in Memgraph coordinator's name is written as 'coordinator_{id}'."
  [bolt-server]
  (first (str/split bolt-server #":")))

(defn get-current-leader
  "Returns the name of the current leader. If there is no leader or more than one leader, throws exception."
  [instances]
  (let [leaders (filter leader? instances)
        num-leaders (count leaders)
        leader
        (cond (= num-leaders 1) (first leaders)
              (= num-leaders 0) nil
              :else (throw (Exception. "Expected at most one leader.")))
        leader-name (if (nil? leader) nil (extract-coord-name (:bolt_server leader)))]
    leader-name))
