(ns ch2.core
  (:import [org.apache.storm StormSubmitter LocalCluster])
  (:require [clojure.string :refer [split]]
            [clojure.java.io :refer [resource]]
            [org.apache.storm
             [clojure :refer :all]
             [config :refer :all]])
  (:gen-class))

;; SPOUTS

(defspout commit-feed-listener ["commit"]
  [conf context collector]
  (let [commits (-> (resource "changelog.txt")
                    slurp
                    (split #"\n"))]
    (spout
     (nextTuple []
                (doseq [commit commits]
                  (emit-spout! collector [commit]))))))

;; BOLTS

(defbolt email-extractor ["email"]
  [{:keys [commit] :as tuple} collector]
  (let [[_ email] (split commit #" ")]
    (emit-bolt! collector [email] :anchor tuple)
    (ack! collector tuple)))

(defbolt email-counter []
  {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [{:keys [email] :as tuple}]
              (swap! counts #(update % email (fnil inc 0)))
              (doseq [[email c] @counts]
                (println email "has count of" c))
              (ack! collector tuple)))))

;; TOPOLOGY

(defn github-commit-count-topology []
  (topology
   {"1" (spout-spec commit-feed-listener)}

   {"2" (bolt-spec {"1" :shuffle} email-extractor)
    "3" (bolt-spec {"2" ["email"]} email-counter)}))

;; RUN

(defn run-local! []
  (let [ten-minutes (* 1000 60 10)
        cluster (LocalCluster.)]
    (.submitTopology cluster "github-commit-count-topology" {TOPOLOGY-DEBUG true} (github-commit-count-topology))
    (Thread/sleep ten-minutes)
    (.killTopology cluster "github-commit-count-topology")
    (.shutdown cluster)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   {TOPOLOGY-DEBUG true
    TOPOLOGY-WORKERS 3}
   (github-commit-count-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))
