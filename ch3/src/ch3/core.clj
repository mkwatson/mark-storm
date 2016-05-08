(ns ch3.core
  (:import [org.apache.storm StormSubmitter LocalCluster Constants]
           [com.google.code.geocoder Geocoder GeocoderRequestBuilder]
           [com.google.code.geocoder.model GeocoderStatus])
  (:require [clojure.string :refer [split]]
            [clojure.java.io :refer [resource]]
            [org.apache.storm
             [clojure :refer :all]
             [config :refer :all]]
            [taoensso.carmine :as car :refer (wcar)])
  (:gen-class))

;; SPOUTS

(defspout checkins ["time" "address"]
  [conf context collector]
  (let [checkins (-> (resource "checkins.txt")
                     slurp
                     (split #"\n"))
        next-emit-index (atom 0)
        number-of-checkins (count checkins)]
    (spout
     (nextTuple []
                (let [checkin (get checkins @next-emit-index)
                      [str-time address] (split checkin #"\, ")
                      time (BigInteger. str-time)]
                  (emit-spout! collector [time address])
                  (swap! next-emit-index (comp #(mod % number-of-checkins) inc)))))))

;; BOLTS

#_(defbolt geocode-lookup ["time" "geocode" "city"] {:prepare true}
  [conf context collector]
  (let [geocoder (Geocoder.)]
    (bolt
     (execute [{:keys [time address]}]
              (let [request (.. (GeocoderRequestBuilder.)
                                (setAddress address)
                                (setLanguage "en")
                                (getGeocoderRequest))
                    response (.geocode geocoder request)
                    status (.getStatus response)]
                (when (.. GeocoderStatus
                          OK
                          (equals status))
                  (let [first-result (first (.getResults response))
                        lat-lng (.. first-result getGeometry getLocation)
                        city (.extractCity first-result)]
                    (emit-bolt! collector [time lat-lng city]))))))))

;; Mock google api calls
(defbolt geocode-lookup ["time" "geocode" "city"] {:prepare true}
  [conf context collector]
  (let [geocoder {"287 Hudson St New York NY 10013" {:lat-lng [40.725612 -74.007916]
                                                     :city "New York"}}]
    (bolt
     (execute [{:keys [time address]}]
              (let [{:keys [lat-lng city]} (geocoder address)]
                (emit-bolt! collector [time lat-lng city]))))))

(def ^:const WINDOW_SIZE_SECONDS 15)

(def ^:private select-time-interval (comp biginteger #(/ % (* WINDOW_SIZE_SECONDS 1000))))

(defbolt time-interval-extractor ["time-interval" "geocode" "city"]
  [{:keys [time geocode city]} collector]
  (let [time-interval (select-time-interval time)]
    (emit-bolt! collector [time-interval geocode city])))

(defn- add-geocode!
  [heatmaps time-interval geocode]
  (swap! heatmaps update time-interval (fnil #(conj % geocode) [])))

(defn- is-tick-tuple
  [tuple]
  (let [source-component (.getSourceComponent tuple)
        source-stream-id (.getSourceStreamId tuple)]
    (and (= source-component (Constants/SYSTEM_COMPONENT_ID))
         (= source-stream-id (Constants/SYSTEM_TICK_STREAM_ID)))))

(defn- emit-heatmap
  [collector heatmaps]
  (let [now (System/currentTimeMillis)
        emit-up-to-time-interval (select-time-interval now)
        time-intervals-available (keys @heatmaps)]
    (doseq [time-interval time-intervals-available]
      (when (<= time-interval emit-up-to-time-interval)
        (emit-bolt! collector (find @heatmaps time-interval))
        (swap! heatmaps dissoc time-interval)))))

(defbolt heatmap-builder ["time-interval" "hotzones"]
  {:prepare true
   :conf {"topology.tick.tuple.freq.secs" WINDOW_SIZE_SECONDS}}
  [conf context collector]
  (let [heatmaps (atom {})]
    (bolt
     (execute [{:keys [time-interval geocode] :as tuple}]
              (if (is-tick-tuple tuple)
                (emit-heatmap collector heatmaps)
                (add-geocode! heatmaps time-interval geocode))))))

(defbolt persistor []
  [{:keys [time-interval hotzones]} collector]
  (try
    (let [k (str "checkins-" time-interval)]
      (wcar {} (car/set k hotzones)))
    (catch Exception e (str "Error persisting for time: " + time-interval))))

;; TOPOLOGY

(defn heatmap-topology []
  (topology
   {"1" (spout-spec checkins :p 4)}

   {"2" (bolt-spec {"1" :shuffle} geocode-lookup :p 8 :conf {"topology.tasks" 64})
    "3" (bolt-spec {"2" :shuffle} time-interval-extractor :p 4)
    "4" (bolt-spec {"3" ["time-interval" "city"]} heatmap-builder :p 4)
    "5" (bolt-spec {"4" :shuffle} persistor :conf {"topology.tasks" 4})}))

;; RUN

(defn run-local! []
  (let [three-minutes (* 1000 60 3)
        cluster (LocalCluster.)]
    (.submitTopology cluster "heatmap-topology" {TOPOLOGY-DEBUG true} (heatmap-topology))
    (Thread/sleep three-minutes)
    (.killTopology cluster "heatmap-topology")
    (.shutdown cluster)))

(defn -main
  []
  (run-local!))
