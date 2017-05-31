(ns ch2.core-test
  (:require [clojure.test :refer :all]
            [ch2.core :refer :all]
            [org.apache.storm.testing :as st]))

(deftest test-topology
  (st/with-simulated-time-local-cluster [cluster]
    (let [result (st/complete-topology cluster
                                       (github-commit-count-topology)
                                       :mock-sources
                                       {"1" [["aaaa foo@bar.com"]
                                             ["bbbb bar@bar.com"]
                                             ["cccc foo@bar.com"]]})]
      (testing "Email Extractor"
        (is (st/ms= [["foo@bar.com"] ["foo@bar.com"] ["bar@bar.com"]]
                    (st/read-tuples result "2")))))))
