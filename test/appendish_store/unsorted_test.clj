(ns appendish-store.unsorted-test
  (:require [clojure.test :as test]
            [appendish-store.unsorted :as unsorted]))

(defn uns-limit
  [lim]
  (unsorted/init {:unsorted-full-threshhold lim}))

(test/deftest constuct-and-fill-test
  (let [uns-0 (uns-limit 1)
        uns-1 (unsorted/append uns-0 :first)]
    ;; empty one should have no items
    (test/is (= #{} (set (unsorted/items uns-0))))
    (test/is (not (unsorted/would-be-full? uns-0 :first)))
    ;; add one item, adding a second would fill it
    (test/is (= #{:first} (set (unsorted/items uns-1))))
    (test/is (unsorted/would-be-full? uns-1 :second))))

(test/deftest fill-then-empty-test
  (let [uns-0 (uns-limit 1)
        uns-1 (unsorted/append uns-0 :first)
        uns-2 (unsorted/drain uns-1)]
    (test/is (empty? (unsorted/items uns-2)))
    (test/is (= (uns-0 uns-2)))))

