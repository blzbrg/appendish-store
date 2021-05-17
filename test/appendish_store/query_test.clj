(ns appendish-store.query-test
  (:require [appendish-store.query :as query]
            [appendish-store.order-blocks :as order-blocks]
            [appendish-store.indexed-blocks :as indexed-blocks]
            [appendish-store.test-lib :refer [keys->items]]
            [clojure.test :as test]))

(test/deftest between-test
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 3]))
        b2 (order-blocks/sorted->block (keys->items [4 5 6]))
        b3 (order-blocks/sorted->block (keys->items [7 8 10]))
        indexed (indexed-blocks/create [b1 b2 b3])]
    (test/is (= (seq (keys->items [2 3 4 5]))
                (query/between indexed 2 5))
             "Bland query")
    (test/is (= (seq (keys->items [4]))
                (query/between indexed 4 4))
             "Only one matches")
    (test/is (= (list)
                (query/between indexed 9 9))
             "Block matches but no items in it match")))
