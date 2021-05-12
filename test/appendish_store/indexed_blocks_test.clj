(ns appendish-store.indexed-blocks-test
  (:require [appendish-store.test-lib :refer [keys->items]]
            [appendish-store.order-blocks :as order-blocks]
            [appendish-store.indexed-blocks :as indexed-blocks]
            [clojure.test :as test]))

(defn mock-indexed
  [blocks]
  (reduce #(assoc %1 (::order-blocks/min-key %2) %2) (sorted-map) blocks))

(test/deftest ingest-non-overlapping-test
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 3]))
        b2 (order-blocks/sorted->block (keys->items [4 5 6]))]
    ;; base case: new block is wholly larger
    (test/is (= (mock-indexed [b1 b2])
                (indexed-blocks/ingest-block (mock-indexed [b1]) b2)))
    ;; new block is smaller than everything else
    (test/is (= (mock-indexed [b1 b2])
                (indexed-blocks/ingest-block (mock-indexed [b2]) b1)))))

(test/deftest ingest-overlaps-fully
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 4]))
        b2 (order-blocks/sorted->block (keys->items [5 6]))
        bn (order-blocks/sorted->block (keys->items [3]))
        bm (order-blocks/sorted->block (keys->items [1 2 3 4]))]
    ;; new block overlaps fully with only block
    (test/is (= (mock-indexed [bm])
                (indexed-blocks/ingest-block (mock-indexed [b1]) bn)))
    ;; new block overlaps fully but we have to walk past one block to get there
    (test/is (= (mock-indexed [bm b2])
                (indexed-blocks/ingest-block (mock-indexed [b1 b2]) bn)))))


(test/deftest ingest-overlaps-two-blocks
  (test/is (= (mock-indexed [(order-blocks/sorted->block (keys->items [1 2 3 4]))
                             (order-blocks/sorted->block (keys->items [5 6 7 8]))])
              (indexed-blocks/ingest-block
               (mock-indexed [(order-blocks/sorted->block (keys->items [1 2 4]))
                              (order-blocks/sorted->block (keys->items [5 7 8]))])
               (order-blocks/sorted->block (keys->items [3 6]))))))

