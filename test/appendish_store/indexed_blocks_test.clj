(ns appendish-store.indexed-blocks-test
  (:require [appendish-store.test-lib :refer [keys->items]]
            [appendish-store.order-blocks :as order-blocks]
            [appendish-store.indexed-blocks :as indexed-blocks]
            [clojure.test :as test]))

(test/deftest ingest-non-overlapping-test
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 3]))
        b2 (order-blocks/sorted->block (keys->items [4 5 6]))]
    ;; base case: new block is wholly larger
    (test/is (= (indexed-blocks/create [b1 b2])
                (indexed-blocks/ingest-block (indexed-blocks/create [b1]) b2)))
    ;; new block is smaller than everything else
    (test/is (= (indexed-blocks/create [b1 b2])
                (indexed-blocks/ingest-block (indexed-blocks/create [b2]) b1)))))

(test/deftest ingest-overlaps-fully
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 4]))
        b2 (order-blocks/sorted->block (keys->items [5 6]))
        bn (order-blocks/sorted->block (keys->items [3]))
        bm (order-blocks/sorted->block (keys->items [1 2 3 4]))]
    ;; new block overlaps fully with only block
    (test/is (= (indexed-blocks/create [bm])
                (indexed-blocks/ingest-block (indexed-blocks/create [b1]) bn)))
    ;; new block overlaps fully but we have to walk past one block to get there
    (test/is (= (indexed-blocks/create [bm b2])
                (indexed-blocks/ingest-block (indexed-blocks/create [b1 b2]) bn)))))


(test/deftest ingest-overlaps-two-blocks
  (test/is (= (indexed-blocks/create [(order-blocks/sorted->block (keys->items [1 2 3 4]))
                                      (order-blocks/sorted->block (keys->items [5 6 7 8]))])
              (indexed-blocks/ingest-block
               (indexed-blocks/create [(order-blocks/sorted->block (keys->items [1 2 4]))
                                       (order-blocks/sorted->block (keys->items [5 7 8]))])
               (order-blocks/sorted->block (keys->items [3 6]))))))

(test/deftest block-overlapping-range-test
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 3]))
        b2 (order-blocks/sorted->block (keys->items [4 5 6]))
        b3 (order-blocks/sorted->block (keys->items [7 8 9]))
        indexed (indexed-blocks/create [b1 b2 b3])]
    (test/is (= (list b2 b3)
                (indexed-blocks/blocks-overlapping-range indexed 5 8))
             "range overlapping two blocks")
    (test/is (= (list b3)
                (indexed-blocks/blocks-overlapping-range indexed 7 8))
             "range overlapping only highest block")
    (test/is (= (list b2)
                (indexed-blocks/blocks-overlapping-range indexed 4 5))
             "range overlapping only one interior block")
    (test/is (= (list)
                (indexed-blocks/blocks-overlapping-range indexed 10 15))
             "range higher than all blocks")
    (test/is (= (list b2)
                (indexed-blocks/blocks-overlapping-range indexed 5 5))
             "size one range contained in a block")
    (test/is (= (list b2)
                (indexed-blocks/blocks-overlapping-range indexed 6 6))
             "size one range at top edge of a block")
    (test/is (= (list b2)
                (indexed-blocks/blocks-overlapping-range indexed 4 4))
             "size one range at bottom edge of a block")))
