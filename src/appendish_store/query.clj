(ns appendish-store.query
  (:require [appendish-store.indexed-blocks :as indexed-blocks]
            [appendish-store.order-blocks :as order-blocks]))

(defn between
  "Return a sequence of items from the indexed block store that are between low-bound and
  high-bound (inclusive both)."
  [indexed low-bound high-bound]
  (let [blocks (indexed-blocks/blocks-overlapping-range indexed low-bound high-bound)
        big-seq (apply concat blocks)]
    (->> (indexed-blocks/blocks-overlapping-range indexed low-bound high-bound)
         ;; get only the sorted items from each block
         (map ::order-blocks/sorted)
         ;; combine all of the block items
         (flatten)
         ;; drop everything below the low bound
         (drop-while #(< (compare (order-blocks/order-key %) low-bound) 0))
         ;; keep only what is at or below the upper bound
         (take-while #(<= (compare (order-blocks/order-key %) high-bound) 0)))))