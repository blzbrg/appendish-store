(ns appendish-store.indexed-blocks
  (:require [appendish-store.order-blocks :as order-blocks]))

(defn create
  "Create a new indexed store. Can be optionally given a collection of non-overlapping blocks to
  populate it with. Blocks that overlap must be added with ingest instead."
  ([] (sorted-map))
  ([blocks] (reduce #(assoc %1 (::order-blocks/min-key %2) %2)
                    (create)
                    blocks)))

(defn wholly-larger-than
  "Return if larger is wholly larger than smaller (and any overlap is only equal elements)."
  [smaller larger]
  (>= (compare (::order-blocks/min-key larger) (::order-blocks/max-key smaller))
      0))

(defn new-kvs-from-ingest
  "Given seq of [key block] pairs in order from high to low and a new block, return sequence of [key
  new-block] pairs to assoc into an ordered map to include the new block"
  [keys-blocks new-block]
  ;; This function deliberately does not compare against key, opting instead of explicitly fetch the
  ;; min and max from blocks. However, do not be fooled, this does not mean it does not depend on
  ;; knowledge of whether the key is the min or max. The "merge as much as we can" case implicitly
  ;; depends on this knowledge to avoid changing keys.
  (loop [acc (list)
         keys-blocks keys-blocks
         new-block new-block]
    (if (or (empty? keys-blocks) ; order matters, second case is safe if this is false
            (wholly-larger-than (second (first keys-blocks)) new-block))
      ;; new-block is wholly larger than the next one or smaller than all blocks, just insert it
      ;; and we are done.
      (conj acc [(::order-blocks/min-key new-block) new-block])
      ;; new block overlaps with current block, or is wholly smaller than it
      (let [[[key block] & kb-rest] keys-blocks]
        (cond
          ;; new block is fully contained in current block: just merge it and we are done
          (order-blocks/fully-contained? new-block block)
          (conj acc [key (order-blocks/merge-blocks block new-block)])

          ;; new-block overlaps current block, merge as much as we can without changing minimum of
          ;; current block
          (order-blocks/overlapping? block new-block)
          (let [[bottom top] (order-blocks/splitv (::order-blocks/sorted new-block)
                                                  (::order-blocks/min-key block)) ; up to and including
                top-block (order-blocks/sorted->block top)
                bottom-block (order-blocks/sorted->block bottom)
                new-block (order-blocks/merge-blocks top-block block)]
            (recur (conj acc [key new-block]) kb-rest bottom-block))

          ;; new block is less than block, keep walking downwards
          :else (recur acc kb-rest new-block))))))

(defn ingest-block
  [indexed new-block]
  (into indexed (new-kvs-from-ingest (rseq indexed) new-block)))

(defn blocks-overlapping-range
  [indexed low-bound high-bound]
  (->> (map second (rseq indexed)) ; get just the blocks
       ;; drop blocks whose bottom is above the high bound.
       (drop-while #(> (compare (::order-blocks/min-key %) high-bound) 0))
       ;; keep blocks whose high is at or above the low bound
       (take-while #(>= (compare (::order-blocks/max-key %) low-bound) 0))
       (reverse)))
