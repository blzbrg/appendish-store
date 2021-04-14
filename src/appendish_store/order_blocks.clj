(ns appendish-store.order-blocks)

(def item->key ::key)

(defn sorted->block
  [sorted]
  {::min-key (item->key (first sorted))
   ::max-key (item->key (last sorted))
   ::sorted (vec sorted)})

(defn unsorted->block
  [unsorted]
  (sorted->block (sort-by item->key unsorted)))

(defn within-block
  [block key]
  (and (<= key (::max-key block)) (>= key (::min-key block))))

(defn overlapping?
  [block-1 block-2]
  (or (within-block block-1 (::min-key block-2))
      (within-block block-1 (::max-key block-2))))

(defn append
  [first second]
  ;; Assumption: depends on vector
  (apply conj first second))

(defn append-block
  [old-block new-block]
  {::min-key (::min-key old-block)
   ::max-key (::max-key new-block)
   ::sorted (append (::sorted old-block) (::sorted new-block))})

(defn ^:private item-less-than
  [key item]
  (< (item->key item) key))

(defn split
  [coll split-key]
  (let [pred (partial item-less-than split-key)]
    [(vec (take-while pred coll)) (vec (drop-while pred coll))]))

(defn select-next
  [sort-by coll-a coll-b]
  (let [cmp (compare (sort-by (first coll-a)) (sort-by (first coll-b)))]
    (cond
      (<= cmp 0) [(first coll-a) (next coll-a) coll-b]
      (> cmp 0) [(first coll-b) coll-a (next coll-b)])))

(defn merge-sorted-by
  [sort-by coll-a coll-b]
  (loop [new [] ; assumption: vector
         a coll-a
         b coll-b]
    (cond
      (empty? a) (append new b)
      (empty? b) (append new a)
      :else (let [[n new-a new-b] (select-next sort-by a b)]
              (recur (conj new n) new-a new-b))))) ; assumption: vector

(defn merge-blocks
  [low-block high-block]
  (if (not (overlapping? low-block high-block))
    (append-block low-block high-block)
    (let [; split low to get minimal part which overlaps with high
          [unchanged-low overlap-from-low] (split (::sorted low-block) (::min-key high-block))
          ;; merge the minimal overlap with the high block
          merged (merge-sorted-by item->key overlap-from-low (::sorted high-block))
          ;; new sorted is the merged followed by the unchanged
          new-sorted (append unchanged-low merged)]
      (sorted->block new-sorted))))
