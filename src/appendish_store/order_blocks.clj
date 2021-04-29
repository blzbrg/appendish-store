(ns appendish-store.order-blocks)

(def item->key ::key)

(defn sorted->block
  [sorted]
  {::min-key (item->key (first sorted))
   ::max-key (item->key (last sorted))
   ::sorted sorted})

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

(defn append-block
  [old-block new-block]
  {::min-key (::min-key old-block)
   ::max-key (::max-key new-block)
   ::sorted (concat (::sorted old-block) (::sorted new-block))})

(defn ^:private item-less-than
  [key item]
  (< (item->key item) key))

(defn split
  [coll split-key]
  (let [pred (partial item-less-than split-key)]
    [(take-while pred coll) (drop-while pred coll)]))

(defn select-next
  [sort-by coll-a coll-b]
  (let [cmp (compare (sort-by (first coll-a)) (sort-by (first coll-b)))]
    (cond
      (<= cmp 0) [(first coll-a) (next coll-a) coll-b]
      (> cmp 0) [(first coll-b) coll-a (next coll-b)])))

(defn merge-sorted-by
  [sort-by coll-a coll-b]
  (loop [new [] ;; use vectors within this function for efficient append
         a coll-a
         b coll-b]
    (cond
      ;; concat takes us out of vector land, but this is ok since it is the base case
      (empty? a) (concat new b)
      (empty? b) (concat new a)
      :else (let [[n new-a new-b] (select-next sort-by a b)]
              ;; append to vector using conj (will not work for other types)
              (recur (conj new n) new-a new-b)))))

(defn merge-blocks
  [low-block high-block]
  (if (not (overlapping? low-block high-block))
    (append-block low-block high-block)
    (let [; split low to get minimal part which overlaps with high
          [unchanged-low overlap-from-low] (split (::sorted low-block) (::min-key high-block))
          ;; merge the minimal overlap with the high block
          merged (merge-sorted-by item->key overlap-from-low (::sorted high-block))
          ;; new sorted is the merged followed by the unchanged
          new-sorted (concat unchanged-low merged)]
      ;; avoid looking at sorted at all to compute new min and max. The new min and new max are
      ;; always one of the old min or old max (respectively).
      {::min-key (min (::min-key low-block) (::min-key high-block))
       ::max-key (max (::max-key low-block) (::max-key high-block))
       ::sorted new-sorted})))

