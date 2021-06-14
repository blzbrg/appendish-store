(ns appendish-store.unsorted)

(def empty-storage [])
(def default-full-thresh 200)

;; === Getters ===

(defn items
  "Return seqable of items in unsorted, in any order"
  [unsorted]
  (::unsorted unsorted))

(defn next-ref
  [unsorted]
  (::next-ref unsorted))

;; === Modification ===

(defn append
  [unsorted item]
  (update unsorted ::unsorted conj item)) ; assumption: vector

(defn drain
  "Return unsorted with all items removed. This is used to preserve config etc. while emptying the
  collection"
  [unsorted]
  (assoc unsorted ::unsorted empty-storage))

;; === Internal predicates ===

(defn over-thresshold?
  [{unsorted ::unsorted threshhold ::full-threshhold} next-item]
  (> (inc (count unsorted)) threshhold))

(defn would-be-full?
  "Check if the unsorted would be full after adding `next-item`. If ::full-pred is present use it,
  otherwise use over-thresshold? as pred."
  [{pred ::full-pred :as unsorted} next-item]
  ((or pred over-thresshold?) unsorted next-item))

;; === Construction ===

(defn init
  "Create a data structure representing the unsorted - including an empty collection for the data
  and configuration/constants."
  [{full-pred :unsorted-full-pred full-thresh :unsorted-full-threshhold next-ref :next-ref}]
  (let [base {::next-ref next-ref ::unsorted empty-storage}]
    (if full-pred
      (assoc base ::full-pred full-pred)
      (assoc base ::full-threshhold (or full-thresh default-full-thresh)))))
