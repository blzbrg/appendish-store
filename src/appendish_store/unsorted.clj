(ns appendish-store.unsorted)

(def empty-storage [])

(defn append-to-unsorted
  [unsorted item]
  (conj unsorted item)) ; assumption: vector

(defn over-thresshold?
  [{unsorted ::unsorted threshhold ::full-threshhold} next-item]
  (> (inc (count unsorted)) threshhold))

(defn would-be-full?
  "Check if the unsorted would be full after adding `next-item`. If ::full-pred is present use it,
  otherwise use over-thresshold? as pred."
  [{pred ::full-pred :as unsorted} next-item]
  ((or pred over-thresshold?) unsorted next-item))

(defn init
  "Create a data structure representing the unsorted - including an empty collection for the data
  and configuration/constants."
  [{full-pred :unsorted-full-pred full-thresh :unsorted-full-threshhold blocks-ref :blocks-store-ref}]
  (cond-> {::full-threshhold (or full-thresh 200)
                                   ::blocks-ref blocks-ref
                                   ::unsorted empty-storage}
    full-pred (assoc ::full-pred full-pred)))