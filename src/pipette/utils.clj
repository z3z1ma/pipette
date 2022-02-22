(ns pipette.utils
  (:require [clojure.string])
  (:import (java.net URLEncoder)))

;; Use more clojure-esque error raising
(defmacro validate?
  "Checks for a condition raising an exception if false
   otherwise returning true"
  [condition failure-message]
  `(when-not ~condition (try (throw (Exception. ~failure-message)) (finally (println ~failure-message) (System/exit 1)))))

;; For now, we URL encode by default since env is typically utilized in JDBC resolution
(defn interpolate-env-vars
  "Given an input string, returns a new string with all env vars found
   in input in ${} format replaced with the value of the variable"
  [input_string]
  (let [env-map (apply conj (for [env_var (System/getenv)]
                              {(format "${%s}" (.getKey env_var))
                               (some-> (.getValue env_var)
                                       (URLEncoder/encode "UTF-8")
                                       (.replace "+" "%20"))}))]
    (clojure.string/replace input_string
                            (re-pattern (apply str
                                               (interpose "|"
                                                          (map #(java.util.regex.Pattern/quote %)
                                                               (keys env-map)))))
                            env-map)))

(defn config-parse-pks-raw
  "Extracts vector of keys from column map"
  [columns]
  (seq (map #(get % :name)
            (filter #(true? (% :pk))
                    columns))))

(def config-parse-pks (memoize config-parse-pks-raw))

(defn config-parse-non-pks-raw
  "Extracts vector of keys from column map"
  [columns]
  (seq (map #(get % :name)
            (filter #(not (true? (% :pk)))
                    columns))))

(def config-parse-non-pks (memoize config-parse-non-pks-raw))

;; TODO: Make this a multimethod or abstract the "unstructured type"
(defn config-parse-unstructured-cols-raw
  "Extracts vector of json keys from column map"
  [columns]
  (doall (map #(get % :name)
              (filter #(or (= "jsonb" (% :type))
                           (= "json" (% :type)))
                      columns))))

(def config-parse-unstructured-cols (memoize config-parse-unstructured-cols-raw))

(defn config-parse-col-names-raw
  "Extracts vector of keys from column map"
  [columns]
  (doall (map #(keyword (get % :name)) columns)))

(def config-parse-col-names (memoize config-parse-col-names-raw))

(defn config-get-primary-keys-raw
  "Extracts vector of keys from column map"
  [columns]
  (seq (map #(get % :name)
            (filter #(true? (% :pk))
                    columns))))

(def config-get-primary-keys (memoize config-get-primary-keys-raw))

(defn config-get-non-keys-raw
  "Extracts vector of keys from column map"
  [columns]
  (seq (map #(get % :name)
            (filter #(not (true? (% :pk)))
                    columns))))

(def config-get-non-keys (memoize config-get-non-keys-raw))

(defn record-drop-empty-strings
  "Remove empty strings from hash map"
  [rec]
  (apply dissoc rec (keep #(-> % val (not= "") (if nil (key %))) rec)))

(defn record-strip-null-pks
  "Used when we need to enforce constraints during the load, can also
   be done via the jq preprocessor integration"
  [records pk-columns]
  (doall
   (remove (fn [rec]
             (some #(nil? (get rec %))
                   pk-columns))
           records)))
