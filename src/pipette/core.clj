(ns pipette.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [clojure.string]
            [yaml.core :as yaml]
            [jsonista.core :as json]
            [pipette.utils :as utils]
            [pipette.interface :as interface]
            ;; TODO: Make the below a dynamic pull from adapter directory?
            [pipette.adapters.postgres])
  (:gen-class))

;; ---------------- Config Specification --------------------------

(s/def ::version int?)
(s/def :p.destination/adapter #(contains? #{:postgres
                                            :snowflake
                                            :mysql
                                            :hive
                                            :spark
                                            :sqlite} (keyword %)))
(s/def :p.destination/jdbc string?)
(s/def :p.destination/schema string?)
(s/def :p.destination/target string?)
(s/def :p.destination/ingestion-strategy #{"merge"
                                           "append"
                                           "refresh"})
(s/def :p.destination/storage-strategy #{"json"
                                         "explicit"
                                         "explicit-json-overflow"
                                         "csv-array"
                                         "text"})
(s/def :p.column/name string?)
(s/def :p.column/pk boolean?)
(s/def :p.column/type string?)
(s/def :p.column/base (s/keys :req-un [:p.column/name]
                              :opt-un [:p.column/pk
                                       :p.column/type]))
(s/def :p.destination/columns (s/coll-of :p.column/base :min-count 1))
(s/def :p.config/destination (s/keys :req-un [:p.destination/adapter
                                              :p.destination/jdbc
                                              :p.destination/target
                                              :p.destination/schema
                                              :p.destination/storage-strategy
                                              :p.destination/ingestion-strategy]
                                     :opt-un [:p.destination/columns]))
(s/def :p.config/destinations (s/map-of keyword?
                                        :p.config/destination))
(s/def :p.config/targets (s/map-of keyword? 
                                   :p.config/destinations))
(s/def :p.config/configuration (s/keys :req-un [:p.config/targets
                                                ::version]))

;; --------------------- Stdin reader methods ------------------------------

(def temp-dir (System/getProperty "java.io.tmpdir"))
(def sys-type (System/getProperty "os.name"))
(def jq-exe (cond
              (clojure.string/includes? sys-type "Windows") "jq-win64.exe"
              (clojure.string/includes? sys-type "Darwin") "jq-osx-amd64"
              :else "jq-linux64"))

(defn copy-uri-to-file [uri file]
  (with-open [in (io/input-stream uri)
              out (io/output-stream file)]
    (io/copy in out)))

(defn read-stdin-chunks
  "Reads from the stdin buffer lazily outputing partitions
   as record sets of a defined size"
  [batch-size jq-query]
  (partition batch-size
             batch-size
             []
             (for [input #_{:clj-kondo/ignore [:unresolved-symbol]}
                   (if (nil? jq-query)
                     (line-seq (java.io.BufferedReader. *in*))
                     ;; we have a local copy of the linux-amd-64 jq binary at resources/jq -- we can access with io/resource?
                     (let [cmd [(str (io/file temp-dir jq-exe)) "-c" "--unbuffered" jq-query]
                           proc (.exec (Runtime/getRuntime) (into-array cmd))
                           _ (future (with-open [wtr (io/writer (.getOutputStream proc))]
                                       (io/copy (io/reader *in*) wtr)))]
                       (line-seq (io/reader (.getInputStream proc)))))]
               ;; Consider fault tolerance here, pros/cons
               (json/read-value input json/keyword-keys-object-mapper))))

;; --------------------- Config reader methods ------------------------------

(defn config-exists
  "This function verifies if the config file exists at the path
   passed and that it is indeed a YAML"
  [path]
  (and (.exists (io/file path))
       (.isFile (io/file path))
       (or (.endsWith path ".yaml")
           (.endsWith path ".yml"))))

(def cli-options
  [["-c" "--config-path FILE" "Pipette config file"
    :missing "--config-path is required"
    :validate [config-exists "No config found at specified path"]]
   ["-t" "--target NAME" "Pipette target name. One config file may have many defined targets"
    :missing "--target is required"]
   ["-b" "--batch-size INT" "Number of records to consume from buffer before emitting to targets"
    :parse-fn #(Integer/parseInt %)
    :default 5000]
   ["-q" "--jq QUERY" (str "A jq query to apply to stdin. Often used if input in not new line delimited and needs a preprocessing "
                           "step we don't want to couple into the application layer. Uses an embedded jq resolved based on os.name")]
   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

(defn validate-config [configuration-file]
  (let [parsed (s/conform :p.config/configuration configuration-file)]
    (if (s/invalid? parsed)
      (throw (ex-info "Invalid loader config" (s/explain-data :p.config/configuration configuration-file)))
      parsed)))

(def tracked-writes (atom ()))

;; --------------------- Main block ------------------------------

(defn -main [& args]
  (let [parsed_args (parse-opts args cli-options)
        arguments (parsed_args :arguments)
        options (parsed_args :options)
        summary (parsed_args :summary)
        errors (parsed_args :errors)
        show-help (options :help)
        config-path (options :config-path)
        target-namespace (options :target)]
    (utils/validate? (or (nil? errors) (true? show-help))
               (clojure.string/join ", " errors))
    (case show-help
      true (println summary)
      (let [config-file (validate-config (yaml/from-file config-path))
            config (get-in config-file [:targets (keyword target-namespace)])
            _ (utils/validate? (not (nil? config))
                               (format "Target namespace %s was not found in config file %s"
                                       target-namespace
                                       config-path))
            targets (filter #(contains? (set (map keyword arguments))
                                        (first %))
                            config)
            _ (utils/validate? (not (nil? (seq targets)))
                               (format "No valid configs in namespace %s passed in from args %s"
                                       target-namespace
                                       arguments))]

        (println "Preparing targets")

        (when (and (options :jq) (not (.exists (io/file temp-dir jq-exe))))
          (copy-uri-to-file (io/resource jq-exe) (io/file temp-dir jq-exe))
          (.setExecutable (io/file temp-dir jq-exe) true))

        (doseq [[target-name target-config] targets]
          (interface/prepare-stream-target target-name target-config))

        (println "Consuming stream")

        (doseq [records (read-stdin-chunks (options :batch-size) (options :jq))]
          (doseq [[_ target-config] targets]
            (swap! tracked-writes conj
                   (future (interface/database-ingest-records records
                                                              target-config)
                           (println (str "...verified batch ingestion into `"
                                         (target-config :target)
                                         "` by database..."))))))


        (mapv deref (deref tracked-writes))
        (println "Complete")

        (System/exit 0)))))
