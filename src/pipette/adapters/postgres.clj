(ns pipette.adapters.postgres
  (:require [pipette.interface :as interface]
            [pipette.utils :as utils]
            [clojure.string]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as prepare]
            [next.jdbc.result-set :as rs]
            [jsonista.core :as json]
            [honey.sql :as sql]
            [honey.sql.helpers :as h])
  (:import (org.postgresql.util PGobject))
  (:import (java.sql PreparedStatement)))

;; Initialize database connection :jdbcUrl
(def database (atom #{}))

;; --------------- Protocol extension for Postgres driver ----------------------

(def ->json json/write-value-as-string)
(def <-json #(json/read-value % (json/object-mapper {:decode-key-fn keyword})))

(defn ->pgobject
  "Transforms Clojure data to a PGobject that contains the data as
  JSON. PGObject type defaults to `jsonb` but can be changed via
  metadata key `:pgtype`"
  [x]
  (let [pgtype (or (:pgtype (meta x)) "jsonb")]
    (doto (PGobject.)
      (.setType pgtype)
      (.setValue (->json x)))))

(defn <-pgobject
  "Transform PGobject containing `json` or `jsonb` value to Clojure
  data."
  [^org.postgresql.util.PGobject v]
  (let [type  (.getType v)
        value (.getValue v)]
    (if (#{"jsonb" "json"} type)
      (when value
        (with-meta (<-json value) {:pgtype type}))
      value)))

(extend-protocol prepare/SettableParameter
  clojure.lang.IPersistentMap
  (set-parameter [m ^PreparedStatement s i]
    (.setObject s i (->pgobject m)))

  clojure.lang.IPersistentVector
  (set-parameter [v ^PreparedStatement s i]
    (.setObject s i (->pgobject v))))

(extend-protocol rs/ReadableColumn
  org.postgresql.util.PGobject
  (read-column-by-label [^org.postgresql.util.PGobject v _]
    (<-pgobject v))
  (read-column-by-index [^org.postgresql.util.PGobject v _2 _3]
    (<-pgobject v)))

;; --------------------- Postgres helper functions ------------------------------

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn db-array->vector
  "Converts Java PgArray returned by JDBC to a Clojure vec"
  [arr]
  (as-> (.toString arr) s
    (subs s 1 (- (count s) 1))
    (clojure.string/split s #",")))

(defmethod interface/database-read-table :postgres
  [db-config n-records]
  (let [query (-> (h/select :*)
                  (h/from (keyword (str (db-config :schema) "." (db-config :target))))
                  (h/limit (int n-records))
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-truncate-table :postgres
  [db-config]
  (let [query (-> (h/truncate (keyword (str (db-config :schema) "." (db-config :target))))
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-reset-seq :postgres
  [db-config]
  (with-open [con (jdbc/get-connection @database)]
    (let [query (format "SELECT PG_GET_SERIAL_SEQUENCE('\"%s\".\"%s\"', 'record_number') AS rec_seq"
                        (db-config :schema)
                        (db-config :target))
          rec_seq (-> (jdbc/execute! con [query])
                      (nth 0)
                      (:rec_seq))]
      (jdbc/execute! con [(str "ALTER SEQUENCE "
                               rec_seq
                               " RESTART WITH 1")]))))

(defmethod interface/database-check-primary-index :postgres
  [db-config]
  (let [keys (reduce (fn [acc nxt] (str acc "', '" nxt)) (utils/config-get-primary-keys (db-config :columns)))
        query (str "SELECT irel.relname AS resolved_index
                   FROM pg_index AS i
                   JOIN pg_class AS trel ON trel.oid = i.indrelid
                   JOIN pg_namespace AS tnsp ON trel.relnamespace = tnsp.oid
                   JOIN pg_class AS irel ON irel.oid = i.indexrelid
                   CROSS JOIN LATERAL unnest (i.indkey) WITH ORDINALITY AS c (colnum, ordinality)
                   JOIN pg_attribute AS a ON trel.oid = a.attrelid AND a.attnum = c.colnum
                   WHERE a.attname IN ('" keys "') AND tnsp.nspname = '" (db-config :schema) "' AND trel.relname = '" (db-config :target) "'")
        output (with-open [con (jdbc/get-connection @database)]
                 (jdbc/execute! con [query]))]
    (println (str "Index found for merge strat: "
                  (some-> output
                          (get 0)
                          (get :pg_class/resolved_index))))
    (some-> output
            (get 0)
            (get :pg_class/resolved_index))))

(defmethod interface/database-table-exists :postgres
  [db-config]
  (let [query (str "SELECT EXISTS ("
                   (-> (h/select :1)
                       (h/from [:information_schema.tables])
                       (h/where [:and
                                 [:= :table_schema (db-config :schema)]
                                 [:= :table_name (db-config :target)]])
                       (sql/format {:inline true})
                       (nth 0))
                   ") AS exists")]
    (with-open [con (jdbc/get-connection @database)]
      (-> (jdbc/execute! con [query])
          (nth 0)
          (get :exists)))))

(defmethod interface/database-make-index :postgres
  [db-config] 
  (when-let [keys (map keyword (utils/config-get-primary-keys (db-config :columns)))]
    (let [query (as-> (h/alter-table (keyword (str (db-config :schema) "." (db-config :target)))) $
                  (apply h/add-index $ :primary-key keys)
                  (sql/format $ {:quoted true}))]
      (with-open [con (jdbc/get-connection @database)]
        (jdbc/execute! con query)))))

;; --------------------- Database make table methods ------------------------------

(defmethod interface/database-make-table [:postgres :json]
  [db-config]
  (let [query (-> (h/create-table (keyword (str (db-config :schema) "." (db-config :target))))
                  (h/with-columns [[:record_id [:raw "uuid"] [:default [:raw "uuid_generate_v4()"]]]
                                   [:record_number [:raw "serial"]]
                                   [:data [:raw "jsonb"]]
                                   [:_etl_loaded_at [:raw "timestamp"]]])
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-make-table [:postgres  :text]
  [db-config]
  (let [query (-> (h/create-table (keyword (str (db-config :schema) "." (db-config :target))))
                  (h/with-columns [[:record_id [:raw "uuid"] [:default [:raw "uuid_generate_v4()"]]]
                                   [:record_number [:raw "serial"]]
                                   [:data [:raw "text"]]
                                   [:_etl_loaded_at [:raw "timestamp"]]])
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-make-table [:postgres :csv-array]
  [db-config]
  (let [query (-> (h/create-table (keyword (str (db-config :schema) "." (db-config :target))))
                  (h/with-columns [[:record_id [:raw "uuid"] [:default [:raw "uuid_generate_v4()"]]]
                                   [:record_number [:raw "serial"]]
                                   [:data [:raw "text[]"]]
                                   [:_etl_loaded_at [:raw "timestamp"]]])
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-make-table [:postgres :explicit]
  [db-config]
  (let [_ (utils/validate? (not (nil? (db-config :columns))) "Columns not specified in config with explicit storage-strategy")
        _ (utils/validate? (not (nil? (utils/config-get-primary-keys (db-config :columns)))) "Primary key(s) not specified in config with explicit storage-strategy")
        c-construct (doall (for [column (db-config :columns)]
                             [(keyword (column :name))
                              [:raw (str (or (column :type) "text"))]]))
        query (-> (h/create-table (keyword (str (db-config :schema) "." (db-config :target))))
                  (h/with-columns (conj c-construct [:_etl_loaded_at [:raw "timestamp"]]))
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-make-table [:postgres :explicit-json-overflow]
  [db-config]
  (let [_ (utils/validate? (not (nil? (db-config :columns))) "Columns not specified in config with explicit storage-strategy")
        c-construct (doall (for [column (db-config :columns)]
                             [(keyword (column :name))
                              [:raw (str (or (column :type) "text"))]]))
        query (-> (h/create-table (keyword (str (db-config :schema) "." (db-config :target))))
                  (h/with-columns (conj c-construct [:data [:raw "jsonb"]] [:_etl_loaded_at [:raw "timestamp"]]))
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

;; --------------------- Database batch prep methods ------------------------------

(defn pg-honey-prepare-json
  "This lets us handle json in explicit and patial schema definitions"
  [record json-columns]
  (apply conj (doall (map (fn [[column-name data]]
                            (if (some #{column-name} (map keyword json-columns))
                              {column-name [:lift data]}
                              {column-name data}))
                          record))))

(defmethod interface/database-prepare-record-batch [:postgres :json]
  [records _]
  (let [etl_loaded_at (java.time.LocalDateTime/now)]
    (doall (for [record records]
             {:data [:lift record]
              :_etl_loaded_at etl_loaded_at}))))

(defmethod interface/database-prepare-record-batch [:postgres :text]
  [records _]
  (let [etl_loaded_at (java.time.LocalDateTime/now)]
    (doall (for [record records]
             {:data (str record)
              :_etl_loaded_at etl_loaded_at}))))

(defmethod interface/database-prepare-record-batch [:postgres :csv-array]
  [records _]
  (let [etl_loaded_at (java.time.LocalDateTime/now)]
    (doall (for [record records]
             {:data [:lift (clojure.string/split
                            (str record) ",")]
              :_etl_loaded_at etl_loaded_at}))))

(defmethod interface/database-prepare-record-batch [:postgres :explicit]
  [records db-config]
  (let [column-map (db-config :columns)
        column-names (utils/config-parse-col-names column-map)
        pk-columns (utils/config-get-primary-keys column-map)
        json-columns (seq (utils/config-parse-unstructured-cols column-map))
        etl_loaded_at (java.time.LocalDateTime/now)
        ;; Batch transducers
        prepared-records (if (and (boolean pk-columns)
                                  (boolean (db-config :enforce-constraint)))
                           (utils/record-strip-null-pks records (doall (map keyword pk-columns)))
                           records)]
    (doall (for [record prepared-records]
             (cond-> record
               ;; Record transducers
               (boolean json-columns) (pg-honey-prepare-json json-columns)
               true (select-keys column-names)
               true (utils/record-drop-empty-strings)
               true (assoc :_etl_loaded_at etl_loaded_at))))))

(defmethod interface/database-prepare-record-batch [:postgres :explicit-json-overflow]
  [records db-config]
  (let [column-map (db-config :columns)
        column-names (utils/config-parse-col-names column-map)
        pk-columns (utils/config-get-primary-keys column-map)
        json-columns (utils/config-parse-unstructured-cols column-map)
        etl_loaded_at (java.time.LocalDateTime/now)
        ;; Batch transducers
        prepared-records (if (and (boolean pk-columns)
                                  (boolean (db-config :enforce-constraint)))
                           (utils/record-strip-null-pks records (doall (map keyword pk-columns)))
                           records)]
    (doall
     (for [record prepared-records]
       (merge (cond-> record
                ;; Record transducers
                (boolean (seq json-columns)) (pg-honey-prepare-json json-columns)
                true (select-keys column-names)
                true (utils/record-drop-empty-strings)
                true (assoc :_etl_loaded_at etl_loaded_at))
              {:data [:lift (apply dissoc
                                   record
                                   (select-keys record column-names))]})))))

;; --------------------- Database ingestion methods ------------------------------

(defmethod interface/database-ingest-records [:postgres :upsert]
  [records db-config]
  (let [keys (map keyword (utils/config-get-primary-keys (db-config :columns)))
        non-keys (conj (map keyword (utils/config-get-non-keys (db-config :columns)))
                       :_etl_loaded_at)
        query (as-> (h/insert-into (keyword (str
                                             (db-config :schema) "." (db-config :target)))) $
                (h/values $ (interface/database-prepare-record-batch records
                                                                     db-config))
                (apply h/on-conflict $ keys)
                (apply h/do-update-set $ non-keys)
                (sql/format $ {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

(defmethod interface/database-ingest-records [:postgres :insert]
  [records db-config]
  (let [query (-> (h/insert-into (keyword (str (db-config :schema)
                                               "."
                                               (db-config :target))))
                  (h/values (interface/database-prepare-record-batch records
                                                                     db-config))
                  (sql/format {:quoted true}))]
    (with-open [con (jdbc/get-connection @database)]
      (jdbc/execute! con query))))

;; --------------------- Adapter preparation methods ------------------------------

(defmethod interface/prepare-stream-target :postgres
  [target-name db-config]
  ;; Swap atom with conn
  (reset! database
          (jdbc/get-datasource {:jdbcUrl (utils/interpolate-env-vars (get db-config :jdbc))
                                :stringtype "unspecified"}))
  ;; Ensure valid strategies
  (when (some #(= (db-config :storage-strategy) %) ["json" "text" "csv-array"])
    (utils/validate? (not= (db-config :ingestion-strategy) "merge")
               (format "Target %s located in file however however the storage strategy %s is incompatible with ingestion stategy %s Ingestion strategy must be configured as `refresh` or `append`"
                       target-name (db-config :storage-strategy) (db-config :ingestion-strategy))))
  (case (interface/database-table-exists db-config)
    ;; Table exists, ensure we are ready to ingest
    true  (do (when (= (db-config :ingestion-strategy) "refresh")
                [(interface/database-truncate-table db-config)
                 (println "Cleaned target")])
              (when (and (= (db-config :ingestion-strategy) "merge")
                         (not (interface/database-check-primary-index db-config)))
                [(interface/database-make-index db-config)
                 (println "Rebuilt index")])
              (when (and (some #(= (db-config :storage-strategy) %) ["json" "text" "csv-array"])
                         (= (db-config :ingestion-strategy) "refresh"))
                [(interface/database-reset-seq db-config)
                 (println "Reset record sequence")]))
    ;; Table does not exist, create it
    false (do (interface/database-make-table db-config)
              (when (= (db-config :ingestion-strategy) "merge")
                (interface/database-make-index db-config))
              (println "Created table"))))
