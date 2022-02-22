(ns pipette.interface)

;; --------------------- Database helper functions ------------------------------

(defmulti database-read-table
  (fn [db-config _] (keyword (get db-config :adapter))))

(defmethod database-read-table :default
  [db-config _]
  (throw (ex-info "Read is unsupported for this db adapter" db-config)))

;; --------------------- Database make table methods ------------------------------

(defmulti database-make-table
  (fn [db-config] [(keyword (get db-config :adapter))
                   (keyword (get db-config :storage-strategy))]))

(defmethod database-make-table :default
  [db-config]
  (throw (ex-info "Unsupported storage strategy in config" db-config)))

;; --------------------- Database truncate table methods ---------------------------

(defmulti database-truncate-table
  (fn [db-config] (keyword (get db-config :adapter))))

(defmethod database-truncate-table :default
  [db-config]
  (throw (ex-info "Adapter has not implemented table truncation" db-config)))

;; --------------------- Database reset serial methods ----------------------------

(defmulti database-reset-seq
  (fn [db-config] (keyword (get db-config :adapter))))

(defmethod database-reset-seq :default
  [db-config]
  (throw (ex-info "Adapter has not implemented table serial reset" db-config)))

;; --------------------- Database check index methods ----------------------------

(defmulti database-check-primary-index
  (fn [db-config] (keyword (get db-config :adapter))))

(defmethod database-check-primary-index :default
  [db-config]
  (throw (ex-info "Adapter has not implemented index lookup" db-config)))

;; --------------------- Database create index methods ----------------------------

(defmulti database-make-index
  (fn [db-config] (keyword (get db-config :adapter))))

(defmethod database-make-index :default
  [db-config]
  (throw (ex-info "Adapter has not implemented create index" db-config)))

;; --------------------- Database table exists methods ----------------------------

(defmulti database-table-exists
  (fn [db-config] (keyword (get db-config :adapter))))

(defmethod database-table-exists :default
  [db-config]
  (throw (ex-info "Adapter has not implemented table existence check" db-config)))

;; --------------------- Database batch prep methods ------------------------------

(defmulti database-prepare-record-batch
  (fn [_ db-config] [(keyword (get db-config :adapter))
                     (keyword (get db-config :storage-strategy))]))

(defmethod database-prepare-record-batch :default
  [_ db-config]
  (throw (ex-info "Unsupported storage strategy in config" db-config)))

;; --------------------- Adapter preparation methods ------------------------------

(defmulti prepare-stream-target
  (fn [_ target-config] (keyword (get target-config :adapter))))

(defmethod prepare-stream-target :default
  [target-name target-config]
  (throw (ex-info (format "Unsupported or missing adapter `%s` in config target %s"
                          (target-config :adapter)
                          target-name) target-config)))

;; --------------------- Database ingestion methods ------------------------------

(def database-ingest-map (-> (make-hierarchy)
                             (derive :merge :upsert)
                             (derive :refresh :insert)
                             (derive :append :insert)
                             (atom)))

(defmulti database-ingest-records
  (fn [_ db-config]
    [(keyword (get db-config :adapter))
     (keyword (get db-config :ingestion-strategy))])
  :hierarchy database-ingest-map)

(defmethod database-ingest-records :default
  [_ db-config]
  (throw (ex-info "Unsupported or missing ingestion strategy in config" db-config)))
