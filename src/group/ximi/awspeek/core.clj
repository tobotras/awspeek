(ns group.ximi.awspeek.core
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.aws.rds :as rds]
            ;;[clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as prep]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [clojure-ini.core :as ini]
            [com.climate.claypoole :as cp]
            ;;[clj-async-profiler.core :as prof]
            [clojure.tools.cli :as cli]
            [clojure.string :as str])
  (:gen-class))

;; Tools
(defn tools-env [var & [default]]
  (if-let [value (System/getenv var)]
    (if (number? default)
      (Integer/parseUnsignedInt value)
      value)
    default))

;; Configuration
(def max-object-size (* 1024 1024 1024)) ;1GB
(def data-store {:dbtype "postgresql"
                 :dbname   (tools-env "DB_NAME" "ximi")
                 :host     (tools-env "DB_HOST" "localhost")
                 :user     (tools-env "DB_USER" "ximi")
                 :password (tools-env "DB_PASS" "ximipass")})

;;-------------

;; FIXME: globals

(def regexps [])
(def db-conn (atom nil))
(def match-stmt (atom nil))
(def match-count (atom 0))
(def t-pool (atom nil))
(def cli-opts (atom {}))

(defn precompile-regex
  "Precompile regexps, removing '^...$' if found"
  [re-spec]
  (let [re-string (:regex re-spec)
        re (.substring (java.lang.String. ^java.lang.String re-string)
                       (if (= (first re-string) \^)
                         1
                        0)
                       (- (count re-string)
                          (if (= (first (take-last 1 re-string)) \$)
                            1
                            0)))]
    (assoc re-spec :pattern (re-pattern (str/escape re {\\ "\\"})))))
  

(defn load-regexps []
  ;; SELECT REGEXPS.LABEL, REGEXPS.REGEX, DATA_CLASSES.NAME FROM REGEXPS INNER JOIN DATA_CLASSES ON REGEXPS.CLASS = DATA_CLASS.ID;
  (let [request {:select [:regexps.id :regexps.label :regexps.regex [:data_classes.name :class]]
                 :from [:regexps]
                 :right-join [:data_classes [:= :regexps.class :data_classes.id]]}
        rs (jdbc/execute! @db-conn (sql/format request) {:builder-fn rs/as-unqualified-lower-maps})]
    (alter-var-root (var regexps) (fn [_] (mapv precompile-regex rs)))))

;; Assumption: UNIX env
(def profile-region
  (memoize #(-> (System/getenv "HOME")
                (io/file ".aws" "config")
                ini/read-ini
                (get-in [(str "profile " (System/getenv "AWS_PROFILE")) "region"]))))

(defn mark-match [asset resource location folder object re-id]
  (when (nil? @match-stmt)
    (swap! match-stmt
           (fn [_]
             (jdbc/prepare @db-conn ["insert into matches (asset, resource, location, folder, file, regexp)
                                      values((select id from assets where name=?),?,?,?,?,?)"]))))
  (prep/set-parameters @match-stmt [asset resource location folder object re-id])
  (.addBatch ^java.sql.PreparedStatement @match-stmt)
  (swap! match-count inc))

(defn grep-line [asset resource location folder file line]
  (when (nil? @t-pool)
    (swap! t-pool (constantly (cp/threadpool 4))))
  ;;FIXME: stops after some 2mln lines :-O
  ;;(cp/upfor t-pool [re regexps]
  (doseq [re regexps]
    (when (re-find (:pattern re) line)
      (mark-match asset resource location folder file (:id re)))))

(defn gzipped-stream? [^java.io.BufferedInputStream stream]
  (let [header (byte-array 2)]
    (doto stream
      (.mark 2)
      (.read header)
      (.reset))
    (if (and (= (first header) 31)
             (= (second header) -117))
      (do
        (when (:verbosity @cli-opts)
          (println "Gzipped stream found"))
        true)
      false)))

(defn commit-batch! []
  (when @match-stmt
    (.executeBatch ^org.postgresql.jdbc.PgPreparedStatement @match-stmt)
    (swap! match-stmt (constantly nil)))
  (.commit ^org.postgresql.jdbc.PgConnection @db-conn))

(defn grep-stream [s asset resource location folder object]
  (let [stream (if (gzipped-stream? s)
                       (java.util.zip.GZIPInputStream. s)
                       s)
        lines (line-seq (io/reader stream))]
    (doseq [[i line] (map-indexed vector lines)]
      (when (and (zero? (mod i 300000))
                 (:verbosity @cli-opts))
        (println i (java.util.Date.)))
      (grep-line asset resource location folder object line))
    (commit-batch!)))

(defn grep-s3-object [bucket object-name]
  (let [stream (io/input-stream (:input-stream (s3/get-object bucket object-name)))]
    (grep-stream stream "AWS" "S3" (profile-region) bucket object-name)))

(defn process-s3-bucket [bucket]
  (println "Bucket:" bucket)
  ;; TODO: pagination?
  (let [objects (->> (s3/list-objects {:bucket-name bucket})                   
                     (filter #(= (first %) :object-summaries))
                     first
                     second)]
    ;;objects (second (first (filter #(= (first %) :object-summaries) object-list)))]
    (doseq [obj objects :when (<= (:size obj) max-object-size)]
      (println "Object key:" (:key obj) ", size" (:size obj))
      (grep-s3-object bucket (:key obj)))))

(defn check-aws-creds []
  (when-not (System/getenv "AWS_PROFILE")
    (println "AWS_PROFILE not set, cannot access AWS")
    (System/exit 2)))

(defn process-s3 []
  (check-aws-creds)
  ;; TODO: pagination?
  (doseq [bucket (s3/list-buckets)]
    (process-s3-bucket (:name bucket))))

;;--------------------------------------------------------

(defn hostname []
  (-> "hostname"
      shell/sh
      :out
      str/trim-newline))

(defn process-local-file [file-name]
  (println "Processing" file-name)
  (let [file (io/file file-name)
        dirName (-> file
                    .getAbsoluteFile
                    .getParent)]
    (-> file-name
        io/input-stream
        (grep-stream "Filesystem" "Local file" (hostname) dirName (.getName file)))))

(defn text-column? [col]
  ;; FIXME: datatype IDs are PostgreSQL specific, I guess!
  (let [text-data-types ["12"]]
    (some #(= (:type col) %) text-data-types)))

(defn process-row [asset resource location folder file row]
  (doseq [key (keys row)]
    (grep-line asset resource location folder file (get row key))))

(defn process-columns [asset resource location folder conn cols table-name]
  (let [text-cols (filter text-column? cols)]
    (if (empty? text-cols)
      (println table-name "has no text columns")
      (let [cols-list (mapv #(keyword (:name %)) text-cols)
            rs (jdbc/execute! conn (sql/format {:select cols-list
                                                :from (keyword table-name)
                                                :limit 1}) {:builder-fn rs/as-unqualified-lower-maps})]
        (doseq [row rs]
          (process-row asset resource location folder table-name row))))))

(defn process-psql-table [asset resource location folder conn ^org.postgresql.jdbc.PgDatabaseMetaData metadata table-name]
  (println "* * Processing table" table-name)
  (let [rs (.getColumns metadata nil nil table-name nil)]
    (loop [cols []]
      (if (.next rs)
        (recur (conj cols {:name (.getString rs "COLUMN_NAME")
                           :type (.getString rs "DATA_TYPE")}))
        (process-columns asset resource location folder conn cols table-name)))))

(defn process-psql-tables [asset resource location folder conn metadata tables]
  (if (empty? tables)
    (println "* * No tables")
    (doseq [table-name tables]
      (process-psql-table asset resource location folder conn metadata table-name)
      (commit-batch!))))

(defn process-psql [asset datasource]
  (if-let [conn (jdbc/get-connection datasource)]
    (let [metadata (.getMetaData conn)
          rs (.getTables metadata nil nil nil (into-array ["TABLE"]))]
      (loop [tables []]
        (if (.next rs)
          (recur (conj tables (.getString rs "TABLE_NAME")))
          (process-psql-tables asset (:dbtype datasource) (:host datasource) (:dbname datasource) conn metadata tables))))
    (println "Can't connect to DB")))

;;-----------------------------------------------------

(defn discover-db [datasource]
  (with-open [conn (jdbc/get-connection datasource)]
    (let [rs (jdbc/execute! conn
                            ["SELECT datname FROM pg_database WHERE datistemplate = false AND datname <> 'postgres' AND datname <> 'rdsadmin'"]
                            {:builder-fn rs/as-unqualified-lower-maps})]
      (map #(:datname %) rs))))

(defn process-rds-postgres [instance]
  (let [status (:dbinstance-status instance)]
    (if (= status "available")
      (let [endpoint (:endpoint instance)
            datasource {:dbtype "postgresql"
                        :user (:master-username instance)
                        :host (:address endpoint)
                        :port (:port endpoint)
                        :password "qwe123QWE123"} ;;FIXME!!
            _ (println "Connecting to" endpoint)
            db-list (discover-db datasource)]
        (if (empty? db-list)
          (println "* No databases in the instance")
          (doseq [db-name db-list]
            (println "* Processing database" db-name)
            (process-psql "AWS" (assoc datasource :dbname db-name)))))
      (println (format "* Instance status: '%s', skipping" status)))))

(defn process-rds-instance [instance]
  (println "RDS instance:" (:dbinstance-identifier instance))
  (if (= (:engine instance) "postgres")
    (process-rds-postgres instance)
    (println (format "Unsupported engine: '%s'" (:engine instance)))))

(defn process-rds []
  (check-aws-creds)
  (doseq [instance (:dbinstances (rds/describe-db-instances))]
    (process-rds-instance instance)))
;;-----------------------------------------------------

(defn usage [parsed]
  (println "Usage:")
  (println (:summary parsed))
  (System/exit 1))

(defn -main [& args]
  (let [parsed (cli/parse-opts args
                               [["-v" "--verbose" "Verbosity"
                                 :id :verbosity
                                 :default 0
                                 :update-fn inc]
                                ["-s" "--aws-s3" "Process AWS S3 storage"]
                                ["-r" "--aws-rds" "Process AWS RDS tables"]
                                ["-f" "--file FILE" "Proces local text file"
                                 :validate [#(.exists (io/file %)) "No such file"]]
                                ["-d" "--dbname DATABASE" "PostgreSQL database name"]
                                ["-h" "--host HOSTNAME" "PostgreSQL server host"]
                                ["-p" "--port PORT" "PostgreSQL server port"
                                 :default 5432
                                 :parse-fn #(Integer/parseInt %)
                                 :validate [#(< 0 % 65536) "Invalid port number"]]
                                ["-u" "--user USERNAME" "PostgreSQL server username"]
                                ["-w" "--password PASS" "PostgreSQL server password"]]
                               )
        err (:errors parsed)]
    (swap! cli-opts (constantly (:options parsed)))
    (when err
      (println (first err))
      (System/exit 0))
    (with-open [c (jdbc/get-connection data-store
                                       {:auto-commit false
                                        :reWriteBatchedInserts true})]
      (swap! db-conn (constantly c))
      (load-regexps)
      (cond
        (:aws-s3 @cli-opts) (process-s3)
        (:aws-rds @cli-opts) (process-rds)
        (:file @cli-opts) (process-local-file (:file @cli-opts))
        (:dbname @cli-opts) (let [datasource {:dbtype "postgresql"
                                              :host (:host @cli-opts)
                                              :port (:port @cli-opts)
                                              :user (:user @cli-opts)
                                              :password (:password @cli-opts)
                                              :dbname (:dbname @cli-opts)}]
                              (if (some nil? (vals datasource))
                                (usage parsed)
                                (process-psql "Omprem" datasource)))
        :else (usage parsed))))
  (when (pos? @match-count)
    (println "Matches registered:" @match-count))
  (System/exit 0))                      ; or else
