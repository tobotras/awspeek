(ns awspeek.core
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.aws.ec2 :as ec2]
            [amazonica.aws.eks :as eks]
            [kubernetes-api.core :as k8s]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.set :as set]
            [yaml.core :as yaml]
            [next.jdbc :as jdbc]
            [honey.sql :as sql])
  (:gen-class))

(def max-object-to-dump (* 1024 1024 100)) ;100Mb

;; DB

(def db-config {})
(def regexps {})

(defn sql! [request]
  (let [req (sql/format request)]
    (println "SQL:" req "of type" (type req) "of size" (count req))
    (jdbc/execute! db-config req)))

(defn load-regexps []
  ;; SELECT REGEXPS.LABEL, REGEXPS.REGEX, DATA_CLASSES.NAME FROM REGEXPS INNER JOIN DATA_CLASSES ON REGEXPS.CLASS = DATA_CLASS.ID;
  (let [regexps (sql! {:select [:regexps.label :regexps.regex :data_classes.name]
                       :from [:regexps]
                       :right-join [:data_classes [:= :regexps.class :data_classes.id]]})]
    (pp/pprint regexps)
    regexps
    )
  )

(defn db-init [cfg]
  (println "DB-init")
  (alter-var-root (var db-config) (constantly cfg))
  (alter-var-root (var regexps) (load-regexps))
  (println "JDBC:")
  (let [res (jdbc/execute! db-config ["SET SEARCH_PATH = ximi, public"])]
    (println "setting search path:")
    (pp/pprint res)))
                           

(defn grep-line [line])

(defn mark-match [asset resource folder object match])

(defn grep-object [bucket & {:keys [key size]}]
  (let [lines (-> (s3/get-object bucket key)
                  :input-stream
                  io/reader
                  line-seq)]
    (doseq [line lines]
      (when-let [the-match (grep-line line)] 
        (mark-match "AWS" "S3" bucket key the-match)))))

(defn dump-bucket [bucket]
  (println "Bucket:" bucket)
  ;; TODO: pagination?
  (let [object-list (s3/list-objects {:bucket-name bucket})
        objects (second (first (filter #(= (first %) :object-summaries) object-list)))]
    (doseq [obj objects :when (<= (:size obj) max-object-to-dump)]
      (println "Object key: " (:key obj) ", size" (:size obj))
      (grep-object bucket obj))))

(defn dump-s3 []
  (let [bucket-list (s3/list-buckets)]
    (doseq [bucket bucket-list]
      (dump-bucket (:name bucket)))))

(defn dump-instance [& {:keys [instance-id public-dns-name tags key-name]}]
  (print "InstanceId:" instance-id)
  (when public-dns-name
    (print", DNS:" public-dns-name))
  (doseq [tag tags :when (= (:key tag) "Name")]
    (print ", name:" (:value tag)))
  (when key-name
    (print ", key:" key-name))
  (println))

(defn access-instance [i]
  (let [{:keys [key-name public-ip-address]} i]
    (when (and key-name public-ip-address)
      (let [key-file (io/file (System/getenv "HOME") ".ssh" (str key-name ".pem"))]
        (when (.exists key-file)
          (let [res (shell/sh "ssh" "-oUserKnownHostsFile=/dev/null" "-oStrictHostKeyChecking=no"
                              "-i" (str key-file) (str "admin@" (:public-ip-address i)) "id")]
            (when (= (:exit res) 0)
              (println "SSH into VM:" (:out res)))))))))

(defn dump-ec2 []
  (let [reservations (:reservations (ec2/describe-instances))]
    (doseq [r reservations]
      (doseq [i (:instances r)]
        (pp/pprint i)
        (dump-instance i)
        (access-instance i)))))

(defn cluster-name-eq? [arn-name name]
  (let [[_ aws-name] (clojure.string/split arn-name #"/")]
    (= aws-name name)))

;; Assumptions:
;; AWS CLI is installed
;; aws eks update-kubeconfig -> reading creds off ~/.kube/config
(defn cli-get-token [exec]
  (-> shell/sh
      (apply (into [(:command exec)] (:args exec)))
      :out
      (json/read-str :key-fn keyword)
      :status
      (select-keys [:token])))

(defn load-creds [name]
  (let [kubecfg (yaml/from-file (io/file (System/getenv "HOME") ".kube" "config"))
        ca-data (->> kubecfg
                     :clusters
                     (some #(when (cluster-name-eq? (:name %) name)
                              (get-in % [:cluster :certificate-authority-data]))))
        client-data (->> kubecfg
                         :users
                         (some #(when (cluster-name-eq? (:name %) name)
                                  (:user %))))
        creds (merge {:ca-cert ca-data}
                     (set/rename-keys client-data
                                      {:client-certificate-data :client-cert
                                       :client-key-data :client-key}))]
    (if (or (:token creds)
            (:client-key creds))
      creds
      (if (:exec creds)
        (cli-get-token (:exec creds))
        (do
          (println "Cannot load creds from kube config")
          (System/exit 0))))))

(defn dump-k8s-data [desc]
  (let [name (:name desc)
        endpoint (:endpoint desc)
        creds (load-creds name)]
    (println "Credentials for" name ":")
    (pp/pprint creds)
    (if-let [cluster (k8s/client endpoint creds)]
      (k8s/explore cluster)
      (println "Cannot connect to endpoint"))))

(defn dump-cluster [cluster]
  (let [desc (:cluster (eks/describe-cluster {:name cluster}))]
    ;;(pp/pprint desc)
    (print "Name:" (:name desc))
    (print ", status:" (:status desc))
    (print ", network:" (get-in desc [:kubernetes-network-config :service-ipv4cidr]))
    (println)
    (dump-k8s-data desc)))

(defn dump-k8s []
  (let [clusters (eks/list-clusters)]
    (doseq [c (:clusters clusters)]
      (dump-cluster c))))

;; ----------
(defn tools-env [var & [default]]
  (if-let [value (System/getenv var)]
    (if (number? default)
      (Integer/parseUnsignedInt value)
      value)
    default))
;;-------------

;; Assumption: AWS credentials default to ~/.aws/credentials
(defn -main [& args]
  (db-init {:dbtype "postgresql"
            :dbname   (tools-env "DB_NAME" "ximi")
            :host     (tools-env "DB_HOST" "localhost")
            :user     (tools-env "DB_USER" "ximi")
            :password (tools-env "DB_PASS" "ximipass")})

  (let [to-dump (or (seq args) ["s3" "ec2" "eks"])]
    (when (some #{"s3"} to-dump)
      (dump-s3))
    (when (some #{"ec2"} to-dump)
      (dump-ec2))
    (when (some #{"eks"} to-dump)
      (dump-k8s)))
  (System/exit 0))
