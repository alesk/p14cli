(ns pk14cli.rpt2
 (:require
  [clojure.java.io :as io]
  [clj-time.core :refer [date-time hours plus]]
  [clj-time.format :refer [parse formatter]]
  ))

;; mapping from field id to field name
(def fields {
             "41" :tid
             "42" :mid
             "99" :err_msg
             "7"  :timestamp
             "11" :transaction_id
             "4"  :amount
             "39" :error_code
             "63" :tlv
             })

;; regex to extract pattern
;;
;; 2013-10-21 12:16:02,397(kar ne kaj krame )parse:\n<isomsg>....</isomsg>
;;
(def error-entry-regex #"(?s)(\d{4}-.{17})[^\n]+?\n+<isomsg>\s*(.*?)\s*</isomsg>" )

;; parses timestamp of form YYYY-MM-DD hh:mm:ss,SSS"
;; must use H for hour (0-23) not h (0-11)
(defn parse-timestamp [timestamp]  (parse (formatter "YYYY-MM-DD HH:mm:ss,SSS") timestamp))

;; fast check if content of isomsg has error_code = 0
(defn error-entry? [content] (neg? (.indexOf content "id=\"39\" value=\"00\"")))

;; extract fields from isomsg
(defn field-seq [xml-content]
  (map rest (re-seq #"<field id=\"([^\"]+)\" value=\"([^\"]+)\"" xml-content)))

;; converts isomsg xml to {}
(defn parse-error-xml [xml-content]
  (into {} (map (fn [[id val]] [(get fields id (keyword id)) val])
                (field-seq xml-content))))

;; extract errorneous log entries, converts isomsg to {}
;; and adds server-timestamp corrected for host-time-diff
(defn extract-errors [content host-time-diff]
  (let [
        pairs (map rest (re-seq  error-entry-regex content))
        pairs-with-errors (filter error-entry? pairs)
        ]
    (map
     (fn [[timestamp xml-content]]
       (assoc
         (parse-error-xml xml-content)
         :server-timestamp (plus (parse-timestamp timestamp) (hours host-time-diff))
       ))
     pairs-with-errors)
    ))



(defn process-files
  "Extracts errors from log files sequentially."
  [ file-names server-time-diff]
           (reduce (fn [acc file-name]
                     (println "Processing: " (.toString file-name))
                     (into acc (extract-errors (slurp file-name) server-time-diff)))
                   [] file-names))


(defn pprocess-files
  "Extract erros from log files in parallel."
  [ file-names server-time-diff]
  (let [chunks (partition-all 1 (map #(.toString %) file-names))
        process-log (fn [file]
                       (println file)
                       (read-log-file file server-time-diff)
                      )
        ]
       (pmap process-log  (map #(.toString %) file-names))
    ))




;;
(defn profile-with [fn]
  (time
   (let [files (filter #(.isFile %) (file-seq (io/file "resources")))
         results (sort-by :server-timestamp (fn (take 7 files) 1))]
   (println "Errors extracted: " (count results)))))

(profile-with process-files)
(profile-with pprocess-files)

(defn process-files2 [file-names server-time-diff]
  (flatten (pmap
     (fn [files]
       (process-files files server-time-diff))
     (partition-all 2 file-names)
  )))

(profile-with process-files2)



(defn read-log-file [file-name server-time-diff]
  (with-open [rdr (clojure.java.io/reader file-name)]
    (:pairs
     (persistent!
      (reduce
       (fn [acc line]
         (cond
          (has? line "<isomsg>")  (assoc! acc
                                          :state :read-isomsg
                                          :last-isomsg (transient []))

          (has? line "</isomsg>")
          (let [isomsg (apply str (persistent! (acc :last-isomsg)))]
            (if (error-entry? isomsg)
              (assoc! acc
                      :state :read-log
                      :pairs (conj (acc :pairs)
                                   (into
                                    (parse-error-xml isomsg)
                                    {:server-time-diff (plus (parse-timestamp (subs (acc :last-log) 0 23)) server-time-diff)}
                                   )))
              (assoc! acc :state :read-log)
              ))
          :else
          (if (= (acc :state) :read-isomsg)
            (assoc!  acc :last-isomsg (conj! (acc :last-isomsg) line))
            (assoc!  acc :last-log line))
          ))

       (transient {:state :read-log :last-log nil :last-isomsg nil :pairs []})
       (line-seq rdr))))))




(time (read-log-file "temp-logs/log4j_pk14_prod.log.5" (hours 1)))
