(ns pk14cli.rpt2
 (:require
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
(defn parse-timestamp [timestamp] (parse (formatter "YYYY-MM-DD hh:mm:ss,SSS") timestamp))

;; fast check if content of isomsg has error_code = 0
(defn error-entry? [content] (neg? (.indexOf content "<field id=\"39\" value=\"00\"/>")))

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
         :server-timestamp plus (parse-timestamp timestamp) (hours host-time-diff))
       )
     pairs-with-errors)
    ))



(def a (slurp "/Users/ales/prj/jj/pk14cli/resources/a.log"))
(extract-errors a 1)
