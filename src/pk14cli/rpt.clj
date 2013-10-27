(ns pk14cli.rpt
 (:import (java.util Date Calendar)
          (java.lang Integer)))

; field id:
; 41 tid
; 42 mid
; 99 err msg
; 7 timestamp
; 11 transaction id
; 4 amount
; 39 error code
; 63 TLV

(def root (.getCanonicalFile (clojure.java.io/file "./..")))
(def HOST_TIME_DIFF 1)
; koliko vrstic prikazem
(def SHOW {"39" 0 ; #{telefonski broj nije u sistemu ili nije pripejd}
           "53" 2 ; #{proslo je regularno vreme za storniranje transakcije}
           "76" 2 ;#{Nije pronadjena originalna uspesna transakcija sa tim podacima}
           })

(defn calendar [year month day hour minutes seconds mseconds]
  (let [cal (Calendar/getInstance)]
    (.set cal Calendar/YEAR year)
    (.set cal Calendar/MONTH (- month 1))
    (.set cal Calendar/DAY_OF_MONTH day)
    (.set cal Calendar/HOUR_OF_DAY hour)
    (.set cal Calendar/MINUTE minutes)
    (.set cal Calendar/SECOND seconds)
    (.set cal Calendar/MILLISECOND mseconds)
    cal))

(defn ts [ts-log f7-ts ]
  (let [
        [_ & dat] (re-find #"(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)" f7-ts)
        year (.get ts-log Calendar/YEAR)
        dat2 (map #(java.lang.Integer/parseInt %) dat)
        [mon1, day, hour, minutes, seconds] dat2]    
    (calendar year mon1 day hour minutes seconds 0)))

(def files
  (let [
        lroot (clojure.java.io/file root "ssityy@lpetrol2/")
        isd (.isFile lroot)
        fls (.listFiles lroot)
        justLogs (filter #(and (.isFile %) (.. % getName (startsWith "log4j_pk14_prod.log"))) fls)
        ] justLogs))

; to je znak, da je ok, da ni bilo napake
(def fok "<field id=\"39\" value=\"00\"/>" )

(defn notok [x]
  "pogledamo, ce ima polje 39 razlicno od 00; kar pomeni napaka"
  (= -1 (.indexOf x fok)))

(defn parse [ts-log_iso]
  "isomsg v dict"
  (let [
        [ts-log iso] ts-log_iso
        key-val (re-seq #"id=\"(\d+)\"\s+value=\"(.*?)\"" iso)
        rec (reduce (fn [m tuple]
                    (assoc m
                       (nth tuple 1)
                       (nth tuple 2)))
                  {}
                  key-val)
        {f7-ts "7" f63-pcrec "63" } rec
        pc (if f63-pcrec (second(re-find #"PC0002(\d\d)" f63-pcrec)) "-1")
        fin (-> rec
            (assoc :ts-log ts-log)
            (assoc :pc pc)
            ;(assoc :ts ts-log))
            (assoc :ts (if f7-ts
                         (ts ts-log f7-ts)
                         ts-log)))
        ] fin ))

(defn extract-errors [c]
  "prebere ven <isomsg>..</isomsg> samo ce imajo napako, zraven pobira se letnico iz log timestampa"
  ;(println fok)
  (let [
        ; 2013-10-21 12:16:02,397(kar ne kaj krame )parse:\n<isomsg>....</isomsg>
        _msgs (re-seq #"(?s)((\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+))(?:[\p{Print}\x3f\p{Blank}]+parse:\s+)(<isomsg>.*?</isomsg>)" c)
        msgs (map (fn [x]
                    (let[dat (drop 2 (take 8 x))
                         isomsg (nth x 8)
                         dat2 (map #(java.lang.Integer/parseInt %) dat)
                         [year mon1 day hour minutes seconds] dat2                         
                         cal (calendar year mon1 day (+ hour HOST_TIME_DIFF) minutes seconds 0)]                      
                      (list cal isomsg))) _msgs)
        err (filter #(notok (second %)) msgs)
        ]  err))

(defn separate-by-err [res, err]
  (reduce (fn [m dict]
            (let [{k "39" msg "99" amount "4" } dict
                  {arr k} m
                  new (if arr (conj arr dict) [dict])
                  newm (assoc m k new)
                  ]newm))
            res
            err))

(def formater (java.text.SimpleDateFormat. "dd.MM.yyyy HH:mm:ss") )
(defn format-date
    [x]
  (.format formater (.getTime x)))

(defn prn [res]
  (doseq [x res]
    (let [
          [a b] x
          ; tukaj pogledam, da ni slucajno za isto kodovec opisov
          names (reduce (fn [s d] (conj s (get d "99"))) #{} b)  
          ; katere vrstce bo izpisat
          sample (take (get SHOW a 12) b)]
      (println (format "\n%s %s %s" a (count b) names))
      (doseq [s sample]              
              (println (format "%s, %s, %s, %s, %s, %s, %s" (format-date (:ts s)) a (:pc s)(get s "41") (get s "42") (get s "11") (get s "4")))))))

(defn proc-file [fname]
  "vrne array napak"
  (println "file" fname)
  (let [
        content (slurp fname)
        err (extract-errors content)
        parsed (map parse err)
       ] (doall parsed))) ; realizirat je treba sekvenco, da se lahko res izvaja paralelno

(defn process-files [files]
  (let [
    ; prebere napake iz fajlov, tole je tocka kjer se da paralizirat
    tm_start (System/currentTimeMillis)
    err (pmap proc-file files)
    ;posortira vse napaka gleda na timeptamp
    res (reduce separate-by-err {} err)
    tm_end (System/currentTimeMillis)
    _ (println "Parse time (ms):" (- tm_end tm_start) tm_start tm_end)

    sorted (reduce (fn [m tuple]
                     (let [[k arr] tuple]
                     (assoc m k (reverse(sort-by :ts arr)))))
                   {}
                     res)
    _ (prn sorted)
  ]))

; samo za razvoj procesiram tole
(process-files [(clojure.java.io/file root "a.log")])

(defn main []
  (println "root" root)
  (println "timestamp, rc, pc, tid, mid, transaction, amount")
  (process-files files)
  (shutdown-agents))
