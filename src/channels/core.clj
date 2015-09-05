(ns channels.core
  (:require [clojure.core.async :refer [<! >! <!! chan go sliding-buffer buffer timeout]]
            [qbits.alia :as alia]
            [qbits.hayt :as q]))

;; Data storage

;; Model

(defn create-response [op-id fqdn status]
  {:operation-id op-id
   :fqdn fqdn
   :status status})

(defn task-from-response [response]
  {:operation_id (str (:operation-id response))
   :status (name (:status response))
   :target (:fqdn response)})

(defn handle-response [system response]
  (let [new-task (task-from-response response)]
    (alia/execute-chan (:session system)
                  (q/insert :tasks
                            (q/values new-task)))))

(defn create-response-handling-channel [system]
  (let [ch (chan)]
    (go (let [resp (<! ch)]
          (handle-response system resp)))
    ch))

(comment (defn create-response-handling-channels [system n]
           (into [] (for [i (range n)]
                      (create-response-handling-channel system)))))

(defn send-response [system resp]
  (let [ch  (create-response-handling-channel system)]
    (go (>! ch resp))))

(defn count-tasks [system]
  (let [session (:session system)
        result (alia/execute session (q/select :tasks (q/columns (q/count*))))]
    (:count (first result))))

(defn find-task-by-operation-id [system op-id]
  (let [out-ch (chan)
        in-ch (alia/execute-chan (:session system) (q/select :tasks (q/where {:operation_id op-id})))]
    (go
      (>! out-ch (first (<! in-ch))))
    out-ch))

;; Main

(def cluster (alia/cluster {:contact-points ["localhost"]}))
(def session (alia/connect cluster))
(alia/execute session "USE clj_channels;")
(defn cassandra-shutdown []
  (alia/shutdown cluster))

(def system {:session session})

(defn find-by-opid-sync [op-id]
  (let [task-ch (find-task-by-operation-id system op-id)]
    (<!! task-ch)))

(defn test-performance [n]
  (do
    (time
     (dotimes [i n]
       (send-response system (create-response i "RNC-69" :success))))
    (time
     (while (< (count-tasks system) n)
       (Thread/sleep 500)))))

