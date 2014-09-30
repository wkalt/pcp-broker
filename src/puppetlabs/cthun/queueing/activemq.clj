(ns puppetlabs.cthun.queueing.activemq
  (:require [clojure.edn :as edn]
            [puppetlabs.puppetdb.mq :as mq]
            [puppetlabs.puppetdb.cheshire :as json]
            [clamq.protocol.consumer :as mq-cons]
            [clamq.protocol.connection :as mq-conn]
            [clojure.tools.logging :as log]
            [puppetlabs.cthun.queueing :refer [QueueingService]]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [puppetlabs.trapperkeeper.services :refer [service-context]]))

;; This is a bit rude/lazy, reaching right into puppetdb sources we've
;; copied into our tree.  If this proves out we should talk to
;; puppetdb about extracting puppetlabs.puppetdb.mq into a common library.

(defn queue-message
  [queues topic message]
  (let [mq-spec "vm://localhost?create=false"
        mq-endpoint topic]
    (log/info "queueing message" message)
    (with-open [conn (mq/activemq-connection mq-spec)]
      (mq/connect-and-publish! conn mq-endpoint (pr-str message)))))

(defn subscribe-to-topic
  [queues topic callback-fn]
  (let [mq-spec "vm://localhost?create=false"]
    (with-open [conn (mq/activemq-connection mq-spec)]
      (let [consumer (mq-conn/consumer conn
                                       {:endpoint   topic
                                        :on-message (fn [message]
                                                      (log/info "consuming message" (:body message))
                                                      (callback-fn (edn/read-string (:body message))))
                                        :transacted true
                                        :on-failure #(log/error "error consuming message" (:exception %))})]
        (mq-cons/start consumer)))))

(defservice queueing-service
  "activemq implementation of the queuing service"
  QueueingService
  []
  (init
   [this context]
   (log/info "Initializing activemq service")
   (assoc context :broker (mq/build-embedded-broker "tmp/activemq")))
  (start
   [this context]
   (let [broker (:broker (service-context this))]
     (log/info "Starting activemq")
     (mq/start-broker! broker)
     context))
  (stop
   [this context]
   (let [broker (:broker (service-context this))]
     (log/info "Stopping activemq")
     (mq/stop-broker! broker)
     context))

  (queue-message
   [this topic message]
   (queue-message (:broker (service-context this)) topic message))
  (subscribe-to-topic
   [this topic callback-fn]
   (subscribe-to-topic (:broker (service-context this)) topic callback-fn)))