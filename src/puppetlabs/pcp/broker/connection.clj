(ns puppetlabs.pcp.broker.connection
  (:require [clojure.string :as str]
            [puppetlabs.experimental.websockets.client :as websockets-client]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.pcp.protocol :as p]
            [schema.core :as s]
            [slingshot.slingshot :refer [throw+ try+]])
  (:import (clojure.lang IFn)))

(def Websocket
  "Schema for a websocket session"
  Object)

(def Codec
  "Message massaging functions"
  {:decode IFn
   :encode IFn})

(defprotocol ConnectionInterface
  "Operations on the Connection type"
  (summarize [connection]
    "Returns a ConnectionLog suitable for logging."))

(declare -summarize)

(s/defrecord Connection
             [websocket :- Websocket
              remote-address :- s/Str
              created-at :- p/ISO8601
              codec :- Codec
              common-name :- (s/maybe s/Str)
              uri :- (s/maybe p/Uri)]
  ConnectionInterface
  (summarize [c] (-summarize c)))

(def ConnectionLog
  "summarize a connection for logging"
  {:commonname (s/maybe s/Str)
   :remoteaddress s/Str})

(s/defn make-connection :- Connection
  "Return the initial state for a websocket"
  [websocket :- Websocket codec :- Codec]
  ;; NOTE(ale): the 'map->...' constructor comes from schema.core's defrecord
  (map->Connection
    {:websocket      websocket
     :codec          codec
     ;; NOTE(ale): Extract IP address and port out of the string representation
     ;; of the InetAddress instance ('hostname/socket' format), so that we
     ;; don't end up with a remote-address like '/0:0:0:0:0:0:0:1:56824'. See:
     ;; http://docs.oracle.com/javase/7/docs/api/java/net/InetAddress.html#getHostAddress%28%29
     ;; http://stackoverflow.com/questions/6932902/apache-mina-how-to-get-the-ip-from-a-connected-client
     :remote-address (try+
                       (second
                         (str/split (.. (websockets-client/remote-addr websocket) (toString)) #"/"))
                       (catch Exception _
                         ""))
     :common-name    (try+ (when-let [cert (first (websockets-client/peer-certs websocket))]
                             (ks/cn-for-cert cert))
                           (catch Exception _
                             nil))
     :created-at     (ks/timestamp)}))

(s/defn -summarize :- ConnectionLog
  [connection :- Connection]
  (let [{:keys [common-name remote-address]} connection]
    {:commonname common-name
     :remoteaddress remote-address}))
