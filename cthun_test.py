#!/usr/bin/env python

'''
WebSocket client based on ws4py

Initial design inspired by:
https://ws4py.readthedocs.org/en/latest/sources/clienttutorial/

Another library Autobahn worth checking:
http://autobahn.ws/python/
'''


import sys
import time
import logging
import argparse
from collections import namedtuple

try:
    from ws4py.client import WebSocketBaseClient
    from ws4py.manager import WebSocketManager
    from ws4py import format_addresses, configure_logger
except ImportError:
    print("ws4py is required to run cthun_ws_test "
          "(try 'sudo pip install ws4py')")
    sys.exit(1)


# Tokens

CONNECTION_CHECK_INTERVAL = 2      # [s]
SEND_INTERVAL             = 0.001  # [s]


# Globals

logger = None


# Errors

class RequestError(Exception):
    ''' Exception due to an invalid request '''
    pass


# Client

class EchoClient(WebSocketBaseClient):
    def __init__(self, url, mgr):
        self._mgr = mgr
        WebSocketBaseClient.__init__(self, url)

    def handshake_ok(self):
        logger.info("Opening %s" % format_addresses(self))
        self._mgr.add(self)

    def received_message(self, msg):
        logger.info("### Received a message: %s" % msg)



# Configuration

ScriptOptions = namedtuple('ScriptOptions', ['url',
                                             'concurrency',
                                             'interval',
                                             'verbose',
                                             'message',
                                             'num'])

SCRIPT_DESCRIPTION = '''WebSocket client to execute load tests.'''

def parseCommandLine(argv):
    # Define
    parser = argparse.ArgumentParser(description = SCRIPT_DESCRIPTION)
    parser.add_argument("url",
                        help = "server url (ex. ws//:localhost:8080/hello)"),
    parser.add_argument("concurrency",
                        help = "number of concurrent connections")
    parser.add_argument("-m", "--message",
                        help = "message to be sent to the server",
                        default = None)
    parser.add_argument("-n", "--num",
                        help = "number of messages to be sent",
                        default = 1)
    parser.add_argument("-i", "--interval",
                        help = "connection check interval; default: %d s"
                               % CONNECTION_CHECK_INTERVAL,
                        default = CONNECTION_CHECK_INTERVAL)
    parser.add_argument("-v", "--verbose",
                        help = "verbose",
                        action = "store_true")

    # Parse and validate
    args = parser.parse_args()

    try:
        concurrency = int(args.concurrency)
        if concurrency < 1:
            raise ValueError()
    except ValueError:
        raise RequestError('concurerncy must be a positive integer')

    if not args.url.startswith("ws"):
        raise RequestError('invalid url')

    try:
        interval = int(args.interval)
        if interval < 0:
            raise ValueError()
    except ValueError:
        raise RequestError('interval must be a positive integer')

    num = None
    if args.message:
        try:
            num = int(args.num)
            if num < 0:
                raise ValueError()
        except ValueError:
            raise RequestError('invalid number of messages')
    else:
        num = 0

    return ScriptOptions(args.url, concurrency, interval, args.verbose,
                         args.message, num)



# The actual script

def run(script_options):
    global logger
    level  = logging.DEBUG if script_options.verbose else logging.INFO
    logger = configure_logger(level = level)

    mgr = WebSocketManager()

    try:
        mgr.start()
        clients = []

        # Connect
        for connection_idx in range(script_options.concurrency):
            client = EchoClient(script_options.url, mgr)
            client.connect()
            clients.append(client)

        logger.info("%d clients are connected" % (connection_idx + 1))

        # Send
        if script_options.message is not None:
            logger.info("Sending messages")
            for client in clients:
                for _ in range(script_options.num):
                    client.send(script_options.message)
                    time.sleep(SEND_INTERVAL)

        # Sleep before disconnecting
        time.sleep(script_options.interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        logger.info("Disconnecting!")
        mgr.close_all()
        mgr.stop()
        mgr.join()


def main(argv = None):
    if argv is None:
        argv = sys.argv

    try:
        script_options = parseCommandLine(argv)
        print script_options
        run(script_options)
    except RequestError as e:
        print("Invalid request: %s" % e)
        return 1
    except Exception as e:
        logger.exception(str(e))
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())