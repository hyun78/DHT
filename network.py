import asyncio
import json
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

NETWORK_MAGIC_VALUE= "Sound body, sound code."
NETWORK_PORT = 19999
NETWORK_LISTEN_ADDR = "0.0.0.0" #or 127.0.0.1
NETWORK_UDP_MTU = 1024
NETWORK_BROADCAST_ADDR = "255.255.255.255"
EMULATE_BROADCAST = True
EMULATE_ADDR = "10.0.0.{num}"


class Network:
    class UDPListener(asyncio.DatagramProtocol):
        def __init__(self, network):
            self._network = network

        def datagram_received(self, data, addr):
            logging.debug("Packet received from {addr}, length {len}".format(addr=addr, len=len(data)))
            try:
                s = data.decode(encoding="utf-8", errors="strict")
                message = json.loads(s)
                if not "_magic" in message:
                    raise Exception("No magic value")
                if not message["_magic"] == NETWORK_MAGIC_VALUE:
                    raise Exception("Invalid magic value")
                self._network.message_arrived(message, addr)
            except UnicodeError as e:
                logging.warning("Invalid unicode character: " + str(e))
            #except Exception as e:
            #    logging.warning("Cannot parse packet: " + str(e))

        def error_received(self, err):
            logging.warning("Error received in UDPListener: " + str(err))

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        socket = loop.create_datagram_endpoint(
            lambda: self.UDPListener(self), local_addr=(NETWORK_LISTEN_ADDR, NETWORK_PORT),
            reuse_address=True, allow_broadcast=True,
        )

        (self._socket, _) = loop.run_until_complete(socket)

    def send_message(self, message, addr):
        if not "_magic" in message:
            message["_magic"] = NETWORK_MAGIC_VALUE
        s = json.dumps(message)
        try:
            b = s.encode(encoding='utf-8', errors='strict')
            if len(b) > NETWORK_UDP_MTU:
                logging.error("Too large send message: {orig} over {limit}".format(orig=len(b), limit=NETWORK_UDP_MTU))
            else:
                logging.debug("Sending {bytes} bytes of message".format(bytes=len(b)))
                if EMULATE_BROADCAST and addr[0] == NETWORK_BROADCAST_ADDR:
                    logging.debug("Emulation mode enabled")
                    for i in range(2, 255):
                        self._socket.sendto(b, (EMULATE_ADDR.format(num=i), addr[1]))
                else:
                    self._socket.sendto(b, addr)
        except Exception as e:
            logging.error("Cannot encode a send message: " + str(e))

    def message_arrived(self, message, addr):
        logging.debug("Message received from {addr}, {message}".format(addr=addr, message=message))

    def abort(self):
        self._socket.abort()

    def close(self):
        self._socket.close()


