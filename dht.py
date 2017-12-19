import asyncio

import network
import timer
from enum import Enum
import logging
import datetime
import time

import cli
import hashlib

_SHORT = datetime.timedelta(seconds=1)
_LONG = datetime.timedelta(seconds=5)
_MARGIN = 2
_REPEAT = _MARGIN * (_LONG / _SHORT)
# input : python string
# output : hex string without 0x
def hashfunc(msg):
    msg = msg.encode()
    m = hashlib.sha512()
    m.update(msg)
    hashed_msg = m.digest()
    int_msg = int.from_bytes(hashed_msg,byteorder='big')
    hashval = hex(int_msg)[2:]
    return hashval

class DHT(network.Network, timer.Timer): #상속 받음 
    class State(Enum):
        START = 1
        MASTER = 2
        SLAVE = 3
        CLI = 4
    def key_insertion(self,key,value):
        hashval = hashfunc(key)
        try :
            if (key in self.table[hashval].keys()):
                logging.info("insertion failed : duplicate insertion")
            else:
                self.table[hashval][key] = (key,value)
                logging.info("insertion successed")
                return True
        except:
            self.table[hashval]= {}
            self.table[hashval][key] = (key,value)
        return False
    def key_deletion(self):

        pass

    def update_peer_list(self):

        for (_, timer) in self._context.heartbeat_timer.items(): # 타이머 초기화 하자
            timer.cancel()
        self._context.heartbeat_timer.clear()
        self._context.timestamp = time.time() #타이머 다시시작

        message = {
            "type": "leader_is_here",
            "uuid": self.uuid,
            "timestamp": self._context.timestamp,
            "peer_count": len(self._context.peer_list) + 1 #자신을 포함함
        }
        self.send_message(message, (network.NETWORK_BROADCAST_ADDR, network.NETWORK_PORT)) #위의 메시지를 브로드캐스트 한다 

        index = 0
        self._context.peer_index = {} #마스터도 가지고 있자.
        for (uuid, addr) in self._context.peer_list: #피어 하나하나에게 보내는 메시지 
            self._context.heartbeat_timer[uuid] = \
                self.async_trigger(lambda: self.master_heartbeat_timeout(uuid), _LONG / 2) #타이머 설정
            index += 1
            message = {
                "type": "peer_list",
                "uuid": self.uuid,
                "timestamp": self._context.timestamp,
                "peer_index": index, #피어마다 인덱스 부여 
                "peer_uuid": uuid,
                "peer_addr": addr,
            }
            self._context.peer_index[index] = (uuid,addr)#마스터도 가지고 있자.
            self.send_message(message, (network.NETWORK_BROADCAST_ADDR, network.NETWORK_PORT))

    def message_arrived(self, message, addr):
        if message["uuid"] == self.uuid:
            return
        logging.debug("Message received from {addr}, {message}".format(addr=addr, message=message))

        if message["type"] == "hello":
            if self._state == self.State.START:
                self._context.messages.append((message, addr))
            elif self._state == self.State.MASTER:
                if not (message["uuid"], addr) in self._context.peer_list:
                    self._context.peer_list.append((message["uuid"], addr))
                    self._context.peer_list.sort(reverse=True)
                    self.update_peer_list()

                    self.master_peer_list_updated()
        elif message["type"] == "heartbeat_ping":
            message = {
                "type": "heartbeat_pong",
                "uuid": self.uuid,
                "timestamp": time.time(),
            }
            self.send_message(message, addr)
        elif message["type"] == "heartbeat_pong":
            if self._state == self.State.MASTER:
                client_uuid = message["uuid"]
                if client_uuid in self._context.heartbeat_timer:
                    prev = self._context.heartbeat_timer[client_uuid]
                    prev.cancel()
                    self._context.heartbeat_timer[client_uuid] = \
                        self.async_trigger(lambda: self.master_heartbeat_timeout(client_uuid), _LONG/2)
            elif self._state == self.State.SLAVE:
                master_uuid = message["uuid"]
                if self._context.master_uuid == master_uuid:
                    self._context.heartbeat_timer.cancel()
                    self._context.heartbeat_timer = self.async_trigger(self.slave_heartbeat_timeout, _LONG/2)
        elif message["type"] == "leader_is_here":
            if self._state == self.State.START or \
                    (self._state == self.State.SLAVE and self._context.master_timestamp < message["timestamp"]):
                self._context.cancel()
                self._state = self.State.SLAVE
                self._context = self.SlaveContext()
                self._context.master_uuid = message["uuid"]
                self._context.master_addr = addr
                self._context.peer_count = int(message["peer_count"])
                self._context.master_timestamp = message["timestamp"]
                asyncio.ensure_future(self.slave(), loop=self._loop)
                pass
        elif message["type"] == "peer_list":
            if self._state == self.State.SLAVE:
                if self._context.master_timestamp == message["timestamp"]:
                    self._context.peer_index[message["peer_index"]] = (message["peer_uuid"], message["peer_addr"])

                    if (len(self._context.peer_index) + 1) == self._context.peer_count: 
                        self._context.peer_list = []
                        for i in range(1, self._context.peer_count):
                            self._context.peer_list.append(self._context.peer_index[i])
                        self.slave_peer_list_updated()
        elif message["type"] == "search":
            logging.info("Client request: search")

            pass
        elif message["type"] == "insert":
            logging.info("Client request: insert")
            ####################################
            #validation check
            key_val = message['key']
            value = message['value']
            hash_val = hashfunc(key_val)
            #get my index
            idx = int(hash_val,16) // len(self._context.peer_count)
            my_idx = None
            if (idx == my_idx): # 내가 바로 주인인 경우
                #내 테이블에 저장하고, 내 주변 테이블에 복제한다.
                if (self.key_insertion(key_val,value)):
                    dup_message = {
                        'type' : 'duplication',
                        'uuid' : self.uuid,
                        'key' : key_val,
                        'value': value
                    }
                    neer_idx = (idx+1) % len(self._context.peer_index)
                    addr = self._context.peer_index[neer_idx]
                    self.send_message(dup_message,addr)
                    neer_idx = (idx-1) % len(self._context.peer_index)
                    addr = self._context.peer_index[neer_idx]
                    self.send_message(dup_message,addr)
            else: # 주인이 아닌 경우, 주인에게 전송해 준다.
                msg = {
                    'type': 'insertion',
                    'uuid': self.uuid,
                    'key': key_val,
                    'value': value
                }
                addr = self._context.peer_index[idx]
                self.send_message(msg,addr)
            ####################################
            pass
        elif message["type"] == "delete":
            logging.info("Client request: delete")
            pass
        elif message["type"] =="duplication":
            logging.info("Client request: duplication")
            #dup메시지의 경우 묻지도 따지지도 않고 그냥 저장한다
            key_val = message['key']
            value = message['value']
            self.key_insertion(key_val,value)
            pass
        elif message['type'] == "CLI_connect":
            logging.info("Client request: CLI_connect")
            if (self._state!=self.State.START):
                message['type'] = 'CLI_response'
                message['uuid'] = self.uuid
                message['peers'] = self._context.peer_list
                message['peer_index'] = self._context.peer_index
                self.send_message(message,addr)
                logging.info("sended cli response")
            else:
                logging.info("start context node should not send cli response")

            pass
        elif message['type'] =="CLI_response":
            logging.info("Client request: CLI_response")
            logging.info("uuid : {uuid}".format(uuid=message['uuid']))
            logging.info("peers : {peers}".format(peers=message['peers']))
            self._context.node_info.append(message['peer_index'])
            
            broad_cast_addr = (network.NETWORK_BROADCAST_ADDR,network.NETWORK_PORT)
            message ={
                'type':'CLI_connect',
                'uuid': self.uuid
            }
            
            #self.send_message(message,broad_cast_addr)
            pass

    def master_peer_list_updated(self):
        logging.info("Peer list updated: I'm MASTER with {peers} peers".format(peers=len(self._context.peer_list)))
        for (uuid, addr) in self._context.peer_list:
            logging.info("Peer list updated: PEER[{peer}]".format(peer=str((uuid, addr))))

    def slave_peer_list_updated(self):
        logging.info("Peer list updated: MASTER[{master}] with {peers} peers".format(
            master=str((self._context.master_uuid, self._context.master_addr)), peers=len(self._context.peer_list)))
        for (uuid, addr) in self._context.peer_list:
            logging.info("Peer list updated: PEER[{peer}]".format(peer=str((uuid,addr))))

    async def slave_heartbeat_timeout(self):
        if self._context.heartbeat_send_job is not None:
            self._context.heartbeat_send_job.cancel()
        self._state = self.State.START
        self._context = self.StartContext()
        asyncio.ensure_future(self.start(), loop=self._loop)

    async def master_heartbeat_timeout(self, client_uuid):
        client = None
        for (uuid, addr) in self._context.peer_list:
            if uuid == client_uuid:
                client = (uuid, addr)
        self._context.peer_list.remove(client)
        self.update_peer_list()
        self.master_peer_list_updated()

    class StartContext:
        def __init__(self):
            self.hello_job = None
            self.timeout_job = None
            self.messages = []

        def cancel(self):
            if self.hello_job is not None:
                self.hello_job.cancel()
            if self.timeout_job is not None:
                self.timeout_job.cancel()
            pass

    class MasterContext:
        def __init__(self):
            self.peer_list = []
            self.timestamp = time.time()
            self.heartbeat_send_job = None
            self.heartbeat_timer = {}
            self.peer_index = {}
        def cancel(self):
            if self.heartbeat_send_job is not None:
                self.heartbeat_send_job.cancel()
            for (_, timer) in self.heartbeat_timer.items():
                timer.cancel()
            pass

    class SlaveContext:
        def __init__(self):
            self.peer_list = []
            self.peer_index = {}
            self.peer_count = 0
            self.master_addr = None
            self.master_uuid = None
            self.master_timestamp = None
            self.heartbeat_send_job = None
            self.heartbeat_timer = None

        def cancel(self):
            if self.heartbeat_send_job is not None:
                self.heartbeat_send_job.cancel()
            if self.heartbeat_timer is not None:
                self.heartbeat_timer.cancel()
            pass

    async def master(self):
        async def heartbeat_send():
            for (_, addr) in self._context.peer_list:
                message = {
                    "type": "heartbeat_ping",
                    "uuid": self.uuid,
                    "timestamp": time.time(),
                }
                self.send_message(message, addr)
        self._context.heartbeat_send_job = self.async_period(heartbeat_send, _SHORT)
        pass

    async def slave(self):
        async def heartbeat_send():
            message = {
                "type": "heartbeat_ping",
                "uuid": self.uuid,
                "timestamp": time.time(),
            }
            self.send_message(message, self._context.master_addr)

        self._context.heartbeat_timer = self.async_trigger(self.slave_heartbeat_timeout, _LONG / 2)
        self._context.heartbeat_send_job = self.async_period(heartbeat_send, _SHORT)
        pass

    async def start(self):
        self._context = self.StartContext()
        async def hello():
            message = {
                "type": "hello",
                "uuid": self.uuid,
            }
            logging.debug("Sending HELLO message")
            self.send_message(message, (network.NETWORK_BROADCAST_ADDR, network.NETWORK_PORT))

        async def timeout():
            self._context.hello_job.cancel()
            logging.info("Cannot find any existing leader.")
            if len(self._context.messages) == 0:
                logging.info("Cannot find any peer. I am the leader.")
                self._state = self.State.MASTER
                self._context = self.MasterContext()
                asyncio.ensure_future(self.master(), loop=self._loop)
            else: #리더 선출 
                max_val = self.uuid
                max_addr = None
                unique_addr = set()
                for (message, addr) in self._context.messages:
                    if message["uuid"] > max_val:
                        max_val = message["uuid"]
                        max_addr = addr
                    if message["uuid"] != self.uuid:
                        unique_addr.add((message["uuid"], addr))
                if max_addr is None:
                    #I am the leader
                    sorted_list = list(unique_addr)
                    sorted_list.sort(reverse=True)
                    self._context = self.MasterContext()
                    self._state = self.State.MASTER
                    self._context.peer_list = sorted_list
                    asyncio.ensure_future(self.master(), loop=self._loop)
                    logging.info("I am the leader of {peers} peers".format(peers=len(sorted_list)))
                else:
                    #I am the slave
                    #self._context = self.SlaveContext()
                    #self._state = self.State.SLAVE
                    pass

            if self._state == self.State.MASTER:
                self.update_peer_list()
                self.master_peer_list_updated()

        self._context.hello_job = self.async_period(hello, _SHORT)
        self._context.timeout_job = self.async_trigger(timeout, _LONG)

        pass

    def __init__(self, loop,cli_option=False):
        if not cli_option:
            network.Network.__init__(self, loop)
            timer.Timer.__init__(self, loop)
            self._state = self.State.START
            self._loop = loop
            self._context = None
            ##############inserted code here#####################
            self.table = {} # key-value 정보
            #ex 
            # key = LeeKH , value = CHEOGO
            # hashval = hashfunc(key)
            # try :
            #   table[hashval][key] = 
            # except:
            #   table[hashval]= {}
            #   table[hashval][key] = value
            #######################end###########################
            import uuid
            self.uuid = str(uuid.uuid1())

            asyncio.ensure_future(self.start(), loop=self._loop)
        else:
            network.Network.__init__(self, loop)
            timer.Timer.__init__(self, loop)
            self._state = self.State.CLI
            self._loop = loop
            self._context = None
            ##############inserted code here#####################
            self.table = {} # key-value 정보
            #ex 
            # key = LeeKH , value = CHEOGO
            # hashval = hashfunc(key)
            # try :
            #   table[hashval][key] = 
            # except:
            #   table[hashval]= {}
            #   table[hashval][key] = value
            #######################end###########################
            import uuid
            self.uuid = str(uuid.uuid1())
            async def cli_start():
                self._context = self.CLI_Context()
                #모든 노드 검사 
                broad_cast_addr = (network.NETWORK_BROADCAST_ADDR,network.NETWORK_PORT)
                message ={
                    'type':'CLI_connect',
                    'uuid': self.uuid
                }
                self.send_message(message,broad_cast_addr) #모든 노드에 보내기
                asyncio.ensure_future(self.cli(),loop=self._loop)

            asyncio.ensure_future(cli_start(),loop=self._loop)
    class CLI_Context:
        def __init__(self):
            self.node_info= []
            pass


    async def cli(self):

        print("Starting CLI ... ")
        while True:
            print("v : view all nodes \n c : connect to nodes with index \n q: quit")
            option_ = input("type a message type \n")
            if (option_=='v'):
                print("Available nodes")
                print(self._context.node_info)
                pass
            elif (option_=='c'):
                print("Available nodes")
                print(self._context.node_info)            
                nidx = input("type a node index\n")
                flag = True
                idx = 0
                for i in range(len(self._context.node_info)):
                    if nidx in self._context.node_info[i].keys():
                        print("valid node index")
                        flag = False
                        idx = i
                        break
                if flag:
                    print("invalid node index")
                    continue
                addr = self._context.node_info[idx][nidx]
                print("options \n i : insert \n s :search \n d : deletion \n ")
                if (option_=='i'):
                    key_val = input("type key \n")
                    value = input("type value \n")
                    msg = {
                        'type':'insertion',
                        'uuid':self.uuid,
                        'key':key_val,
                        'value':value
                    }
                    self.send_message(msg,addr)
                elif (option_=='s'):
                    pass
                elif (option_=='d'):
                    pass
                else:
                    print("not implemented option")
            elif (option_=='q'):
                break
            else:
                print("not implemented option")
        return 
