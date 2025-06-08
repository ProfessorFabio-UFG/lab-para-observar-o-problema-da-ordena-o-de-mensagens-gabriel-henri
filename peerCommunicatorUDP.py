from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
from queue import PriorityQueue

handShakeCount = 0
PEERS = []
logical_clock = 0  # [MODIFICADO] Relógio lógico de Lamport
myself = -1        # [MODIFICADO] ID do processo
ACKs = {}          # [MODIFICADO] Armazena ACKs recebidos
message_queue = PriorityQueue()  # [MODIFICADO] Fila de mensagens com (timestamp, sender_id, msg_number)


sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  clientSock.send(pickle.dumps(req))
  msg = clientSock.recv(2048)
  clientSock.close()
  return pickle.loads(msg)

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    global handShakeCount, logical_clock, ACKs, message_queue
    logList = []
    stopCount = 0

    while handShakeCount < N:
      msgPack, _ = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        handShakeCount += 1

    while True:
      msgPack, sender = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)

      if msg[0] == -1:
        stopCount += 1
        if stopCount == N:
          break

      elif msg[0] == 'MSG':  # [MODIFICADO]
        ts, sender_id, msg_number = msg[1:]
        logical_clock = max(logical_clock, ts) + 1
        ACKs[(sender_id, msg_number)] = ACKs.get((sender_id, msg_number), set())
        ACKs[(sender_id, msg_number)].add(myself)

        # Reenvia ACK para todos
        ack = pickle.dumps(('ACK', sender_id, msg_number, logical_clock))
        for addr in PEERS:
          sendSocket.sendto(ack, (addr, PEER_UDP_PORT))

        # Adiciona à fila
        message_queue.put((ts, sender_id, msg_number))

      elif msg[0] == 'ACK':  # [MODIFICADO]
        _, sender_id, msg_number, ack_ts = msg
        logical_clock = max(logical_clock, ack_ts) + 1
        ACKs[(sender_id, msg_number)] = ACKs.get((sender_id, msg_number), set())
        ACKs[(sender_id, msg_number)].add(sender)

      # Verifica mensagens entregáveis
      while not message_queue.empty():
        ts, sender_id, msg_number = message_queue.queue[0]
        if len(ACKs.get((sender_id, msg_number), set())) == N + 1:  # N + eu
          message_queue.get()
          print(f'Mensagem ordenada: {msg_number} de {sender_id}')
          logList.append((sender_id, msg_number))
        else:
          break

    # Envia o log ao servidor
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    clientSock.send(pickle.dumps(logList))
    clientSock.close()

    with open(f'logfile{myself}.log', 'w') as logFile:
      logFile.writelines(str(logList))

    handShakeCount = 0
    exit(0)

def waitToStart():
  (conn, _) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  conn.send(pickle.dumps('Peer process '+str(msg[0])+' started.'))
  conn.close()
  return msg

# MAIN EXECUTION
registerWithGroupManager()
while True:
  print('Esperando sinal para iniciar...')
  myself, nMsgs = waitToStart()
  if nMsgs == 0:
    exit(0)

  time.sleep(5)
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()

  PEERS = getListOfPeers()

  # Envia handshakes
  for addrToSend in PEERS:
    msg = ('READY', myself)
    sendSocket.sendto(pickle.dumps(msg), (addrToSend, PEER_UDP_PORT))

  while (handShakeCount < N):
    pass

  # Envia mensagens com relógio lógico
  for msgNumber in range(nMsgs):
    time.sleep(random.uniform(0.01, 0.1))
    logical_clock += 1
    msg = ('MSG', logical_clock, myself, msgNumber)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print(f'Sent MSG {msgNumber} at time {logical_clock}')

  # Envia fim
  for addrToSend in PEERS:
    sendSocket.sendto(pickle.dumps((-1, -1)), (addrToSend, PEER_UDP_PORT))
