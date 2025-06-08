from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
from queue import PriorityQueue

# Variáveis globais
handShakeCount = 0
PEERS = [] # Lista de IPs de outros peers
logical_clock = 0
myself = -1        # ID do processo (0 a N-1)
ACKs = {}          # Armazena ACKs recebidos: {(sender_id, msg_number): {set_of_ack_senders_ips}}
message_queue = PriorityQueue()  # Fila de mensagens com (timestamp, sender_id, msg_number)

# Locks para variáveis compartilhadas
lock = threading.Lock()
start_event = threading.Event() # Para sincronizar o início do envio de mensagens

# N deve ser definido globalmente e atualizado quando o servidor informar
N = -1 # Número total de peers no grupo. Será atualizado pelo servidor.

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
  return pickle.loads(msg) # Retorna uma lista de IPs

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    global handShakeCount, logical_clock, ACKs, message_queue, N
    logList = []
    stopCount = 0

    print(f"Peer {myself}: MsgHandler started. Waiting for {N} handshakes.")

    # Loop para aguardar os handshakes 'READY'
    while True:
      msgPack, sender_addr = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)

      if msg[0] == 'READY':
        with lock:
          handShakeCount += 1
          print(f"Peer {myself}: Received READY from {sender_addr[0]}. handShakeCount = {handShakeCount}/{N}")
          if handShakeCount == N: # N é o número total de peers
            start_event.set() # Sinaliza que todos os peers estão prontos
            print(f"Peer {myself}: All {N} peers are READY. Signaled start_event.")
            break # Sai do loop de handshake

      # Se alguma outra mensagem chegar antes dos handshakes, ela será ignorada
      # ou pode ser tratada como um erro/colocada em uma fila temporária.
      # Para este exemplo, assumimos que 'READY' chegam primeiro.
      else:
        print(f"Peer {myself}: Received non-READY message during handshake phase: {msg}")

    print(f"Peer {myself}: Handshake phase complete. Entering message processing loop.")

    # Loop principal para processar mensagens e ACKs
    while True:
      msgPack, sender_addr = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)

      with lock: # Proteger o acesso a variáveis globais compartilhadas
        if msg[0] == -1: # Sinal de término
          stopCount += 1
          print(f"Peer {myself}: Received STOP signal from {sender_addr[0]}. stopCount = {stopCount}/{N}")
          if stopCount == N: # Todos os N peers enviaram o sinal de término
            print(f"Peer {myself}: All peers signaled stop. Exiting message processing loop.")
            break # Sai do loop principal

        elif msg[0] == 'MSG':
          ts, sender_id, msg_number = msg[1:]
          logical_clock = max(logical_clock, ts) + 1 # Atualiza relógio lógico
          
          # Adiciona o ACK do próprio peer
          ACKs[(sender_id, msg_number)] = ACKs.get((sender_id, msg_number), set())
          ACKs[(sender_id, msg_number)].add(myself) # Usamos myself (ID do peer) como identificador no ACK set

          # Envia ACK para todos os peers (incluindo a si mesmo)
          ack = pickle.dumps(('ACK', sender_id, msg_number, logical_clock))
          for addr_ip in PEERS: # PEERS contém apenas IPs
            sendSocket.sendto(ack, (addr_ip, PEER_UDP_PORT))
          print(f'Peer {myself}: Sent ACK for MSG {msg_number} from {sender_id} at time {logical_clock}.')

          # Adiciona a mensagem à fila de prioridade
          message_queue.put((ts, sender_id, msg_number))

        elif msg[0] == 'ACK':
          _, sender_id, msg_number, ack_ts = msg
          logical_clock = max(logical_clock, ack_ts) + 1 # Atualiza relógio lógico
          
          # Se o sender_addr[0] (IP) não é um ID do peer, é um problema.
          # Idealmente, o ACK deveria incluir o ID do peer que o enviou, não apenas o IP de origem do pacote UDP.
          # Para simplificar, vou usar o ID do peer `myself` para o próprio ACK e o IP do remetente para os demais.
          # Melhor seria se o ACK contivesse o ID do peer remetente.
          # Por enquanto, assumimos que `myself` e `sender_id` (nos ACKs) são os IDs dos peers.
          # Ajuste para usar IDs: Se o ACK contém o ID do peer remetente, use-o.
          # Caso contrário, você pode tentar mapear IPs para IDs ou usar IPs no conjunto de ACKs.
          # Aqui, vou usar o ID do peer que *envia* o ACK, assumindo que esteja em `msg[1]`.
          # Como seu ACK original era ('ACK', sender_id_orig_msg, msg_number_orig_msg, ack_ts)
          # Precisamos adicionar o ID do peer que enviou ESTE ACK.
          # Vou assumir que o GroupManager pode fornecer IDs ou que o peer que envia o ACK inclua seu ID.
          # Para este modelo, se `sender_addr[0]` é o IP do remetente, e `PEERS` é uma lista de IPs,
          # então usar `sender_addr[0]` no conjunto de ACKs é consistente.

          # ACKs[(sender_id, msg_number)] = ACKs.get((sender_id, msg_number), set())
          # ACKs[(sender_id, msg_number)].add(sender_addr[0]) # Adiciona o IP do remetente do ACK
          
          # Melhoria: O ACK deve conter o ID do peer que enviou o ACK.
          # Supondo que 'ACK' agora seja ('ACK', original_sender_id, original_msg_number, ack_ts, ack_sender_id)
          # Se não for o caso, a contagem de N ACKs pode ser problemática.
          # Por enquanto, manteremos a lógica de usar myself no ACK para si mesmo e sender_addr[0] para outros.
          # A forma mais robusta é que o ACK tenha ('ACK', original_sender_id, original_msg_number, ack_ts, ID_DO_REMETENTE_DO_ACK)
          # Para manter compatibilidade com seu ACK original:
          # `sender_id` é o ID do peer que enviou a mensagem original (MSG).
          # `sender_addr[0]` é o IP do peer que enviou ESTE ACK.
          # Precisamos de uma forma de mapear `sender_addr[0]` para um ID de peer para contagem.
          # Ou simplesmente contar ACKs por IP. Se N é o número de IPs em PEERS + 1 (para o próprio),
          # então contar IPs pode funcionar.

          # Correção: O set() deve conter IDs dos peers, não IPs diretamente, se `N` for sobre IDs.
          # A maneira mais simples é ter o ACK conter o ID do remetente do ACK.
          # Ex: ack = pickle.dumps(('ACK', sender_id, msg_number, logical_clock, myself))
          # Se o ACK recebido for assim: `_, sender_id_orig, msg_number_orig, ack_ts, ack_sender_id = msg`
          # Então: `ACKs[(sender_id_orig, msg_number_orig)].add(ack_sender_id)`
          
          # Por enquanto, vamos manter a lógica atual de IDs e IPs, mas essa é uma área para refatorar.
          # Para o POC, o `add(myself)` para o próprio ACK e depois os IPs dos outros pode funcionar se N é o total de peers.
          # Se N é o número total de peers, e cada peer inclui o IP do remetente no set, então:
          # ACKs[(sender_id, msg_number)] = ACKs.get((sender_id, msg_number), set())
          # ACKs[(sender_id, msg_number)].add(sender_addr[0])
          # Isso exigiria que N seja a contagem de IPs que o server espera.

          # Vamos assumir que o 'myself' é um ID único para cada peer (0, 1, 2...)
          # E que os ACKs de outros peers serão identificados por seu `myself` ID.
          # Para isso, o ACK precisaria enviar o ID do remetente do ACK.
          # Ou, simplificando, se `N` é o número total de peers no grupo (incluindo eu),
          # e eu já adicionei `myself` no set, os outros `N-1` ACKs virão de IPs distintos.
          # Vamos modificar o envio do ACK para incluir o ID do remetente:
          # `ack = pickle.dumps(('ACK', sender_id, msg_number, logical_clock, myself))` (no MSG handler)

          # E o recebimento do ACK:
          if len(msg) == 5: # Verifica se o ACK tem o ID do remetente
              _, original_sender_id, original_msg_number, ack_ts, ack_sender_id = msg
              ACKs[(original_sender_id, original_msg_number)] = ACKs.get((original_sender_id, original_msg_number), set())
              ACKs[(original_sender_id, original_msg_number)].add(ack_sender_id) # Adiciona o ID do peer que enviou o ACK
              print(f'Peer {myself}: Received ACK for MSG {original_msg_number} from {original_sender_id} (ACK by {ack_sender_id}) at time {logical_clock}.')
          else:
              print(f"Peer {myself}: Received malformed ACK message: {msg}")


        # Verifica mensagens entregáveis
        while not message_queue.empty():
          ts, sender_id, msg_number = message_queue.queue[0]
          # N é o número total de peers (incluindo eu).
          # Esperamos N ACKs, um de cada peer no grupo.
          if len(ACKs.get((sender_id, msg_number), set())) == N:
            message_queue.get() # Remove da fila
            print(f'Peer {myself}: Delivered message: {msg_number} from {sender_id}.')
            logList.append((sender_id, msg_number))
          else:
            break # A mensagem no topo da fila ainda não tem todos os ACKs ou não é a próxima a ser entregue

    # Envia o log ao servidor
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    clientSock.send(pickle.dumps(logList))
    clientSock.close()
    print(f"Peer {myself}: Sent log to server. Log length: {len(logList)}")

    with open(f'logfile{myself}.log', 'w') as logFile:
      logFile.writelines(str(logList))
    print(f"Peer {myself}: Log saved to logfile{myself}.log")

    # Reinicializar variáveis globais para o próximo ciclo, se houver
    with lock:
      handShakeCount = 0
      start_event.clear() # Limpa o evento para o próximo ciclo
      ACKs.clear() # Limpa ACKs para o próximo ciclo
      # message_queue.queue.clear() # Não há um método clear, mas pode ser re-instanciada
      # Ou, como a fila deve estar vazia após entrega, não é estritamente necessário.
      # Reinstanciar para garantia: message_queue = PriorityQueue()
    
    print(f"Peer {myself}: MsgHandler thread finished its cycle. Ready for next.")
    # A thread não chama exit(0) para permitir múltiplos ciclos.

def waitToStart():
  global N # Declarar N como global para poder modificá-lo
  (conn, _) = serverSock.accept()
  msgPack = conn.recv(1024)
  # msg agora conterá (peer_id, n_msgs_to_send, total_peers_in_group)
  peer_id, nMsgs, total_peers_in_group = pickle.loads(msgPack)
  N = total_peers_in_group # Atualiza o N global
  print(f"Peer {peer_id}: Received start signal. Will send {nMsgs} messages in a group of {N} peers.")
  conn.send(pickle.dumps(f'Peer process {peer_id} started.'))
  conn.close()
  return peer_id, nMsgs

# MAIN EXECUTION
registerWithGroupManager()
# Criar a thread MsgHandler uma vez e deixá-la rodando para vários ciclos
msgHandler_thread = MsgHandler(recvSocket)
msgHandler_thread.start()

while True:
  print('Esperando sinal para iniciar um novo ciclo...')
  # myself e nMsgs são atualizados a cada ciclo
  myself, nMsgs = waitToStart()
  if nMsgs == 0:
    print(f"Peer {myself}: Received signal to terminate. Exiting.")
    # Sinaliza para a thread MsgHandler terminar se precisar
    # (por exemplo, enviando um pacote (-1, -1) para si mesmo)
    # Por simplicidade, assumimos que o processo será encerrado.
    break # Sai do loop principal e o processo será encerrado

  time.sleep(2) # Pequeno atraso para garantir que todos os peers estejam prontos para receber

  PEERS_FROM_GM = getListOfPeers()
  # Filtra o próprio IP da lista de peers para não enviar para si mesmo
  # (Se PEERS contém apenas IPs de *outros* peers, a lógica de N ACKs precisa ser N+1 no peer)
  # Se N é o total de peers, e cada um envia ACKs (inclusive a si mesmo), o count é N.
  # Portanto, PEERS deve ser a lista de IPs de TODOS os peers, incluindo a si mesmo,
  # ou a lógica de loop de envio deve se ajustar.
  # Para simplificar, PEERS será a lista de IPs obtida do GM, e eu enviarei para todos nessa lista.
  PEERS = PEERS_FROM_GM

  # Envia handshakes 'READY' para todos os peers (incluindo a si mesmo se estiver na lista)
  # O próprio peer também conta seu 'READY' localmente no handshakeCount
  for addrToSend in PEERS:
    msg = ('READY', myself)
    sendSocket.sendto(pickle.dumps(msg), (addrToSend, PEER_UDP_PORT))

  # Lógica para o próprio peer contar seu handshake
  with lock:
    handShakeCount += 1 # Conta o próprio handshake
    if handShakeCount == N:
      start_event.set()

  # Espera o evento de início ser setado (todos os peers estão prontos)
  print(f'Peer {myself}: Waiting for all peers to be READY ({N} total).')
  start_event.wait()
  print(f'Peer {myself}: All peers are READY. Starting message sending.')

  # Envia mensagens com relógio lógico
  for msgNumber in range(nMsgs):
    time.sleep(random.uniform(0.01, 0.1)) # Atraso aleatório para simular tráfego
    with lock:
      logical_clock += 1 # Incrementa o relógio lógico
      current_ts = logical_clock
    
    # Formato da mensagem: ('MSG', timestamp, sender_id, msg_number)
    msg = ('MSG', current_ts, myself, msgNumber)
    msgPack = pickle.dumps(msg)
    
    # Envia para todos os peers na lista (incluindo a si mesmo, se o IP estiver na lista PEERS)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print(f'Peer {myself}: Sent MSG {msgNumber} at time {current_ts}')

  # Envia sinal de término para todos os peers
  print(f"Peer {myself}: Finished sending {nMsgs} messages. Sending STOP signals.")
  for addrToSend in PEERS:
    sendSocket.sendto(pickle.dumps((-1, -1)), (addrToSend, PEER_UDP_PORT))

  # Não faz um `msgHandler_thread.join()` aqui. A thread continua a processar.
  # As variáveis globais (ACKs, message_queue, handShakeCount) são resetadas
  # no final do `MsgHandler.run()`, quando ele detecta `N` sinais de término.
  # O `start_event` também é limpo, preparando para o próximo ciclo.
  print(f"Peer {myself}: Cycle complete. Waiting for MsgHandler to finish processing.")