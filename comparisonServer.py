from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)
print(f"Server listening on port {SERVER_PORT}...")

def mainLoop():
    while True:
        nMsgs = promptUser()
        if nMsgs == 0:
            print("Terminating server...")
            break

        # Conecta ao GroupManager para obter a lista de peers
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op":"list"}
        msg = pickle.dumps(req)
        clientSock.send(msg)
        msg = clientSock.recv(2048)
        clientSock.close()
        peerList = pickle.loads(msg)
        print(f"List of Peers: {peerList}")

        # Determina o número total de peers para este ciclo
        num_peers_in_cycle = len(peerList)
        if num_peers_in_cycle == 0:
            print("No peers registered. Please register peers before starting a cycle.")
            continue

        # Inicia os peers, passando o número total de peers
        startPeers(peerList, nMsgs, num_peers_in_cycle)
        print('Now, wait for the message logs from the communicating peers...')

        # Espera pelos logs e compara, usando o número total de peers
        waitForLogsAndCompare(nMsgs, num_peers_in_cycle)

    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList, nMsgs, total_peers_count):
    # Conecta a cada peer e envia o sinal de 'iniciar'
    peerNumber = 0
    for peer_ip in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer_ip, PEER_TCP_PORT))
        # Envia o ID do peer, o número de mensagens E o número total de peers (N)
        msg = (peerNumber, nMsgs, total_peers_count)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()
        peerNumber += 1

def waitForLogsAndCompare(N_MSGS, N_TOTAL_PEERS_IN_CYCLE):
    # Loop para esperar pelos logs de mensagem para comparação
    numPeersReceivedLogs = 0
    msgs = [] # Cada elemento é a lista de tuplas (sender_id, msg_number) recebida de um peer

    # Recebe os logs de mensagens dos processos peer
    while numPeersReceivedLogs < N_TOTAL_PEERS_IN_CYCLE:
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768) # Aumentado para acomodar logs maiores
        print (f'Received log from peer {addr}')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeersReceivedLogs += 1

    unordered_rounds = 0

    # Compara as listas de mensagens para verificar a ordem
    # Itera sobre cada posição de mensagem (msg_number)
    for j in range(N_MSGS): # Itera N_MSGS vezes para comparar todas as mensagens enviadas
        if not msgs or j >= len(msgs[0]): # Verifica se há logs e se a posição j existe
            print(f"Skipping comparison for message index {j}: logs might be incomplete or empty.")
            break

        first_peer_msg = msgs[0][j] # Mensagem na posição j do primeiro peer

        # Compara a mensagem do primeiro peer com a mensagem na mesma posição dos outros peers
        for i in range(1, N_TOTAL_PEERS_IN_CYCLE): # Itera do segundo peer até o último
            if j >= len(msgs[i]): # Garante que a mensagem na posição j existe para o peer i
                print(f"Warning: Peer {i} log is shorter than expected for message index {j}.")
                unordered_rounds += 1 # Considerar como desordenado ou um problema
                continue

            if first_peer_msg != msgs[i][j]:
                unordered_rounds += 1
                print(f"Mismatch found at message index {j}: Peer 0 has {first_peer_msg}, Peer {i} has {msgs[i][j]}")
                break # Uma vez que uma desordem é encontrada para esta rodada, passe para a próxima mensagem

    print (f'Found {unordered_rounds} unordered message rounds')


# Inicia o servidor:
mainLoop()