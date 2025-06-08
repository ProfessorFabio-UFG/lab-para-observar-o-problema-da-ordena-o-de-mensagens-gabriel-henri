from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
  serverSock = socket(AF_INET, SOCK_STREAM)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(6)
  print(f"GroupManager listening on port {port}...")
  while(1):
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(2048)
    req = pickle.loads(msgPack)
    if req["op"] == "register":
      # Adiciona o membro à lista de membership
      membership.append((req["ipaddr"],req["port"]))
      print (f'Registered peer: {req["ipaddr"]}:{req["port"]}')
    elif req["op"] == "list":
      # Prepara a lista de IPs para ser enviada
      peer_ips = [m[0] for m in membership]
      print (f'List of peers sent to server: {peer_ips}')
      conn.send(pickle.dumps(peer_ips))
    else:
      # Opcional: Enviar uma resposta para operações desconhecidas
      print(f'Received unknown operation: {req["op"]}')
      conn.send(pickle.dumps({"status": "error", "message": "Unknown operation"}))

  conn.close() # Esta linha nunca será alcançada devido ao loop infinito

serverLoop()