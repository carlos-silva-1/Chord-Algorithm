import rpyc
from rpyc import ThreadedServer
import threading
import hashlib
import os

ID = None
sucessor = 0
antecessor = 0
files = []
self_conn = None # conexao local

nodesList = {
    1: {'host': 'localhost', 'port': 10000},
    2: {'host': 'localhost', 'port': 10001},
    3: {'host': 'localhost', 'port': 10002},
    4: {'host': 'localhost', 'port': 10003},
    5: {'host': 'localhost', 'port': 10004},
    6: {'host': 'localhost', 'port': 10005},
    7: {'host': 'localhost', 'port': 10006},
    8: {'host': 'localhost', 'port': 10007},
    9: {'host': 'localhost', 'port': 10008},
    10: {'host': 'localhost', 'port': 10009}
}  # configuração das replicas


def createServerNode(id):
    global ID
    ID = id

    class Node(rpyc.Service):
        print("Node" + str(id))

        def on_connect(self, conn):
            print("Conexão estabelecida.")

        def on_disconnect(self, conn):
            print("Ralando")

        def exposed_alterar_sucessor(self, novoValor):
            global sucessor
            sucessor = novoValor

        def exposed_alterar_antecessor(self, novoValor):
            global antecessor
            print("antecessor")
            antecessor = novoValor

        # atualiza a tabela. Buscando as chaves que deveriam ter ido para o nó corrente
        # mas não foram, porque ele estava desconectado
        # deve ser chamada pelo nó inserido no seu antecessor
        def exposed_update_keys_on_connect(self):
            global ID, files
            for f in files:
                if hash(f[0]) != ID:
                    self.exposed_insert_file(f[0], f[1])
                    self.delete(f[0])

        def exposed_send_file(self, key, file):
            self.insert(key, file)

        # se um nó for retirado do sistema, seus arquivos devem ser redistribuidos aos outros nós
        def exposed_redistribute_files(self):
            global antecessor, files
            for k in nodesList.keys():
                if k == antecessor:
                    conn = rpyc.connect(nodesList[k]["host"], nodesList[k]["port"])
                    for f in files:
                        conn.root.send_file(f[0], f[1])
                    conn.close()
                    break

        # insere um arquivo no nó atual
        def insert(self, key, file):
            global files
            files.append([key, file])
            
            if not os.path.exists("node_" + str(ID)):
                os.makedirs("node_" + str(ID))
            
            with open("node_" + str(ID) + "/copied_file.txt", 'w') as f:
                f.write(file)

        # confere se deve inserir o arquivo neste nó,
        # se sim o insere,
        # se não chama a mesma função no sucessor
        def exposed_insert_file(self, key, file):
            global ID, sucessor
            hashed_key = hash(key)
            print("hashed_key: " + str(hashed_key))
            # se a chave está entre no intervalo [no atual, sucessor)
            if hashed_key >= ID and hashed_key < sucessor:
                self.insert(key, file)
            # se a chave está entre o id do nó atual e o id do primeiro nó do anel
            elif hashed_key >= ID and sucessor < ID:
                self.insert(key, file)
            else:
                for k in nodesList.keys():
                    if k == sucessor:
                        conn = rpyc.connect(nodesList[k]["host"], nodesList[k]["port"])
                        conn.root.insert_file(key, file)
                        conn.close()
                        break

        # retorna um arquivo do nó atual pela chave
        def get_file(self, key):
            global files
            for f in files:
                if f[0] == key:
                    return f[1]

        # confere se o arquivo a ser pego está neste nó
        # se sim o pega,
        # se não, chama a mesma função no sucessor
        def exposed_retrieve_file(self, key):
            global ID, sucessor
            hashed_key = hash(key)
            # se a chave está entre o id do nó atual e o id do nó sucessor
            if hashed_key >= ID and hashed_key < sucessor:
                retrieved = self.get_file(key)
                return retrieved
            # se a chave está entre o id do nó atual e o id do primeiro nó do anel
            elif hashed_key >= ID and sucessor < ID:
                retrieved = self.get_file(key)
                return retrieved
            else:
                for k in nodesList.keys():
                    if k == sucessor:
                        conn = rpyc.connect(nodesList[k]["host"], nodesList[k]["port"])
                        retrieved = conn.root.retrieve_file(key)
                        conn.close()
                        return retrieved

        # remove o arquivo do nó
        def delete(self, key):
            global files
            for f in files:
                if f[0] == key:
                    files.remove(f)

        # confere se o arquivo a ser apagado está neste nó
        # se sim o apaga,
        # se não, chama a mesma função no sucessor
        def exposed_delete_file(self, key):
            global ID, sucessor
            hashed_key = hash(key)
            # se a chave está entre o id do nó atual e o id do nó sucessor
            if hashed_key >= ID and hashed_key < sucessor:
                self.delete(key)
            # se a chave está entre o id do nó atual e o id do primeiro nó do anel
            elif hashed_key >= ID and sucessor < ID:
                self.delete(key)
            else:
                for k in nodesList.keys():
                    if k == sucessor:
                        conn = rpyc.connect(nodesList[k]["host"], nodesList[k]["port"])
                        conn.root.delete_file(key)
                        conn.close()
                        break

    srv = ThreadedServer(
        Node, hostname=nodesList[id]["host"], port=nodesList[id]["port"])
    srv.start()


def identify_node():
    id = int(input("Escolha seu node: "))
    load_node(id)


def load_node(id):
    global ID
    ID = id
    nodes = threading.Thread(target=createServerNode, args=(id,))
    nodes.start()
    update_nodes()


def find_successor(id):
    cont = (id - 1) % len(nodesList)
    global antecessor
    global sucessor

    if(id == 10):
        try:
            conn = rpyc.connect(nodesList[1]['host'], nodesList[1]['port'])
            sucessor = cont
            conn.root.alterar_sucessor(id)
            conn.close()
        except:
            pass
    while(cont > 0):
        if(id != cont):
            try:
                conn = rpyc.connect(
                    nodesList[cont]['host'], nodesList[cont]['port'])
                antecessor = cont
                conn.root.alterar_sucessor(id)
                conn.close()
                break
            except:
                pass
        cont = cont - 1
    print("\n")


def find_antecessor(id):
    global sucessor
    global antecessor
    cont = (id + 1) % len(nodesList)

    if(id == 1):
        try:
            conn = rpyc.connect(nodesList[10]['host'], nodesList[10]['port'])
            antecessor = cont
            conn.root.alterar_sucessor(id)
            conn.close()
        except:
            pass

    while(cont <= len(nodesList)):
        if(id != cont):
            try:
                conn = rpyc.connect(
                    nodesList[cont]['host'], nodesList[cont]['port'])
                sucessor = cont
                conn.root.alterar_antecessor(id)
                conn.close()
                break
            except:
                pass
        cont = cont + 1


def update_nodes():
    global ID, self_conn
    find_successor(ID)
    find_antecessor(ID)
    self_conn = rpyc.connect(nodesList[ID]['host'], nodesList[ID]['port'])
    try:
        conn = rpyc.connect(nodesList[antecessor]['host'], nodesList[antecessor]['port'])
        conn.root.update_keys_on_connect()
        conn.close()
    except:
        pass


def hash(key):
    hashed_key = hashlib.sha256(key.encode('utf-8')).hexdigest()
    hashed_key_int = int(hashed_key, 16)
    return (hashed_key_int % len(nodesList)) + 1


def inserir_aquivo():
    global self_conn
    filename = input("Digite o nome do arquivo: ")
    with open(filename, 'r') as f:
        data = f.read()
        self_conn.root.insert_file(filename, data)


def procurar_arquivo():
    global self_conn
    filename = input("Digite o nome do arquivo: ")
    retrieved_file = self_conn.root.retrieve_file(filename)
    print(str(retrieved_file))


def deletar_arquivo():
    global self_conn
    filename = input("Digite o nome do arquivo: ")
    self_conn.root.delete_file(filename)


def imprime_arquivos():
    global files
    for f in files:
        print("----------")
        print(str(f[0]))
        print(str(f[1]))


def redistribui_arquivos():
    global self_conn
    self_conn.root.redistribute_files()


def menu():
    print("\nEscolha uma das opções:\n1-Inserir um arquivo\n2-Procurar um Arquivo\n3-Deletar um arquivo\n4-Sair\n5- Atualizar Algoritmo\n6- Imprimir Arquivos\n7- Antecessor\n8- Sucessor")


def interface():
    while(True):
        menu()
        print("\n")
        op = int(input("Selecione sua escolha:"))
        if op == 1:
            inserir_aquivo()
        elif op == 2:
            procurar_arquivo()
        elif op == 3:
            deletar_arquivo()
        elif op == 4:
            redistribui_arquivos()
            os._exit(0)
        elif op == 5:
            return
        elif op == 6:
            imprime_arquivos()
        elif op == 7:
            print("antecessor: " + str(antecessor))
        elif op == 8:
            print("sucessor: " + str(sucessor))


def main():
    identify_node()
    interface()


main()
