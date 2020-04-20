import socket
import threading
import time
import hashlib

m = 6
class DataStore:
    def __init__(self):
        self.data = {}
    def insert(self, key, value):
        self.data[key] = value
    def delete(self, key):
        del self.data[key]
    def search(self, search_key):
        print('Search key', search_key)
        
        if search_key in self.data:
            return self.data[search_key]
        else:
            print('Not found')
            print(self.data)
            return None
        
class NodeInfo:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port 
    def __str__(self):
        return self.ip + str(self.port)   
class Node:
    def __init__(self, ip, port):
        self.nodeinfo = NodeInfo(ip, port)
        self.id = self.hash(str(self.nodeinfo))
        print(self.id)
        self.predecessor = None
        self.successor = ""
        self.finger_table = ""
        self.data_store = DataStore()
        self.request_handler = RequestHandler()
    
    def hash(self, message):
        digest = hashlib.sha256(message.encode()).hexdigest()
        digest = int(digest, 16) % m
        return digest

    def process_requests(self, message):
        operation = message.split("|")[0]
        result = "Done"
        if operation == 'insert':
            print('Inserting...')
            data = message.split('|')[1].split(":") 
            key = data[0]
            value = data[1]
            self.data_store.insert(key, value)
            result = 'Inserted'
        if operation == 'search':
            print('Seaching...')
            key = message.split('|')[1].split(":")[0]
            result = self.search(key)
        print(result)
        return result
    def serve_requests(self, conn, addr):
        with conn:
            print('Connected by', addr)
            
            data = conn.recv(1024)
            
            data = str(data.decode('utf-8'))
            data = data.strip('\n')
            print(data)
            data = self.process_requests(data)
            print('Sending', data)
            data = bytes(str(data), 'utf-8')
            conn.sendall(data)
    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.nodeinfo.ip, self.nodeinfo.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.serve_requests, args=(conn,addr))
                t.start()   
    def search(self, key):
        result = self.data_store.search(key)
        print('Inside search', result)
        if result == None or result == 'None':
            print('Looking remotely')
            data = 'search|' + key
            data = self.request_handler.send_message('127.0.0.1', 5555, data)
            data = data.decode('utf-8')
            print(data)
            return data
        return result
    def join(self):
        pass
    def check_predecessor(self):
        pass
    def find_predecessor(self):
        pass
    def find_successor(self, search_id):
        pass
    def closest_preceding_node(self, search_id):
        pass
    def lookup(self, search_id):
        pass
    def fix_fingers(self):
        pass
    def stabilize(self):
        pass
    def notify(self):
        pass
    


class FingerTable:
    def __init__(self, my_id):
        self.table = []
        for i in range(m):
            x = pow(2, i)
            entry = my_id + x 
            node = NodeInfo("", -1)
            self.table.append( (entry, node) )
        
    def print(self):
        for entry in self.table:
            print('Val:',entry[0], entry[1].port)
class RequestHandler:
    def __init__(self):
        pass
    def send_message(self, ip, port, message):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
  
        # connect to server on local computer 
        s.connect((ip,port)) 
        s.send(message.encode('utf-8')) 
        data = s.recv(1024) 
        s.close()
        return str(data)
        

port = input('Enter port')
node = Node('127.0.0.1', int(port))
node.start()
