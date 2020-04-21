import socket
import threading
import time
import hashlib
import random
import sys
from copy import deepcopy
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
        self.id = self.hash(str(self) )
    def hash(self, message):
        digest = hashlib.sha256(message.encode()).hexdigest()
        digest = int(digest, 16) % pow(2,m)
        return digest
    def __str__(self):
        return self.ip + "|" + str(self.port)   
class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)
        self.nodeinfo = NodeInfo(ip, port)
        self.id = self.hash(str(self.nodeinfo))
        print(self.id)
        self.predecessor = None
        self.successor = None
        self.finger_table = FingerTable(self.id, self.nodeinfo)
        self.data_store = DataStore()
        self.request_handler = RequestHandler()
    
    def hash(self, message):
        digest = hashlib.sha256(message.encode()).hexdigest()
        digest = int(digest, 16) % pow(2,m)
        return digest

    def process_requests(self, message):
        operation = message.split("|")[0]
        args = []
        if( len(message.split("|")) > 1):
            args = message.split("|")[1:]
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
        
        if operation == "join_request":
            print("join request recv")
            result  = self.join_request_from_other_node(int(args[0]))

        if operation == "find_predecessor":
            print("finding predecessor")
            result = self.find_predecessor(int(args[0]))

        if operation == "find_successor":
            print("finding successor")
            result = self.find_successor(int(args[0]))

        if operation == "get_successor":
            print("getting successor")
            result = self.get_successor()

        if operation == "get_predecessor":
            print("getting predecessor")
            result = self.get_predecessor()

        if operation == "get_id":
            print("getting id")
            result = self.get_id()

        if operation == "notify":
            print("notifiying")
            self.notify(int(args[0]),args[1],args[2])
    
        # print(result)
        return str(result)
    def serve_requests(self, conn, addr):
        with conn:
            print('Connected by', addr)
            
            data = conn.recv(1024)
            
            data = str(data.decode('utf-8'))
            data = data.strip('\n')
            # print(data)
            data = self.process_requests(data)
            print('Sending', data)
            data = bytes(str(data), 'utf-8')
            conn.sendall(data)
    def start(self):
        thread_for_stabilize = threading.Thread(target = self.stabilize)
        thread_for_stabilize.start()
        thread_for_fix_finger = threading.Thread(target=  self.fix_fingers)
        thread_for_fix_finger.start()
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
    def join_request_from_other_node(self, node_id):
        """ will return successor for the node who is requesting to join """
        return self.find_successor(node_id)

    def join(self,node_ip, node_port):
        data = 'join_request|' + str(self.id)
        succ = self.request_handler.send_message(node_ip,node_port,data)
        ip,port = self.get_ip_port(succ)
        self.successor = NodeInfo(ip,port)
        self.finger_table.table[0][1] = self.successor
        self.predecessor = None

    def check_predecessor(self):
        pass
    def find_predecessor(self, search_id):
        print("finding pred for id ", search_id)
        if self.predecessor is not None and  self.successor.id == self.predecessor.id:
            return str(self.nodeinfo)
        if search_id > self.id and search_id <= self.successor.id:
            return str(self.nodeinfo)
        else:
            new_node_hop = self.closest_preceding_node(search_id)
            print("new node hop finding hops in find predecessor" , new_node_hop)
            if new_node_hop is None or new_node_hop == "None":
                return str(self.nodeinfo)
            ip, port = self.get_ip_port(new_node_hop)
            if self.port == int(port):
                return str(self.nodeinfo)
            else:
                print(self.port, port)
            data = self.request_handler.send_message(ip , port, "find_predecessor|"+str(search_id))
            return data

    def find_successor(self, search_id):
        print("finding succ for id ", search_id)
        if search_id > self.id and search_id <= self.successor.id:
            return str(self.successor)
        elif search_id > self.id and self.successor.id <= self.id:
            return str(self.successor)

        predecessor =  str(self.closest_preceding_node(search_id))
           
        #predecessor = self.find_predecessor(search_id)
        ip,port = self.get_ip_port(predecessor)
        print(ip ,port , "in find successor, data of successor")
        if(ip == self.ip and int(port) == self.port):
            return str(self.successor)
        else:
            print("NOT SAME")
        data = self.request_handler.send_message(ip , port, "get_successor")
        return data
        
    def closest_preceding_node(self, search_id):
        print('Searching Closest preceding node for ', search_id)
        for i in list(reversed(range(m))):
            ridx = i
            #ridx = i + 1
            #if ridx > m - 1:
            #    ridx = 0
            if self.finger_table.table[i][0] > self.id and self.finger_table.table[ridx][0] < search_id:
                print('Found closest preceding node as ', str(self.finger_table.table[i][1] ) )
                return str(self.finger_table.table[i][1] )
            
        return str(self.nodeinfo)

    def stabilize(self):
        while True:
            print('Stabilizing')

            if self.successor is None:
                time.sleep(10)
                continue
            
            data = "get_predecessor"
            x = None
            if self.successor.ip == self.ip  and self.successor.port == self.port:
                x = self.predecessor
            else:
                print('NOT EQUAL',str(self.successor), self.ip, self.port)
                result = self.request_handler.send_message(self.successor.ip , self.successor.port , data)
                print('PREDECESSOR of ', str(self.successor), " is ", result)
                if result is not None and str(result).strip() != 'None':
                    print('Res',str(result))
                    ip , port = self.get_ip_port(result)
                    x = NodeInfo(ip, port) 

            
            if x is not None:
                #self.predecessor = self.nodeinfo                       
                            
                if x.id  > self.id and x.id < self.successor.id or ( str(self.successor) == str(self.nodeinfo) ) :
                    self.successor = x
                    #self.finger_table.table[0][1] = x
                elif self.successor.id < self.id and x.id < self.successor.id:
                    self.successor = x
            if self.successor.port != self.nodeinfo.port:
                print('Notifiying ...')
                self.request_handler.send_message(self.successor.ip , self.successor.port, "notify|"+ str(self.id) + "|" + str(self.nodeinfo) )

            
            if self.successor is not None:
                print("my succ" , self.successor.id)
            if self.predecessor is not None:
                print("my pred" , self.predecessor.id)
            #self.finger_table.print()
            print('Exiting stabilizing...')
            time.sleep(10)

    def notify(self, node_id , node_ip , node_port):
        print('GOT NOTIFIED',node_id, node_ip, node_port)
        if self.predecessor is None:
            print("I AM NONE - PRED")
        else:
            print("I AM PRED:", str(self.predecessor))
        if self.predecessor is None or self.predecessor == "None" or ( node_id > self.predecessor.id and node_id < self.id ) :
            self.predecessor = NodeInfo(node_ip,int(node_port))
            print('PREDECESSOR UPDATED', str(self.predecessor) )
        else:
            print('PREDECESSOR NOT UPDATED')
    def fix_fingers(self):
        while True:

            #random_index = random.randint(2,m-1)
            next = self.finger_table.get_next()
            print("in fix fingers , fixing index", next)
            find_id = self.id + pow(2, next) 
            find_id = find_id % pow(2, m)
            data = self.find_successor( find_id )
            print('Successor is ', data)
            ip, port = self.get_ip_port(data)
            node = NodeInfo(ip, port)
            
            self.finger_table.fix_fingers(node)
            print(self.finger_table)
            time.sleep(10)
    def get_successor(self):
        return str(self.successor)
    def get_predecessor(self):
        return str(self.predecessor)
    def get_id(self):
        return str(self.id)
    def get_ip_port(self, string_format):
        return string_format.strip().split('|')[0] , int(string_format.strip().split('|')[1])
    


class FingerTable:
    def __init__(self, my_id, node):
        self.table = []
        self.next = 0
        
        for i in range(m):
            x = pow(2, i)
            entry = (my_id + x) % pow(2,m)
            node = node
            self.table.append( [entry, node] )
        
    def get_next(self):
        return self.next
    def fix_fingers(self, node):
        self.table[self.next][1] = node
        
        self.next = self.next + 1
        if self.next == m:
            self.next = 0
    def __str__(self):
        s = ""
        for index, entry in enumerate(self.table):
            if entry[1] is None:
                s += 'Finger Table Entry:' +  str(index) + " Val: None"
            else:
                s += 'Finger Table Entry:' + str(index) + ' Val: ' + str(entry[0]) + " " + str(entry[1])

            s += '\n'
        return s
    def print(self):
        for index, entry in enumerate(self.table):
            if entry[1] is None:
                print('finger table entry', index, "None")
            else:
                print('finger table entry', index, 'Val:',entry[0], entry[1].ip , entry[1].port)
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
        return data.decode("utf-8") 
        


ip = "127.0.0.1"

if len(sys.argv) == 3:
    print("joining ring")
    node = Node(ip, int(sys.argv[1]))
    print(node.id)

    node.join(ip,int(sys.argv[2]))
    node.start()

if len(sys.argv) == 2:
    print("creating ring")
    port = int(sys.argv[1])
    node = Node(ip, port)
    node.predecessor = None
    node.successor = NodeInfo(ip, node.port)
    #node.finger_table.table[0][1] = node
    node.start()
    

