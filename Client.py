import socket

def main():
	#ip = input("Give the ip address of a node")
	ip = "127.0.0.1"
	#port = 9000
	port = int(input("Give the port number of a node"))
	
	while(True):
		print("************************MENU*************************")
		print("PRESS ***********************************************")
		print("1. TO ENTER *****************************************")
		print("2. TO SHOW ******************************************")
		print("3. TO DELTE *****************************************")
		print("4. TO EXIT ******************************************")
		print("*****************************************************")
		choice = input()
		sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

		sock.connect((ip,port))

		if(choice == '1'):
			key = input("ENTER THE KEY : ")
			val = input("ENTER THE VALUE : ")
			message = "insert|" + str(key) + ":" + str(val)
			sock.send(message.encode('utf-8'))
			data = sock.recv(1024)
			data = str(data.decode('utf-8'))
			print(data)

		elif(choice == '2'):
			key = input("ENTER THE KEY")
			message = "search|" + str(key)
			sock.send(message.encode('utf-8'))
			data = sock.recv(1024)
			data = str(data.decode('utf-8'))
			print("The value corresponding to the key is : ",data)

		elif(choice == '3'):
			key = input("ENTER THE KEY")
			message = "delete|" + str(key)
			sock.send(message.encode('utf-8'))
			data = sock.recv(1024)
			data = str(data.decode('utf-8'))
			print(data)

		elif(choice == '4'):
			print("Closing the socket")
			sock.close()
			print("Exiting Client")
			exit()
			
		else:
			print("INCORRECT CHOICE")



if __name__ == '__main__':
	main()
