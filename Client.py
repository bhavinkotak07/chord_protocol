def menu(self):
	sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	ip = input("Give the ip address of a node")
	ip = "127.0.0.1"
	port = int(input("Give the port number of a node"))
	s.connect((ip,port))

	while(true):
		print("************************MENU*************************")
		print("PRESS ***********************************************")
		print("1. TO ENTER *****************************************")
		print("2. TO SHOW ******************************************")
		print("3. TO DELTE *****************************************")
		print("4. TO EXIT ******************************************")
		print("*****************************************************")
		choice = input()
		if(choice == '1'):
			key = input("ENTER THE KEY")
			val = input("ENTER THE VALUE")
			message = "insert" + str(key) + "|" + str(val)
		    sock.send(message.encode('utf-8'))
		    data = s.recv(1024)
		    print(data)
		elif(choice == '2'):
			key = input("ENTER THE KEY")
			message = "search" + str(key) + "|" + str(val)
			sock.send(message.encode('utf-8'))
			print(data)
		elif(choice == '3'):
			key = input("ENTER THE KEY")
			message = "delete" + str(key) + "|" + str(val)
			sock.send(message.encode('utf-8'))
			print(data)
		elif(choice == '4'):
			print("Closing the socket")
			s.close()
			print("Exiting Client")
			system.exit(0)
		else:
			print("INCORRECT CHOICE")