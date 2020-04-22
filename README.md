## GENERAL DESCRIPTION
____________________

CHORD is a simple Peer to Peer protocol which implements a Distributed Hash Table detailed as per the paper - [Stoica, Ion, Robert Morris, David Karger, M. Frans Kaashoek, and Hari Balakrishnan. "Chord: A scalable peer-to-peer lookup service for internet applications." ACM SIGCOMM Computer Communication Review 31, no. 4 (2001): 149-160.](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)

This project has two components, the Peer (`Node_vishal_2.py`) and the Client(`Client.py`).

### The Peer:

The Peer program defines a distributed network of nodes which are self aware of their postion in the CHORD architecture which is a ring. Each node of the CHORD architecture is aware of it's successor and predecessor.First of all any node joins the CHORD network simply by calculating an ID on the basis of it's ip and port number then any new node coming in the ring joins the ring by communicating with any of the node in the ring and finding the its successor and hence it's position in the ring.

### DHT Client:

The client program is used to connect to the CHORD network for storing, retreival and deletion of key-value pairs on the nodes.

## USAGE & EXAMPLES
_________________

### The Peer:

*Usage:* 

For the first node joining the ring
`python3 Node_vishal_2.py port_number`
here port_number is the port at which the node will listen for requests.

For any forth coming nodes into the ring
`python3 Node_vishal_2.py its_port_number existing_port_number`
here "its_port_number" is the port at which the node will listen for requests and  "existing_port_number" is the port number of any of the other pre existing nodes in the ring.

### The Client:

*Usage:* `python3 Client.py`

The client is menu driven where we need to provide inputs like the port number of the node which the client wants to connect to and then the option according to the task the client wants to perform like insert, search, delete etc.