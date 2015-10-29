name:Prateek Joshi     email:prateekjoshi2013@ufl.edu|
name: Sharique Hussain email:sharique88@ufl.edu		 | 	
---------------------------------------------------------


What is working ???
1)Any node in the N-node network (where N is greater than 1) is able to find the correct location of the file key k is stored in O(log N) time. N > 1 because we are choosing m(the number of bits to be truncated from the hash of the id) as a function of log(N).
2)Node Join is implemented correctly.
3)All the finger tables are updated correctly after node join, using the algorithm described in chord protocol research paper.
4)A request will keep track of the count of hops traversed and send the count to master once key is found. Master will keep track of total requests processed and shutdown when all requests are processed.
 
Largest network we could manage ???
Largest network we successfully ran was with 50000 nodes with average hops 7.9895954 for 2500050 file requests. For 100000 nodes outofmemory started coming.
 

 
 Implementation Details
Project: Project3
1) We take number of nodes and number of requests from commad prompt
2) We dynamically choose the value of m (the number of bits taken from the hash ) based on the number of nodes.
3) Node values are then chosen from the hash value using sha-1 hashing of unique strings simmillar to IP adresses assigned to each node.
4) Then we initialize the network by creating nodes using akka actors representing each node in the network ,each node having its own finger table.
5) Then we join a node using the node-join api provided in chord protocol research paper.
6) After the network stabilization we start file requests with random keys between (0,2^m) using a ticker of 1 second delay from each node ,each node firing number of requests equal to the numrequest from command prompt. 
7) We get the output as average number of hops per request.



--------------------------------------------------------------------------------
Results:
number of nodes = 10 , requests = 50
Total hops for 550 requests are 1194, Average hops = 2.1709092
number of nodes = 100 , requests = 50
Total hops for 5050 requests are 19507, Average hops = 3.8627722
number of nodes = 1000 , requests = 50
Total hops for 50050 requests are 292601, Average hops = 5.846174
number of nodes = 10000 , requests = 50
Total hops for 500050 requests are 3684540, Average hops = 7.3683434
number of nodes = 20000 , requests = 50
Total hops for 1000050 requests are 7725148, Average hops = 7.724762
number of nodes = 50000 , requests = 50
Total hops for 2500050 requests are 19974389, Average hops = 7.9895954
----------------------------------------------------------

Instructions for running the code
__________________________________________________________________

1)sbt "project project3" "run <number of nodes> <number of requests>"
2)sbt "project project3-bonus" "run <number of nodes> <number of requests>"

