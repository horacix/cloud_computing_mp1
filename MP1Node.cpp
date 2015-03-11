/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        MemberListEntry *m = new MemberListEntry(1, 0, 0, 0);
        memberNode->memberList.push_back(*m);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    static char s[1024];
    MessageHdr *msg = reinterpret_cast<MessageHdr*>(data);
	
	switch (msg->msgType) {
	    case JOINREQ: {
            Address *addr = reinterpret_cast<Address*>(data + sizeof(MessageHdr));
            long heartbeat = *(data + (size - sizeof(long)) );

#ifdef DEBUGLOG
            sprintf(s, "JOINREQ Received from %s", addr->getAddress().c_str());
	        log->LOG(&memberNode->addr, s);
#endif
            recvJoinRequest(addr, heartbeat);
            break;
	    }
        default: {

#ifdef DEBUGLOG
            sprintf(s, "Received %s!", data);
	        log->LOG(&memberNode->addr, s);
#endif
            memberNode->inGroup = true;
            // deserialize and copy membership list
/*
stringstream ss("bla bla");
string s;

while (getline(ss, s, ' ')) {
 cout << s << endl;
}
*/
            stringstream ss(data);
            string st;
            getline(ss, st, '#');
            size_t size;
            int msgType = stoi(st, &size);
            switch (msgType) {
                case JOINREP: {
#ifdef DEBUGLOG
	                log->LOG(&memberNode->addr, "JOINREP Received");
#endif
                    // parse membershiplist
                    getline(ss, st, '#');
                    int mlsize = stoi(st, &size);
                    for (int i = 0; i < mlsize; ++i) {
                        // id
                        getline(ss, st, '#');
                        int id = stoi(st, &size);
                        // port
                        getline(ss, st, '#');
                        short port = (short)stoi(st, &size);
                        // Heartbeat
                        getline(ss, st, '#');
                        long heartbeat = stol(st, &size);
                        // Timestamp
                        getline(ss, st, '#');
                        long timestamp = stol(st, &size);

                        MemberListEntry *m = new MemberListEntry(id, port, heartbeat, par->getcurrtime());
                        memberNode->memberList.push_back(*m);
                        
                        string str_addr = to_string(id) + ":" + to_string(port);
                        Address *node_addr = new Address(str_addr);
                        log->logNodeAdd(&memberNode->addr, node_addr);
                    }
                }
            };
            break;
        }
	};
}


void MP1Node::recvJoinRequest(Address *node, long heartbeat) {
    string node_addr = node->getAddress();
    size_t pos = node_addr.find(":");
	int id = stoi(node_addr.substr(0, pos));
	short port = (short)stoi(node_addr.substr(pos + 1, node_addr.size()-pos-1));
	
    MemberListEntry *m = new MemberListEntry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(*m);
    log->logNodeAdd(&memberNode->addr, node);
    
    sendJoinReply(m);
}

void MP1Node::sendJoinReply(MemberListEntry *node) {

    std::stringstream ss;
    MessageHdr msg;
    msg.msgType = JOINREP;
    ss << msg.msgType;

    // Serialize memberList
    std::vector<MemberListEntry> *memberList = &memberNode->memberList;
    ss << "#" << memberList->size();
    for (std::vector<MemberListEntry>::iterator it = memberList->begin() ; 
        it != memberList->end(); ++it) {
        ss << "#" << it->getid() << "#" << it->getport() << "#" << 
            it->getheartbeat() << "#" << it->gettimestamp();
    }
    
    Address *a = new Address(to_string(node->getid()) + ":" + to_string(node->getport()));
    emulNet->ENsend(&memberNode->addr, a, ss.str());
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	memberNode->heartbeat += 1;
	

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
