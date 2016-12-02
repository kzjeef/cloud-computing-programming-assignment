/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

//#define VERBOSE
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

static Address makeAddressFrom(int id, short port) {
    Address addr;
    memcpy(&addr.addr[0], &id, sizeof(int));
    memcpy(&addr.addr[4], &port, sizeof(short));
    return addr;
}

static int getIdFromAddress(const Address &address) {
    int id = 0;
    memcpy(&id, &address.addr[0], sizeof(int));
    return id;
}

static short getPortFromAddress(const Address &address) {
    short port = 0;
    memcpy(&port, &address.addr[4], sizeof(short));
    return port;
}

static bool memberEntryhasMarkFail(MemberListEntry &entry, int cur_time, int tfail) {
    assert(entry.timestamp <= cur_time);
    if ((cur_time - entry.timestamp) >= tfail)
        return true;
    return false;
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
	/* int id = *(int*)(&memberNode->addr.addr); */
	/* int port = *(short*)(&memberNode->addr.addr[4]); */

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

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef VERBOSE
        sprintf(s, "[MSG] send JOINREQ to :%s " , joinaddr->getAddress().c_str());
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
    return 0;
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

std::unique_ptr<MessageSelfInfo>
parseHeaderInfo(char *afterHeader) {
    std::unique_ptr<MessageSelfInfo> joinReq(new MessageSelfInfo);
    memcpy(joinReq->addr_, afterHeader, 6);
    memcpy(&joinReq->heartbeat_, afterHeader + 6, sizeof(long));
    return joinReq;
}

unique_ptr<Address>
getSenderAddress(std::unique_ptr<MessageSelfInfo> &info) {
    std::unique_ptr<Address> addr(new Address());
    memcpy(addr->addr, info->addr_, 6);
    return addr;
}



/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */


    

    MessageHdr *header = (MessageHdr *)data;
    char *afterHeader = (char *)data + sizeof(MessageHdr);

#ifdef VERBOSE
    log->LOG(&memberNode->addr, "[MSG] got message: type:%d size:%d",
             header->msgType, size);
#endif

    if (header->msgType == JOINREQ) {
        assert(size - sizeof(MessageHdr) - 1 == (sizeof(long) + 6));
        
        std::unique_ptr<MessageSelfInfo> joinReq(new MessageSelfInfo);

        memcpy(joinReq->addr_, afterHeader, 6);
        memcpy(&joinReq->heartbeat_, afterHeader + 6, sizeof(long));

        Address incomingMsgAddress;
        memcpy(incomingMsgAddress.addr, joinReq->addr_, 6);
#ifdef VERBOSE
        log->LOG(&memberNode->addr, "[COMM] [JOINREQ] message from :%s" ,incomingMsgAddress.getAddress().c_str());
#endif
        /* when receive from other peer's join request, answer it. and update the local list. */
        /* First search for the member  */

        auto found_it =
            std::find_if(this->memberNode->memberList.begin(),
                         this->memberNode->memberList.end(),
                         [incomingMsgAddress](const MemberListEntry &entry) -> bool {
                             return entry.id == getIdFromAddress(incomingMsgAddress) &&
                                 entry.port == getPortFromAddress(incomingMsgAddress);
                         });
        if (found_it != this->memberNode->memberList.end()) {
            found_it->heartbeat = std::max(found_it->heartbeat, joinReq->heartbeat_);
            found_it->timestamp = par->getcurrtime();
        } else {
            MemberListEntry member(
                getIdFromAddress(incomingMsgAddress),
                getPortFromAddress(incomingMsgAddress),
                joinReq->heartbeat_,
                par->getcurrtime());
            this->memberNode->memberList.push_back(member);
            log->logNodeAdd(&memberNode->addr, &incomingMsgAddress);
#ifdef VERBOSE
            log->LOG(&memberNode->addr, "add new member in member list:member %s totalMember:%d"
                     , incomingMsgAddress.getAddress().c_str(), this->memberNode->memberList.size());
#endif

            sendSimpleMessageToAddress(JOINREP, incomingMsgAddress);
        }
    } else if (header->msgType == JOINREP) {
        /* receive ping message. */
        auto senderInfo = parseHeaderInfo(afterHeader);
        assert(senderInfo.get() != nullptr);
        auto senderaddr = getSenderAddress(senderInfo);
#ifdef VERBOSE
        log->LOG(&memberNode->addr, "[COMM] [JOINREP] from :%s", senderaddr->getAddress().c_str());
#endif
        memberNode->inGroup = true;
    } else if (header->msgType == PING_AA) {
        /* receive ping message. */
        auto senderInfo = parseHeaderInfo(afterHeader);
        assert(senderInfo.get() != nullptr);
        auto senderaddr = getSenderAddress(senderInfo);

#ifdef VERBOSE
        log->LOG(&memberNode->addr, "[COMM] [PING_AA] from :%s", senderaddr->getAddress().c_str());
#endif

        bool haveThisMember = onReceivePingAAMessage(senderaddr, senderInfo);
        if (haveThisMember)
            sendSimpleMessageToAddress(PING_AA_REP, *senderaddr);
        else {
            MemberListEntry member(
                getIdFromAddress(*senderaddr),
                getPortFromAddress(*senderaddr),
                senderInfo->heartbeat_,
                par->getcurrtime());

            this->memberNode->memberList.push_back(member);
            log->logNodeAdd(&memberNode->addr, senderaddr.get());
#ifdef VERBOSE
            log->LOG(&memberNode->addr, "add new member in member list:member %s totalMember:%d"
                     , senderaddr->getAddress().c_str(), this->memberNode->memberList.size());
#endif
        }


    } else if (header->msgType == PING_AA_REP) {
        /* we have ping response. */
        /* update new heartbeat  form peer */
        auto senderInfo = parseHeaderInfo(afterHeader);
        assert(senderInfo.get() != nullptr);
        auto senderaddr = getSenderAddress(senderInfo);

#ifdef VERBOSE
        log->LOG(&memberNode->addr, "[COMM] [PING_AA_REP] from :%s", senderaddr->getAddress().c_str());
#endif

        onReceivePingAAMessage(senderaddr, senderInfo);
    }

    return true;
}

bool
MP1Node::onReceivePingAAMessage(unique_ptr<Address> &addr, std::unique_ptr<MessageSelfInfo> &senderInfo) {
    auto it = findMemberListEntryByAddress(addr);
    if (it == this->memberNode->memberList.end()) {
        /* Just drop the info */
#ifdef VERBOSE
        log->LOG(&memberNode->addr, "warning: receive ping message from none member.");
#endif
        return false;
    } else {
#ifdef VERBOSE
        int old_heartbeat = it->heartbeat;
#endif
        it->heartbeat = std::max(it->heartbeat, senderInfo->heartbeat_);
        it->timestamp = par->getcurrtime();
#ifdef VERBOSE
        log->LOG(&memberNode->addr, "update node heartbeat addr:%s     old:%d new:%d "
                 , addr->getAddress().c_str(), old_heartbeat, it->heartbeat);
#endif
        return true;
    }
}

vector<MemberListEntry>::iterator
MP1Node::findMemberListEntryByAddress(const std::unique_ptr<Address> &addr) {
    auto found_it =
        std::find_if(this->memberNode->memberList.begin(),
                     this->memberNode->memberList.end(),
                     [&addr](const MemberListEntry &entry) -> bool {
                         return entry.id == getIdFromAddress(*addr) &&
                             entry.port == getPortFromAddress(*addr);
                     });
    return found_it;
}

void MP1Node::sendSimpleMessageToAddress(enum MsgTypes type, Address &addr) {
    MessageHdr *msg;
    size_t msgsize = sizeof(MessageHdr) + sizeof(addr.addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = type;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef VERBOSE
    log->LOG(&memberNode->addr, "[MSG] send msg type:%d size:%d", type, msgsize);
#endif

    emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
    free(msg);
}

typedef vector<char> buffer_t;

template <typename T>
void pushTypeToVector(const T *p, buffer_t &buf) {
    const char *ptr =  (const char *)p;

    for (unsigned int i = 0; i < sizeof(*p); i++) {
        buf.push_back(ptr[i]);
    }
}

/* if buffer is not enough, will free original buffer and return new buffer.
   require buffer is malloced.
   if use original buffer, just return null.
 */
static void pushHeaderToMessage(buffer_t &buf, enum MsgTypes msgType,  Member *member) {
    pushTypeToVector(&msgType, buf);
    pushTypeToVector(member->addr.addr, buf);
    pushTypeToVector(&member->heartbeat, buf);
 }

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    /* First increase my heartbeat  */
    this->memberNode->heartbeat++;
    
    /* Send ping message. */

    vector<char> buf(1024);

    pushHeaderToMessage(buf, PING_AA, this->memberNode);

    int member_list_type_cnt = 0;

    for (MemberListEntry &m : this->memberNode->memberList) {
        if (!memberEntryhasMarkFail(m, par->getcurrtime(), TFAIL)) {
            member_list_type_cnt += (sizeof(m.id) + sizeof(m.port) + sizeof(m.heartbeat));
        }
    }

    pushTypeToVector(&member_list_type_cnt, buf);
    for (MemberListEntry &m : this->memberNode->memberList) {
        if (!memberEntryhasMarkFail(m, par->getcurrtime(), TFAIL)) {
            pushTypeToVector(&m.id, buf);
            pushTypeToVector(&m.port, buf);
            pushTypeToVector(&m.heartbeat, buf);
        }
    }

    /* sen ping message to all hist member. */
    for (MemberListEntry &m : this->memberNode->memberList) {
        Address toAddr = makeAddressFrom(m.id, m.port);
        if (!memberEntryhasMarkFail(m, par->getcurrtime(), TFAIL)) {
#ifdef VERBOSE
            log->LOG(&memberNode->addr, "[COMM] [PING] send to  %s", toAddr.getAddress().c_str() );
#endif

            // TODO : 
            sendSimpleMessageToAddress(PING_AA, toAddr);
        }
    }

//    log->LOG(&memberNode->addr, "before remove %d", this->memberNode->memberList.size());
    this->memberNode->memberList.erase(remove_if(
        this->memberNode->memberList.begin(),
        this->memberNode->memberList.end(), [&](const MemberListEntry &entry) {
          bool remove = (par->getcurrtime() - entry.timestamp) >= TREMOVE;
          if (remove) {
#ifdef VERBOSE
            log->LOG(&memberNode->addr, "will remove addr:%d port:%d", entry.id,
                     entry.port);
#endif
            Address toAddr = makeAddressFrom(entry.id, entry.port);
            log->logNodeRemove(&this->memberNode->addr, &toAddr);
          }
          return remove;
        }), this->memberNode->memberList.end());
//    log->LOG(&memberNode->addr, "after remove %d", this->memberNode->memberList.size());
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
< * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
