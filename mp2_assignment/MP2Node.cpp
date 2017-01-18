/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

#include <algorithm>


class QuorumStateJudger : public TransStateJudger {
  virtual bool judgeTransState(int ackedReplica, int all);
};

bool QuorumStateJudger::judgeTransState(int acked, int all) {
  return acked * 2 > all;
}

static QuorumStateJudger g_QuorumJudger;

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


  if (curMemList.size() == 0 && ring.size() == 0)
    return;

  if (curMemList.size() > 0 && ring.size() == 0) {
    // ring was empty, cur member list was not .
    change = true;
    ring = curMemList;
    recalcReplicaList();
  } else if (curMemList.size() == 0 && ring.size() > 0) {
    // new member was empty. remove all.
    change = true;
    hasMyReplicas.clear();
    haveReplicasOf.clear();
  } else {
    // both have content.
    vector<Node> diff;
    set_symmetric_difference(
      curMemList.begin(),
      curMemList.end(),
      ring.begin(),
      ring.end(),
      back_inserter(diff));

    if (diff.size() > 0)
      change = true;

    ring = curMemList;
    recalcReplicaList();
  }



	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

  if (this->ring.size() > 0 && change) {
      stabilizationProtocol();
  }

  
}

// update hasMyRelicas and hasRelicaesOf
void MP2Node::recalcReplicaList() {

  /* loop twice to find the replicate. */
  auto curInNode = find_if(ring.begin(), ring.end(), [&](Node &n) -> bool {
      return n.nodeAddress == this->memberNode->addr;
    });

  assert(curInNode != ring.end());

  /* assert ring size must be >= 3 */
  assert(ring.size() >= 3);

  /* hasMyReplicas: next two */
  if (ring.end() - curInNode >= 2) {
    this->hasMyReplicas.push_back(*(curInNode + 1));
    this->hasMyReplicas.push_back(*(curInNode + 2));
  } else if (curInNode - ring.end() >= 1) {
    this->hasMyReplicas.push_back(*(curInNode + 1));
    this->hasMyReplicas.push_back(*ring.begin());
  } else  {
    this->hasMyReplicas.push_back(*ring.begin());
    this->hasMyReplicas.push_back(*(ring.begin() + 1));
  }

  /* haveReplicasOf: pervious two  */
  if (curInNode - ring.begin() >= 2) {
    this->haveReplicasOf.push_back(*(curInNode - 2));
    this->haveReplicasOf.push_back(*(curInNode - 1));
  } else if (curInNode - ring.begin() >= 1) {
    this->haveReplicasOf.push_back(*(curInNode - 1));
    this->haveReplicasOf.push_back(*(ring.end() - 1));
  } else {
    this->haveReplicasOf.push_back(*(ring.end() - 1));
    this->haveReplicasOf.push_back(*(ring.end() - 2));
  }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::coordinatorSendMessage(Address &addr,
                                     Address &targetAddr,
                                     unique_ptr<Message> &msg) {

  // if message was send to our self, don't send message, direct process it.
  // include, do the location, and update the coordinator's state, just like got
  // the message, and send back the reply.

  if (onGoingTrans_.count(msg->transID) > 0) {
    auto trans = onGoingTrans_[msg->transID];
    trans->onOneReplicaSend();
  } else {
    auto trans = TransState::transState(msg->transID);
    trans->setKeyValue(msg->key, msg->value, msg->type); /* TODO: some message don't have value. can be refiner. */
    onGoingTrans_.emplace(msg->transID, trans);
  }

  if (this->memberNode->addr == targetAddr) {
    processOneMessage(*msg);
  } else {
    emulNet->ENsend(&this->memberNode->addr,
                    &targetAddr,
                    msg->toString());
  }
  
}


void MP2Node::coordinatorGotMessage(Message &msg) {
  /* update the transion id table. */
  if (onGoingTrans_.count(msg.transID) <= 0) {
    log->LOG(&this->memberNode->addr, "Error: wrong message with transId got: %d", msg.transID);
    return;
  }

  auto trans = onGoingTrans_[msg.transID];

  if (msg.type == REPLY) {
    trans->onOneReplicaAcked();
    if (trans->checkSatisifyReplica(&g_QuorumJudger)) {
      if (!trans->transSuccess()) {
        trans->markTransSuccess();
        /* first succes, we print log. */
        switch(trans->getType()) {
        case CREATE:
          log->logCreateSuccess(&this->memberNode->addr, true, msg.transID, trans->getKey(), trans->getValue());
          break;
        case UPDATE:
          log->logUpdateSuccess(&this->memberNode->addr, true, msg.transID, trans->getKey(), trans->getValue());
          break;
        case DELETE:
          log->logDeleteSuccess(&this->memberNode->addr, true, msg.transID, trans->getKey());
          break;
        default:
          break;
        }
      } else {
        if (trans->isAllReplyGot()) {
          onGoingTrans_.erase(msg.transID);
        }
      } 
    }
  } else if (msg.type == READREPLY) {
    /* push read message to list. */
    trans->onOneReadReplicaAcked(msg.value);
    if (trans->checkSatisifyReplica(&g_QuorumJudger) == true) {
      if (!trans->transSuccess()) {
        trans->markTransSuccess();
        log->logReadSuccess(&this->memberNode->addr,true, msg.transID, trans->getKey(), trans->getReadValue());
      } else {
        if (trans->isAllReplyGot()) {
          onGoingTrans_.erase(msg.transID);
        }
      }
    }
  } else {
    abort();
  }
  /* if success, log it, or if it was failed. */
}

int MP2Node::nextTransId() {
  return g_transID++;
}

static ReplicaType fromIndexToReplicaType(int i) {
  switch(i) {
  case 0:
    return PRIMARY;
  case 1:
    return SECONDARY;
  case 2:
    return TERTIARY;
  default:
    abort();
    return PRIMARY;
  }
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
  vector<Node> replicas  = findNodes(key);
  int transId = nextTransId();

  for (int i = 0; i < replicas.size() ; i++) {
    unique_ptr<Message> msg(new Message(transId,
                                        this->memberNode->addr,
                                        CREATE,
                                        key,
                                        value,
                                        fromIndexToReplicaType(i)));
    coordinatorSendMessage(this->memberNode->addr, replicas[i].nodeAddress, msg);
  }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
  vector<Node> replicas  = findNodes(key);
  int transId = nextTransId();
  for (int i = 0; i < replicas.size() ; i++) {
    unique_ptr<Message> msg(new Message(transId,
                                        this->memberNode->addr,
                                        READ,
                                        key));
    coordinatorSendMessage(this->memberNode->addr, replicas[i].nodeAddress, msg);
  }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */

  vector<Node> replicas  = findNodes(key);
  int transId = nextTransId();
  for (int i = 0; i < replicas.size() ; i++) {
    unique_ptr<Message> msg(new Message(transId,
                                        this->memberNode->addr,
                                        UPDATE,
                                        key,
                                        value,
                                        fromIndexToReplicaType(i)));

    coordinatorSendMessage(this->memberNode->addr, replicas[i].nodeAddress, msg);
  }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
  vector<Node> replicas  = findNodes(key);
  int transId = nextTransId();
  for (int i = 0; i < replicas.size() ; i++) {
    unique_ptr<Message> msg(new Message(transId,
                                        this->memberNode->addr,
                                        DELETE,
                                        key));

    coordinatorSendMessage(this->memberNode->addr, replicas[i].nodeAddress, msg);
  }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
  if (ht->count(key) > 0) {
    return false;
  } else {
    return ht->create(key, value);
  }
  /* TODO: how to do with replicaType ?  */
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
  return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
  return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
  return ht->deleteKey(key);
}

void MP2Node::replyMessage(Address &target, Address &from, int transId, bool success) {
  Message msg(transId, from, REPLY, success);
  if (this->memberNode->addr == target) {
    processOneMessage(msg);
  } else {
    emulNet->ENsend(&from, &target, msg.toString());
  }
}

void MP2Node::replyReadMessage(Address &target, Address &from, int transId, string &value) {
  Message msg(transId, from, READREPLY, value);
  if (this->memberNode->addr == target) {
    processOneMessage(msg);
  } else {
    emulNet->ENsend(&from, &target, msg.toString());
  }
}

void MP2Node::processOneMessage(Message &msg) {
    string read_result;
    bool op_res = false;
    if (msg.type == READ || msg.type == UPDATE || msg.type == DELETE) {
      /* check if hash table has the result. */
      if (ht->count(msg.key) <= 0) {
        replyMessage(msg.fromAddr, this->memberNode->addr, false, msg.transID);
        return;
        /* next message. */
      }
    }
    switch(msg.type)  {
    case CREATE:
      op_res = createKeyValue(msg.key, msg.value, msg.replica);
      break;
    case UPDATE:
      op_res = updateKeyValue(msg.key, msg.value, msg.replica);
      break;
    case DELETE:
      op_res = deletekey(msg.key);
      break;
    case READ:
      read_result = readKey(msg.key);
      break;
    case REPLY:
    case READREPLY:
      coordinatorGotMessage(msg);
      break;
    }

    if (msg.type != REPLY && msg.type != READREPLY) {
      if (msg.type != READ) 
        replyMessage(msg.fromAddr, this->memberNode->addr, op_res, msg.transID);
      else
        replyReadMessage(msg.fromAddr, this->memberNode->addr, msg.transID, read_result);
    }
}


/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */


	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
    Message msg(message);
  

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

  /* TODO:  Stabilization after failure (recreate three replicas after failure). */
}
