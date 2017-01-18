/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#include <memory>
#include <unordered_map>


class TransStateJudger {
public:
  virtual bool judgeTransState(int ackedReplica, int allReplica) = 0;
};

/**
 * Coordinator side class maintain the state of each transition.
 */
class TransState {
public:
  int transId_;

  ReplicaType transType_;
  string      key_;
  string      value_;
  MessageType type_;

  int ackedReplica_;
  int allReplica_;
  bool transSuccess_;

  vector<string> ackedReadValue_;
  TransState(int transId) {
    transId_ = transId;
    ackedReplica_ = 0;
    allReplica_ = 0;
  }

  static shared_ptr<TransState> transState(int transId) {
    return make_shared<TransState>(transId);
  }

  void setKeyValue(const string & key, const string &value, MessageType type) {
    key_ = key;
    value_ = value;
    type_ = type;
    transSuccess_ = false;
  }

  string getKey() { return key_; }
  string getValue() { return value_; }
  string getReadValue() {return ackedReadValue_[0]; }

  int getAllReplica() { return allReplica_; }
  int getAckedReplica() { return ackedReplica_; }
  MessageType getType () { return type_; }

  void markTransSuccess() { transSuccess_ = true; }
  bool transSuccess() { return transSuccess_; }
  bool isAllReplyGot() { return allReplica_ == ackedReplica_; }

  void onOneReplicaAcked() { ++ackedReplica_; }
  void onOneReadReplicaAcked(const string &value) { ++ackedReplica_; ackedReadValue_.push_back(value); }
  void onOneReplicaSend() { ++allReplica_; }
  bool checkSatisifyReplica(TransStateJudger *judger) {
    return judger->judgeTransState(ackedReplica_, allReplica_);
  }
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;

	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

  // Coodinator state management.
  unordered_map<int, shared_ptr<TransState> > onGoingTrans_;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
  void recalcReplicaList();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

  // My helper functions.
  int nextTransId();

  void coordinatorSendMessage(Address &addr, Address &targetAddr, unique_ptr<Message> &msg);
  void coordinatorGotMessage(Message &msg);

  void replyMessage(Address &target, Address &from, int transId, bool success);
  void replyReadMessage(Address &target, Address &from, int transId, string &value);

  void processOneMessage(Message &msg);

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();
};

#endif /* MP2NODE_H_ */
