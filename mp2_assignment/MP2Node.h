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
#include <set>

enum struct TransResult {
	OnGoing,
	Success,
	Fail
};

class TransState;

class TransStateJudger {
public:
  virtual TransResult judgeTransState(TransState *state) = 0;
};


class TransReplicaStatus {
public:
  bool replyed_;  /// already got the replica message replyed
  bool success_;  /// the message was success or not.
  long sendTime_; /// time of send the message
  TransReplicaStatus() {
    replyed_ = false;
    success_ = false;
    sendTime_ = -1;
  }

  static
  unique_ptr<TransReplicaStatus>
      transReplicaStatusOf(long sendTime) {
    auto ret = unique_ptr<TransReplicaStatus>(new TransReplicaStatus());
    ret->sendTime_ = sendTime;
    return ret;
  }

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

  int ackedSuccReplica_;
  int ackedFailReplica_;
  int allReplica_;
  bool transHaveResult;
  map<string, int> ackedReadValue_;
  map<string, unique_ptr<TransReplicaStatus> > replicaAddrs_;

  TransState(int transId) {
    transId_ = transId;
    ackedSuccReplica_ = 0;
		ackedFailReplica_ = 0;
    allReplica_ = 0;
  }

  static shared_ptr<TransState> transState(int transId) {
    return make_shared<TransState>(transId);
  }

  void setKeyValue(const string & key, const string &value, MessageType type) {
    key_ = key;
    value_ = value;
    type_ = type;
    transHaveResult = false;
  }

  string getKey() { return key_; }
  string getValue() { return value_; }
  string getReadValue() {
    /* Find the max value object in set. */
    return max_element(ackedReadValue_.begin(), ackedReadValue_.end())->first;
  }

  MessageType getType () { return type_; }

  void markTransHaveResult() { transHaveResult = true; }
  bool transResult() { return transHaveResult; }
  bool isAllReplyGot() { return allReplica_ == (ackedSuccReplica_ + ackedFailReplica_); }

  void onOneReplicaAcked(const string &address, long currentTime, bool succ) {
    replicaAddrs_[address]->success_ = succ;
    replicaAddrs_[address]->replyed_ = true;
    if (succ) {
        ++ackedSuccReplica_;
    } else {
      ++ackedFailReplica_;
    };
  }

  void onOneReadReplicaAcked(const string &address, long currentTime, const string &value) {
    replicaAddrs_[address]->success_ = true;
    replicaAddrs_[address]->replyed_ = true;
    ++ackedSuccReplica_;
    if (ackedReadValue_.count(value) > 0)
      ++ackedReadValue_[value];
    else
      ackedReadValue_.emplace(value, 0);
  }

  void onOneReplicaSend(const string &address, long currentTime) {
    ++allReplica_;
    replicaAddrs_.insert(make_pair(address, TransReplicaStatus::transReplicaStatusOf(currentTime)));
  }

  TransResult checkSatisifyReplica(TransStateJudger *judger) {
    return judger->judgeTransState(this);
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

    void logServerOperation(bool res,  int transId, MessageType type, string &key,  string &value);

  void checkTimeoutTransaction();
};

#endif /* MP2NODE_H_ */
