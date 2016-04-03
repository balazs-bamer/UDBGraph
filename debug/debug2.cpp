/*
COPYRIGHT COMES HERE
*/

/*
TODO:
attach
~Transaction vs exception
hash and pred in checkResults
check payload management
test edge update, edge creation checks
*/

#include<csignal>
#include<cstring>
#include<iostream>
#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

/** SIGSEGV handler, throws a DebugException. */
extern void handleSigsegv(int sig) {
    throw DebugException("SIGSEGV!");
}

void UPS_CALLCONV udbgraphErrorHandler(int level, const char *message) {
    cerr << "udbgraphErrorHandler: " << level << ": " << message << endl;
}

void notReady() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
                shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
	}
	catch(exception &e) {
		const char * const what = e.what();
		if(strstr(what, "Record size was not set") == nullptr) {
			cout << "notReady: " << e.what() << endl;
		}
	}
}

void singleInsertCreate() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->create("debug2a.udbg", 0644);
		shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
		db->close();
	}
	catch(exception &e) {
		cout << "singleInsertCreate: " << e.what() << endl;
	}
}

void singleInsertOpen() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 2, "debug2");
		db->open("debug2a.udbg");
		shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
	}
	catch(exception &e) {
		cout << "singleInsertOpen: " << e.what() << endl;
	}
}

void verMismatch() {
	try {
		shared_ptr<Database> db = Database::newInstance(2, 1, "debug2");
		db->open("debug2a.udbg");
	}
	catch(exception &e) {
		const char * const what = e.what();
		if(strstr(what, "Invalid version or application name") == nullptr) {
			cout << "verMismatch: " << e.what() << endl;
		}
	}
}

void nameMismatch() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug3");
		db->open("debug2a.udbg");
	}
	catch(exception &e) {
		const char * const what = e.what();
		if(strstr(what, "Invalid version or application name") == nullptr) {
			cout << "nameMismatch: " << e.what() << endl;
		}
	}
}

class MoreWritesPerTrans : public Payload {
protected:
	static payloadType _staticType;

	char * content = nullptr;

	int maxLen = 0;

public:
    /** Sets type for instance. */
    MoreWritesPerTrans(payloadType pt) : Payload(pt) {}
    
	~MoreWritesPerTrans() {
		delete[] content;
	}

	/** Static PayloadType ID for GEFactory. */
	static payloadType id() { return _staticType; }

	/** Sets the static PayloadType ID for GEFactory. */
	static void setID(payloadType pt) { _staticType = pt; }

	/** Used in GEFactory to create a shared_ptr holding a new class instance. */
	static shared_ptr<GraphElem> create(shared_ptr<Database> &db, payloadType pt) {
		return shared_ptr<GraphElem>(new Node(db, std::unique_ptr<Payload>(new MoreWritesPerTrans(pt))));
	}

	void fill(size_t len) {
		if(len > 9999) {
			len = 9999;
		}
		if(maxLen < len) {
			delete[] content;
			maxLen = len;
			content = new char[maxLen + 1];
		}
		int div10[] = {1000, 100, 10, 1};
        char remain[] = "xxxx abcd\n";
        int i;
        for(i = 0; i < len; i++) {
            int i10 = i % 10;
            if(i10 < 4) {
                content[i] = '0' + ((i - i10) / div10[i10]) % 10;
            }
            else {
                content[i] = remain[i10];
            }
        }
        content[i] = 0;
	}

	virtual void serialize(Converter &conv) { conv << content; }
};
	
payloadType MoreWritesPerTrans::_staticType;

void moreWritesPerTrans() {
	MoreWritesPerTrans::setID(GEFactory::reg(MoreWritesPerTrans::create));
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->open("debug2a.udbg");
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, MoreWritesPerTrans::id());
			MoreWritesPerTrans& pl = dynamic_cast<MoreWritesPerTrans&>(node->pl());
			pl.fill(1100);
			db->write(node, tr);
			pl.fill(8000);
			node->write(tr);
			tr.commit();
		}
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, MoreWritesPerTrans::id());
			MoreWritesPerTrans& pl = dynamic_cast<MoreWritesPerTrans&>(node->pl());
			pl.fill(100);
			db->write(node, tr);
			pl.fill(8000);
			node->write(tr);
			pl.fill(2000);
			node->write(tr);
			tr.commit();
		}
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, MoreWritesPerTrans::id());
			MoreWritesPerTrans& pl = dynamic_cast<MoreWritesPerTrans&>(node->pl());
			pl.fill(100);
			db->write(node, tr);
			pl.fill(8000);
			node->write(tr);
			pl.fill(200);
			node->write(tr);
			tr.commit();
		}
	}
	catch(exception &e) {
		cout << "singleInsertOpen: " << e.what() << endl;
	}
}
	
/** Creates edges leading from node to root. */
void hashTableInsertsIn(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	for(int i = 0; i < n; i++) {
		shared_ptr<GraphElem> edge = GEFactory::create(db, PT_EMPTY_DEDGE);
		edge->setStartRootEnd(node);
		db->write(edge, tr);
	}
}

/** Creates edges leading from root to node. */
void hashTableInsertsOut(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	for(int i = 0; i < n; i++) {
		shared_ptr<GraphElem> edge = GEFactory::create(db, PT_EMPTY_DEDGE);
		edge->setEndRootStart(node);
		db->write(edge, tr);
	}
}

/** Creates undirected edges between node and root. */
void hashTableInsertsUn(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	for(int i = 0; i < n; i++) {
		shared_ptr<GraphElem> edge = GEFactory::create(db, PT_EMPTY_UEDGE);
		edge->setStartRootEnd(node);
		db->write(edge, tr);
	}
}

typedef void HashTableFuncComb(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n);

void hashTableInserts(shared_ptr<Database> &db, HashTableFuncComb *func, int n) {
	Transaction tr = db->beginTrans();
	shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
	db->write(node, tr);
	func(db, tr, node, n);
	tr.commit();
}

void hashTableInsertsI(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsIn(db, tr, node, n);
}

void hashTableInsertsO(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsOut(db, tr, node, n);
}

void hashTableInsertsU(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsUn(db, tr, node, n);
}

void hashTableInsertsIO(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsIn(db, tr, node, n);
	hashTableInsertsOut(db, tr, node, n);
}

void hashTableInsertsOI(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsOut(db, tr, node, n);
	hashTableInsertsIn(db, tr, node, n);
}

void hashTableInsertsIU(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsIn(db, tr, node, n);
	hashTableInsertsUn(db, tr, node, n);
}

void hashTableInsertsUI(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsUn(db, tr, node, n);
	hashTableInsertsIn(db, tr, node, n);
}

void hashTableInsertsOU(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsOut(db, tr, node, n);
	hashTableInsertsUn(db, tr, node, n);
}

void hashTableInsertsUO(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsUn(db, tr, node, n);
	hashTableInsertsOut(db, tr, node, n);
}

void hashTableInsertsIOU(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node, int n) {
	hashTableInsertsIn(db, tr, node, n);
	hashTableInsertsOut(db, tr, node, n);
	hashTableInsertsUn(db, tr, node, n);
}

void hashTableInserts() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->create("debug2-hash-insert.udbg", 0644);
		for(int e = 0; e < 4; e++) {
			int n = 1 << e;
			hashTableInserts(db, hashTableInsertsI, n);
			hashTableInserts(db, hashTableInsertsO, n);
			hashTableInserts(db, hashTableInsertsU, n);
			hashTableInserts(db, hashTableInsertsIO, n);
			hashTableInserts(db, hashTableInsertsOI, n);
			hashTableInserts(db, hashTableInsertsIU, n);
			hashTableInserts(db, hashTableInsertsUI, n);
			hashTableInserts(db, hashTableInsertsUO, n);
			hashTableInserts(db, hashTableInsertsOU, n);
			hashTableInserts(db, hashTableInsertsIOU, n);
		}
		db->close();
	}
	catch(exception &e) {
		cout << "hashTableInserts: " << e.what() << endl;
	}
}

typedef void CheckEndsFunc(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge);

void checkEndsNodeInsteadofEdge(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge) {
	node1->setStartRootEnd(node2);
}

void checkEndsSet(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge) {
	edge->setEnds(node1, node2);
	edge->setEnds(node1, node2);
}

void checkEndsNotNode1(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge) {
	edge->setStartRootEnd(edge);
}

void checkEndsNotNode2(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge) {
	edge->setEndRootStart(edge);
}

void checkEndsLoop(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge) {
	edge->setEnds(node1, node1);
}

void checkEndsNotSet(shared_ptr<Database> &db, Transaction &tr, shared_ptr<GraphElem> &node1, shared_ptr<GraphElem> &node2, shared_ptr<GraphElem> &edge) {
	// do not set them
}

void checkEnds(CheckEndsFunc *func, const char * const expectedMessage) {
	shared_ptr<Database> db = Database::newInstance(1, 2, "debug2");
	db->open("debug2a.udbg");
	Transaction tr = db->beginTrans();
	shared_ptr<GraphElem> node1 = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
	db->write(node1);
	shared_ptr<GraphElem> node2 = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
	db->write(node2);
	shared_ptr<GraphElem> edge = GEFactory::create(db, PT_EMPTY_UEDGE);
	try {
		func(db, tr, node1, node2, edge);
		db->write(edge);
		// this should not be reached.
		cout << "checkEnds: expected exception haven\'t arrived:" << expectedMessage << endl;
	}
	catch(exception &e) {
		const char * const what = e.what();
		if(strstr(what, expectedMessage) == nullptr) {
			cout << "checkEnds: " << e.what() << endl;
		}
	}
	tr.abort();
	db->close();
}

void checkEnds() {
	checkEnds(checkEndsNodeInsteadofEdge, "This method may only be called on an Edge.");
	checkEnds(checkEndsSet, "Ends are already set");
	checkEnds(checkEndsNotNode1, "End must be node");
	checkEnds(checkEndsNotNode2, "End must be node");
	checkEnds(checkEndsLoop, "Two ends must be different (no loop edges allowed)");
	checkEnds(checkEndsNotSet, "Edge ends not set");
}


int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
	signal(SIGSEGV, handleSigsegv);
	InitStatic globalStaticInitializer;
	Database::setErrorHandler(udbgraphErrorHandler);
	notReady();
	singleInsertCreate();
	singleInsertOpen();
	verMismatch();
	nameMismatch();
	moreWritesPerTrans();
	hashTableInserts();
	checkEnds();
	// cout << "After hash insert - insert: " << UpsCounter::getInsert() << "  erase: " << UpsCounter::getErase() << "  find: " << UpsCounter::getFind() << endl;
    return 0;
}
