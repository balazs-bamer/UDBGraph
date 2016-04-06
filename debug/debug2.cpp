/*
COPYRIGHT COMES HERE
*/

// Functions with name test* are called from main. All other functions are called from these with name test*. */

/*
TODO:
~Transaction vs exception
hash and pred in checkResults
check payload management
test edge update
*/

#include<string>
#include<csignal>
#include<cstring>
#include<iostream>
#include"udbgraph.h"
#include"debug-util.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

const char mainFileName[] = "debug2a.udbg";

/** SIGSEGV handler, throws a DebugException. */
extern void handleSigsegv(int sig) {
    throw DebugException("SIGSEGV!");
}

void UPS_CALLCONV udbgraphErrorHandler(int level, const char *message) {
    cerr << "udbgraphErrorHandler: " << level << ": " << message << endl;
}

void testNotReady() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
                shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
	}
    catch(exception &e) {
        checkException(e, "testNotReady", "Record size was not set");
	}
}

void testSingleInsertCreate() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->create(mainFileName, 0644);
		shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
		db->close();
	}
	catch(exception &e) {
		cout << "testSingleInsertCreate: " << e.what() << endl;
	}
}

void testSingleInsertOpen() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 2, "debug2");
		db->open(mainFileName);
		shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
	}
	catch(exception &e) {
		cout << "testSingleInsertOpen: " << e.what() << endl;
	}
}

void testVerMismatch() {
	try {
		shared_ptr<Database> db = Database::newInstance(2, 1, "debug2");
		db->open(mainFileName);
	}
	catch(exception &e) {
        checkException(e, "testVerMismatch", "Invalid version or application name");
	}
}

void testNameMismatch() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug3");
		db->open(mainFileName);
	}
	catch(exception &e) {
        checkException(e, "testNameMismatch", "Invalid version or application name");
	}
}

void testMoreWritesPerTrans() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->open(mainFileName);
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
			ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
			pl.fill(1100);
			db->write(node, tr);
			pl.fill(8000);
			node->write(tr);
			tr.commit();
		}
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
			ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
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
			shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
			ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
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
		cout << "testMoreWritesPerTrans: " << e.what() << endl;
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

void testHashTableInserts() {
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
		cout << "testHashTableInserts: " << e.what() << endl;
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
	db->open(mainFileName);
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
        checkException(e, "checkEnds", expectedMessage);
	}
	tr.abort();
	db->close();
}

void testCheckEnds() {
	checkEnds(checkEndsNodeInsteadofEdge, "This method may only be called on an Edge.");
	checkEnds(checkEndsSet, "Ends are already set");
	checkEnds(checkEndsNotNode1, "End must be node");
	checkEnds(checkEndsNotNode2, "End must be node");
	checkEnds(checkEndsLoop, "Two ends must be different (no loop edges allowed)");
	checkEnds(checkEndsNotSet, "Edge ends not set");
}

void testAttach() {
	const char before[] = "First content before attach.";
	const char after[] = "Second content after attach.";
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->open(mainFileName);
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
			ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
			pl.set(before);
			db->write(node, tr);
			tr.commit();
			tr = db->beginTrans(false);
// overwrite the first one
			node->attach(tr);
			pl.set(after);
			node->write(tr);
			tr.commit();
		}
	}
	catch(exception &e) {
		cout << "testAttach: " << e.what() << endl;
	}
    StringInFile finder(mainFileName);
	if(finder.find(before)) {
		cout << "testAttach: content before attach should not be present." << endl;
	}
	if(!finder.find(after)) {
		cout << "testAttach: content after attach should be present." << endl;
	}
    try {
        shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
        db->open(mainFileName);
        {
            Transaction tr = db->beginTrans(false);
            shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
            node->attach(tr);
            tr.commit();
        }
    }
    catch(exception &e) {
        checkException(e, "testAttach", "Illegal state in Database::doAttach: DU");
    }
}

int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
	signal(SIGSEGV, handleSigsegv);
	InitStatic globalStaticInitializer;
    ClassicStringPayload::setID(GEFactory::reg(ClassicStringPayload::create));
    Database::setErrorHandler(udbgraphErrorHandler);
	testNotReady();
	testSingleInsertCreate();
	testSingleInsertOpen();
	testVerMismatch();
	testNameMismatch();
	testMoreWritesPerTrans();
	testHashTableInserts();
	testCheckEnds();
	testAttach();
	// cout << "After hash insert - insert: " << UpsCounter::getInsert() << "  erase: " << UpsCounter::getErase() << "  find: " << UpsCounter::getFind() << endl;
    return 0;
}
