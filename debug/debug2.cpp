/*
COPYRIGHT COMES HERE
*/

// Functions with name test* are called from main. All other functions are called from these with name test*. */

/*
TODO:
test one GE with more RO transactions
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

void testWriteReadonly() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 2, "debug2");
		db->open(mainFileName);
		Transaction tr = db->beginTrans(TT::RO);
		shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node, tr);
	}
	catch(exception &e) {
        checkException(e, "testWriteReadonly", "Trying to write during a read-only transaction.");
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
			Transaction tr = db->beginTrans(TT::RW);
			shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
			ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
			pl.fill(1100);
			db->write(node, tr);
			pl.fill(8000);
			node->write(tr);
			tr.commit();
		}
		{
			Transaction tr = db->beginTrans(TT::RW);
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
			Transaction tr = db->beginTrans(TT::RW);
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
			Transaction tr = db->beginTrans(TT::RW);
			shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
			ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
			pl.set(before);
			db->write(node, tr);
			tr.commit();
			tr = db->beginTrans(TT::RW);
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
            Transaction tr = db->beginTrans(TT::RW);
            shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
            node->attach(tr);
            tr.commit();
        }
    }
    catch(exception &e) {
        checkException(e, "testAttach", "Trying to read an element for invalid key.");
    }
}

void testGetEdges() {
	try {
		// first create the node and edges we need
		shared_ptr<GraphElem> node, writeableEdge;
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->create(mainFileName, 0644);
//		db->open(mainFileName);
		try {
			Transaction tr = db->beginTrans(TT::RW);
			// create a node and keep it for later use
			node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
			db->write(node, tr);
			// create the later writeable empty edge
			writeableEdge = GEFactory::create(db, PT_EMPTY_UEDGE);
			writeableEdge->setStartRootEnd(node);
			db->write(writeableEdge, tr);
			// empty undir
			shared_ptr<GraphElem> edge = GEFactory::create(db, PT_EMPTY_UEDGE);
			edge->setStartRootEnd(node);
			db->write(edge, tr);
			// empty directed from root to node
			edge = GEFactory::create(db, PT_EMPTY_DEDGE);
			edge->setEndRootStart(node);
			db->write(edge, tr);
			// empty directed from node to root
			edge = GEFactory::create(db, PT_EMPTY_DEDGE);
			edge->setStartRootEnd(node);
			db->write(edge, tr);
			// int:1 directed from root to node
			edge = GEFactory::create(db, IntPayload::id());
            IntPayload& pl = dynamic_cast<IntPayload&>(edge->pl());
            pl.set(1);
			edge->setEndRootStart(node);
			db->write(edge, tr);
			// int:2 directed from root to node
			edge = GEFactory::create(db, IntPayload::id());
            pl = dynamic_cast<IntPayload&>(edge->pl());
            pl.set(2);
			edge->setEndRootStart(node);
			db->write(edge, tr);
			// now try to collect edges from root
			QueryResult result;
            db->getRootEdges(result, EdgeEndType::Any, Filter::allpass(), tr);
			int cnt;
            if((cnt = result.size()) != 6) {
                cout << "testGetEdges: wrong number of any edges from root: " << cnt << endl;
            }
			tr.commit();
		}
		catch(exception &e) {
			cout << "testGetEdges 1: " << e.what() << endl;
		}
		// now test for expected behaviour
		try {
			int cnt;
			Transaction trw = db->beginTrans(TT::RW);
			writeableEdge->attach(trw);
			Transaction trr = db->beginTrans(TT::RO);
			node->attach(trr);
			QueryResult result;
			try {
				node->getEdges(result, EdgeEndType::Un, Filter::allpass(), trr, false);
			}
			catch(exception &e) {
        		checkException(e, "testGetEdges", "Attempting a read-only transaction on an elem already present in a read-write one.");
			}
			result.clear();
			node->getEdges(result, EdgeEndType::Un, Filter::allpass(), trr);
			if((cnt = result.size()) != 1) {
				cout << "testGetEdges: wrong number of undirected edges: " << cnt << endl;
			}
			result.clear();
			node->getEdges(result, EdgeEndType::Any, Filter::allpass(), trr);
			if((cnt = result.size()) != 5) {
				cout << "testGetEdges: wrong number of any edges: " << cnt << endl;
			}
			result.clear();
			node->getEdges(result, EdgeEndType::In, Filter::allpass(), trr);
			if((cnt = result.size()) != 3) {
				cout << "testGetEdges: wrong number of incoming edges: " << cnt << endl;
			}
			result.clear();
			node->getEdges(result, EdgeEndType::Out, Filter::allpass(), trr);
			if((cnt = result.size()) != 1) {
				cout << "testGetEdges: wrong number of outgoing edges: " << cnt << endl;
			}
			result.clear();
			node->getEdges(result, EdgeEndType::Any, PayloadTypeFilter::get(PT_EMPTY_DEDGE), trr);
			if((cnt = result.size()) != 2) {
				cout << "testGetEdges: wrong number of directed edges filtered by payload type: " << cnt << endl;
			}
			result.clear();
			IntPayloadFilter ipf(1);
			node->getEdges(result, EdgeEndType::In, ipf, trr);
			if((cnt = result.size()) != 1) {
				cout << "testGetEdges: wrong number of directed edges filtered by content: " << cnt << endl;
			}
		}
		catch(bad_cast &e) { //exception &e) {
			cout << "testGetEdges 2: " << e.what() << endl;
		}

	}
	catch(bad_cast &e) { //exception &e) {
		cout << "testGetEdges 3: " << e.what() << endl;
	}
}

void attachAbort(shared_ptr<Database> &db, AM attachMode, TE trEnd, const char * const expectedAtt, const char * const expectedAb) {
	const char *amName, *teName;
	if(attachMode == AM::KEEP_PL) {
		amName = "AM::KEEP_PL";
	}
	else {
		amName = "AM::READ_PL";
	}
	if(trEnd == TE::AB_KEEP_PL) {
		teName = "TE::AB_KEEP_PL";
	}
	else {
		teName = "TE::AB_REVET_PL";
	}
	Transaction tr = db->beginTrans(TT::RW);
	shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
	ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
	pl.set("1");
	node->write(tr);
	tr.commit();
	pl.set("2");
	tr = db->beginTrans(TT::RW);
	node->attach(tr, attachMode);
	if(strcmp(pl.get(), expectedAtt)) {
		cout << "attachAbort(" << amName << ',' << teName << "): expected after attach: " << expectedAtt << " but got: " << pl.get() << endl;
	}	
	pl.set("3");
	node->write(tr);
	tr.abort(trEnd);
	if(strcmp(pl.get(), expectedAb)) {
		cout << "attachAbort(" << amName << ',' << teName << "): expected after abort: " << expectedAb << " but got: " << pl.get() << endl;
	}	
}

void testPayloadManagement() {
	try {
		shared_ptr<Database> db = Database::newInstance(1, 1, "debug2");
		db->open(mainFileName);
		attachAbort(db, AM::KEEP_PL, TE::AB_KEEP_PL, "2", "3");
		attachAbort(db, AM::KEEP_PL, TE::AB_REVERT_PL, "2", "2");
		attachAbort(db, AM::READ_PL, TE::AB_KEEP_PL, "1", "3");
		attachAbort(db, AM::READ_PL, TE::AB_REVERT_PL, "1", "1");
		// now test per-GraphElem abort behaviour
		Transaction tr = db->beginTrans(TT::RW);
		shared_ptr<GraphElem> node = GEFactory::create(db, ClassicStringPayload::id());
		ClassicStringPayload& pl = dynamic_cast<ClassicStringPayload&>(node->pl());
		pl.set("1");
		node->write(tr);
		tr.commit();
		pl.set("2");
		tr = db->beginTrans(TT::RW);
		node->attach(tr, AM::KEEP_PL);
		node->revertOnAbort();
		pl.set("3");
		node->write(tr);
		tr.abort(TE::AB_KEEP_PL);
		// should be reverted
		if(strcmp(pl.get(), "2")) {
			cout << "testPayloadManagement (with attach AM::KEEP_PL and abort TE::AB_KEEP_PL but individual revert on node): expected after abort: 2 but got: " << pl.get() << endl;
		}	
	}
	catch(exception &e) {
		cout << "testPayloadManagement: " << e.what() << endl;
	}
}
	
int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
	signal(SIGSEGV, handleSigsegv);
	InitStatic globalStaticInitializer;
    ClassicStringPayload::setID(GEFactory::reg(ClassicStringPayload::create));
    IntPayload::setID(GEFactory::reg(IntPayload::create));
    Database::setErrorHandler(udbgraphErrorHandler);
	testNotReady();
	testSingleInsertCreate();
	testSingleInsertOpen();
	testWriteReadonly();
	testVerMismatch();
	testNameMismatch();
	testMoreWritesPerTrans();
	testHashTableInserts();
	testCheckEnds();
	testAttach();
	testGetEdges();
	testPayloadManagement();
	// cout << "After hash insert - insert: " << UpsCounter::getInsert() << "  erase: " << UpsCounter::getErase() << "  find: " << UpsCounter::getFind() << endl;
    return 0;
}
