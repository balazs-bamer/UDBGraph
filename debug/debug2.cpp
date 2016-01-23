/*
COPYRIGHT COMES HERE
*/

#include<cstring>
#include<iostream>
#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

void UPS_CALLCONV udbgraphErrorHandler(int level, const char *message) {
    cerr << level << ": " << message << endl;
}

void notReady() {
	try {
		shared_ptr<Database> db = Database::newInstance(false, 1, 1, "debug2");
                shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
	}
	catch(exception &e) {
		cout << "notReady (expected): " << e.what() << endl;
	}
}

void singleInsertCreate() {
	try {
		shared_ptr<Database> db = Database::newInstance(false, 1, 1, "debug2");
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
		shared_ptr<Database> db = Database::newInstance(false, 1, 1, "debug2");
		db->open("debug2a.udbg");
		shared_ptr<GraphElem> node = GEFactory::create(db, payloadType(PT_EMPTY_NODE));
		db->write(node);
	}
	catch(exception &e) {
		cout << "singleInsertOpen: " << e.what() << endl;
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
	static shared_ptr<GraphElem> create(shared_ptr<Database> db, payloadType pt) {
		return shared_ptr<GraphElem>(new Node(db, std::unique_ptr<Payload>(new MoreWritesPerTrans(pt))));
	}

	void fill(size_t len) {
		if(len > 9999) {
			len = 9999;
		}
		if(maxLen < len) {
			delete[] content;
			content = new char[maxLen = len];
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
		shared_ptr<Database> db = Database::newInstance(false, 1, 1, "debug2");
		db->open("debug2a.udbg");
		{
			Transaction tr = db->beginTrans(false);
			shared_ptr<GraphElem> node = GEFactory::create(db, MoreWritesPerTrans::id());
			MoreWritesPerTrans& pl = dynamic_cast<MoreWritesPerTrans&>(node->pl());
			pl.fill(100);
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

int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
	InitStatic globalStaticInitializer;
	Database::setErrorHandler(udbgraphErrorHandler);
	notReady();
	singleInsertCreate();
	singleInsertOpen();
	moreWritesPerTrans();
    return 0;
}
