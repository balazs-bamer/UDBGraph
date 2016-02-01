/*
COPYRIGHT COMES HERE
*/

#include<map>
#include<unordered_map>
#include<list>
#include<cstring>
#include<iostream>
#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

// parts taken from UpscaleDB samples db2.c

/** Simple class to dump a whole database to stdout. */
class Dump final : public CheckUpsCall {
protected:

    /** Holds an UpscaleDB record and can write its  fixed field to stdout. */
    class Record final : public FixedFieldIO {
    protected:
		/** Record size. */
		static size_t size;

        /** Record type names. */
        static constexpr char recordTypeNames[6][6] = {"root ", "acl ", "node ", "dedge", "uedge", "cont "};

        /** The data itself. */
        uint8_t *record;

		/** Its key. */
		keyType key;

    public:
		/** Sets the static field size. */
        static void setSize(size_t s) {
			if(size == 0) {
		        if(s <= FIELD_VAR_MAX_POS || s > UDB_MAX_RECORD_SIZE) {
        		    throw logic_error("Invalid record size.");
		        }
        		size = s;
		    }
		}

        Record(keyType k, const uint8_t * const rec) : record(new uint8_t[size]), key(k) {
            memcpy(record, rec, size);
        }

        ~Record() { delete[] record; }

        /** No copy constructor. */
        Record(const Record& p) {
            record = new uint8_t[size];
			key = p.key;
            memcpy(record, p.record, size);
        }

        /** Move constructor. */
        Record(Record &&p) noexcept {
            record = p.record;
			key = p.key;
            p.record = nullptr;
        }

        /** No copy assignment. */
        Record& operator=(const Record& p) = delete;

        /** Move assignment. */
        Record& operator=(Record &&p) noexcept;

        /** Gets the fixed field starting at fieldStart, considering the
         * actual record type using FixedFieldIO::getField. */
        uint64_t getField(uint32_t fieldStart) const {
            return FixedFieldIO::getField(fieldStart, record);
        }

        /** Prints the fixed fields to stdout. */
        void print() const;

    protected:
        /** Pretty-prints a field located at addr in record. */
        void print(const char * const name, int addr) const;

        /** Prints record-specific fields. */
        void printRoot() const;

        /** Prints record-specific fields. */
        void printACL() const;

        /** Prints record-specific fields. */
        void printNode() const;

        /** Prints record-specific fields. */
        void printEdge() const;

        /** Prints record-specific fields. */
        void printCont() const;
    };

    /** Array containing field sizes on each fixed field position.
     * Zero means invalid position for that record. */
    static uint32_t pos2sizes[RT_NOMORE][FIELD_VAR_MAX_POS];

    /** UpscaleDB environment. */
    ups_env_t *env;

    /** UpscaleDB database. */
    ups_db_t *db;

    /** Contains the records chained into list. Key is the head key. */
    map<keyType, list<Record>> chains;

    /** Contains the non-head (continued) records before inserting in a chain. */
    unordered_map<keyType, Record> conts;

public:
    /** Constructs a new object with the filename to open. */
    Dump(const char * const name);

    /** Closes everything. */
    ~Dump();

    /** Reads the whole stuff and stores in memory, intended only for debugging. */
    void read();

    /** Moves all records from conts into chains. */
    void postProcess();

    /** Prints everything to stdout. */
    void print();

protected:
    /** Processes a key-record pair. */
    void process(const ups_key_t &key, const ups_record_t &record);
};

size_t Dump::Record::size = 0;

char constexpr Dump::Record::recordTypeNames[6][6];

void Dump::Record::print() const {
	cout << "@:" << key << ' ';
    if(*record < uint8_t(RT_NOMORE)) {
        cout << recordTypeNames[*record];
    }
    else {
        cout << "!dmg: " << *record;
    }
    cout << ' ';
    print("acl", FP_ACL);
    print("next", FP_NEXT);
    print("payl", FP_PAYLOADTYPE);
    switch(*record) {
    case RT_ROOT:
        printRoot();
        break;
    case RT_ACL:
        printACL();
        break;
    case RT_NODE:
        printNode();
        break;
    case RT_DEDGE:
        printEdge();
        break;
    case RT_UEDGE:
        printEdge();
        break;
    case RT_CONT:
        printCont();
        break;
    }
    cout << '\n';
}

void Dump::Record::print(const char * const name, int addr) const {
    cout << '(' << name << ':' << getField(addr) << ')';
}

void Dump::Record::printRoot() const {
    print("verMaj", FPR_VER_MAJOR);
    print("verMaj", FPR_VER_MINOR);
    cout << "(name:";
    for(int i = FPR_APP_NAME; record[i] != 0; i++) {
        cout << record[i];
    }
    cout << ')';
    print("in", FPN_CNT_INEDGE);
    print("out", FPN_CNT_OUTEDGE);
    print("un", FPN_CNT_UNEDGE);
    print("fr", FPN_CNT_FREDGE);
}

void Dump::Record::printACL() const {
    cout << "error: not implemented yet.";
}

void Dump::Record::printNode() const {
    print("in", FPN_CNT_INEDGE);
    print("out", FPN_CNT_OUTEDGE);
    print("un", FPN_CNT_UNEDGE);
    print("fr", FPN_CNT_FREDGE);
}

void Dump::Record::printEdge() const {
    print("start", FPE_NODE_START);
    print("end", FPE_NODE_END);
}

void Dump::Record::printCont() const {
    print("head", FPC_HEAD);
}

Dump::Dump(const char * const name) {
    uint32_t flags = UPS_ENABLE_CRC32;
    ups_parameter_t param[] = {
        {UPS_PARAM_CACHE_SIZE, 64 * 1024 * 1024 },
        {UPS_PARAM_POSIX_FADVISE, UPS_POSIX_FADVICE_NORMAL},
    //    {UPS_PARAM_ENABLE_JOURNAL_COMPRESSION, 1},
        {0, 0}
    };
    check(ups_env_open(&env, name, flags, param));
    flags = 0;
    ups_status_t st = ups_env_open_db(env, &db, 1, flags, nullptr);
    if(st) {
        // try to free env, its result is not interesting any more
        flags = UPS_TXN_AUTO_ABORT;
        ups_env_close(env, flags);
        check(st);
    }
	// find out the record size
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    check(ups_cursor_create(&cursor, db, 0, 0));
    check(ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_FIRST));
    check(ups_cursor_close(cursor));
    Record::setSize(rec.size);
}

Dump::~Dump() {
    uint32_t flags = UPS_TXN_AUTO_ABORT;
    ups_status_t st1 = ups_db_close(db, flags);
    db = nullptr;
    ups_status_t st2 = ups_env_close(env, flags);
    env = nullptr;
    if(st1) {
        check(st1);
    }
    else {
        check(st2);
    }
}

void Dump::read() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    /* create a new cursor */
    check(ups_cursor_create(&cursor, db, 0, 0));
    /* get a cursor to the source database */
    check(ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_FIRST));
    while(true) {
        // process record
        process(key, rec);
        /* fetch the next item, and repeat till we've reached the end
         * of the database */
        ups_status_t st = ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT);
        if (st == UPS_KEY_NOT_FOUND) {
            break;
        }
        check(st);
    };
    /* clean up and return */
    check(ups_cursor_close(cursor));
}

void Dump::postProcess() {
    for(auto &chain : chains) {
        while(true) {
            auto last = chain.second.end();
            last--;
            keyType next = last->getField(FP_NEXT);
            if(next == KEY_INVALID) {
                break;
            }
            auto found = conts.find(next);
            if(found == conts.end()) {
                break;	// broken database
            }
            chain.second.push_back(found->second);
            conts.erase(found);
        }
    }
}

void Dump::print() {
    if(conts.size() != 0) {
        cout << "Broken database: oprhaned non-head records:\n";
        for(auto &kv : conts) {
            cout << "! ";
            kv.second.print();
        }
    }
    for(auto &kv : chains) {
        bool head = true;
        for(auto &record : kv.second) {
            if(!head) {
                cout << "    ";
            }
            record.print();
            head = false;
        }
    }
    cout << endl;
}

void Dump::process(const ups_key_t &k, const ups_record_t &r) {
    keyType key = *reinterpret_cast<keyType*>(k.data);
    uint8_t * const record = reinterpret_cast<uint8_t*>(r.data);
    if(*record != uint8_t(RT_CONT)) {
        // head
        list<Record> chain;
        chain.push_back(Record(key, record));
        chains.insert(pair<keyType, list<Record>>(key, chain));
    }
    else {
        // continued
        conts.insert(pair<keyType, Record>(key, Record(key, record)));
    }
}

int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
    InitStatic globalStaticInitializer;
    try {
        if(argc < 2) {
            cerr << "Usage: dumpdb <database name>\n";
            return 2;
        }
        Dump dump(argv[1]);
        dump.read();
        dump.postProcess();
        dump.print();
    }
    catch(BaseException &e) {
        cerr << e.what() << endl;
        return 1;
    }
    return 0;
}
