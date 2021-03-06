/*
COPYRIGHT COMES HERE
*/

#include<cstdio>
#include<cstring>
#include<csignal>
#include<iostream>
#include<deque>
#include"serializer.h"
#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

#ifndef DEBUG
#error "Testing needs debug functions."
#endif

using namespace std;
using namespace udbgraph;

bool diskBased = false;

/** SIGSEGV handler, throws a DebugException. */
extern void handleSigsegv(int sig) {
	throw DebugException("SIGSEGV!");
}

// caller should delete the returned pointer
char* makeTestFill(int l) {
	if(l < 0) {
		l = 0;
	}
	if(l > 9999) {
		l = 9999;
	}
	int div10[] = {1000, 100, 10, 1};
	char remain[] = "xxxx abcd\n";
	char *cp = new char[l + 1];
	int i;
	for(i = 0; i < l; i++) {
		int i10 = i % 10;
		if(i10 < 4) {
			cp[i] = '0' + ((i - i10) / div10[i10]) % 10;
		}
		else {
			cp[i] = remain[i10];
		}
	}
	cp[i] = 0;
	return cp;
}

void serializeInplace1_ser(Converter &conv, int offset) {
    bool b = (offset >= 18 && offset < 10000);			// 0
    uint16_t u16 = 333;				// 1 
    uint32_t u32 = 1000000000;		// 3
    uint64_t u64 = 1000000000000;	// 7
    int8_t s8 = 'A';				// 15
    int16_t s16 = -333;				// 16
    int32_t s32 = -1000000000;		// 18
    int64_t s64 = -1000000000000;	// 22
    float f = 3.3f;					// 30
    double d = 3.1415926539;		// 34
    uint8_t u8 = 'a';				// 42
    const char *ca = "char array";	// 43
    string s = string("string");	// 61
    conv << b;						// 75
	if(b) {
		char *cp = makeTestFill(offset - 7);
		conv << cp;
		delete[] cp;
	}
	conv << u16 << u32 << u64;
    conv << s8 << s16 << s32 << s64;
    conv << f << d << u8;
    conv << ca;
	conv << s;
}

void serializeInplace(int offset) {
    RecordChain ser1(RT_NODE, 4);
	Converter conv1(ser1);
    // create original contents 
    bool b = (offset >= 18 && offset < 10000);			// 0
    uint16_t u16 = 333;				// 1 
    uint32_t u32 = 1000000000;		// 3
    uint64_t u64 = 1000000000000;	// 7
    int8_t s8 = 'A';				// 15
    int16_t s16 = -333;				// 16
    int32_t s32 = -1000000000;		// 18
    int64_t s64 = -1000000000000;	// 22
    float f = 3.3f;					// 30
    double d = 3.1415926539;		// 34
    uint8_t u8 = 'a';				// 42
    const char *ca = "char array";	// 43
    string s = string("string");	// 61
    conv1 << b;						// 75
	if(b) {
		char *cp = makeTestFill(offset - 7);
		conv1 << cp;
		delete[] cp;
	}
	conv1 << u16 << u32 << u64;
    conv1 << s8 << s16 << s32 << s64;
    conv1 << f << d << u8;
    conv1 << ca;
	conv1 << s;
	ser1.setHeadField(FP_NEXT, KEY_INVALID);
	// create other rc to clone into
    RecordChain ser2(RT_NODE, 4);
	Converter conv2(ser2);
    serializeInplace1_ser(conv2, 3000-offset);
	ser2.clone(ser1);
	// check results
    uint8_t _u8;
    uint16_t _u16;
    uint32_t _u32;
    uint64_t _u64;
    int8_t _s8;
    int16_t _s16;
    int32_t _s32;
    int64_t _s64;
    float _f;
    double _d;
    bool _b;
    char *_ca;
    string _s;
    conv2 >> _b;
	if(_b) {
		char *cp;
        conv2 >> cp;
		delete[] cp;
	}
	conv2 >> _u16 >> _u32 >> _u64;
    conv2 >> _s8 >> _s16 >> _s32 >> _s64;
    conv2 >> _f >> _d >> _u8;
    conv2 >> _ca >> _s;
	if(_b != b || _u16 != u16 || _u32 != u32 || _u64 != u64 ||
		_s8 != s8 || _s16 != s16 || _s32 != s32 || _s64 != s64 ||
		_f != f || _d != d || strcmp(ca, _ca) || s != _s) {
		cout << "serializeInplace(" << offset <<"): mismatch\n";
	}
	delete[] _ca;    
}

void testSerialize() {
    serializeInplace(0);
    serializeInplace(1010);
    serializeInplace(1000);
    serializeInplace(980);
    serializeInplace(2000);
    serializeInplace(3000);
}

void testAlign(int pad, uint64_t count) {
    RecordChain ser(RT_NODE, 4);
	Converter conv(ser);
	uint8_t p = 1;
	for(int i = 0; i < pad; i++) {
		conv << p;
	}
	for(uint64_t i = 0; i < count; i++) {
		conv << i;
	}
	ser.reset();
	for(int i = 0; i < pad; i++) {
		conv >> p;
	}
	for(uint64_t i = 0; i < count; i++) {
		uint64_t res;
		conv >> res;
		if(res != i) {
			cout << "Alignment problem at pad " << pad << '\n';
		}
	}
}

void testAlignment() {
	for(int i = 0; i < 8; i++) {
		testAlign(i, 1000);
	}
	Converter::enableUnalign();
	for(int i = 0; i < 8; i++) {
		testAlign(i, 1000);
	}
}

void testFixedIO() {
	uint8_t u8 = 3;
	uint32_t u32 = 12345678;
	uint64_t u64 = 12345678987654321;
	uint8_t array[1000];
	array[0] = 0;
	FixedFieldIO::setField(FP_LOCK, u8, array);
	FixedFieldIO::setField(FP_PAYLOADTYPE, u32, array);
	FixedFieldIO::setField(FP_NEXT, u64, array);
	if(FixedFieldIO::getField(FP_LOCK, array) != u8 ||
		FixedFieldIO::getField(FP_PAYLOADTYPE, array) != u32 ||
		FixedFieldIO::getField(FP_NEXT, array) != u64) {
		cout << "FixedFieldIO problem.\n";
	}
}

void testCounterMap() {
	CounterMap<keyType, countType> cnt;
	cnt.inc(1);
	cnt.inc(2);
	cout << "1: " << cnt.count(1) << " 2: " << cnt.count(2) << endl;
	cnt.inc(1);
	cnt.dec(2);
	cout << "1: " << cnt.count(1) << " 2: " << cnt.count(2) << endl;
	cnt.inc(1);
	cout << "1: " << cnt.count(1) << " 2: " << cnt.count(2) << endl;
	cnt.dec(1);
	cnt.dec(1);
	cnt.dec(1);
//	cnt.dec(2); logic_error
	cout << "1: " << cnt.count(1) << " 2: " << cnt.count(2) << endl;
}

void check(ups_status_t st) {
	if(st) {
		throw UpsException(st);
	}
}

void writeKeys(deque<keyType> &&keys, const char * const label) {
	cout << label << " [" << keys.size() << "]: ";
	for(keyType &key : keys) {
		cout << '(' << key << ") ";
	} 
	cout << '\n';
}

//#define COUT_LOADSAVE

void loadSave(bool isNode, countType edgesIn, countType edgesOut, countType edgesUn, int payLen, bool readOnce, uint64_t recordSize) {
#ifdef COUT_LOADSAVE
	cout << "Node: " << isNode << "  In: " << edgesIn << "  Out: " << edgesOut << "  Un: " << edgesUn << "  payLen: " << payLen << "  readOnce: " << readOnce << "  recordSize: " << recordSize << '\n';
#endif
	// create database and record chain
	char filename[] = "debug1-test-load-save.udbg";
	RecordChain::setRecordSize(recordSize);
    ups_env_t *env = nullptr;
    ups_db_t *db = nullptr;
	uint32_t flags = UPS_ENABLE_TRANSACTIONS | /*UPS_FLUSH_WHEN_COMMITTED |*/ (!diskBased ? UPS_IN_MEMORY : UPS_ENABLE_CRC32);
    ups_parameter_t param[] = {
        {UPS_PARAM_CACHE_SIZE, static_cast<uint64_t>(diskBased ? 64 * 1024 * 1024 : 0) },
        {UPS_PARAM_POSIX_FADVISE, UPS_POSIX_FADVICE_NORMAL},
      //  {UPS_PARAM_ENABLE_JOURNAL_COMPRESSION, 1},
        {0, 0}
    };
	remove(filename);
    check(ups_env_create(&env, filename, flags, 0644, param));
    flags = 0;
    ups_parameter_t param2[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT64},
        {UPS_PARAM_RECORD_SIZE, recordSize},
//        {UPS_PARAM_RECORD_COMPRESSION, 1},
        {0, 0}
    };
    ups_status_t st = ups_env_create_db(env, &db, 1, flags, param2);
    if(st) {
        // try to free env, its result is not interesting any more
        flags = UPS_TXN_AUTO_ABORT;
        ups_env_close(env, flags);
        check(st);
    }
	ups_txn_t *tr;
    check(ups_txn_begin(&tr, env, nullptr, nullptr, 0));
    RecordChain::setRecordSize(recordSize);
    KeyGenerator<keyType> *keygen = new KeyGenerator<keyType>(KEY_ROOT);
	RecordChain rc(isNode ? RT_NODE : RT_DEDGE, 4);
	rc.setKeyGen(keygen);
	rc.setDB(db);
	Converter conv(rc);

	// fill record chain
	if(isNode) {
		rc.setHeadField(FPN_IN_BUCKETS, edgesIn);
		rc.setHeadField(FPN_OUT_BUCKETS, edgesOut);
		rc.setHeadField(FPN_UN_BUCKETS, edgesUn);
		rc.appendMissingRecords();
        rc.reset();	// hash table sizes changed
        rc.fillHashTable(FPN_IN_BUCKETS);
		rc.fillHashTable(FPN_OUT_BUCKETS);
		rc.fillHashTable(FPN_UN_BUCKETS);
	}
	char *testFill = makeTestFill(payLen);
	conv << testFill;

	// save record chain
	std::deque<keyType> oldKeys;
	keyType key = keygen->nextKey();
	rc.save(oldKeys, key, tr);

	// load record chain
	rc.clear();
	if(readOnce) {
		rc.load(key, tr, RCState::FULL);
		if(isNode) {
#ifdef COUT_LOADSAVE
			cout << "edge count: " << rc.getHeadField(FPN_CNT_INEDGE) << '\n';
#endif
		}
	}
	else {
		rc.load(key, tr, RCState::HEAD);
		if(isNode) {
#ifdef COUT_LOADSAVE
			cout << "edge count: " << rc.getHeadField(FPN_CNT_INEDGE) << '\n';
#endif
		}
#ifdef COUT_LOADSAVE
		writeKeys(rc.getKeys(), "   head");
#endif
		rc.load(key, tr, RCState::PARTIAL);
#ifdef COUT_LOADSAVE
		writeKeys(rc.getKeys(), "partial");
#endif
		rc.load(key, tr, RCState::FULL);
	}
#ifdef COUT_LOADSAVE
	writeKeys(rc.getKeys(), "   full");
#endif

	// check result
	if(isNode) {
		rc.checkHashTable(FPN_IN_BUCKETS);
		rc.checkHashTable(FPN_OUT_BUCKETS);
		rc.checkHashTable(FPN_UN_BUCKETS);
	}
	char* result;
	conv >> result;
	for(int i = 0; i < payLen; i++) {
		char res = result[i];
		if(res == 0) {
			cout << "premature end at: " << i << '\n';
			break;
		}
		if(res != testFill[i]) {
			cout << "expected char: " << int(testFill[i]) << ", but got: " << int(res) << '\n';
		}
	}
	delete[] result;
	delete[] testFill;
#ifdef COUT_LOADSAVE
	cout << endl;
#endif
	
	// close everything
	check(ups_txn_commit(tr, 0));
	flags = 0;
    flags = UPS_TXN_AUTO_ABORT | UPS_AUTO_CLEANUP;
	delete keygen;
    check(ups_env_close(env, flags));
}

void testLoadSave() {
//void loadsave(bool isNode, int edgesIn, int edgesOut, int edgesUn, int payLen, bool readOnce, uint64_t recordSize) {
	int recordSizes[] = {1024, 256};
	int payloadSizes[] = {0, 10, 800, 1800, 3800};
	int hashTableSizes[2][5] = {{5, 29, 31, 59, 61}, {5, 127, 129, 251, 253}};
//	int hashTableSizes[2][3] = {{5, 31, 61}, {5, 129, 253}};
	for(int rs : recordSizes) {
		for(int p : payloadSizes) {
			for(int r1 = 0; r1 < 2; r1++) {
				loadSave(false, 0, 0, 0, p, r1 == 1, rs);
				for(int i : hashTableSizes[rs == 256 ? 0 : 1]) {
					for(int o : hashTableSizes[rs == 256 ? 0 : 1]) {
						for(int u : hashTableSizes[rs == 256 ? 0 : 1]) {
							loadSave(true, i, o, u, p, r1 == 1, rs);
						}
					}
				}
			}
		}
    }
}

int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
	diskBased = argc > 1 && !strcmp(argv[1], "-d");
	signal(SIGSEGV, handleSigsegv);
	RecordChain::setRecordSize(UDB_DEF_RECORD_SIZE);
	EndianInfo::initStatic();
    FixedFieldIO::initStatic();
    GEFactory::initStatic();
	cout << "-d means disk-based UpscaleDB, now " << (diskBased ? "disk" : "memory") << " based." << endl;
	testSerialize();
	testAlignment();
	testFixedIO();
	testCounterMap();
	testLoadSave();
    return 0;
}
