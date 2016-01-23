/*
COPYRIGHT COMES HERE
*/

#include<cstring>
#include<iostream>
#include"serializer.h"
#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace std;
using namespace udbgraph;

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
		int div10[] = {1000, 100, 10, 1};
		char remain[] = "xxxx abcd\n";
		char *cp = new char[offset - 7];
		int i;
        for(i = 0; i < offset - 8; i++) {
            int i10 = i % 10;
            if(i10 < 4) {
                cp[i] = '0' + ((i - i10) / div10[i10]) % 10;
			}
			else {
                cp[i] = remain[i10];
			}
		}
        cp[i] = 0;
        cout << '<' << i << ">\n";
		conv << cp;
		delete[] cp;
	}
	conv << u16 << u32 << u64;
    conv << s8 << s16 << s32 << s64;
    conv << f << d << u8;
    conv << ca;
	conv << s;
}

void serializeInplace1_deser(Converter &conv) {
    uint8_t u8;
    uint16_t u16;
    uint32_t u32;
    uint64_t u64;
    int8_t s8;
    int16_t s16;
    int32_t s32;
    int64_t s64;
    float f;
    double d;
    bool b;
    char *ca;
    string s;
    conv >> b;
	if(b) {
		char *cp;
        conv >> cp;
//        cout << '<' << cp << ">\n";
		delete[] cp;
	}
	conv >> u16 >> u32 >> u64;
    conv >> s8 >> s16 >> s32 >> s64;
    conv >> f >> d >> u8;
    conv >> ca >> s;
    cout << u8 << ' ' << u16 << ' ' << u32 << ' ' << u64 << endl;
    cout << s8 << ' ' << s16 << ' ' << s32 << ' ' << s64 << endl;
    cout << f << ' ' << d << ' ' << b << endl;
    cout << ca << ' ' << s << endl;
	delete[] ca;
}

void serializeInplace1(int offset) {
    RecordChain ser1(RT_NODE, 4);
	Converter conv1(ser1);
	cout << "\nWRITE:\n";
    serializeInplace1_ser(conv1, offset);
	ser1.setHeadField(FP_NEXT, KEY_INVALID);
    RecordChain ser2(RT_NODE, 4);
	Converter conv2(ser2);
    serializeInplace1_ser(conv2, 3000-offset);
	ser2.clone(ser1);
	cout << "\nREAD:\n";
    serializeInplace1_deser(conv2);
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

int main(int argc, char** argv) {
#if USE_NVWA == 1
    nvwa::new_progname = argv[0];
#endif
	RecordChain::setRecordSize(UDB_DEF_RECORD_SIZE);
	EndianInfo::initStatic();
    FixedFieldIO::initStatic();
    GEFactory::initStatic();
	cout << "START" << endl;
    serializeInplace1(0);
    serializeInplace1(1010);
    serializeInplace1(1000);
    serializeInplace1(980);
    serializeInplace1(2000);
    serializeInplace1(3000);
	testCounterMap();
    return 0;
}
