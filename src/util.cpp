/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#include"exception.h"
#include"util.h"

#ifdef BACKTRACE
#include<execinfo.h>
#include<cxxabi.h>
#endif

#include<iostream>

using namespace std;
using namespace udbgraph;

bool EndianInfo::littleEndian;

void EndianInfo::initStatic() noexcept {
    union {
        uint32_t i;
        char c[4];
    } bint = {0x01020304};
    littleEndian = bint.c[0] == 4;
}
