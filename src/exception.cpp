/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#include"exception.h"

#ifdef BACKTRACE
#include<execinfo.h>
#include<cxxabi.h>
#endif

#include<iostream>

using namespace std;
using namespace udbgraph;

void BaseException::appendBacktrace() noexcept {
#ifdef BACKTRACE
    append("\n");
    void* addrlist[STACK_LEN];
    int addrlen = backtrace(addrlist, STACK_LEN);
    if(addrlen == 0 ) {
        append("Error acquiring backtrace.\n");
        return;
    }
    // resolve addresses into strings containing "filename(function+address)",
    // this array must be free()-ed
    char** symbollist = backtrace_symbols(addrlist, addrlen);
    if(symbollist == nullptr) {
        append("Error acquiring backtrace.\n");
        return;
    }
    // Iterate over the returned symbol lines. Skip the first two, these are
    // this function and the constructors.
    for (int i = 4; i < addrlen; i++) {
        append("- ");
        char *begin_name = nullptr, *begin_offset = nullptr, *end_offset = nullptr;
        // find parentheses and +address offset surrounding the mangled name:
        // ./module(function+0x15c) [0x8048a6d]
        for (char *p = symbollist[i]; *p; ++p) {
            switch(*p) {
            case '(':
                begin_name = p;
                break;
            case '+':
                begin_offset = p;
                break;
            case ')':
                if(begin_offset != nullptr) {
                    end_offset = p;
                }
            }
            if(end_offset != nullptr) {
                break;
            }
        }
        if (begin_name != nullptr && begin_offset != nullptr &&
                end_offset != nullptr && begin_name < begin_offset) {
            *begin_name++ = '\0';
            *begin_offset++ = '\0';
            *end_offset = '\0';
            // mangled name is now in [begin_name, begin_offset) and caller
            // offset in [begin_offset, end_offset).
            if(!demangle(begin_name)) {
                // if failed, append mangled name
                append(begin_name);
            }
            append(": ");
            append(begin_offset);
        }
        else {
            // couldn't parse the line? print the whole line.
            append(symbollist[i]);
        }
        append("\n");
    }
    free(symbollist);
#endif
}

void BaseException::makeDescr(const char * const name, const char * const cause) noexcept {
    append(name);
    append(": ");
    append(cause);
    appendBacktrace();
}

bool BaseException::demangle(const char * const name) noexcept {
#ifdef BACKTRACE
    int status;
    char *result = abi::__cxa_demangle(name, nullptr, nullptr, &status);
    if(result != nullptr) {
        append(result);
        free(result);
        return true;
    }
#endif
    return false;
}

