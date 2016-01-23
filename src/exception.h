/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#ifndef UDB_EXCEPTION_H
#define UDB_EXCEPTION_H

#if USE_NVWA == 1
#include"debug_new.h"
#endif
#include"udbgraph_config.h"

#ifdef DEBUG
#ifdef __GLIBC__
#ifdef __GLIBCXX__
#define BACKTRACE 1
#endif
#endif
#endif

#include<cstring>
#include<exception>
#include<ups/upscaledb.h>

namespace udbgraph {

/* Programmer errors regarding UDBGraph code are reported using logic_error.
These are only there for development, a mature code should never throw them.

Exceptions are thrown by value and must be
-caught by reference
-rethrown by plain throw (without any identifier)
in order to preserve polimorphism.*/

#define DESCR_STR_LEN 1023
#ifdef BACKTRACE
#define LINE_LEN 256
#define STACK_LEN 20
#define WHAT_STR_LEN (DESCR_STR_LEN + LINE_LEN * STACK_LEN)
#else
#define WHAT_STR_LEN DESCR_STR_LEN
#endif

    /** Common base class for all custom exceptions. It stores the description in
     * a character array, and if enabled, appends a demangled backtrace to it.
     * Backtrace id enabled if DEBUG is defined and we use a glibc and glibcxx. */
    class BaseException : public std::exception {
    protected:
        /** Error description, to be initialized with static const pointers or literals. */
        char descr[WHAT_STR_LEN + 1];

        /** Appends s to end of descr, considering the free space left. */
        void append(const char * const s) noexcept {
            strncat(descr, s, WHAT_STR_LEN - strlen(descr));
        }

        /** Appends demangled back trace if enabled. Solution taken from Timo Bingmann's article:
         * https://panthema.net/2008/0901-stacktrace-demangled/ */
        void appendBacktrace() noexcept;

        /** Constructs exception description using the given name and cause, appending
         * backtrace if enabled. */
        void makeDescr(const char * const name, const char * const cause) noexcept;

    private:
        /** Appends the demangled name to descr, returns true if anything appended. */
        bool demangle(const char * const name) noexcept;

    public:
        /** Returns the description. */
        const char* what() const noexcept { return descr; }
    };

    /** Exception class for UpscaleDB BaseException handling, uses its built-in messages.*/
    class UpsException : public BaseException {
    private:
        /** The error code. */
        ups_status_t errn;

    public:
        /** Registers the error code. */
        UpsException(ups_status_t st) noexcept : errn(st) {
            makeDescr("UpsException", ups_strerror(errn));
        }

        /** Returns the error code. */
        ups_status_t getErrno() const noexcept { return errn; }
    };

    /** Exception class for graph management related errors, base class for more specific ones. */
    class GraphException : public BaseException {
    protected:
        GraphException(const char * const name, const char * const cause) noexcept {
            makeDescr(name, cause);
        }
    };

    /** Exception for reporting access permission related errors. */
    class PermissionException : public GraphException {
    public:
        PermissionException(const char * const cp) noexcept : GraphException("PermissionException", cp) {}
    };

    /** Exception for reporting transaction-related errors. */
    class TransactionException : public GraphException {
    public:
        TransactionException(const char * const cp) noexcept : GraphException("TransactionException", cp) {}
    };

    /** Exception for reporting locking problems, for example an other transaction
     * locks a graph elem we want to perform an action on. */
    class LockedException : public GraphException {
    public:
        LockedException(const char * const cp) noexcept : GraphException("LockedException", cp) {}
    };

    /** Exception for reporting problems regarding accessing deleted or not registered elements. */
    class ExistenceException : public GraphException {
    public:
        ExistenceException(const char * const cp) noexcept : GraphException("ExistenceException", cp) {}
    };

    /** Exception for reporting database management related problems. */
    class DatabaseException : public GraphException {
    public:
        DatabaseException(const char * const cp) noexcept : GraphException("DatabaseException", cp) {}
    };

    /** Exception for reporting illegal method use, for example setting end on a node. */
    class IllegalMethodException : public GraphException {
    public:
        IllegalMethodException(const char * const cp) noexcept : GraphException("IllegalMethodException", cp) {}
    };

    /** Exception for reporting illegal arguments. */
    class IllegalArgumentException : public GraphException {
    public:
        IllegalArgumentException(const char * const cp) noexcept : GraphException("IllegalArgumentException", cp) {}
    };
}

#endif

