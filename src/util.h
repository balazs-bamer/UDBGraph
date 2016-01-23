/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#ifndef UDB_UTIL_H
#define UDB_UTIL_H

#include<unordered_map>
#include<mutex>

#if USE_NVWA == 1
#include"debug_new.h"
#endif

namespace udbgraph {

    /** Associative container supporting incrementing and decrementing values
     *	by key and querying count by key. valueT should be an integer type. The class
     * is not thread-safe.
     */
    template<typename keyT, typename valueT>
    class CounterMap final {
    protected:
        /** Hash map for storage. */
        std::unordered_map<keyT, valueT> content;

    public:
        /** Increments the value for the key if present, or inserts 1 if not. */
        void inc(keyT key) {
            auto found = content.find(key);
            if(found == content.end()) {
                content.insert(std::pair<keyT, valueT>(key, 1));
            }
            else {
                (found->second)++;
            }
        }

        /** Decrements the value for the key if present, and if the result is 0,
         * removes it.
        @return true if the key was not found or was just deleted, so always when
        after method call the key is not any more in the counter. */
        bool dec(keyT key) {
            auto found = content.find(key);
            if(found == content.end()) {
                return true;
            }
            else {
                if(--(found->second) == 0) {
                    content.erase(found);
                    return true;
                }
            }
            return false;
        }

        /** Returns the count for the key if found, otherwise 0. */
        valueT count(keyT key) const {
            auto found = content.find(key);
            if(found == content.end()) {
                return 0;
            }
            else {
                return found->second;
            }
        }
    };

    /** Class providing a unique key generator by incrementing a counter. T must be an
        integer type. The class is not thread-safe.*/
    template<typename T>
    class KeyGenerator final {
    protected:
        /** The counter variable, the actual value will be returned next time.
         * Initial value is 0.*/
        T counter;

    public:
        /** Initializes the counter. */
        KeyGenerator(T start) noexcept { counter = start; }

        /** Returns the next value and advances the counter. */
        T nextKey() noexcept { return counter++; }
    };

    /** Class for simultaneously locking two mutexes and have the destructor for
     * RAII cleanup in case of an exception. */
    class LockGuard2 final {
    protected:
        /** The mutex to deal with. */
        std::mutex &mtx1, &mtx2;
    public:
        /** Locks the mutexes. */
        LockGuard2(std::mutex &m1, std::mutex &m2) : mtx1(m1), mtx2(m2) {
            std::unique_lock<std::mutex> lock1(mtx1, std::defer_lock);
            std::unique_lock<std::mutex> lock2(mtx2, std::defer_lock);
            std::lock(lock1, lock2);
        }

        /** Unlocks the mutexes. */
        ~LockGuard2() { mtx1.unlock(); mtx2.unlock(); }
    };

    /** Class for determining and holding endian info. */
    class EndianInfo {
    protected:
        /** True if we are on a little-endian architecture. */
        static bool littleEndian;

    public:
        /** Sets endianness for future use.
         * X86 and x64 are little-endian. Called in a static instance of class
         * InitStatic to register built-in types. */
        static void initStatic() noexcept;

        /** Returns true if we are on little-endian arch. */
        static bool isLittle() { return littleEndian; }
    };

}

#endif
