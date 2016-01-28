/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#ifndef UDB_SERIALIZER_H
#define UDB_SERIALIZER_H

#include<list>
#include<deque>
#include<cstdint>
#include<algorithm>
#include<ups/upscaledb.h>
#if USE_NVWA == 1
#include"debug_new.h"
#endif

#include"udbgraph_config.h"
#include"exception.h"
#include"util.h"

namespace udbgraph {

/** Application name length stored in root including terminating 0. */
#define APP_NAME_LENGTH 32

/** Default record size for UpscaleDB. */
#define UDB_DEF_RECORD_SIZE 1024

/** Maximal record size for UpscaleDB. */
#define UDB_MAX_RECORD_SIZE 1048576

    /** Key type for UpscaleDB. */
    typedef uint64_t keyType;

    /** Type for quantities stored in fixed record fields. */
    typedef uint32_t countType;

    /** UDBGraph graph elem type. */
    typedef uint32_t payloadType;

    /** Main record type for UpscaleDB. Classic enum, will be
     * converted to int. RT_ACL stands for access control list, which will be
    implemented in a future version. */
    enum RecordType {
        RT_ROOT, RT_ACL, RT_NODE, RT_DEDGE, RT_UEDGE, RT_CONT, RT_NOMORE
    };

    /** Common fixed field positons for all sort of records. */
    enum FieldPosCommon {
        FP_RECORDTYPE, FP_LOCK, FP_RES1, FP_RES2,
        FP_ACL,
        FP_NEXT = FP_ACL + sizeof(keyType),
        FP_PAYLOADTYPE = FP_NEXT + sizeof(keyType),	// 12
        FP_VAR = FP_PAYLOADTYPE + sizeof(payloadType)
    };

    /** Node fixed field positions in byte, part of root. */
    enum FieldPosNode {
        FPN_CNT_INEDGE = FP_VAR,
        FPN_CNT_OUTEDGE = FPN_CNT_INEDGE + sizeof(countType),
        FPN_CNT_UNEDGE = FPN_CNT_OUTEDGE + sizeof(countType),
        FPN_CNT_FREDGE = FPN_CNT_UNEDGE + sizeof(countType),
        FPN_VAR = FPN_CNT_FREDGE + sizeof(countType)	// 32
    };

    /** Root fixed field positions in byte.*/
    enum FieldPosRoot {
        FPR_VER_MAJOR = FPN_VAR,
        FPR_VER_MINOR = FPR_VER_MAJOR + sizeof(countType),
        FPR_APP_NAME = FPR_VER_MINOR + sizeof(countType),
        FPR_VAR = FPR_APP_NAME + APP_NAME_LENGTH
    };

    /** Edge (directed and undirected) fixed field positions in byte. */
    enum FieldPosEdge {
        FPE_NODE_START = FP_VAR,
        FPE_NODE_END = FPE_NODE_START + sizeof(keyType),
        FPE_VAR = FPE_NODE_END + sizeof(keyType)	// 32
    };

    /** Access control list fixed field positions in byte. */
    enum FieldPosACL {
        FPA_VAR = FP_VAR
        /* more fields come here */
    };

    /** Continued fixed field positions in byte. Records of a GraphElem form
    a double linked list to enhance the possibility of cleanup in case of an application
    crash or power outage. */
    enum FieldPosCont {
        FPC_HEAD = FP_PAYLOADTYPE + sizeof(payloadType),
        FPC_PREV = FPC_HEAD + sizeof(keyType),
        FPC_VAR = FPC_PREV + sizeof(keyType)	// 32
    };

    /** Special key values. */
    enum KeyValues : keyType {
        KEY_INVALID, KEY_ACL, KEY_ROOT, KEY_NOMORE
    };

    /** Special access control list values. ACL_FREE means free access to all, so
    the current version setting the ACL field to zero will be compatible with future
    versions. ACL_DB_PRIV means restricted access for DB management purposes in future
    verisons. */
    enum AccessControlValues : keyType {
        ACL_FREE, ACL_DB_PRIV, ACL_NOMORE
    };

    /** States for RecordChain. State represents what is actually read: if the
    chain consists of only the head it will be HEAD etc. */
    enum class RCState {
        /** No content. */
        EMPTY,

        /** Only the head is read. */
        HEAD,

        /** Content read partially, including the first record holding the beginning
         * of payload. */
        PARTIAL,

        /** Full content read. */
        FULL
    };

#define MAXMACRO(x,y) ((int(x))>(int(y))?(int(x)):(int(y)))
#define FIELD_VAR_MAX_POS MAXMACRO(MAXMACRO(MAXMACRO(FPN_VAR,FPR_VAR),MAXMACRO(FPE_VAR,FPC_VAR)),FPA_VAR)

    /** Common base class for classes performing UpscaleDB operations. */
    class CheckUpsCall {
    protected:
        /** Checks the return value and throws Exception if error. */
        void check(ups_status_t st) {
            if(st) {
                throw UpsException(st);
            }
        }
    };


    /** Base class to perform fixed field input/output. Used also in Dump. */
    class FixedFieldIO {
    protected:
        /** Array containing field sizes on each fixed field position.
         * Zero means invalid position for that record. */
        static uint32_t pos2sizes[RT_NOMORE][FIELD_VAR_MAX_POS];

    public:
        /** Initializes pos2sizes. */
        static void initStatic() noexcept;

        /** Sets the fixed field starting at fieldStart to the needed value,
         * considering the actual record type in the specified array.
         * The method assumes a valid record type at *array.
         * If debugging is enabled, positions and limits are checked using
         * the pos2Sizes array. */
        static void setField(uint32_t fieldStart, uint64_t value, uint8_t * const array);

        /** Gets the fixed field starting at fieldStart, considering the
         * actual record type from the specified array. The method assumes a valid
         * record type at *array. If debugging is enabled,
         * positions and limits are checked using the pos2Sizes array. */
        static uint64_t getField(uint32_t fieldStart, uint8_t * const array);

    protected:
        static void checkPosition(uint32_t fieldStart, int recordType);
    };

    /** Class to contain serialized native types, 0 delimited char arrays and strings.
     * in a chain of UpscaleDB records.
     * The class Converter and its caller code is responsible of appropriate
     * assembly and extraction as no type information is stored.
     * This class is not thread-safe. */
    class RecordChain final : CheckUpsCall {
    protected:

        /** A record in UpscaleDB to be accessed byte-wise. */
        class Record final : public CheckUpsCall, public FixedFieldIO {
        protected:
            /** UpscaleDB record size. */
            static size_t size;

            /** Pointer to the content. */
            uint8_t * record;

            /** Index for assemblying / extracting the content. */
			int index;

            /** Key in UpscaleDB if already persisted. */
            keyType key = keyType(KEY_INVALID);

            /** Structure to use in UpscaleDB, its data field points to key. */
            ups_key_t upsKey;

            /** Structure to use in UpscaleDB, its data field equals to record. */
            ups_record_t upsRecord;

        public:
            /** Array to index with RecordType to get the starting record indices. */
            static constexpr uint32_t recordVarStarts[] = {FPR_VAR, FPA_VAR, FPN_VAR, FPE_VAR, FPE_VAR, FPC_VAR};

            /** Gets the static record size. */
            static size_t getSize() { return size; }

            /** Sets the static record size. */
            static void setSize(size_t s);

            /** Checks the record size to make sure it was set. */
            static void checkSize();

            /** Constructs a new record in memory with predefined
             * UpscaleDB record size. Used right before reading. */
            Record();

            /** Constructs a new record in memory with predefined
             * UpscaleDB record size. */
            Record(RecordType rt, payloadType pType);

            /* Constructs a new record with prefilled byte array content.
            Record(uint8_t * const content, keyType key) :
                record(content), index(0), key(key) {}*/

            /** Destructor deletes the in-memory byte array. */
			~Record() { delete[] record; }

            /** No copy constructor. */
			Record(const Record& p) = delete;

            /** Move constructor. */
            Record(Record &&p) noexcept ;

            /** No copy assignment. */
			Record& operator=(const Record& p) = delete;

            /** Move assignment. */
            Record& operator=(Record &&p) noexcept;

            /** Clones the other instance here, does not set index.*/
            void clone(const Record &other) noexcept;

            /** Resets the index to the end of the fixed fields. */
            void reset() { index = recordVarStarts[record[FP_RECORDTYPE]]; }

            /** Returns the key. */
            keyType getKey() const { return key; }

            /** Sets the key. */
            void setKey(keyType k) { if(key == keyType(KEY_INVALID)) { key = k; } }

            /** Sets the fixed field starting at fieldStart to the needed value,
             * considering the actual record type using FixedFieldIO::setField. */
            void setField(uint32_t fieldStart, uint64_t value) {
                FixedFieldIO::setField(fieldStart, value, record);
            }

            /** Gets the fixed field starting at fieldStart, considering the
             * actual record type using FixedFieldIO::getField. */
            uint64_t getField(uint32_t fieldStart) {
                return FixedFieldIO::getField(fieldStart, record);
            }

            /** Puts a byte in the record. Returns true if
             * success, false if the record was full. */
            bool operator<<(const uint8_t byte) noexcept;

            /** Gets a byte from the record without knowing if
             * there is real content to read or we exceed
             * the amount of data once stored. Returns true
             * if the index was less than the record size. */
            bool operator>>(uint8_t &byte) noexcept;

            /** Read the record from db using the transaction. Calls check
             * if status was not UPS_KEY_NOT_FOUND, otherwise returns it and let the
             * caller handle it. */
            ups_status_t read(ups_db_t *db, keyType key, ups_txn_t *tr) noexcept;

            /** Write the record in db using the transaction. */
            void write(ups_db_t *db, ups_txn_t *tr);
        };

        /** UpscaleDB database pointer. */
        ups_db_t *db = nullptr;

        /** UpscaleDB key generator. */
        KeyGenerator<keyType> *keyGen = nullptr;

        /** State of this object. */
        RCState state = RCState::EMPTY;

        /** List of records assembled or to extract from. */
        std::list<Record> content;

        /** Iterator in content pointing to the Record to extract from or write into. */
        std::list<Record>::iterator iter = content.begin();

        /** Payload type for the content. */
        payloadType pType;

    public:
        /** Sets the record size. Must be called before the first RecordChain
        is instantiated. */
        static void setRecordSize(size_t s);

        /** Sets recordType. */
        RecordChain(RecordType rt, payloadType pt);

        /** Move constructor disabled. */
        RecordChain(RecordChain &&d) = delete;

        /** Copy constructor disabled. */
        RecordChain(const RecordChain &t) = delete;

        /** Move assignment disabled. */
        RecordChain& operator=(RecordChain &&d) = delete;

        /** Does not allow copying. */
        RecordChain& operator=(const RecordChain &t) = delete;

        /** Sets UpscaleDB db if not set yet. */
        void setDB(ups_db_t *d) { if(db == nullptr) db = d; }

        /** Sets keyGen if not set yet. */
        void setKeyGen(KeyGenerator<keyType> *kg) { if(keyGen == nullptr) keyGen = kg; }

        /** Clones the other instance here. Changes are already committed in UpscaleDB,
        or aborted, so no records are written now.*/
        void clone(const RecordChain &other);

        /** Resets the content iterator to the beginning and all Record indexes
        right after the fixed part. */
        void reset();

        /** Clears the record list. */
        void clear();

        /** Returns the chain status. */
        RCState getState() { return state; }

        /** Sets the fixed field starting at fieldStart to the needed value
         * in the first record considering the actual record type. */
        void setHeadField(uint32_t fieldStart, uint64_t value);

        /** Gets the fixed field starting at fieldStart from the first record,
         * considering the actual record type. */
        uint64_t getHeadField(uint32_t fieldStart);

        /** Gathers the old keys from content. */
        std::deque<keyType> getKeys() const;

        /** Removes all records after the one pointed by iter. */
        void stripLeftover();

        /** Reads the chain content to the requested level. Sets state according
         * the actual read stuff, e. g. if only head record existed, FULL. Throws
        exception if EMPTY was requested. */
        void read(keyType key, ups_txn_t *tr, RCState level);

        /** Writes actual content into db, considering the old record keys in
         * oldKeys. The overlapping part with the content will be updated,
         * the plus content is inserted, or the surplus old records removed.
        @param oldKeys the old keys
        @param key the already known GraphElem key for this elem. The containing
        GraphElem must know the new key before calling GraphElem.insert, but
        the same value is needed for the first record.
        @param the UpscaleDB transaction to use. */
        void write(std::deque<keyType> &oldKeys, keyType key, ups_txn_t *upsTr);

        /** Implementation: assembles a byte. */
        void write(uint8_t b);

        /** Implementation: extracts a byte. */
        void read(uint8_t &b);
    };

    /** A wrapper class for RecordChain to hide unnecessary parts from Payload providing
    (de)serialization of some native types. */
    class Converter final {
    protected:
        /** The RecordChain we wrap. */
        RecordChain &chain;

    public:
        Converter(RecordChain &s) : chain(s) {}

        /** Record assembly: stores a byte. */
        Converter& operator<<(uint8_t b) {
            chain.write(b);
            return *this;
        }

        /** Record assembly: guarantees signed and unsigned 8-64 bit native integers, float, double
        Stores everyting as little endian.*/
        template<typename T>
        Converter& operator<<(const T t) {
            union {
                T orig;
                uint8_t bytes[sizeof(T)];
            };
            orig = t;
            if(EndianInfo::isLittle()) {
                for(int i = 0; i < sizeof(T); i++) {
                    chain.write(bytes[i]);
                }
            }
            else {
                for(int i = sizeof(T) - 1; i >= 0; i--) {
                    chain.write(bytes[i]);
                }
            }
            return *this;
        }

        /** Record assembly: stores a byte. */
        Converter& operator<<(int8_t b) {
            chain.write(uint8_t(b));
            return *this;
        }

        /** Record assembly: stores a bool as a byte. */
        Converter& operator<<(bool b) {
            chain.write(uint8_t(b));
            return *this;
        }

        /** Record assembly: stores a 0-delimited char array and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& write(const char * const p);

        /** Record assembly: stores a 0-delimited char array and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(const char * const p) {
            return write(p);
        }

        /** Record assembly: stores a 0-delimited char array and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(char *p) {
            return write(const_cast<char *>(p));
        }

        /** Record assembly: stores a string and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(const std::string &s);

        /** Record assembly: stores a string and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(std::string &s) {
            return *this << const_cast<const std::string &>(s);
        }

        /** Extracts a byte from the actual record. */
        Converter& operator>>(uint8_t &b) {
            chain.read(b);
            return *this;
        }

        /** Extracts a byte from the actual record.
         * Guarantees signed and unsigned 8-64 bit integers, float, double
        Reads everyting as little endian.*/
        template<typename T>
        Converter& operator>>(T& t) {
            union {
                T orig;
                uint8_t bytes[sizeof(T)];
            };
            if(EndianInfo::isLittle()) {
                for(int i = 0; i < sizeof(T); i++) {
                    chain.read(bytes[i]);
                }
            }
            else {
                for(int i = sizeof(T) - 1; i >= 0; i--) {
                    chain.read(bytes[i]);
                }
            }
            t = orig;
            return *this;
        }

        /** Extracts a byte from the actual record. */
        Converter& operator>>(int8_t& b);

        /** Extracts a byte as bool from the actual record. */
        Converter& operator>>(bool &b);

        /** Extracts a 0-delimited character array.
         * Based on the stored length, allocates memory first, so it must be deleted after use. */
        Converter& operator>>(char *&p);

        /** Extracts a string.
         * Based on the stored length, allocates memory first. */
        Converter& operator>>(std::string &s);
    };
}

#endif
