/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#ifndef UDB_SERIALIZER_H
#define UDB_SERIALIZER_H

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

    /** Type for indexing along the records and hash. */
    typedef uint64_t indexType;

    /** UDBGraph graph elem type. */
    typedef uint32_t payloadType;

    /** Main record type for UpscaleDB. Classic enum, will be
     * converted to int. RT_ACL stands for access control list, which will be
    implemented in a future version. */
    enum RecordType {
        RT_INVALID, RT_ROOT, RT_ACL, RT_NODE, RT_DEDGE, RT_UEDGE, RT_CONT, RT_NOMORE
    };

    /** Common fixed field positons for all sort of records. */
    enum FieldPosCommon {
        FP_RECORDTYPE, FP_LOCK, FP_RES1, FP_RES2,
        FP_ACL,
        FP_NEXT = FP_ACL + sizeof(keyType),
        FP_PAYLOADTYPE = FP_NEXT + sizeof(keyType),
        FP_VAR = FP_PAYLOADTYPE + sizeof(payloadType) // 24
    };

    /** Relative field positions for bucket management. */
    enum FieldRel {
        FR_USED = sizeof(countType),
        FR_DELETED = FR_USED + sizeof(countType),
        FR_SPAN = FR_DELETED + sizeof(countType)
    };

    /** Node fixed field positions in byte, part of root. */
    enum FieldPosNode {
        FPN_IN_BUCKETS = FP_VAR,
        FPN_IN_USED = FPN_IN_BUCKETS + sizeof(countType),
        FPN_IN_DELETED = FPN_IN_USED + sizeof(countType),
        FPN_OUT_BUCKETS = FPN_IN_DELETED + sizeof(countType),
        FPN_OUT_USED = FPN_OUT_BUCKETS + sizeof(countType),
        FPN_OUT_DELETED = FPN_OUT_USED + sizeof(countType),
        FPN_UN_BUCKETS = FPN_OUT_DELETED + sizeof(countType),
        FPN_UN_USED = FPN_UN_BUCKETS + sizeof(countType),
        FPN_UN_DELETED = FPN_UN_USED + sizeof(countType),
        FPN_VAR = FPN_UN_DELETED + sizeof(countType) // 60
    };

    /** Root fixed field positions in byte. Together with a smallest hash table
    length of 5 this structure implies record sizes >= 240. 256 is a good smallest
    value. */
    enum FieldPosRoot {
        FPR_VER_MAJOR = FPN_VAR,
        FPR_VER_MINOR = FPR_VER_MAJOR + sizeof(countType),
        FPR_APP_NAME = FPR_VER_MINOR + sizeof(countType),
        FPR_VAR = FPR_APP_NAME + APP_NAME_LENGTH // 100
    };

    /** Edge (directed and undirected) fixed field positions in byte. */
    enum FieldPosEdge {
        FPE_NODE_START = FP_VAR,
        FPE_NODE_END = FPE_NODE_START + sizeof(keyType),
        FPE_VAR = FPE_NODE_END + sizeof(keyType) // 40
    };

    /** Access control list fixed field positions in byte. */
    enum FieldPosACL {
        FPA_VAR = FP_VAR
        /* more fields come here */
    };

    /** Continued fixed field positions in byte. Records of a GraphElem form
    a linked list to enhance the possibility of cleanup in case of an application
    crash or power outage. */
    enum FieldPosCont {
        FPC_HEAD = FP_VAR,
        FPC_VAR = FPC_HEAD + sizeof(keyType) // 32
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

    /** Identifiers of record chain sections used to index RecordChain.hashStart*. */
    enum RCSection {
        RCS_IN, RCS_OUT, RCS_UN, RCS_PAY, RCS_NOMORE
    };

#define MAXMACRO(x,y) ((static_cast<int>(x))>(static_cast<int>(y))?(static_cast<int>(x)):(static_cast<int>(y)))
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

    /** Common base class for unalignment management. */
    class Unalignment {
    protected:
        /** True if unaligned memory access is enabled. */
        static bool allowUnalign;

    public:
        /** Enables unaligned memory access. During record (dis)assembly.
         * Disabled by default. Enabling this on architectures not supporting
         * it will cause bus errors and program termination. */
        static void enableUnalign() { allowUnalign = true; }
    };

    /** Base class to perform fixed field input/output. Used also in Dump. */
    class FixedFieldIO : Unalignment {
    private:
        /** Array containing field sizes on each fixed field position.
         * Zero means invalid position for that record. */
        static countType pos2sizes[RT_NOMORE][FIELD_VAR_MAX_POS];

    public:
        /** Initializes pos2sizes. */
        static void initStatic() noexcept;

        static void setField(countType fieldStart, uint8_t value, uint8_t * const array);

        /** Sets the fixed field using the templated version. */
        static void setField(countType fieldStart, uint16_t value, uint8_t * const array);

        /** Sets the fixed field using the templated version. */
        static void setField(countType fieldStart, uint32_t value, uint8_t * const array);

        /** Sets the fixed field using the templated version. */
        static void setField(countType fieldStart, uint64_t value, uint8_t * const array);

        /** Gets the fixed field starting at fieldStart, considering the
         * actual record type from the specified array. The method assumes a valid
         * record type at *array. If debugging is enabled,
         * positions and limits are checked using the pos2Sizes array. The 4
        main unsigned integer types are selected using ifs to let the pos2sizes
        array resolve field width. This frees the programmer from remembering
        field sizes. */
        static uint64_t getField(countType fieldStart, uint8_t * const array);

    protected:
        /** Performs the task of getField without any check during debuigging.
            If len given as hint, it overrides the pos2sizes lookup. */
        static uint64_t doGetField(countType fieldStart, uint8_t * const array, uint8_t len = 0);

        /** Sets the fixed field starting at fieldStart to the needed value,
         * considering the actual record type in the specified array.
         * The method assumes a valid record type at *array.
         * If debugging is enabled, positions and limits are checked using
         * the pos2Sizes array. */
        template<typename T>
        static void doSetField(countType fieldStart, T value, uint8_t * const array) {
            union {
                T orig;
                uint8_t bytes[sizeof(T)];
            };
            if(EndianInfo::isLittle()) {
                if(allowUnalign) {
                    *(reinterpret_cast<T*>(array + fieldStart)) = value;
                }
                else {
                    orig = value;
                    for(int i = 0; i < sizeof(T); i++) {
                        array[fieldStart++] = bytes[i];
                    }
                }
            }
            else {
                orig = value;
                for(int i = sizeof(T) - 1; i >= 0; i--) {
                    array[fieldStart++] = bytes[i];
                }
            }
        }

    private:
        static void checkPosition(countType fieldStart, RecordType recordType, uint8_t width = 0);
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
            static countType size;

            /** Maximal number of keys in a record. */
            static countType keysPerRecord;

            /** Pointer to the content. */
            uint8_t * record;

            /** Index for assemblying / extracting the content. */
			int index;

            /** Key in UpscaleDB if already persisted. */
            keyType key = static_cast<keyType>(KEY_INVALID);

            /** Structure to use in UpscaleDB, its data field points to key. */
            ups_key_t upsKey;

            /** Structure to use in UpscaleDB, its data field equals to record. */
            ups_record_t upsRecord;

        public:
            /** Array to index with RecordType to get the starting record indices. */
            static constexpr uint32_t recordVarStarts[] = {0, FPR_VAR, FPA_VAR, FPN_VAR, FPE_VAR, FPE_VAR, FPC_VAR};

            /** Array to index with recordType containing the start of the first
             * bucket array in keyType units such that the array begins aligned to
             * keyType. Meaningful for RT_NODE, RT_ROOT and RT_CONT, otherwise 0. */
            static constexpr countType hashStarts[] = {
                0,
                (recordVarStarts[RT_ROOT] + sizeof(keyType) - 1) / sizeof(keyType),
                0,
                (recordVarStarts[RT_NODE] + sizeof(keyType) - 1) / sizeof(keyType),
                0, 0,
                (recordVarStarts[RT_CONT] + sizeof(keyType) - 1) / sizeof(keyType)
            };


            /** Gets the static record size. */
            static countType getKeysPerRecord() { return keysPerRecord; }

            /** Gets the static record size. */
            static countType getSize() { return size; }

            /** Sets the static record size. */
            static void setSize(countType s);

            /** Checks the record size to make sure it was set. */
            static void checkSize();

            /** Constructs a new record in memory with predefined
             * UpscaleDB record size. Used right before reading. */
            Record();

            /** Constructs a new record in memory with predefined
             * UpscaleDB record size. */
            Record(RecordType rt, payloadType pType);

            /** Constructs a new head record using the raw read data. */
            Record(keyType k, const uint8_t * const rec);

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

            /** Sets the index to the specified position. */
            void setIndex(countType pos) { index = pos; }

            /** Returns the key. */
            keyType getKey() const { return key; }

            /** Sets the record key. */
            void setKey(keyType k) { if(key == static_cast<keyType>(KEY_INVALID)) { key = k; } }

            /** Sets the fixed field starting at fieldStart to the needed value,
             * considering the actual record type using FixedFieldIO::setField. */
            void setField(uint32_t fieldStart, uint8_t value) {
                FixedFieldIO::setField(fieldStart, value, record);
            }

            /** Sets the fixed field starting at fieldStart to the needed value,
             * considering the actual record type using FixedFieldIO::setField. */
            void setField(uint32_t fieldStart, uint16_t value) {
                FixedFieldIO::setField(fieldStart, value, record);
            }

            /** Sets the fixed field starting at fieldStart to the needed value,
             * considering the actual record type using FixedFieldIO::setField. */
            void setField(uint32_t fieldStart, uint32_t value) {
                FixedFieldIO::setField(fieldStart, value, record);
            }

            /** Sets the fixed field starting at fieldStart to the needed value,
             * considering the actual record type using FixedFieldIO::setField. */
            void setField(uint32_t fieldStart, uint64_t value) {
                FixedFieldIO::setField(fieldStart, value, record);
            }

            /** Gets the fixed field starting at fieldStart, considering the
             * actual record type using FixedFieldIO::getField. */
            uint64_t getField(uint32_t fieldStart) const {
                return FixedFieldIO::getField(fieldStart, record);
            }

            /** Writes the key at the specified pos position. */
            void writeKey(countType pos, keyType key);

            /** Reads the key from the specified pos position. */
            keyType readKey(countType pos) const;

            /** Puts a byte in the record. Returns true if
             * success, false if the record was full. */
            bool operator<<(const uint8_t byte) noexcept;

            /** Reads this type if it fits in the record using reinterpret_cast.
             * Called only on little-endian.
             * @return the number of bytes read, or could have been read if
             * less than sizeof(t). */
            template<typename T>
            countType read(T &t) {
                countType remaining = size - index;
                if(remaining >= sizeof(t)) {
                    t = *(reinterpret_cast<T*>(record + index));
                    index += sizeof(t);
                    return sizeof(t);
                }
                return remaining;
            }

            /** Writes (a part of the) character string into the record considering
             * its length. Returns the actually written length. */
            uint64_t write(const char *cp, uint64_t len) noexcept;

            /** Gets a byte from the record without knowing if
             * there is real content to read or we exceed
             * the amount of data once stored. Returns true
             * if the index was less than the record size. */
            bool operator>>(uint8_t &byte) noexcept;

            /** Writes this type if it fits in the record using reinterpret_cast.
             * Called only on little-endian.
             * @return the number of bytes read, or could have been read if
             * less than sizeof(t). */
            template<typename T>
            countType write(T t) {
                countType remaining = size - index;
                if(remaining >= sizeof(t)) {
                    *(reinterpret_cast<T*>(record + index)) = t;
                    index += sizeof(t);
                    return sizeof(t);
                }
                return remaining;
            }

            /** Reads (a part of the) character string from the record considering
             * its length. Returns the actually read length. */
            uint64_t read(char *cp, uint64_t len) noexcept;

            /** Loads the record from db using the transaction. Calls check
             * if status was not UPS_KEY_NOT_FOUND, otherwise returns it and let the
             * caller handle it. */
            ups_status_t load(ups_db_t *db, keyType key, ups_txn_t *tr) noexcept;

            /** Write the record in db using the transaction. */
            void save(ups_db_t *db, ups_txn_t *tr);
        };

        /** Primes to use as open addressing hash buckets count. These residue
         * along the geometric series a[n] = 2^(1/4 + n/2) to ensure rare hash
         * table reallocation. */
        static countType constexpr primes[] = {
                5, 11, 19, 29, 37, 53, 79, 109, 151, 211, 307, 431, 607, 863,
                1217, 1723, 2437, 3449, 4871, 6883, 9743, 13781, 19483, 27551, 38971,
                55109, 77933, 110221, 155863, 220447, 311743, 440863, 623477, 881743,
                1246963, 1763491, 2493949, 3526987, 4987901, 7053971, 9975803, 14107889,
                19951579, 28215799, 39903161, 56431601, 79806341, 112863197, 159612679,
                225726419, 319225331, 451452823, 638450719, 902905657, 1276901429,
                1805811263, 2553802819, 3611622607
            };

        /** Number of usable primes. */
        static countType constexpr primesLen = sizeof(primes) / sizeof(uint32_t);

        /** Start indices in content for the hash arrays INcoming, OUTgoing, UNdirected
         * and payload, respectively. (See enum RCSection.)
         * Set by setHashStart only for RT_NODE and RT_ROOT. */
        indexType hashStartRecord[RCS_NOMORE];

        /** Start indices within record for the hash arrays INcoming, OUTgoing, UNdirected
         * and payload, respectively. (See enum RCSection.)
         * Set by setHashStart only for RT_NODE and RT_ROOT. */
        countType hashStartKey[RCS_NOMORE];

        /** UpscaleDB database pointer. */
        ups_db_t *db = nullptr;

        /** UpscaleDB key generator. */
        KeyGenerator<keyType> *keyGen = nullptr;

        /** State of this object. */
        RCState state = RCState::EMPTY;

        /** List of records assembled or to extract from. */
        std::deque<Record> content;

        /** Index in content pointing to the Record to extract from or write into. */
        countType index = 0;

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

        /** Clears the old contents, sets the head record and all related fields. */
        void setHead(keyType k, const uint8_t * const record);

        /** Clones the other instance here. Changes are already committed in UpscaleDB,
        or aborted, so no records are written now.*/
        void clone(const RecordChain &other);

        /** Resets the content index to the beginning and all Record indexes
        right after the fixed part. */
        void reset();

        /** Clears the record list. */
        void clear();

        /** Returns the chain status. */
        RCState getState() { return state; }

        /** Sets the fixed field starting at fieldStart to the needed value
         * in the first record considering the actual record type. */
        void setHeadField(uint32_t fieldStart, uint8_t value);

        /** Sets the fixed field starting at fieldStart to the needed value
         * in the first record considering the actual record type. */
        void setHeadField(uint32_t fieldStart, uint16_t value);

        /** Sets the fixed field starting at fieldStart to the needed value
         * in the first record considering the actual record type. As this
        is the size of hashtable length, a check is made if these lengths
        are being modified. If yes, setHashStart() is called to store
        section limits. */
        void setHeadField(uint32_t fieldStart, uint32_t value);

        /** Sets the fixed field starting at fieldStart to the needed value
         * in the first record considering the actual record type. */
        void setHeadField(uint32_t fieldStart, uint64_t value);

        /** Gets the fixed field starting at fieldStart from the first record,
         * considering the actual record type. */
        uint64_t getHeadField(uint32_t fieldStart) const;

        /** Gathers the old keys from content. */
        std::deque<keyType> getKeys() const;

        /* Adds an edge to the indicated array and writes the changed records
         * to disk. If there was no more free space in the free array, inserts a
         * new record. */
        void addEdge(keyType edgeKey, FieldPosNode which, ups_txn_t *tr);

        /** Removes all records after the one pointed by index. */
        void stripLeftover();

        /** Reads the chain content from DB to the requested level. Sets state according
         * the actual read stuff, e. g. if only head record existed, FULL. Throws
        exception if EMPTY was requested. */
        void load(keyType key, ups_txn_t *tr, RCState level);

        /** Saves actual content into db, considering the old record keys in
         * oldKeys. The overlapping part with the content will be updated,
         * the plus content is inserted, or the surplus old records removed.
        @param oldKeys the old keys
        @param key the already known GraphElem key for this elem. The containing
        GraphElem must know the new key before calling GraphElem.insert, but
        the same value is needed for the first record.
        @param the UpscaleDB transaction to use. */
        void save(std::deque<keyType> &oldKeys, keyType key, ups_txn_t *upsTr);

        /** Implementation: assembles a byte. */
        void write(uint8_t b);

        /** Writes this type if it fits in the record. Returns true if succeeded. */
        template<typename T>
        bool write(T t) {
            countType written = content[index].write(t);
            if(written == sizeof(t)) {
                return true;
            }
            else {
                if(written == 0) {
                    content.push_back(Record(RT_CONT, pType));
                    index++;
                    content[index].write(t);
                    return true;
                }
                else {
                    return false;
                }
            }
        }

        /** Writes a character string using its length info to prevent calculating
         * it again. */
        void write(const char *cp, uint64_t len);

        /** Implementation: extracts a byte. */
        void read(uint8_t &b);

        /** Reads this type if it fits in the record. Returns true if succeeded. */
        template<typename T>
        bool read(T &t) {
            countType read = content[index].read(t);
            if(read == sizeof(t)) {
                return true;
            }
            else {
                if(read == 0) {
                    index++;
                    if(index == content.size()) {
                        throw DebugException("No more data in RecordChain to read.");
                    }
                    content[index].read(t);
                    return true;
                }
                else {
                    return false;
                }
            }
        }

        /** Reads a character string using its length info. */
        void read(char *cp, uint64_t len);

        /** Reads a character string using its length info. */
        void read(std::string &s, uint64_t len);

    protected:
        /** Calculates the total number of keys in a bucket array, together with
         * the fill space above the used buckets with prime count.
         * In order to let one array grow without affecting others or the payload,
         * each array is padded to a multiple of full record size + the minimal
         * possible bucket length. */
        indexType calcHashLen(countType buckets) const noexcept;

        /** Calculates the payload start for a given head record. */
        indexType calcPayloadStart(Record &rec) const noexcept;

        /** Fills hashStart* arrays using the bucket lengths in head field or the
         * given Record if any. */
        void setHashStart(const Record * const rec = nullptr);

        /** Calculates the record and key positions for the given hash table
         * and hash index. */
        void calcTableIndices(FieldPosNode which, countType index, indexType &indRecord, countType &indKey) const;

        /** Returns the bucket value at index for the given table. */
        keyType getHashContent(FieldPosNode which, countType index) const;

        /** Sets the bucket value at index for the given table.
        @return the index of modified record in content. */
        indexType setHashContent(FieldPosNode which, countType index, keyType key);

        /** Calculates a hash table index for the given table, key and displacement. */
        countType hash(FieldPosNode which, keyType key, countType disp);

#ifdef DEBUG
    public:
        /** Fills the given hash table with test pattern. Available only for debugging. */
        void fillHashTable(FieldPosNode which);

        /** Tests the given hash table against a test pattern. Available only for debugging. */
        void checkHashTable(FieldPosNode which);

        /** Appends missing records such that hashStartRecord[RCS_PLAY] will point
         * to the last one. */
        void appendMissingRecords();
#endif
    };

    /** A wrapper class for RecordChain to hide unnecessary parts from Payload providing
    (de)serialization of some native types. */
    class Converter final : public Unalignment {
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

        /** Record assembly: stores an int. */
        Converter& operator<<(int16_t b) { return writeNative(b); }

        /** Record assembly: stores an int. */
        Converter& operator<<(int32_t b) { return writeNative(b); }

        /** Record assembly: stores an int. */
        Converter& operator<<(int64_t b) { return writeNative(b); }

        /** Record assembly: stores an int. */
        Converter& operator<<(uint16_t b) { return writeNative(b); }

        /** Record assembly: stores an int. */
        Converter& operator<<(uint32_t b) { return writeNative(b); }

        /** Record assembly: stores an int. */
        Converter& operator<<(uint64_t b) { return writeNative(b); }

        /** Record assembly: stores a float. */
        Converter& operator<<(float b) { return writeNative(b); }

        /** Record assembly: stores a double. */
        Converter& operator<<(double b) { return writeNative(b); }

        /** Record assembly: stores a long double. */
        Converter& operator<<(long double b) { return writeNative(b); }

        /** Record assembly: stores a 0-delimited char array and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(const char * const p) { return write(p); }

        /** Record assembly: stores a 0-delimited char array and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(char *p) { return write(const_cast<char *>(p)); }

        /** Record assembly: stores a string and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(const std::string &s) { return write(s.c_str()); }

        /** Record assembly: stores a string and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& operator<<(std::string &s) { return write(s.c_str()); }

        /** Extracts a byte from the actual record. */
        Converter& operator>>(uint8_t &b) {
            chain.read(b);
            return *this;
        }

        /** Extracts a byte from the actual record. */
        Converter& operator>>(int8_t& b);

        /** Extracts a byte as bool from the actual record. */
        Converter& operator>>(bool &b);

        /** Extracts an int from the actual record. */
        Converter& operator>>(int16_t &b) { return readNative(b); }

        /** Extracts an int from the actual record. */
        Converter& operator>>(int32_t &b) { return readNative(b); }

        /** Extracts an int from the actual record. */
        Converter& operator>>(int64_t &b) { return readNative(b); }

        /** Extracts an int from the actual record. */
        Converter& operator>>(uint16_t &b) { return readNative(b); }

        /** Extracts an int from the actual record. */
        Converter& operator>>(uint32_t &b) { return readNative(b); }

        /** Extracts an int from the actual record. */
        Converter& operator>>(uint64_t &b) { return readNative(b); }

        /** Extracts a float from the actual record. */
        Converter& operator>>(float &b) { return readNative(b); }

        /** Extracts a double from the actual record. */
        Converter& operator>>(double &b) { return readNative(b); }

        /** Extracts a long double from the actual record. */
        Converter& operator>>(long double &b) { return readNative(b); }

        /** Extracts a 0-delimited character array.
         * Based on the stored length, allocates memory first, so it must be deleted after use. */
        Converter& operator>>(char *&p);

        /** Extracts a string.
         * Based on the stored length, allocates memory first. */
        Converter& operator>>(std::string &s);

    protected:
        /** Writes the value into the record. If the architecture is little-endian,
         * it performs the write using direct cast if there is enough space in the
         * record. */
        template<typename T>
        Converter& writeNative(const T t) {
            union {
                T orig;
                uint8_t bytes[sizeof(T)];
            };
            if(EndianInfo::isLittle()) {
                if(!allowUnalign || !chain.write(t)) {
                    orig = t;
                    for(int i = 0; i < sizeof(T); i++) {
                        chain.write(bytes[i]);
                    }
                }
            }
            else {
                orig = t;
                for(int i = sizeof(T) - 1; i >= 0; i--) {
                    chain.write(bytes[i]);
                }
            }
            return *this;
        }

        /** Record assembly: stores a 0-delimited char array and first its length
         * as uint64_t. It is needed because the reader allocates memory for stuff read.*/
        Converter& write(const char * const p);

        /** Extracts a byte from records. If the architecture is little-endian
         * and the value fits the record without overlapping into the next one,
         * it is read using direct cast. */
        template<typename T>
        Converter& readNative(T& t) {
            union {
                T orig;
                uint8_t bytes[sizeof(T)];
            };
            if(EndianInfo::isLittle()) {
                if(!allowUnalign || !chain.read(t)) {
                    orig = 0;
                    for(int i = 0; i < sizeof(T); i++) {
                        chain.read(bytes[i]);
                    }
                    t = orig;
                }
            }
            else {
                orig = 0;
                for(int i = sizeof(T) - 1; i >= 0; i--) {
                    chain.read(bytes[i]);
                }
                t = orig;
            }
            return *this;
        }
    };
}

#endif
