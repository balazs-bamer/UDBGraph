/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#ifndef UDB_UDBGRAPH_H
#define UDB_UDBGRAPH_H

#include<map>
#include<unordered_map>
#include<unordered_set>
#include<memory>
#include<utility>
#include<ups/upscaledb.h>

#if USE_NVWA == 1
#include"debug_new.h"
#endif

#include"serializer.h"
#include"exception.h"
#include"udbgraph_config.h"

#ifdef DEBUG
#include<iostream>
#endif

namespace udbgraph {

    /** Handle type for transaction handles inside the library. */
    typedef uint64_t transHandleType;

    /** Invalid and start values for transaction handles. */
    enum TransactionHandle {
        TR_INV, TR_NOMORE
    };

    /** Only technical use between Database and GraphElem. */
    enum class TransactionEnd {
        /** Commit with keeping payloads as they are. */
        COMMIT,

        /** Abort with keeping payloads as they are. However, individual GraphElems
         * may have been instructed to revert their payload to the content at transaction
         * start. These will be reverted. */
        ABORT_KEEP_PL, AB_KEEP_PL = TransactionEnd::ABORT_KEEP_PL,

        /** Abort with reverting payloads to the content at transaction start. */
        ABORT_REVERT_PL, AB_REVERT_PL=TransactionEnd::ABORT_REVERT_PL
    };

    /** Alias for less typing. */
    typedef TransactionEnd TE;

    /** Type of transaction: read-only or read-write. */
    enum class TransactionType {
        /** Read-only transaction. */
        RO,

        /** Read-write tranaction. */
        RW
    };

    /** Alias for less typing. */
    typedef TransactionType TT;

    /** Mode of attaching detached elems to an existing transaction. */
    enum class AttachMode {
        /** Keep the payload and write its contents to GraphElem::chain*.
         * GraphElem::chain* are not read from disc. */
        KEEP_PL,

        /** Read the payload from disc and overwrite the current one. */
        READ_PL
    };

    /** Alias for less typing. */
    typedef AttachMode AM;

    /** Possible Graph Elem states describing chainOrig, chainNew and transaction states.
    These states are independent of the GraphElem payload changes. */
    enum class GEState {
        /** A partially loaded object after transaction end without representation
         * in the application. */
        INV,

        /** A new object independent of both the disk DB and the Database
         * object with unknown key. Both chainOrig and chainNew are empty.*/
        DU,

        /** An object after closing a read-write transaction or all referencing
         * read-only ones. The object is in the disk DB and it has a known key.
         * Note: an other transaction may delete it meanwhile. Both chainOrig
         * and chainNew are empty.*/
        DK,

        /** An object once present in the disk DB but later deleted during the
         * open transaction. It did not exist before the transaction. It is registered
         * in the Database. Both chainOrig and chainNew are empty.*/
        NN,

        /** An object in open transaction and written to disk DB. chainOrig is
         * empty as it did not exist before the transaction, but chainNew is filled. */
        NC,

        /** An object in open transaction, before which it already existed.
         * Thus chainOrig and chainNew are filled. */
        CC,

        /** An object deleted in an open transaction, but existed before starting it.
         * chainOrig is filled but chainNew is empty. */
        CN,

        /** An Edge being deleted as a side effect of a node delete in a transaction.
        chainOrig is partially filled, chainNew is empty. This Edge has no representation
        outside the library.*/
        PN,

        /** A Node being modified as a side effect of an edge creation or deletion
         * in a transaction. Both chainOrig and chainNew are partially filled. This Node has
         * no representation outside the library. */
        PP
    };

    /** Returns string representations of the states for debugging. */
    std::string toString(GEState s);

    /** Empty payload types for graph elems Node, DirEdge, UndirEdge. PT_NOMORE means
    the first free value. PT_ANY and PT_INVALID are synonyms. */
    enum PayloadType {
        PT_ANY, PT_INVALID = PT_ANY, PT_EMPTY_NODE, PT_EMPTY_DEDGE, PT_EMPTY_UEDGE, PT_NOMORE
    };

    /** Edge end types at a node. */
    enum class EdgeEndType {
        Any, In, Out, Un
    };

    class Transaction;
    class Filter;
    class GraphElem;
    class Node;
    class DirEdge;
    class UndirEdge;
    class GEFactory;

    typedef std::unordered_map<transHandleType, ups_txn_t*> upsTransMapType;
    typedef std::unordered_map<keyType, std::shared_ptr<GraphElem>> lockedElemsMapType;
    typedef std::unordered_map<transHandleType, lockedElemsMapType> transLockedElemsMapType;

    /** Hash set to contain the result of a query of neighbouring nodes or edges.
     * It stores them as shared_ptr<GraphElem>, so the caller must know if they
     * are edges or nodes. */
    typedef std::unordered_set<std::shared_ptr<GraphElem>> QueryResult;

    /** Common class for UpscaleDB environments and databases holding all transaction
     * and GraphElem-related status information.
    Instances of this class cannot be copied, they can be only accessed via shared_ptr */
    class Database final : CheckUpsCall, public std::enable_shared_from_this<Database> {
    protected:
        /** The UpscaleDB environment in use. */
        ups_env_t *env = nullptr;

        /** The UpscaleDB database in use. */
        ups_db_t *db = nullptr;

        /** Mutex for accessing UpscaleDB and member structures. UDBGraph offers
        small and quick operations, so making other threads waiting for one operation
        to end won't hurt overall performance much. Moreover, the underlying UpscaleDB
        does the same: "upscaledb is thread-safe and can be used from multiple
        threads without problems. However, it is not yet concurrent; it uses a big
        lock to make sure that only one thread can access the upscaledb environment
        at a time." This means we have not a big room for real concurrency. */
        mutable std::mutex accessMtx;

        /** Automatic record index counter holding the next free value.
         * Number 0 is invalid, number 1 is for ACL management, number 2 is the
         * global root node. */
        KeyGenerator<keyType> *keyGen = nullptr;

        /** Maps transaction handles to UpscaleDB ups_txn_t* */
        upsTransMapType upsTransactions;

        /** Maps transaction handles to sets of GraphElem shared ptrs. */
        transLockedElemsMapType transLockedElems;

        /** Countrs open read-only transaction for each elem. This, transLockedElems
         * and upsTransactions are the structures to register GraphElems. */
        CounterMap<keyType, size_t> roTransCounter;

        /** Set listing all GraphElem shared ptrs currently locked in a transaction. */
        lockedElemsMapType allLockedElems;

        /** True if the database is open and functional. */
        bool ready = false;

        /** Major version number. Database and application with the same major version
         * number should be compatible.
         * This is the value the application sets to check against the DB or create the DB with. */
        uint32_t verMajor;

        /** Minor version number. Minor version number does not affect database
         * and application compatibility.
         * This is the value the application sets to check against the DB or create the DB with. */
        uint32_t verMinor;

        /** Application name, maximal length is APP_NAME_LENGTH - 1.
         * This is the value the application sets to check against the DB or create the DB with. */
        std::string appName;

        /** See the static newInstance method. */
        Database(uint32_t vmaj, uint32_t vmin, std::string name) noexcept :
            verMajor(vmaj), verMinor(vmin), appName(name.substr(0, APP_NAME_LENGTH - 1)) {}

    public:
        /** Creates a Database instance, which is usable only after an open or create call.
         * The shared_ptr holding the instance is used only outside this library. Inside
         * it is always stored as weak_ptr to prevent memory leak.
          @param vmaj application major version number. Database and application with
            the same major version number should be compatible This is the value the
            application sets to check against the DB or create the DB with.
          @param vmin application minor version number. Minor version number does
            not affect database and application compatibility. This is the value the
            application sets to check against the DB or create the DB with.
           @param name application name, maximal length is APP_NAME_LENGTH - 1. string.size()
          counts multibyte characters as more than one unit in size. If one does
          not consider this, truncating the name at APP_NAME_LENGTH - 1 may truncate
          multibyte characters. */
        static std::shared_ptr<Database> newInstance(uint32_t vmaj, uint32_t vmin, std::string name) noexcept {
            return std::shared_ptr<Database>(new Database(vmaj, vmin, name));
        }

        /** Sets the global UpscaleDB error handler. */
        static void setErrorHandler(void UPS_CALLCONV (*errHand)(int level, const char *message)) {
             ups_set_error_handler(errHand);
        }

        /** Move constructor disabled. */
        Database(Database &&d) = delete;

        /** Copy constructor disabled. */
        Database(const Database &t) = delete;

        /** Destructor closes the database if still open. */
        ~Database() noexcept;

        /** Move assignment disabled. */
        Database& operator=(Database &&d) = delete;
        
		/** Does not allow copying. */
        Database& operator=(const Database &t) = delete;

        /** Creates and opens a database with the specified filename, access
         * bits and UpscaleDB record size. Also creates the global root node.
        If the record size is so small, that the fixed fields for a record type would
        completely fill, or is bigger than UDB_MAX_RECORD_SIZE (1M), DebugException
        is thrown. */
        void create(const char *filename, uint32_t mode = 0644, size_t recordSize = UDB_DEF_RECORD_SIZE);

        /** Creates and opens a database with the specified filename, access
         * bits and UpscaleDB record size. Also creates the global root node.
        If the record size is so small, that the fixed fields for a record type would
        completely fill, or is bigger than UDB_MAX_RECORD_SIZE (1M), DebugException
        is thrown. */
        void create(const std::string filename, uint32_t mode = 0644, size_t recordSize = UDB_DEF_RECORD_SIZE) {
            create(filename.c_str(), mode, recordSize);
        }

        /** Opens an existing database, checks the version and application using the
         * root node and searches the maximal key to initialize autoIndex. */
        void open(const char *filename);

        /** Opens an existing database, checks the version and application using the
         * root node and searches the maximal key to initialize autoIndex. */
        void open(const std::string filename) {
            open(filename.c_str());
        }

        /** Closes the database, may throw exception on error. Use this instead of
        relying on destructor if cleanup error management is important. */
        void close();

        /** Flushes the database to disc. TODO check what happens if a transaction
        is open. */
        void flush();

        /** Begins a transaction, which may be read-only if needed. */
        Transaction beginTrans(TransactionType tt = TT::RW);

        /** Writes (inserts or updates) the element in the database using the specified
         * transaction. On update, the elem is overwritten in DB and the operation
         * reuses as many records as possible. The excess records are deleted or the
         * missing ones are created. */
        void write(std::shared_ptr<GraphElem> &ge, Transaction &tr);

        /** Writes (inserts or updates) the element in the database using an on-the-fly
         * transaction. On update, the elem is overwritten in DB and the operation
         * reuses as many records as possible. The excess records are deleted or the
         * missing ones are created. */
        void write(std::shared_ptr<GraphElem> &ge);

        /** Technical use only. */
        void exportDB(RecordChain &rc) { rc.setDB(db); }

        /** Technical use only. */
        void exportAutoIndex(RecordChain &rc) { rc.setKeyGen(keyGen); }

        /** Attaches the given elem if it is detached, does nothing if its state
         * is NC or CC, throws exception otherwise. If it was detached, the elem
        is re-read from disc. am controls whether the payload is preserved or
        read from disc, see AttachMode.*/
        void attach(std::shared_ptr<GraphElem> ge, Transaction &tr, AttachMode am = AM::KEEP_PL);

        /** Collects all edges connecting the root into res with given direction and
         * matching fltEdge. The function marks all returned items in the transaction.
         * If omitFailed is set, leaves out items causing permission or transaction
         * exception, otherwise throws exception and returns without changing anything.
         * This exception is thrown even if the causing edge would not be included
         * considering the given filter. May not be called on edges. */
        void getRootEdges(QueryResult &res, EdgeEndType direction, Filter &fltEdge, Transaction &tr, bool omitFailed = false);

        /** Collects all edges connecting the root into res with given direction and
         * matching fltEdge. The function uses a temporary transaction and the returned
         * edges will have state GEState::DK (if no more transactions reference them).
         * If omitFailed is set, leaves out items causing permission or transaction
         * problems, otherwise throws exception and returns without changing anything.
         * This exception is thrown even if the causing edge would not be included
         * considering the given filter. May not be called on edges. */
        void getRootEdges(QueryResult &res, EdgeEndType direction, Filter &fltEdge, bool omitFailed = false);

    protected:
        /** See attach. */
        void doAttach(std::shared_ptr<GraphElem> ge, Transaction &tr, AttachMode am);

        /** Throws exception if the object is not ready. */
        void isReady();

        /** Performs closing without mutex check. */
        void doClose();

        /** Aborts or commits a transaction tr. The desired operation is
        determined by te. If omitClosed is true, closed database won't throw
        exception. The inner structure related to this transaction
        will be cleared even if an UpscaleDB error occurs, so the tranaction
        cannot be repeated. */
        void endTrans(Transaction &tr, TransactionEnd te, bool omitClosed = false);

        /** See beginTrans.
        @param alreadyLocked if true, the returned object's destructor will function
        inside a locked Database method. */
        Transaction doBeginTrans(TransactionType tt, bool alreadyLocked = false);

        /** See endTrans. */
        void doEndTrans(Transaction &tr, TransactionEnd te);

        /** Looks up the map containing elems related to the transaction denoted by
         * th and checks if th is not stale (belonging to an old Transaction).
        Lookup occurs in transLockedElems. */
        transLockedElemsMapType::iterator getCheckTransLocked(transHandleType th);

        /** Checks if the given key locking is compatible with the tr. */
        void checkKeyVsTrans(keyType key, Transaction &tr) const;

        /** Checks if the GraphElem with the given key is member of an other
         * (so not the one owning th) transaction. More precisely, if
        tr.readonly XOR containing.readonly is true, throws exception.
        Exception also comes when both are RW and the container is an other
        one. */
        void checkAlienBeforeWrite(keyType key, Transaction &tr) const;

        /** Checks all elems in toCheck. */
        void checkAlienBeforeWrite(std::deque<keyType> &toCheck, Transaction &tr) const;

        /** Checks ACL for the given elem. Now only checks if the ACL key is ACL_FREE. */
        void checkACL(std::shared_ptr<GraphElem> &ge, Transaction &tr) const;

        /** Checks ACL for all elems. If the elems are registered here, check is
         * done using this, otherwise it reads them partially to reach the ACL key.
         * Registration is needed anyway. This function call does not alter anything
         * in the registration structures until all checks are finished.
        @param toCheck list of keys to possible register and whose ACL is to be checked.
        @param foundLockedElems iterator to the map containing the locked elems for the transaction.
        @param tr the current transaction.
        @returns the GraphElems corresponding to the keys in toCheck. */
        std::deque<std::shared_ptr<GraphElem>> checkACLandRegister(std::deque<keyType> &toCheck, transLockedElemsMapType::iterator &foundLockedElems, Transaction &tr);

        /** Registers the elem in the appropriate structures. */
        void registerElem(std::shared_ptr<GraphElem> &ge, transLockedElemsMapType::iterator &foundLockedElems, Transaction &tr);

        /** Reads the graph elem identified by the key known to be missing from the
         * registry to the given record chain level. */
        std::shared_ptr<GraphElem> doBareRead(keyType key, RCState level, ups_txn_t *upsTr);

        /** Reads the graph elem identified by the key to the given record chain level. */
        std::shared_ptr<GraphElem> doRead(keyType key, Transaction &tr, RCState level);

        /** Performs actual write. */
        void doWrite(std::shared_ptr<GraphElem> &ge, Transaction &tr);

        /** Implementation of GraphElem::getEdges(QueryResult&, direction, &fltEdge, Transaction&)
         * operating on the node identified by key. */
        void getEdges(QueryResult &res, std::shared_ptr<GraphElem> &ge, EdgeEndType direction, Filter &fltEdge, Transaction &tr, bool omitFailed = false);

        /** Implementation of GraphElem::getEdges(QueryResult&, direction, &fltEdge)
         * operating on the node identified by key. */
        void getEdges(QueryResult &res, std::shared_ptr<GraphElem> &ge, EdgeEndType direction, Filter &fltEdge, bool omitFailed = false);

        /** Implementation of GraphElem::getNeighbours(QueryResult&, EdgeEndType, Filter&, Filter&, Transaction&)
         * operating on the node identifierd by key. */
        void getNeighbours(QueryResult &res, std::shared_ptr<GraphElem> &ge, EdgeEndType direction, Filter &fltEdge, Filter &fltNode, Transaction &tr, bool omitFailed = false);

        /** Implementation of GraphElem::getNeighbours(QueryResult&, EdgeEndType, Filter&, Filter&)
         * operating on the node identifierd by key. */
        void getNeighbours(QueryResult &res, std::shared_ptr<GraphElem> &ge, EdgeEndType direction, Filter &fltEdge, Filter &fltNode, bool omitFailed = false);

        /** Implementation of GraphElem::getEdges(QueryResult&, direction, &fltEdge, Transaction&)
         * operating on ge. */
        void doGetEdges(QueryResult &res, std::shared_ptr<GraphElem> &ge, EdgeEndType direction, Filter &fltEdge, Transaction &tr, bool omitFailed = false);

        /* Implementation of GraphElem::getNeighbours(QueryResult&, EdgeEndType, Filter&, Filter&, Transaction&)
         * operating on ge. */
        void doGetNeighbours(QueryResult &res, std::shared_ptr<GraphElem> &ge, EdgeEndType direction, Filter &fltEdge, Filter &fltNode, Transaction &tr, bool omitFailed = false);

        /** Implementation of GraphElem::getStart(Transaction &tr)
         * operating on the edge identifierd by key. */
        std::shared_ptr<GraphElem> getStart(std::shared_ptr<GraphElem> &ge, Transaction &tr);

        /** Implementation of GraphElem::getStart()
         * operating on the edge identifierd by key. */
        std::shared_ptr<GraphElem> getStart(std::shared_ptr<GraphElem> &ge);

        /** Implementation of GraphElem::getEnd(Transaction &tr)
         * operating on the edge identifierd by key. */
        std::shared_ptr<GraphElem> getEnd(std::shared_ptr<GraphElem> &ge, Transaction &tr);

        /** Implementation of GraphElem::getEnd()
         * operating on the edge identifierd by key. */
        std::shared_ptr<GraphElem> getEnd(std::shared_ptr<GraphElem> &g);

        /* Returns the UpscaleDB transaction pointer of the given Transaction.
        ups_txn_t *getUpsTr(Transaction &tr);*/

        /** Searches maximum key in UpscaleDB and returns the key after it. */
        keyType getFirstFreeKey();

        friend class GraphElem;
        friend class Transaction;
    };

    /** A class encapsulating a transaction handle. A read-write transaction locks
    all its participating GraphElements against concurrent use, a read-only
    one only against concurrent read-write transactions. Other parts of the graph
    can be freely modified.

    The instances are never stored in the library, are only used in actual method
    calls. They should be passed by reference, since the class does not have
    copy constructor or copy assignemnt op. The reason is to allow RAII do a cleanup
    after an exception. The instances track any commit or abort and copying them
    would get around this tracking.

    A future version of this class will hold information of the user performing
    current database operations. As no Database connection object is designed, this
    new version will be created by Database using user credentials. Each operation
    on this transaction object will be checked against the access control list of the
    intended graph elems. The new database format will contain a new special elem,
    type: ACL, gathered under the ACL root node and unaccessible to the application.

    The UDBGraph versions with and without ACL will be compatible with the following
    restrictions:
    - A database without ACL can be accessed by both software.
    - A database with ACL will expose only the free fields to a software without ACL
    - A database with ACL will be accessible according to the ACL definied inside
    when accessed by a software with ACL. */
    class Transaction final {
    protected:
        /** Automatic counter for creating handles. */
        static transHandleType counter;

        /** Handle for using in Database. */
        transHandleType handle;

        /** Database reference for abort and commit calls. */
        std::weak_ptr<Database> db;

        /** True if the transaction is read-only. */
        TransactionType type;

        /** True if the transaction has already been committed or aborted. */
        bool over = false;

        /** True if the calling Database method already locks the Database
         * object access. Only the protected constructor sets it. */
        bool alreadyLocked = false;

        /** Creates the object, can be called only by Database::beginTrans.
        Calling from the application yields useless instance, since it won't
        be registered in Database. */
        Transaction(std::shared_ptr<Database> d, TransactionType trType) noexcept : handle(counter++), db(d), type(trType) {}

    public:
        /** The destructor calls abort to allow RAII cleanup after an exception.
        Important: if an explicite abort() or commit() was called before, the
        destructor has no effect. This means each transaction should be explicitely
        committed if meant so. */
        ~Transaction();

        Transaction(Transaction &&t) noexcept;

        Transaction(const Transaction &t) = delete;

        Transaction& operator=(Transaction &&t) noexcept;

        Transaction& operator=(const Transaction &t) = delete;

        /** Returns the handle for Database. */
        transHandleType getHandle() const;

        /** Returns if the transaction is read-only. */
        bool isReadonly() const noexcept { return type == TT::RO; }

        /** Aborts the transaction. */
        void abort(TE te = TE::ABORT_KEEP_PL);

        /** Commits the transaction. */
        void commit() { db.lock()->endTrans(*this, TransactionEnd::COMMIT); }

        friend class Database;
    };

    /** An abstract class representing the payload in GraphElem subclasses.
    Payload subclasses have only default constructor, because no constructor may
    receive parameters. Because this object is independent of the carrying GraphElem,
	changes to it between a write and a commit operation won't be persisted. */
    class Payload {
    private:
        /** Although this could be found out using the static field, it is more
         * convenient to avoid writing the getter method in every subclass. */
        payloadType _type;

    protected:
        /** Sets type. */
        Payload(payloadType pt) : _type(pt) {}

        /** Saves all user field content into chainNew. Important to write subclasses
         * such that they append to the content written by the superclass.
        Here does nothing. */
        virtual void serialize(Converter &conv) const {}

        /** Loads chainNew content, here does nothing. */
        virtual void deserialize(Converter &conv) {}

    public:
        /** Destructs everything. */
        virtual ~Payload() {}

        /** Returns the payload type. */
        payloadType getType() const { return _type; }

        friend class GraphElem;
    };

    /** Base class for filtering payloads when retrieving more edges or nodes. This
     * implementation matches everything. */
    class Filter {
    private:

        static Filter defaultFilter;

    public:
        /** Destructs everything. */
        virtual ~Filter() {}

        /** Returns true if this filter is suitable for filtering the given payload
         * and the filter contents match the payload contents. This base implementation
        always returns true. The implementations in subclasses will have to cast
        pl to the intended payload type to be able to perform the filtering.
        If the cast fails, false must be returned, because different payload types
        may arrive. */
        virtual bool match(const Payload * const pl) const noexcept { return true; }

        /** Returns the static member of the same type matching everyting. */
        static Filter& allpass() { return defaultFilter; }
    };

    /** Filters only using the payload type. */
    class PayloadTypeFilter final : public Filter {
    protected:
        /** The type to compare to. */
        payloadType type;

        /** Map containing instances used so far. */
        static std::map<payloadType, PayloadTypeFilter> store;

    public:
        /** Stores the payload type, default is PT_ANY. */
        PayloadTypeFilter(payloadType t = PT_ANY) noexcept : type(t) {}

        /** Checks if the given payload matches the stored type. It returns true
        if the stored type in this is PT_ANY. */
        virtual bool match(const Payload * const pl) const noexcept { return type == PT_ANY || pl->getType() == type; }

        /** Looks up pt in store. If found, returns a reference to the instance.
         * If not found, inserts it and returns the new reference. This is a convenience
        mechanism for passing PayloadTypeFilters inline without instantiating in a
        local variable. */
        static PayloadTypeFilter& get(payloadType pt);
    };

    /** A common abstract base class for nodes and edges. This class and subclasses
     * may be used only wrapped in a shared_ptr. Neither this class, nor its subclasses
     * are intended for further dubclassing by the application. */
    class GraphElem : public std::enable_shared_from_this<GraphElem> {
    protected:
        /** All existing GraphElem objects have this set to be able to reach the database instance. */
        std::weak_ptr<Database> db;

        /** Record type the RecordChains' head element uses. */
        RecordType recordType;

        /** The payload we want to load and store. We need unique_ptr because
        the GraphElem constructor may fail and then the Payload item leaks in the
        static create function. */
        std::unique_ptr<Payload> payload;

        /** Actual state of this instance. */
        GEState state = GEState::DU;

        /** True if the payload should be reverted to its content at transaction
         * start on transaction abort. False by default. */
        bool willRevertOnAbort = false;

        /** Key for the first record of this elem. An elem gets a real key only when
        it is about to be written in DB. The GEFactory creation mechanism is too
        early in time for that. */
        keyType key = static_cast<keyType>(KEY_INVALID);

        /** ACL key, now set to ACL_FREE. */
        keyType aclKey = static_cast<keyType>(ACL_FREE);

        /** The original record chain of this object. It is empty if the object
         * did not exist before the transaction, filled if it existed, and is
         * partially filled if the object was loaded as a side effect. */
        RecordChain chainOrig;

        /** The new record chain of this object. It is empty if the object
         * is new or was deleted in the transaction, filled if it is written,
         * and is partially filled if the object was loaded as a side effect. */
        RecordChain chainNew;

        /** Converter wrapping chainNew to hide it from payload. */
        Converter converter;

        /** Local counter of uses in read-only transactions. */
        countType roTransCounter = 0;

        /** Should not be instantiated. This constructor creates the two Serializer
        instances and passes the Database's inner UpscaleDB database pointer
        to them. */
        GraphElem(std::shared_ptr<Database> &d, RecordType rt, std::unique_ptr<Payload> pl);

    public:
        /** Does not allow instantiation. */
        GraphElem(const GraphElem &other) = delete;

        /** Does not allow instantiation. */
        GraphElem(GraphElem &&other) = delete;

        virtual ~GraphElem() {}

        /** Move assignment disabled. */
        GraphElem& operator=(GraphElem &&d) = delete;

        /** Does not allow copying. */
        GraphElem& operator=(const GraphElem &t) = delete;

        /** Returns the key of the head element. */
        keyType getKey() const noexcept { return key; }

        /** Returns the element state. */
        GEState getState() const noexcept { return state; }

        /** Returns the elem type, which can be RT_ROOT, RT_NODE, RT_DEDGE, RT_UEDGE
        type identifiers. */
        RecordType getType() const noexcept { return recordType; }

        /** Gets the ACL key. */
        keyType getACLkey() const noexcept { return aclKey; }

        /** Sets the willRevertOnAbort flag. */
        void revertOnAbort() { willRevertOnAbort = true; }

        /** Returns the native pointer to the payload. It is forbidden to define
         * operator= in a Payload subclass and overwrite the pointed value!
         * It is also forbidden to free this pointer. The return value should be
         * casted and saved into a pointer of the contained type and accessed there. */
        Payload* pl();

        /** Convenience wrapper for elems registered in Database or with known key.
        See Database::write. */
        void write() { auto ge = shared_from_this(); db.lock()->write(ge); }

        /** Convenience wrapper for elems registered in Database or with known key.
        See Database::write. */
        void write(Transaction &tr) { auto ge = shared_from_this(); db.lock()->write(ge, tr); }

        /** Convenience wrapper for elems registered in Database or with known key.
        See Database::attach. */
        void attach(Transaction &tr, AttachMode am = AttachMode::KEEP_PL)
        { auto ge = shared_from_this(); db.lock()->attach(ge, tr, am); }

        /** Registers the two endpoints for the brand new edge if they are not
         * set yet. Arguments must represent nodes. For undirected edges, start
         * and end order does not matter. If the arguments are edges, their key
        is invalid or the ends are already set, or they are the same,
        the method throws exception.
        This method would belong to edge but it is here to save the application
        from nasty casts. This method throws exception if not called on Edge. 
		The ends are marked only in the edge now. Writing it to disc will register
		it to the edges. */
        void setEnds(std::shared_ptr<GraphElem> &start, std::shared_ptr<GraphElem> &end);

        /** Registers the start endpoint and root as end for the brand new edge.
         * Argument must represent node. For undirected edges, start
         * and end order does not matter. If the argument is edge, its key
        is invalid or the ends are already set, the method throws exception.
        This method would belong to edge but it is here to save the application
        from nasty casts. This method throws exception if not called on Edge.
		The ends are marked only in the edge now. Writing it to disc will register
		it to the edges. */
        void setStartRootEnd(std::shared_ptr<GraphElem> &start);

        /** Registers the end endpoint and root as start for the brand new edge.
         * Argument must represent node. For undirected edges, start
         * and end order does not matter. If the argument is edge, its key
        is invalid or the ends are already set, the method throws exception.
        This method would belong to edge but it is here to save the application
        from nasty casts. This method throws exception if not called on Edge. 
		The ends are marked only in the edge now. Writing it to disc will register
		it to the edges. */
        void setEndRootStart(std::shared_ptr<GraphElem> &end);

        /** Collects all edges into res with given direction and matching fltEdge.
         * The function marks all returned items in the transaction.
         * If omitFailed is set, leaves out items causing permission or transaction
         * exception, otherwise throws exception and returns without changing anything.
         * This exception is thrown even if the causing edge would not be included
         * considering the given filter. May not be called on edges. */
        void getEdges(QueryResult &res, EdgeEndType direction, Filter &fltEdge, Transaction &tr, bool omitFailed = true);

        /** Collects all edges into res with given direction and matching fltEdge.
         * The function uses a temporary transaction and the returned edges will
         * have state GEState::DK (if no more transactions reference them). If
         * omitFailed is set, leaves out items causing permission or transaction
         * exception, otherwise throws exception and returns without changing
         * anything. This exception is thrown even if the causing edge would not
         * be included considering the given filter. May not be called on edges. */
        void getEdges(QueryResult &res, EdgeEndType direction, Filter &fltEdge, bool omitFailed = true);

        /** Collects all neighbouring nodes matching fltNode into res
         * connected by edges of given direction and matching fltEdge.
         * Root node is never included. The function marks all returned nodes
         * and their edges in the transaction. If omitFailed is set, leaves out
         * items causing permission or transaction exception, otherwise throws
         * exception and returns without changing anything. This exception is thrown
         * even if the causing edge would not be included considering the given
         * filter. May not be called on edges. */
        void getNeighbours(QueryResult &res, EdgeEndType direction, Filter &fltEdge, Filter &fltNode, Transaction &tr, bool omitFailed = true);

        /** Collects all neighbouring nodes matching fltNode into res
         * connected by edges of given direction and matching fltEdge.
         * Root node is never included. The function uses a temporary transaction
         * and the returned nodes will have state GEState::DK (if no more transactions
         * reference them). If omitFailed is set, leaves out items causing permission
         * or transaction problems, otherwise throws exception and returns without
         * changing anything. This exception is thrown even if the causing edge
         * would not be included considering the given filter. May not be called on edges. */
        void getNeighbours(QueryResult &res, EdgeEndType direction, Filter &fltEdge, Filter &fltNode, bool omitFailed = true);

        /** Returns the start node if this is an edge. If not, throws exception.
         * For undirected edge, returns one of the nodes. The function marks the returned
         * node in the transaction, or throws exception if it is held by
        an other transaction clashing this one. Root node may not be returned, an
        IllegalArgumentEception is thrown instead. */
        std::shared_ptr<GraphElem> getStart(Transaction &tr);

        /** Returns the start node if this is an edge. If not, throws exception.
         * For undirected edge, returns one of the nodes. The function uses a temporary
         * transaction and the returned nodes will have state GEState::DK
         * (if no more transactions reference them). Root node may not be returned,
         * an IllegalArgumentEception is thrown instead. */
        std::shared_ptr<GraphElem> getStart();

        /** Returns the end node if this is an edge. If not, throws exception.
         * For undirected edge, returns one of the nodes. The function marks the returned
         * node in the transaction, or throws exception if it is held by
        an other transaction clashing this one. Root node may not be returned, an
        IllegalArgumentEception is thrown instead.*/
        std::shared_ptr<GraphElem> getEnd(Transaction &tr);

        /** Returns the end node if this is an edge. If not, throws exception.
         * For undirected edge, returns one of the nodes. The function uses a temporary
         * transaction and the returned nodes will have state GEState::DK
         * (if no more transactions reference them). Root node may not be returned,
         * an IllegalArgumentEception is thrown instead. */
        std::shared_ptr<GraphElem> getEnd();

protected:
        /** Increments roTransCounter. */
        void incROCnt() noexcept { roTransCounter++; }

        /** Throws exception if the elem is not a node. */
        void assureNode() const;

        /** Thorws exception if the elem is not a node. */
        void assureEdge() const;

        /** Does the necessary checks before setting ends in recordchain. */
        void checkEnds(RecordType rt1, RecordType rt2, keyType key1, keyType key2) const;

        /** Sets key, the head record and deletes the rest. */
        virtual void setHead(keyType key, const uint8_t * const record);

        /** Writes the fixed fields into chainNew. Here does nothing. */
        virtual void writeFixed();

        /** Calls payload->deserialize() and copies chainNew into chainOrig. */
        void payload2Chains();

        /** Performs complete deserializing, calls payload->deserialize(). */
        void deserialize();

        /** Performs the complete serializing, calls payload->serialize(). */
        void serialize(ups_txn_t *upsTr);

        /** Returns the connected elems' keys, which already exist in the database.
         * Here it returns nothing as nodes can always be written.*/
        virtual std::deque<keyType> getConnectedElemsBeforeWrite() { std::deque<keyType> l; return l; }

        /** Checks state if the elem is suitable for writing and throws exception if not. */
        void checkBeforeWrite();

        /** Implementation of GraphElem::getStart(Transaction &tr)
         * operating on the edge identifierd by key. */
        std::shared_ptr<GraphElem> doGetStart(Transaction &tr);

        /** Implementation of GraphElem::getEnd(Transaction &tr)
         * operating on the edge identifierd by key. */
        std::shared_ptr<GraphElem> doGetEnd(Transaction &tr);

        /** Returns the actual keys for the given edge end type in this node.
         * If the direction is Any, all directions are considered.
         * If this node happens to be an edge, it throws exception.
        The function reserves a suitable array, fills it with the keys
        and delimits it with KEY_INVALID. The caller must free the array.*/
        keyType* getEdgeKeys(EdgeEndType direction);

// ----------- state transition functions ------------

        /** Tries to read the record chain to the given level using the stored key,
         * throws exception if not found. Sets state=CC and deserializes into payload
         * if FULL was requested, PP otherwise clearing the contents first if needed.
         * Throws exception if EMPTY was requested. */
        void read(ups_txn_t *tr, RCState level, bool clearFirst = false);

        /** Performs actual insert/update after serializing this. */
        virtual void write(std::deque<std::shared_ptr<GraphElem>> &connected, ups_txn_t *tr);

        /** Sets the new state, updates payload, chainOrig and chainNew after
        according to te's value (ABORT* or COMMIT). */
        void endTrans(TransactionEnd te);

        /** Implementation of doGetStart and doGetEnd, returning a smart pointer
         * to node identified by theEnd. */
        std::shared_ptr<GraphElem> doGetNodeOfEdge(keyType theEnd, Transaction &tr);

        // needed to be able to hide unnecessary interface
        friend class Database;
    };

    /** A common ancestor for Node and Root not intended for subclassing. */
    class AbstractNode : public GraphElem {
    protected:
        /** Cannot be instantiated. . */
        AbstractNode(std::shared_ptr<Database> &d, RecordType rt, std::unique_ptr<Payload> pl) : GraphElem(d, rt, std::move(pl)) {}

        void addEdge(FieldPosNode where, keyType key, ups_txn_t *tr);

        friend class DirEdge;
        friend class UndirEdge;
    };

    /** A general node class represents actual node types in the graph. */
    class Node final : public AbstractNode {
    public:
        /** Constructs a new instance via create. */
        Node(std::shared_ptr<Database> &d, std::unique_ptr<Payload> pl) : AbstractNode(d, RT_NODE, std::move(pl)) {}

    protected:
        /** Writes the fixed fields into chainNew. Here does nothing. */
        virtual void writeFixed() {}
    };

    /** Represents the root node, cannot be subclassed. The root node is a starting
    point of all graphs in the database and also holds the application name and
    version information. This class cannot be used in the GEFactory. */
    class Root final : public AbstractNode {
    protected:
        /** Major version number. Database and application with the same major version
         * number should be compatible.
        This is the value the DB created with or the value read from the DB. */
        uint32_t verMajor;

        /** Minor version number. Minor version number does not affect database
         * and application compatibility.
        This is the value the DB created with or the value read from the DB. */
        uint32_t verMinor;

        /** Application name, maximal length is APP_NAME_LENGTH - 1.
        This is the value the DB created with or the value read from the DB.*/
        std::string appName;

        /** Constructs a new instance. */
        Root(std::shared_ptr<Database> d, uint32_t vmaj, uint32_t vmin, std::string name);

    public:
        bool doesMatch(uint32_t verMajor, uint32_t verMinor, std::string appName);

    protected:
        /** Sets key, the head record and deletes the rest. Extracts version info
        and appName from head record. */
        virtual void setHead(keyType key, const uint8_t * const record);

        /** Writes the fixed fields into chainNew. */
        virtual void writeFixed();

        friend class Database;
    };

    /** Abstract edge class, common ancestor of DirEdge and UndirEdge. not intended for subclassing. */
    class Edge : public GraphElem {
    protected:
        /** Cannot be instantiated. . */
        Edge(std::shared_ptr<Database> &d, RecordType rt, std::unique_ptr<Payload> pl) : GraphElem(d, rt, std::move(pl)) {}

        /** Here it returns the two endpoints if state is DU, otherwise nothing.
        The queue always has the start point at the first place. */
        virtual std::deque<keyType> getConnectedElemsBeforeWrite();

    };

    /** A general directed edge class represents the actual directed edge types in
     * the graph. */
    class DirEdge final : public Edge {
    public:
        /** Constructs a new instance via create. */
        DirEdge(std::shared_ptr<Database> &d, std::unique_ptr<Payload> pl) : Edge(d, RT_DEDGE, std::move(pl)) {}

    protected:
        /** Writes the fixed fields into chainNew. Here does nothing. */
        virtual void writeFixed() {}

        /** Performs actual insert/update after serializing this, including updating ends
        for brand-new edge. */
        virtual void write(std::deque<std::shared_ptr<GraphElem>> &connected, ups_txn_t *tr);
    };

    /** A general directed edge class represents the actual undirected edge types in
     * the graph. */
    class UndirEdge final : public Edge {
    public:
        /** Constructs a new instance via create. */
        UndirEdge(std::shared_ptr<Database> &d, std::unique_ptr<Payload> pl) : Edge(d, RT_UEDGE, std::move(pl)) {}

    protected:
        /** Writes the fixed fields into chainNew. Here does nothing. */
        virtual void writeFixed() {}

        /** Performs actual insert/update after serializing this, including updating ends
        for brand-new edge. */
        virtual void write(std::deque<std::shared_ptr<GraphElem>> &connected, ups_txn_t *tr);
    };

    /** A class for producing GraphElem subclasses. It is able to create Node, DirEdge
     * and UndirEdge instances using EmptyNode, EmptyDirEdge and EmptyUndieEdge
    payloads out-of-the box. However, they may have no practical use in
    applications as these payload are inherently empty. User subclasses of Payload
    must be registered in user modules to be able to created. User code must guarantee
    the constant order of registering regardless of compilation units and architecture
    in order to keep existing database consistency.

    A minimal Payload subclass definition looks like this:

    class SamplePayload : public Payload {
    protected:
        // Holds the ID assigned during GEFactory registration.
        static payloadType _staticType;

        // Here come the payload fields
        SomeType field;

    public:
        // Sets type for instance.
        SamplePayload(payloadType pt) : Payload(pt) {}

        // destructs SomeType
        ~SamplePayload() {
            // delete or whatever
        }

        // Returns the assigned ID for GEFactory.
        static payloadType id() { return _staticType; }

        // Sets the static PayloadType ID for GEFactory.
        static void setID(payloadType pt) { _staticType = pt; }

        // Used in GEFactory to create a shared_ptr holding a new class instance.
        // Here it is a Node, but may be DirEdge and UndirEdge as well.
        static shared_ptr<GraphElem> create(shared_ptr<Database> &db, payloadType pt) {
            return shared_ptr<GraphElem>(new Node(db, std::unique_ptr<Payload>(new SamplePayload(pt))));
        }

        // Here come setter, getter, assignment operators, application logic etc methods

        // Writes all payload fields into the RecordChain via the supplied Converter
        // Writing order is essential for database and deserializer compatibility
        virtual void serialize(Converter &conv) {
            // statements like conv << field;
        }

        // Reads all payload fields from the RecordChain via the supplied Converter
        // Reading order is essential for database and serializer compatibility
        virtual void deserialize(Converter &conv) {
            // statements like conv >> field;
        }
    }
    // The above static field needs to be defined in a .cpp file:
    payloadType SamplePayload::_staticType;

    Registration is done like:
    SamplePayload::setID(GEFactory::reg(SamplePayload::create));

    Order of these statements are also essential. Newer, expanded versions must add
    new statements only after the old ones.

    A Node holding SamplePayload can be created using:
    shared_ptr<GraphElem> node = GEFactory::create(db, SamplePayload::id());

    While an empty node can be created using:
    shared_ptr<GraphElem> node = GEFactory::create(db, static_cast<payloadType>(PT_EMPTY_NODE));
    */
    class GEFactory final {
        /** Creator method type in GraphElem subclasses. */
        typedef std::shared_ptr<GraphElem> (*CreatorFunction) (std::shared_ptr<Database>&, payloadType pt);

    protected:
        /** Map holding type IDs and function pointers. */
        static std::unordered_map<payloadType, CreatorFunction> registry;

        /** Mutual exclusion for type registering. */
        static std::mutex typeMtx;

        /** Counter holding the next free type ID. */
        static payloadType typeCounter;
    public:
        /** Called in a static instance of class InitStatic to register built-in types. */
        static void initStatic();

        /** Called statically by all instantiatable classes like:
        TheClass::setID(GEFactory::reg(TheClass::create)); */
        static payloadType reg(CreatorFunction classCreator);

        /** Creates a class instance based on the given type. If it is unknown,
         * throws DebugException.
        @param db the Database instance to use with. */
        static std::shared_ptr<GraphElem> create(std::shared_ptr<Database> &db, payloadType typeKey);
    };

    /** Empty payload for creating empty nodes, serves as an example. */
    class EmptyNode final : public Payload {
    public:
        /** Sets type. */
        EmptyNode(payloadType pt) : Payload(pt) {}

        /** Static PayloadType ID for GEFactory. */
        static payloadType id() { return static_cast<payloadType>(PT_EMPTY_NODE); }

        /** Used in GEFactory to create a shared_ptr holding a new class instance. */
        static std::shared_ptr<GraphElem> create(std::shared_ptr<Database> &db, payloadType pt) {
            return std::shared_ptr<GraphElem>(std::make_shared<Node>(db, std::unique_ptr<Payload>(new EmptyNode(pt))));
        }
    };

    /** Empty payload for creating empty directed edges, serves as an example. */
    class EmptyDirEdge final : public Payload {
    public:
        /** Sets type. */
        EmptyDirEdge(payloadType pt) : Payload(pt) {}

        /** Static PayloadType ID for GEFactory. */
        static payloadType id() { return static_cast<payloadType>(PT_EMPTY_DEDGE); }

        /** Used in GEFactory to create a shared_ptr holding a new class instance. */
        static std::shared_ptr<GraphElem> create(std::shared_ptr<Database> &db, payloadType pt) {
            return std::shared_ptr<GraphElem>(std::make_shared<DirEdge>(db, std::unique_ptr<Payload>(new EmptyDirEdge(pt))));
        }
    };

    /** Empty payload for creating empty nodes, serves as an example. */
    class EmptyUndirEdge final : public Payload {
    public:
        /** Sets type. */
        EmptyUndirEdge(payloadType pt) : Payload(pt) {}

        /** Static PayloadType ID for GEFactory. */
        static payloadType id() { return static_cast<payloadType>(PT_EMPTY_UEDGE); }

        /** Used in GEFactory to create a shared_ptr holding a new class instance. */
        static std::shared_ptr<GraphElem> create(std::shared_ptr<Database> &db, payloadType pt) {
            return std::shared_ptr<GraphElem>(std::make_shared<UndirEdge>(db, std::unique_ptr<Payload>(new EmptyUndirEdge(pt))));
        }
    };

    /** Class intended only for static initialization of other classes. Global instancing
     * leads to a floating point exception in unordered_map, so create a local instance
     * somewhere at the very beginning of the application. */
    class InitStatic final {
    public:
        InitStatic();
    };
}

#endif

