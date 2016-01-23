/*
COPYRIGHT COMES HERE
*/

#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

namespace udbgraph {


    string toString(GEState s) {
        switch(s) {
        case GEState::NND:
            return string("NND");
        case GEState::NN:
            return string("NN");
        case GEState::NC:
            return string("NC");
        case GEState::CC:
            return string("CC");
        case GEState::CCA:
            return string("CCA");
        case GEState::CN:
            return string("CN");
        case GEState::PN:
            return string("PN");
        case GEState::PP:
            return string("PP");
        }
        throw logic_error("Impossible.");
    }
}

/*Database::Database(Database &&d) {
    lock_guard<mutex> lck(d.accessMtx);
    // copy, move, null original
}

Database& Database::operator=(Database &&d) {
    if(this != &d) {
        LockGuard2(accessMtx, d.accessMtx);
        // copy, move, null original
    }
    return *this;
}*/

Database::~Database() noexcept {
    lock_guard<mutex> lck(accessMtx);
    try {
        doClose();
    }
    catch(exception &e) {
    }
}

void Database::create(const char *filename, uint32_t mode, size_t recordSize) {
    lock_guard<mutex> lck(accessMtx);
    if(ready) {
        throw DatabaseException("create called on open Database!");
    }
    uint32_t flags = UPS_ENABLE_TRANSACTIONS | /*UPS_FLUSH_WHEN_COMMITTED |*/ UPS_ENABLE_CRC32;
    ups_parameter_t param[] = {
        {UPS_PARAM_CACHE_SIZE, 64 * 1024 * 1024 },
        {UPS_PARAM_POSIX_FADVISE, UPS_POSIX_FADVICE_NORMAL},
      //  {UPS_PARAM_ENABLE_JOURNAL_COMPRESSION, 1},
        {0, 0}
    };
    check(ups_env_create(&env, filename, flags, mode, param));
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
    RecordChain::setRecordSize(recordSize);
    keyGen = new KeyGenerator<keyType>(KEY_ROOT);
    shared_ptr<GraphElem> root(new Root(shared_from_this(), verMajor, verMinor, appName));
    Transaction tr = doBeginTrans(false);
    doWrite(root, tr);
    doEndTrans(tr, TransactionEnd::COMMIT);
    ready = true;
}

#include<iostream>

void Database::open(const char *filename) {
    lock_guard<mutex> lck(accessMtx);
    if(ready) {
        throw DatabaseException("open called on open Database!");
    }
    uint32_t flags = UPS_ENABLE_TRANSACTIONS |  UPS_AUTO_RECOVERY | /*UPS_FLUSH_WHEN_COMMITTED |*/ UPS_ENABLE_CRC32;
    ups_parameter_t param[] = {
        {UPS_PARAM_CACHE_SIZE, 64 * 1024 * 1024 },
        {UPS_PARAM_POSIX_FADVISE, UPS_POSIX_FADVICE_NORMAL},
    //    {UPS_PARAM_ENABLE_JOURNAL_COMPRESSION, 1},
        {0, 0}
    };
    check(ups_env_open(&env, filename, flags, param));
    flags = 0;
    ups_status_t st = ups_env_open_db(env, &db, 1, flags, nullptr);
    if(st) {
        // try to free env, its result is not interesting any more
        flags = UPS_TXN_AUTO_ABORT;
        ups_env_close(env, flags);
        check(st);
    }
    keyGen = new KeyGenerator<keyType>(getFirstFreeKey());
    // find out the record size
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    check(ups_cursor_create(&cursor, db, 0, 0));
    check(ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_FIRST));
    check(ups_cursor_close(cursor));
    RecordChain::setRecordSize(rec.size);
// TODO read root and check version
    ready = true;
}

void Database::close() {
    lock_guard<mutex> lck(accessMtx);
    doClose();
}

void Database::flush() {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    check(ups_env_flush(env, 0));
}

Transaction Database::beginTrans(bool readonly) {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    return doBeginTrans(readonly);
}

void Database::endTrans(Transaction &tr, TransactionEnd te) {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    doEndTrans(tr, te);
}

void Database::write(shared_ptr<GraphElem> &ge) {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    Transaction tr = doBeginTrans(false);
    doWrite(ge, tr);
    doEndTrans(tr, TransactionEnd::COMMIT);
}

void Database::write(shared_ptr<GraphElem> &ge, Transaction &tr) {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    doWrite(ge, tr);
}

void Database::write(keyType key) {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    auto foundElem = allLockedElems.find(key);
    if(foundElem == allLockedElems.end()) {
        throw ExistenceException("Elem not registered in database, use db.write().");
    }
    Transaction tr = doBeginTrans(false);
    doWrite(foundElem->second, tr);
    doEndTrans(tr, TransactionEnd::COMMIT);
}

void Database::write(keyType key, Transaction &tr) {
    lock_guard<mutex> lck(accessMtx);
    isReady();
    auto foundElem = allLockedElems.find(key);
    if(foundElem == allLockedElems.end()) {
        throw ExistenceException("Elem not registered in database, use db.write().");
    }
    doWrite(foundElem->second, tr);
}

void Database::isReady() {
    if(!ready) {
        throw DatabaseException("Database not ready!");
    }
}

void Database::doClose() {
    if(ready) {
        ready = false;
        uint32_t flags = 0;
        db = nullptr;
        flags = UPS_TXN_AUTO_ABORT | UPS_AUTO_CLEANUP;
        ups_status_t st = ups_env_close(env, flags);
        env = nullptr;
        check(st);
        delete keyGen;
    }
}

Transaction Database::doBeginTrans(bool readonly) {
    ups_txn_t *h;
    check(ups_txn_begin(&h, env, nullptr, nullptr, readonly ?  UPS_TXN_READ_ONLY : 0));
    Transaction tr = Transaction(shared_from_this(), readonly);
    transHandleType trHandle = tr.getHandle();
    // insert UpscaleDB transaction
    upsTransactions.insert(pair<transHandleType, ups_txn_t*>(trHandle, h));
    // insert an empty map for future transaction member storage
    lockedElemsMapType newMap;
    transLockedElems.insert(pair<transHandleType, lockedElemsMapType>(trHandle, newMap));
    return tr;
}

void Database::doEndTrans(Transaction &tr, TransactionEnd te) {
    transHandleType trHandle = tr.getHandle();
    auto foundUpsTrans = upsTransactions.find(trHandle);
    auto foundLockedElems = getCheckTransLocked(trHandle);
    ups_status_t st;
    // perform the action
    if(te == TransactionEnd::ABORT) {
        st = ups_txn_abort(foundUpsTrans->second, 0);
    }
    else {
        st = ups_txn_commit(foundUpsTrans->second, 0);
    }
    // clean up before reporting the error if any
    // delete from the map containing UpscaleDB transactions
    upsTransactions.erase(trHandle);
    // delete from all locked graph elements
    // TODO remove for(auto it = foundLockedElems->second.cbegin(); it != foundLockedElems->second.cend(); it++) {
    for(auto &kv : foundLockedElems->second) {
        kv.second->endTrans(te);
        // remove element only if this transaction was the last read only one
        // holding it and or the transaction was read-write
        if(roTransCounter.dec(kv.first)) {
            allLockedElems.erase(kv.first);
        }
    }
    // delete this set of locked elems
    transLockedElems.erase(trHandle);
    check(st);
}

inline transLockedElemsMapType::iterator Database::getCheckTransLocked(transHandleType th) {
    transLockedElemsMapType::iterator foundLockedElems;
    foundLockedElems = transLockedElems.find(th);
    if(foundLockedElems == transLockedElems.end()) {
        throw TransactionException("Handle not found, perhaps stale Transaction instance.");
    }
    return foundLockedElems;
}

void Database::checkAlienBeforeWrite(keyType key, Transaction &tr) const {
    bool keyInAll = allLockedElems.find(key) != allLockedElems.end();
    checkAlienBeforeWrite(key, tr, keyInAll);
}

void Database::checkAlienBeforeWrite(keyType key, Transaction &tr, bool keyInAll) const {
    if(keyInAll) {
        size_t countRO = roTransCounter.count(key);
        if(tr.isReadonly()) {
            if(countRO == 0) {
                throw TransactionException("Attempting a read-only transaction on an elem already present in a read-write one.");
            }
        }
        else {
            if(countRO > 0) {
                throw TransactionException("Attempting a read-write transaction on an elem already present in a read-only one.");
            }
            auto foundLockedElems = transLockedElems.find(tr.getHandle());
            if(foundLockedElems->second.find(key) == foundLockedElems->second.end()) {
                throw TransactionException("Attempting to involve an elem in a read-write transaction while already present in an other read-write one.");
            }
        }
    }
    // otherwise detached elem, no problem
}

void Database::checkAlienBeforeWrite(deque<keyType> &toCheck, Transaction &tr) const {
    for(const keyType &key : toCheck) {
        checkAlienBeforeWrite(key, tr);
    }
}

void Database::checkACL(shared_ptr<GraphElem> &ge, Transaction &tr) const {
    if(ge->getACLkey() != keyType(ACL_FREE)) {
        throw PermissionException("Not authorized for an operation on an elem.");
    }
}

void Database::checkACLandRegister(deque<keyType> &toCheck, transLockedElemsMapType::iterator &foundLockedElems, Transaction &tr) {
    deque<shared_ptr<GraphElem>> toBeRegistered;
    for(const keyType &key : toCheck) {
        auto found = allLockedElems.find(key);
        if(found == allLockedElems.end()) {
            // not found, we save it for registering
            // TODO load partially here:
            // shared_ptr<GraphElem> loaded = loadSomehow(*it);
            // checkACL(loaded, tr);
            // toBeRegistered.push_back(loaded);
        }
        else {
            // found, check ACL
            checkACL(found->second, tr);
        }
    }
    for(auto &elem : toBeRegistered) {
        registerElem(elem, foundLockedElems, tr);
    }
}

void Database::registerElem(shared_ptr<GraphElem> &ge, transLockedElemsMapType::iterator &foundLockedElems, Transaction &tr) {
    keyType key = ge->getKey();
    allLockedElems.insert(pair<keyType, shared_ptr<GraphElem>>(key, ge));
    foundLockedElems->second.insert(pair<keyType, shared_ptr<GraphElem>>(key, ge));
    if(tr.isReadonly()) {
        roTransCounter.inc(key);
    }
}

void Database::doWrite(shared_ptr<GraphElem> &ge, Transaction &tr) {
    if(tr.isReadonly()) {
        throw TransactionException("Trying to write during a read-only transaction.");
    }
    if(ge->getState() == GEState::CN) {
        throw ExistenceException("Trying to write an already deleted element.");
    }
    keyType key = ge->getKey();
    GEState state = ge->getState();
    if(key == KEY_INVALID) {
        if(state != GEState::NND) {
            throw logic_error(string("doWrite: illegal state with KEY_INVALID: ") + toString(state));
        }
        ge->key = key = keyGen->nextKey();
    }
    transHandleType trHandle = tr.getHandle();
    auto foundLockedElems = getCheckTransLocked(trHandle);
    /* This must be present because the transaction exists. */
    auto foundUpsTrans = upsTransactions.find(trHandle);
    ups_txn_t *upsTr = foundUpsTrans->second;
    auto foundElem = allLockedElems.find(key);
    if(foundElem == allLockedElems.end()) {
        deque<keyType> toCheck = ge->getConnectedElemsBeforeWrite();
        // check if any of them belong to an other transaction
        checkAlienBeforeWrite(toCheck, tr);
        checkACL(ge, tr);
        // until the middle of the following function call an exception may ruin
        // writing the elem, and no change is made in the registration structures
        checkACLandRegister(toCheck, foundLockedElems, tr);
        registerElem(ge, foundLockedElems, tr);
        // writing brand new element or a detached existing one
        // current state NND, CCA
        switch(state) {
        case GEState::NND:
            ge->insert(upsTr);
            break;
        case GEState::CCA:
            ge->update(upsTr);
            break;
        default:
            throw logic_error(string("doWrite detached: illegal state") + toString(state));
        }
    }
    else {
        // we already know that key is in allLockedElems
        checkAlienBeforeWrite(key, tr, true);
        // writing an elem in the registry, current state NC, CC, NN
        switch(state) {
        case GEState::NN:
        case GEState::NC:
        case GEState::CC:
            ge->update(upsTr);
            break;
        default:
            throw logic_error(string("doWrite attached: illegal state") + toString(state));
        }
    }
}

keyType Database::getFirstFreeKey() {
    ups_key_t key;
    keyType needle = 0;
    key.data = &needle;
    key.flags = UPS_KEY_USER_ALLOC;
    ups_record_t found;
    int bitCount = (key.size = sizeof(keyType)) * 8;
    for(int i = bitCount - 1; i >= 0; i--) {
        keyType prevNeedle = needle;
        needle |= (1 << i);
        found.size = found.flags = found.partial_offset = found.partial_size = 0;
        found.data = nullptr;
        ups_status_t st = ups_db_find(db, nullptr, &key, &found, UPS_FIND_GEQ_MATCH);
        switch(st) {
        case UPS_SUCCESS:
            // nothing to do, needle already has the bit set.
            break;
        case UPS_KEY_NOT_FOUND:
            // reset the previous value
            needle = prevNeedle;
            break;
        default:
            throw UpsException(st);
        }
    }
    return needle + 1;
}

transHandleType Transaction::counter = 0;

GraphElem::GraphElem(shared_ptr<Database> d, RecordType rt, unique_ptr<Payload> pl) :
    db(d), recordType(rt), payload(std::move(pl)), chainOrig(rt, payload->getType()), chainNew(rt, payload->getType()),
    converter(Converter(chainNew)) {
    d->exportDB(chainOrig);
    d->exportDB(chainNew);
    d->exportAutoIndex(chainNew);
}

Payload& GraphElem::pl() {
    // TODO check if partial, load fully
    // TODO check and deserialize if needed
    return *payload;
}

void GraphElem::writeFixed() {
    chainNew.setHeadField(FP_ACL, aclKey);
}

void GraphElem::serialize(ups_txn_t *tr) {
    chainNew.reset();
    writeFixed();
    deque<keyType> oldKeys = chainNew.getKeys();
    chainNew.reset();
    payload->serialize(converter);
    chainNew.stripLeftover();
    chainNew.write(oldKeys, key, tr);
}

void GraphElem::insert(ups_txn_t *tr) {
    serialize(tr);
    switch(state) {
    case GEState::NND:
        state = GEState::NC;
        break;
    case GEState::CCA:
        state = GEState::CC;
        break;
    default:
        throw logic_error(string("doWrite detached: illegal state") + toString(state));
    }
}

void GraphElem::update(ups_txn_t *tr) {
    serialize(tr);
    switch(state) {
    case GEState::NN:
        state = GEState::NC;
        break;
    default:	// against warning
        break;
    }
}

void GraphElem::endTrans(TransactionEnd te) {
    if(te == TransactionEnd::ABORT) {
        switch(state) {
        case GEState::NC:
        case GEState::PN:
        case GEState::PP:
            chainOrig.clear();
            chainNew.clear();
            state = GEState::NND;
            break;
        case GEState::CC:
            chainNew.clone(chainOrig);
            // reset the old content in Payload
            payload->deserialize(converter);
        case GEState::CN:
            state = GEState::CCA;
            break;
        default:
            throw logic_error(string("endTrans abort: illegal state") + toString(state));
            break;
        }
    }
    else {
        switch(state) {
        case GEState::CN:
        case GEState::PN:
        case GEState::PP:
            chainOrig.clear();
            chainNew.clear();
            state = GEState::NND;
            break;
        case GEState::NC:
        case GEState::CC:
            if(db.lock()->isMultithreaded()) {
                chainOrig.clear();
                chainNew.clear();
                state = GEState::NND;
            }
            else {
                chainOrig.clone(chainNew);
                state = GEState::CCA;
            }
            break;
        default:
            throw logic_error(string("endTrans commit: illegal state") + toString(state));
            break;
        }
    }
}

Root::Root(shared_ptr<Database> d, uint32_t vmaj, uint32_t vmin, string name) :
    AbstractNode(d, RT_ROOT, unique_ptr<Payload>(new EmptyNode(PT_EMPTY_NODE))), verMajor(vmaj), verMinor(vmin), appName(name) {
}

void Root::writeFixed() {
    GraphElem::writeFixed();
    chainNew.setHeadField(FPR_VER_MAJOR, verMajor);
    chainNew.setHeadField(FPR_VER_MINOR, verMinor);
    size_t i;
    size_t end = appName.size();
    if(end > APP_NAME_LENGTH - 1) {
        end = APP_NAME_LENGTH - 1;
    }
    for(i = 0; i < end; i++) {
        chainNew.setHeadField(FPR_APP_NAME + i, appName[i]);
    }
    chainNew.setHeadField(FPR_APP_NAME + i, 0);
}

void Edge::setEnds(shared_ptr<GraphElem> &start, shared_ptr<GraphElem> &end) {
    if(chainNew.getHeadField(FPE_NODE_START) != keyType(KEY_INVALID) ||
        chainNew.getHeadField(FPE_NODE_END) != keyType(KEY_INVALID)) {
        throw ExistenceException("Ends are already set.");
    }
    if(start->getType() != RT_NODE || end->getType() != RT_NODE) {
        throw IllegalArgumentException("Ends must be nodes.");
    }
    keyType keyStart = start->getKey();
    keyType keyEnd = end->getKey();
    if(keyStart ==  keyType(KEY_INVALID) || keyEnd == keyType(KEY_INVALID)) {
        throw IllegalArgumentException("Ends must have valid keys.");
    }
    chainNew.setHeadField(FPE_NODE_START, keyStart);
    chainNew.setHeadField(FPE_NODE_END, keyEnd);
}

deque<keyType> Edge::getConnectedElemsBeforeWrite() {
    deque<keyType> result;
    if(state == GEState::NND) {
        //inserting new edge, we need to register it at its ends
        result.push_back(chainNew.getHeadField(FPE_NODE_START));
        result.push_back(chainNew.getHeadField(FPE_NODE_END));
    }
    return result;
}

void Edge::insert(ups_txn_t *tr) {
    GraphElem::insert(tr);
    // TODO update ends
}

unordered_map<payloadType, GEFactory::CreatorFunction> GEFactory::registry;
mutex GEFactory::typeMtx;
payloadType GEFactory::typeCounter = payloadType(PT_NOMORE);

// TODO throws a pile of linker errors
void GEFactory::initStatic() {
    lock_guard<mutex> lck(typeMtx);
    registry.insert(pair<payloadType, CreatorFunction>(payloadType(PT_EMPTY_NODE), EmptyNode::create));
    registry.insert(pair<payloadType, CreatorFunction>(payloadType(PT_EMPTY_DEDGE), EmptyDirEdge::create));
    registry.insert(pair<payloadType, CreatorFunction>(payloadType(PT_EMPTY_UEDGE), EmptyUndirEdge::create));
}

payloadType GEFactory::reg(CreatorFunction classCreator) {
    lock_guard<mutex> lck(typeMtx);
    payloadType typeKey;
    registry.insert(pair<payloadType, CreatorFunction>(typeKey = typeCounter++, classCreator));
    return typeKey;
}

shared_ptr<GraphElem> GEFactory::create(shared_ptr<Database> db, payloadType typeKey) {
    auto it = registry.find(typeKey);
    if (it != registry.end()) {
        if (it->second) {
            return it->second(db, typeKey);
        }
    }
    throw logic_error(string("Unknown graph elem type: ") + to_string(typeKey));
}

InitStatic::InitStatic() {
    EndianInfo::initStatic();
    FixedFieldIO::initStatic();
    GEFactory::initStatic();
}

// TODO does not run in debug1 - other library
//InitStatic globalStaticInitializer;
