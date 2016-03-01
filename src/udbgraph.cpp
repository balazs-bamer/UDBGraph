/*
COPYRIGHT COMES HERE
*/

#include"udbgraph.h"

#ifdef DEBUG
#include<iostream>
#endif

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

namespace udbgraph {

    string toString(GEState s) {
        switch(s) {
        case GEState::INV:
            return string("INV");
        case GEState::DU:
            return string("DU");
        case GEState::DK:
            return string("DK");
        case GEState::NN:
            return string("NN");
        case GEState::NC:
            return string("NC");
        case GEState::CC:
            return string("CC");
        case GEState::CN:
            return string("CN");
        case GEState::PN:
            return string("PN");
        case GEState::PP:
            return string("PP");
        }
        throw DebugException("Impossible.");
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
    Transaction tr = doBeginTrans(false);
    bool matches = dynamic_pointer_cast<Root>(doRead(KEY_ROOT, tr, RCState::HEAD))->doesMatch(verMajor, verMinor, appName);
    doEndTrans(tr, TransactionEnd::ABORT);
    ready = true;
    if(!matches) {
        throw DatabaseException("Invalid version or application name.");
    }
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
        throw ExistenceException("Elem not registered in Database, use db.write().");
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
        throw ExistenceException("Elem not registered in Database, use db.write().");
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

void Database::checkKeyVsTrans(keyType key, Transaction &tr) const {
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

void Database::checkAlienBeforeWrite(keyType key, Transaction &tr) const {
    if(allLockedElems.find(key) != allLockedElems.end()) {
        checkKeyVsTrans(key, tr);
    }
}

void Database::checkAlienBeforeWrite(deque<keyType> &toCheck, Transaction &tr) const {
    for(const keyType &key : toCheck) {
        checkAlienBeforeWrite(key, tr);
    }
}

void Database::checkACL(shared_ptr<GraphElem> &ge, Transaction &tr) const {
    if(ge->getACLkey() != static_cast<keyType>(ACL_FREE)) {
        throw PermissionException("Not authorized for an operation on an elem.");
    }
}

deque<shared_ptr<GraphElem>> Database::checkACLandRegister(deque<keyType> &toCheck, transLockedElemsMapType::iterator &foundLockedElems, Transaction &tr) {
    auto foundUpsTrans = upsTransactions.find(tr.getHandle());
    ups_txn_t *upsTr = foundUpsTrans->second;
    deque<shared_ptr<GraphElem>> toBeRegistered;
    deque<shared_ptr<GraphElem>> result;
    for(const keyType &key : toCheck) {
        auto found = allLockedElems.find(key);
        if(found == allLockedElems.end()) {
            // not found, we read it for registering
            shared_ptr<GraphElem> loaded = doBareRead(key, RCState::HEAD, upsTr);
            checkACL(loaded, tr);
            toBeRegistered.push_back(loaded);
            loaded->checkBeforeWrite();
            result.push_back(loaded);
        }
        else {
            // found, check ACL
            checkACL(found->second, tr);
            found->second->checkBeforeWrite();
            result.push_back(found->second);
        }
    }
    for(auto &elem : toBeRegistered) {
        registerElem(elem, foundLockedElems, tr);
    }
    return result;
}

void Database::registerElem(shared_ptr<GraphElem> &ge, transLockedElemsMapType::iterator &foundLockedElems, Transaction &tr) {
    keyType key = ge->getKey();
    allLockedElems.insert(pair<keyType, shared_ptr<GraphElem>>(key, ge));
    foundLockedElems->second.insert(pair<keyType, shared_ptr<GraphElem>>(key, ge));
    if(tr.isReadonly()) {
        roTransCounter.inc(key);
    }
}

shared_ptr<GraphElem> Database::doBareRead(keyType key, RCState level, ups_txn_t *upsTr) {
    // first try to read the head record
    ups_key_t upsKey;
    ups_record_t upsRecord;
    upsKey.flags = upsKey._flags = 0;
    upsKey.data = &key;
    upsKey.size = sizeof(key);
    upsRecord.flags = upsRecord.partial_offset = upsRecord.partial_size = 0;
    upsRecord.size = 0;
    upsRecord.data = nullptr;
    ups_status_t result = _ups_db_find(db, upsTr, &upsKey, &upsRecord, 0);
    if(result == UPS_KEY_NOT_FOUND) {
        throw ExistenceException("Requested graph element not found in the database.");
    }
    check(result);
    RecordType recType = static_cast<RecordType>(FixedFieldIO::getField(FP_RECORDTYPE, reinterpret_cast<uint8_t *>(upsRecord.data)));
    shared_ptr<GraphElem> ret;
    if(recType == RT_ROOT) {
        shared_ptr<GraphElem> root(new Root(shared_from_this(), 0, 0, ""));
        ret = move(root);
    }
    else {
        payloadType plType = FixedFieldIO::getField(FP_PAYLOADTYPE, reinterpret_cast<uint8_t *>(upsRecord.data));
        shared_ptr<Database> db = shared_from_this();
        ret = GEFactory::create(db, plType);
    }
    ret->setHead(key, reinterpret_cast<uint8_t *>(upsRecord.data));
    ret->read(upsTr, level);
    if(level == RCState::FULL && recType != RT_ROOT) {
        ret->deserialize();
    }
    return ret;
}

shared_ptr<GraphElem> Database::doRead(keyType key, Transaction &tr, RCState level) {
    transHandleType trHandle = tr.getHandle();
    transLockedElemsMapType::iterator foundLockedElems = getCheckTransLocked(trHandle);
    auto foundUpsTrans = upsTransactions.find(trHandle);
    ups_txn_t *upsTr = foundUpsTrans->second;
    lockedElemsMapType::iterator foundElem = allLockedElems.find(key);
    if(foundElem == allLockedElems.end()) {
        // not found, we must read it from disk
        shared_ptr<GraphElem> ret = doBareRead(key, level, upsTr);
        checkACL(ret, tr);
        registerElem(ret, foundLockedElems, tr);
        return ret;
    }
    else {
        lockedElemsMapType::iterator foundInTr = foundLockedElems->second.find(key);
        if(foundInTr != foundLockedElems->second.end()) {
            // we own it, no more checks and registering
            shared_ptr<GraphElem> ret = foundInTr->second;
            ret->read(upsTr, level);
            return ret;
        }
        else {
            // somebody else owns it
            checkKeyVsTrans(key, tr);
            // now it is sure that this transaction is read-only and the one that
            // owns it is also read-only
            shared_ptr<GraphElem> ret = foundElem->second;
            checkACL(ret, tr);
            registerElem(ret, foundLockedElems, tr);
            return ret;
        }
    }
}

void Database::doWrite(shared_ptr<GraphElem> &ge, Transaction &tr) {
    if(tr.isReadonly()) {
        throw TransactionException("Trying to write during a read-only transaction.");
    }
    ge->checkBeforeWrite();
    keyType key = ge->getKey();
    GEState state = ge->getState();
    if(key == KEY_INVALID) {
        if(state != GEState::DU) {
            throw DebugException(string("doWrite: illegal state with KEY_INVALID: ") + toString(state));
        }
        ge->key = key = keyGen->nextKey();
    }
    transHandleType trHandle = tr.getHandle();
    transLockedElemsMapType::iterator foundLockedElems = getCheckTransLocked(trHandle);
    /* This must be present because the transaction exists. */
    auto foundUpsTrans = upsTransactions.find(trHandle);
    ups_txn_t *upsTr = foundUpsTrans->second;
    lockedElemsMapType::iterator foundElem = allLockedElems.find(key);
    deque<shared_ptr<GraphElem>> affected;
    if(foundElem == allLockedElems.end()) {
        // Writing a detached existing elem. First load it to update the fixed fields
        // and hash table if any
        if(state == GEState::DK) {
            ge->read(upsTr, RCState::PARTIAL);
        }
        deque<keyType> toCheck = ge->getConnectedElemsBeforeWrite();
        // check if any of them belong to an other transaction
        checkAlienBeforeWrite(toCheck, tr);
        checkACL(ge, tr);
        // until the middle of the following function call an exception may ruin
        // writing the elem, and no change is made in the registration structures
        affected = checkACLandRegister(toCheck, foundLockedElems, tr);
        registerElem(ge, foundLockedElems, tr);
    }
    else {
        // we already know that key is in allLockedElems
        checkKeyVsTrans(key, tr);
        // if passed, it is sure we already own the elem
    }
    ge->write(affected, upsTr);
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
        ups_status_t st = _ups_db_find(db, nullptr, &key, &found, UPS_FIND_GEQ_MATCH);
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

GraphElem::GraphElem(shared_ptr<Database> &d, RecordType rt, unique_ptr<Payload> pl) :
    db(d), recordType(rt), payload(std::move(pl)), chainOrig(rt, payload->getType()), chainNew(rt, payload->getType()),
    converter(Converter(chainNew)) {
    d->exportDB(chainOrig);
    d->exportDB(chainNew);
    d->exportAutoIndex(chainNew);
}

Payload& GraphElem::pl() {
    switch(state) {
    case GEState::CN:
    case GEState::NN:
        throw IllegalMethodException("Callot get payload on deleted element.");
    case GEState::PN:
    case GEState::PP:
    case GEState::INV:
        throw DebugException("Cannot get payload on GEState::P* or INV.");
    default:
        break;
    }
    if(!payload) { // defined bool conversion
        throw DebugException("No payload in GraphElem.");
    }
    return *payload;
}

void GraphElem::setEnds(shared_ptr<GraphElem> &start, shared_ptr<GraphElem> &end) {
    keyType keyStart = start->getKey();
    keyType keyEnd = end->getKey();
    checkEnds(end->getType(), start->getType(), keyStart, keyEnd);
    chainNew.setHeadField(FPE_NODE_START, keyStart);
    chainNew.setHeadField(FPE_NODE_END, keyEnd);
}

void GraphElem::setStartRootEnd(shared_ptr<GraphElem> &start) {
    keyType keyStart = start->getKey();
    keyType keyEnd = KEY_ROOT;
    checkEnds(RT_ROOT, start->getType(), keyStart, keyEnd);
    chainNew.setHeadField(FPE_NODE_START, keyStart);
    chainNew.setHeadField(FPE_NODE_END, keyEnd);
}

void GraphElem::setEndRootStart(shared_ptr<GraphElem> &end) {
    keyType keyStart = KEY_ROOT;
    keyType keyEnd = end->getKey();
    checkEnds(end->getType(), RT_ROOT, keyStart, keyEnd);
    chainNew.setHeadField(FPE_NODE_START, keyStart);
    chainNew.setHeadField(FPE_NODE_END, keyEnd);
}

void GraphElem::checkEnds(RecordType rt1, RecordType rt2, keyType key1, keyType key2) const {
    if(dynamic_cast<const Edge*>(this) == nullptr) {
        throw IllegalMethodException("setEnds, setStartRootEnd or setEndRootStart may only be called on Edge.");
    }
    if(chainNew.getHeadField(FPE_NODE_START) != static_cast<keyType>(KEY_INVALID) ||
        chainNew.getHeadField(FPE_NODE_END) != static_cast<keyType>(KEY_INVALID)) {
        throw ExistenceException("Ends are already set.");
    }
    if((rt1 != RT_NODE && rt1 != RT_ROOT) || (rt2 != RT_NODE && rt2 != RT_ROOT)) {
        throw IllegalArgumentException("End must be node.");
    }
    if(key1 == KEY_INVALID || key2 == KEY_INVALID) {
        throw IllegalArgumentException("End must have valid key.");
    }
    if(key1 == key2) {
        throw IllegalArgumentException("Two ends must be different (no loop edges allowed).");
    }
}

void GraphElem::setHead(keyType k, const uint8_t * const record) {
    key = k;
    chainOrig.setHead(key, record);
    chainNew.clone(chainOrig);
    aclKey = chainNew.getHeadField(FP_ACL);
}

void GraphElem::writeFixed() {
    chainNew.setHeadField(FP_ACL, aclKey);
}

void GraphElem::deserialize() {
    chainNew.reset();
    payload->deserialize(converter);
}

void GraphElem::serialize(ups_txn_t *tr) {
// TODO check if needed    chainNew.reset();
    writeFixed();
    deque<keyType> oldKeys = chainNew.getKeys();
    chainNew.reset();
    if(state != GEState::PP) {
        // PP hasd nothing to serialize
        payload->serialize(converter);
        chainNew.stripLeftover();
    }
    chainNew.save(oldKeys, key, tr);
}

void GraphElem::checkBeforeWrite() {
    if(state == GEState::CN || state == GEState::NN || state == GEState::INV) {
        throw ExistenceException("Trying to write an already deleted element.");
    }
}

void GraphElem::read(ups_txn_t *tr, RCState level) {
    chainOrig.load(key, tr, level);
    chainNew.clone(chainOrig);
    if(level == RCState::FULL) {
        state = GEState::CC;
    }
    else {
        state = GEState::PP;
    }
}

void GraphElem::write(deque<shared_ptr<GraphElem>> &connected, ups_txn_t *tr) {
    serialize(tr);
    switch(state) {
    case GEState::DU:
        state = GEState::NC;
        break;
    case GEState::DK:
        state = GEState::CC;
        break;
    case GEState::NC:
    case GEState::CC:
    case GEState::PP:
        break;
    default:
        throw DebugException(string("Illegal state in GraphElem::write: ") + toString(state));
    }
}

void GraphElem::endTrans(TransactionEnd te) {
//    if(te == TransactionEnd::ABORT) {
        // TODO if state was CC perhaps reset the old content in Payload
  //  }
    chainOrig.clear();
    chainNew.clear();
    switch(state) {
    case GEState::CN:
    case GEState::NN:
        state = GEState::DU;
        break;
    case GEState::NC:
    case GEState::CC:
        state = GEState::DK;
        break;
    case GEState::PN:
    case GEState::PP:
        state = GEState::INV;
        break;
    default:
        throw DebugException(string("endTrans commit: illegal state") + toString(state));
        break;
    }
}

void AbstractNode::addEdge(FieldPosNode where, keyType edgeKey, ups_txn_t *tr) {
    if(chainNew.getState() < RCState::PARTIAL) {
        // make sure we have the edge arrays
        chainNew.load(key, tr, RCState::PARTIAL);
    }
    chainNew.addEdge(where, edgeKey, tr);
}

Root::Root(shared_ptr<Database> d, uint32_t vmaj, uint32_t vmin, string name) :
    AbstractNode(d, RT_ROOT, unique_ptr<Payload>(new EmptyNode(PT_EMPTY_NODE))), verMajor(vmaj), verMinor(vmin), appName(name) {
}

bool Root::doesMatch(uint32_t verMaj, uint32_t verMin, string appN) {
    return verMaj == verMajor && appN == appName;
}

void Root::setHead(keyType key, const uint8_t * const record) {
    GraphElem::setHead(key, record);
    verMajor = static_cast<uint32_t>(chainNew.getHeadField(FPR_VER_MAJOR));
    verMinor = static_cast<uint32_t>(chainNew.getHeadField(FPR_VER_MINOR));
    uint8_t ch = chainNew.getHeadField(FPR_APP_NAME);
    int i = 1;
    while(ch != 0) {
        appName += static_cast<char>(ch);
        ch = chainNew.getHeadField(FPR_APP_NAME + i++);
    }
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
        chainNew.setHeadField(FPR_APP_NAME + i, static_cast<uint8_t>(appName[i]));
    }
    chainNew.setHeadField(FPR_APP_NAME + i, static_cast<uint8_t>(0));
}

deque<keyType> Edge::getConnectedElemsBeforeWrite() {
    deque<keyType> result;
    // TODO check if needed
//    if(state == GEState::DU) {
    keyType start = chainNew.getHeadField(FPE_NODE_START);
    keyType end = chainNew.getHeadField(FPE_NODE_END);
    if(start == KEY_INVALID || end == KEY_INVALID) {
        throw ExistenceException("Edge ends not set.");
    }
    //inserting new edge, we need to register it at its ends
    result.push_back(start);
    result.push_back(end);
    //}
    return result;
}

void DirEdge::write(deque<shared_ptr<GraphElem>> &connected, ups_txn_t *tr) {
    bool needUpdateEnds = state == GEState::DU;
    GraphElem::write(connected, tr);
    if(needUpdateEnds) {
        // first key is for edge start, the edge comes out of this node
        dynamic_pointer_cast<AbstractNode>(connected[0])->addEdge(FPN_OUT_BUCKETS, key, tr);
        // second key is for edge end, the edge goes into this node
        dynamic_pointer_cast<AbstractNode>(connected[1])->addEdge(FPN_IN_BUCKETS, key, tr);
    }
}

void UndirEdge::write(deque<shared_ptr<GraphElem>> &connected, ups_txn_t *tr) {
    bool needUpdateEnds = state == GEState::DU;
    GraphElem::write(connected, tr);
    if(needUpdateEnds) {
        dynamic_pointer_cast<AbstractNode>(connected[0])->addEdge(FPN_UN_BUCKETS, key, tr);
        dynamic_pointer_cast<AbstractNode>(connected[1])->addEdge(FPN_UN_BUCKETS, key, tr);
    }
}

unordered_map<payloadType, GEFactory::CreatorFunction> GEFactory::registry;
mutex GEFactory::typeMtx;
payloadType GEFactory::typeCounter = static_cast<payloadType>(PT_NOMORE);

void GEFactory::initStatic() {
    lock_guard<mutex> lck(typeMtx);
    registry.insert(pair<payloadType, CreatorFunction>(static_cast<payloadType>(PT_EMPTY_NODE), EmptyNode::create));
    registry.insert(pair<payloadType, CreatorFunction>(static_cast<payloadType>(PT_EMPTY_DEDGE), EmptyDirEdge::create));
    registry.insert(pair<payloadType, CreatorFunction>(static_cast<payloadType>(PT_EMPTY_UEDGE), EmptyUndirEdge::create));
}

payloadType GEFactory::reg(CreatorFunction classCreator) {
    lock_guard<mutex> lck(typeMtx);
    payloadType typeKey;
    registry.insert(pair<payloadType, CreatorFunction>(typeKey = typeCounter++, classCreator));
    return typeKey;
}

shared_ptr<GraphElem> GEFactory::create(std::shared_ptr<Database> &db, payloadType typeKey) {
    auto it = registry.find(typeKey);
    if (it != registry.end()) {
        if (it->second) {
            return it->second(db, typeKey);
        }
    }
    throw DebugException(string("Unknown graph elem type: ") + to_string(typeKey));
}

InitStatic::InitStatic() {
    EndianInfo::initStatic();
    FixedFieldIO::initStatic();
    GEFactory::initStatic();
}
