/*
COPYRIGHT COMES HERE
*/

#include<cstring>
#include<algorithm>
#include"serializer.h"

#ifdef DEBUG
#include<iostream>
#endif

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;

#ifdef DEBUG
ups_status_t udbgraph::_ups_db_insert(ups_db_t *db, ups_txn_t *txn, ups_key_t *key, ups_record_t *record, uint32_t flags) {
    UpsCounter::countInsert++;
    return ups_db_insert(db, txn, key, record, flags);
}

ups_status_t udbgraph::_ups_db_erase(ups_db_t *db, ups_txn_t *txn, ups_key_t *key, uint32_t flags) {
    UpsCounter::countErase++;
    return ups_db_erase(db, txn, key, flags);
}

ups_status_t udbgraph::_ups_db_find(ups_db_t *db, ups_txn_t *txn, ups_key_t *key, ups_record_t *record, uint32_t flags) {
    UpsCounter::countFind++;
    return ups_db_find(db, txn, key, record, flags);
}

uint64_t UpsCounter::countInsert = 0;
uint64_t UpsCounter::countErase = 0;
uint64_t UpsCounter::countFind = 0;
#endif

bool Unalignment::allowUnalign = false;

countType FixedFieldIO::pos2sizes[RT_NOMORE][FIELD_VAR_MAX_POS];

void FixedFieldIO::initStatic() noexcept {
    for(int i = 0; i < RT_NOMORE; i++) {
        pos2sizes[i][FP_RECORDTYPE] = sizeof(uint8_t);
        pos2sizes[i][FP_LOCK] = sizeof(uint8_t);
        pos2sizes[i][FP_RES1] = sizeof(uint8_t);
        pos2sizes[i][FP_RES2] = sizeof(uint8_t);
        pos2sizes[i][FP_ACL] = sizeof(keyType);
        pos2sizes[i][FP_NEXT] = sizeof(keyType);
        pos2sizes[i][FP_PAYLOADTYPE] = sizeof(countType);
    }
    for(int i = 0; i < APP_NAME_LENGTH; i++) {
        pos2sizes[RT_ROOT][FPR_APP_NAME + i] = sizeof(uint8_t);
    }
    pos2sizes[RT_ROOT][FPR_VER_MAJOR] = sizeof(countType);
    pos2sizes[RT_ROOT][FPR_VER_MINOR] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_IN_BUCKETS] = pos2sizes[RT_NODE][FPN_IN_BUCKETS] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_IN_USED] = pos2sizes[RT_NODE][FPN_IN_USED] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_IN_DELETED] = pos2sizes[RT_NODE][FPN_IN_DELETED] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_OUT_BUCKETS] = pos2sizes[RT_NODE][FPN_OUT_BUCKETS] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_OUT_USED] = pos2sizes[RT_NODE][FPN_OUT_USED] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_OUT_DELETED] = pos2sizes[RT_NODE][FPN_OUT_DELETED] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_UN_BUCKETS] = pos2sizes[RT_NODE][FPN_UN_BUCKETS] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_UN_USED] = pos2sizes[RT_NODE][FPN_UN_USED] = sizeof(countType);
    pos2sizes[RT_ROOT][FPN_UN_DELETED] = pos2sizes[RT_NODE][FPN_UN_DELETED] = sizeof(countType);
    pos2sizes[RT_DEDGE][FPE_NODE_START] = pos2sizes[RT_UEDGE][FPE_NODE_START] = sizeof(keyType);
    pos2sizes[RT_DEDGE][FPE_NODE_END] = pos2sizes[RT_UEDGE][FPE_NODE_END] = sizeof(keyType);
    pos2sizes[RT_CONT][FPC_HEAD] = sizeof(keyType);
}

void FixedFieldIO::setField(countType fieldStart, uint8_t value, uint8_t * const array) {
#ifdef DEBUG
    checkPosition(fieldStart, static_cast<RecordType>(*array), sizeof(value));
#endif
    array[fieldStart] = value;
}

void FixedFieldIO::setField(countType fieldStart, uint16_t value, uint8_t * const array) {
#ifdef DEBUG
    checkPosition(fieldStart, static_cast<RecordType>(*array), sizeof(value));
#endif
    doSetField(fieldStart, value, array);
}

void FixedFieldIO::setField(countType fieldStart, uint32_t value, uint8_t * const array) {
#ifdef DEBUG
   checkPosition(fieldStart, static_cast<RecordType>(*array), sizeof(value));
#endif
   doSetField(fieldStart, value, array);
}

void FixedFieldIO::setField(countType fieldStart, uint64_t value, uint8_t * const array) {
#ifdef DEBUG
   checkPosition(fieldStart, static_cast<RecordType>(*array), sizeof(value));
#endif
   doSetField(fieldStart, value, array);
}

inline uint64_t FixedFieldIO::getField(countType fieldStart, uint8_t * const array) {
#ifdef DEBUG
   checkPosition(fieldStart, static_cast<RecordType>(*array));
#endif
   return doGetField(fieldStart, array);
}

uint64_t FixedFieldIO::doGetField(countType fieldStart, uint8_t * const array, uint8_t len) {
    if(len == 0) {
        len = pos2sizes[array[FP_RECORDTYPE]][fieldStart];
    }
    union {
        uint64_t read;
        uint8_t bytes[sizeof(uint64_t)];
    };
    if(EndianInfo::isLittle()) {
        if(allowUnalign) {
            if(len < sizeof(uint32_t)) {
                if(len == sizeof(uint8_t)) {
                    read = array[fieldStart];
                }
                else {
                    read = *(reinterpret_cast<uint16_t*>(array + fieldStart));
                }
            }
            else {
                if(len == sizeof(uint32_t)) {
                    read = *(reinterpret_cast<uint32_t*>(array + fieldStart));
                }
                else {
                    read = *(reinterpret_cast<uint64_t*>(array + fieldStart));
                }
            }
        }
        else {
            read = 0;
            for(int i = 0; i < len; i++) {
                bytes[i] = array[fieldStart++];
            }
        }
    }
    else {
        read = 0;
        for(int i = 1; i <= len; i++) {
            bytes[sizeof(uint64_t) - i] = array[fieldStart++];
        }
    }
    return read;
}

void FixedFieldIO::checkPosition(countType fieldStart, RecordType recordType, uint8_t width) {
    if(recordType >= RT_NOMORE || fieldStart > FIELD_VAR_MAX_POS) {
        throw DebugException(string("Invalid record type (") + to_string(static_cast<int>(recordType)) +
                          string(") or field position (") + to_string(static_cast<int>(fieldStart)) +
                          string(") out of bounds.") );
    }
    countType len = pos2sizes[recordType][fieldStart];
    if(width > 0 && width != len) {
        throw DebugException(string("Invalid position (") + to_string(static_cast<int>(fieldStart)) +
                          string(") required for record type: ") + to_string(static_cast<int>(recordType)) +
                          string(" and type size: ") + to_string(static_cast<int>(width)));
    }
    switch(len) {
    case sizeof(uint8_t):
    case sizeof(uint16_t):
    case sizeof(uint32_t):
    case sizeof(uint64_t):
        return;
    case 0:
        throw DebugException(string("Invalid position (") + to_string(static_cast<int>(fieldStart)) +
                          string(") required for record type: ") + to_string(static_cast<int>(recordType)));
    default:
        throw DebugException(string("Invalid value in pos2sizes: ") + to_string(len));
    }
}

countType RecordChain::Record::size = 0;

countType RecordChain::Record::keysPerRecord = 0;

constexpr uint32_t RecordChain::Record::recordVarStarts[RT_NOMORE];

constexpr uint32_t RecordChain::Record::hashStarts[RT_NOMORE];

void RecordChain::Record::setSize(countType s) {
#ifndef DEBUG
    if(size == 0) {
#endif
        if(s <= hashStarts[RT_ROOT] + (1 + 3 * RecordChain::primes[0]) * sizeof(keyType) ||
                s > UDB_MAX_RECORD_SIZE || s % sizeof(keyType) != 0) {
            throw DebugException("Invalid record size.");
        }
        size = s;
        keysPerRecord = size / sizeof(keyType);
#ifndef DEBUG
    }
#endif
}

inline void RecordChain::Record::checkSize() {
    if(size == 0) {
        throw DatabaseException("Record size was not set.");
    }
}

RecordChain::Record::Record() :
    record(new uint8_t[size]), index(0), key(static_cast<keyType>(KEY_INVALID)) {
    upsKey.flags = upsKey._flags = 0;
    upsKey.data = &key;
    upsKey.size = sizeof(key);
    upsRecord.flags = upsRecord.partial_offset = upsRecord.partial_size = 0;
    upsRecord.size = 0;
    upsRecord.data = nullptr;
    record[FP_RECORDTYPE] = static_cast<uint8_t>(RT_INVALID);
}

RecordChain::Record::Record(keyType k, const uint8_t * const rec) :
    record(new uint8_t[size]), index(0), key(k) {
    upsKey.flags = upsKey._flags = 0;
    upsKey.data = &key;
    upsKey.size = sizeof(key);
    upsRecord.flags = upsRecord.partial_offset = upsRecord.partial_size = 0;
    upsRecord.size = 0;
    upsRecord.data = nullptr;
    memcpy(record, rec, size);
    index = recordVarStarts[*record];
    RecordType rt = static_cast<RecordType>(*record);
}

RecordChain::Record::Record(RecordType rt, payloadType pType) :
    record(new uint8_t[size]), index(recordVarStarts[rt]), key(static_cast<keyType>(KEY_INVALID)) {
    upsKey.flags = upsKey._flags = 0;
    upsKey.data = &key;
    upsKey.size = sizeof(key);
    upsRecord.flags = upsRecord.partial_offset = upsRecord.partial_size = 0;
    upsRecord.size = size;
    upsRecord.data = record;
    // zero and invalidate everything
    memset(record, 0, size);
    record[FP_RECORDTYPE] = static_cast<uint8_t>(rt);
    if(rt == RT_NODE || rt == RT_ROOT) {
        // set bucket counts
        setField(FPN_IN_BUCKETS, primes[0]);
        setField(FPN_OUT_BUCKETS, primes[0]);
        setField(FPN_UN_BUCKETS, primes[0]);
    }
    setField(FP_PAYLOADTYPE, pType);
}

/*RecordChain::Record::Record(const RecordChain::Record &p) :
    record(new uint8_t[size]), index(p.index), key(KEY_INVALID), upsKey(p.upsKey), upsRecord(p.upsRecord) {
    memcpy(record, p.record, size);
    upsRecord.data = record;
    upsKey.data = &key;
}*/

RecordChain::Record::Record(RecordChain::Record &&p) noexcept :
    record(p.record), index(p.index), key(p.key), upsKey(p.upsKey), upsRecord(p.upsRecord) {
    p.record = nullptr;
    p.upsRecord.data = nullptr;
    upsKey.data = &key;
}

/*RecordChain::Record& RecordChain::Record::operator=(const RecordChain::Record &p) noexcept {
    upsRecord = p.upsRecord;
    index = p.index;
    key = KEY_INVALID;
    upsKey = p.upsKey;
    memcpy(record, p.record, size);
    upsRecord.data = record;
    upsKey.data = &key;
    return *this;
}*/

RecordChain::Record& RecordChain::Record::operator=(RecordChain::Record &&p) noexcept {
    upsRecord = p.upsRecord;
    upsKey = p.upsKey;
    record = p.record;
    index = p.index;
    key = p.key;
    p.record = nullptr;
    upsKey.data = &key;
    p.upsRecord.data = nullptr;
    return *this;
}

void RecordChain::Record::clone(const Record &other) noexcept {
    key = other.key;
    memcpy(record, other.record, size);
}

void RecordChain::Record::writeKey(countType pos, keyType key) {
#ifdef DEBUG
    if(pos >= keysPerRecord) {
        throw DebugException("Illegal key index in record.");
    }
#endif
    doSetField(pos * sizeof(keyType), key, record);
}

keyType RecordChain::Record::readKey(countType pos) const {
#ifdef DEBUG
    if(pos >= keysPerRecord) {
        throw DebugException("Illegal key index in record.");
    }
#endif
    return doGetField(pos * sizeof(keyType), record, sizeof(keyType));
}

inline bool RecordChain::Record::operator<<(const uint8_t byte) noexcept {
    if(index == size) {
        return false;
    }
    record[index++] = byte;
    return true;
}

inline uint64_t RecordChain::Record::write(const char *cp, uint64_t len) noexcept {
    uint64_t written = min(len, static_cast<uint64_t>(size - index));
    strncpy(reinterpret_cast<char*>(record + index), cp, written);
    index += written;
    return written;
}

inline bool RecordChain::Record::operator>>(uint8_t &byte) noexcept {
    if(index == size) {
        return false;
    }
    byte = record[index++];
    return true;
}

inline uint64_t RecordChain::Record::read(char *cp, uint64_t len) noexcept {
    uint64_t read = min(len, static_cast<uint64_t>(size - index));
    strncpy(cp, reinterpret_cast<char*>(record + index), read);
    index += read;
    return read;
}

ups_status_t RecordChain::Record::load(ups_db_t *db, keyType k, ups_txn_t *tr) noexcept {
    key = k;
    memset(&upsRecord, 0, sizeof(upsRecord));
    ups_status_t result = _ups_db_find(db, tr, &upsKey, &upsRecord, 0);
    if(result != UPS_KEY_NOT_FOUND) {
        check(result);
        memcpy(record, upsRecord.data, size);
        index = recordVarStarts[*record];
        RecordType rt = static_cast<RecordType>(*record);
    }
    return result;
}

void RecordChain::Record::save(ups_db_t *db, ups_txn_t *tr) {
    uint32_t flags = UPS_OVERWRITE;
    upsRecord.size = size;
    upsRecord.data = record;
    check(_ups_db_insert(db, tr, &upsKey, &upsRecord, flags));
}

countType RecordChain::Record::hashInit(countType startKeyInd, countType remaining) noexcept {
    // TODO use memset
    countType ret = min(keysPerRecord - startKeyInd, remaining);
    memset(record + sizeof(keyType) * startKeyInd, 0, ret * sizeof(keyType));
    /*keyType *hashTable = reinterpret_cast<keyType*>(record);
    countType i = startKeyInd;
    countType ret = 0;
    for(; i < keysPerRecord && ret < remaining; i++, ret++) {
        hashTable[i] = HASH_FREE;
    }*/
    return ret;
}

countType RecordChain::Record::hashCollect(countType startKeyInd, keyType *&dest, countType remaining) const noexcept {
    keyType *source = reinterpret_cast<keyType*>(record);
    countType i = startKeyInd;
    countType ret = 0;
    for(; i < keysPerRecord && ret < remaining; i++, ret++) {
        keyType key = source[i];
        if(key != HASH_FREE && key != HASH_DELETED) {
            *dest = key;
            dest++;
        }
    }
    return ret;
}

uint32_t constexpr RecordChain::primes[];

uint32_t constexpr RecordChain::primesLen;

void RecordChain::setRecordSize(size_t s) {
    Record::setSize(s);
}

RecordChain::RecordChain(RecordType rt, payloadType pt) : pType(pt) {
    Record::checkSize();
    state = RCState::EMPTY;
    content.push_back(Record(rt, pt));
    if(rt == RT_NODE || rt == RT_ROOT) {
        setHashStart();
        hashInit();
    }
    reset();
}

void RecordChain::setHead(keyType key, const uint8_t * const rec) {
    content.clear();
    Record record(key, rec);
    pType = record.getField(FP_PAYLOADTYPE);
    content.push_back(move(record));
// TODO check if needed    index = 0;
    RecordType rt = static_cast<RecordType>(*rec);
    if(rt == RT_NODE || rt == RT_ROOT) {
        setHashStart();
    }
    if(hashStartRecord[RCS_PAY] == 0) {
        if(getHeadField(FP_NEXT) == KEY_INVALID) {
            // no more records
            state = RCState::FULL;
        }
        else {
            // payload is missing
            state = RCState::PARTIAL;
        }
    }
    else {
        // hash is not read
        state = RCState::HEAD;
    }
    reset();
}

void RecordChain::clone(const RecordChain &other) {
    state = other.state;
    pType = other.pType;
    // recordType must remain intact
    for(int i = RCS_IN; i < RCS_NOMORE; i++) {
        hashStartKey[i] = other.hashStartKey[i];
        hashStartRecord[i] = other.hashStartRecord[i];
    }
    auto itThis = content.begin();
    auto itOther = other.content.begin();
    while (itThis != content.end() && itOther != other.content.end()) {
        itThis->clone(*itOther);
        itThis++;
        itOther++;
    }
    if(itThis != content.end()) {
        // too long for the other list, truncate it
        content.erase(itThis, content.end());
    }
    else {
        // too short for the other list, expand and copy leftover
        while(itOther != other.content.end()) {
            Record record(RT_CONT, pType);
            record.clone(*itOther);
            content.push_back(move(record));
            itOther++;
        }
    }
    RecordType rt = static_cast<RecordType>(content.begin()->getField(FP_RECORDTYPE));
    if(rt == RT_NODE || rt == RT_ROOT) {
        setHashStart();
    }
    reset();
}

void RecordChain::reset() {
    index = 0;
    for(Record &rec : content) {
        rec.reset();
    }
    // root would not need it, but only in case
    RecordType rt = static_cast<RecordType>(getHeadField(FP_RECORDTYPE));
    if(rt == RT_NODE || rt == RT_ROOT) {
        index = hashStartRecord[RCS_PAY];
        if(state >= RCState::PARTIAL) {
            content[index].setIndex(hashStartKey[RCS_PAY] * sizeof(keyType));
        }
    }
}

void RecordChain::clear() {
    RecordType rt = static_cast<RecordType>(getHeadField(FP_RECORDTYPE));
    content.clear();
    content.push_back(Record(rt, pType));
    index = 0;
    if(rt == RT_NODE || rt == RT_ROOT) {
        setHashStart();
        hashInit();
    }
    state = RCState::EMPTY;
}

void RecordChain::setHeadField(uint32_t fieldStart, uint8_t value) {
#ifdef DEBUG
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
#endif
    content.begin()->setField(fieldStart, value);
}

void RecordChain::setHeadField(uint32_t fieldStart, uint16_t value) {
#ifdef DEBUG
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
#endif
    content.begin()->setField(fieldStart, value);
}

void RecordChain::setHeadField(uint32_t fieldStart, uint32_t value) {
#ifdef DEBUG
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
#endif
    content.begin()->setField(fieldStart, value);
    if(fieldStart == FPN_IN_BUCKETS || fieldStart == FPN_OUT_BUCKETS || fieldStart == FPN_UN_BUCKETS) {
        setHashStart();
    }
}

void RecordChain::setHeadField(uint32_t fieldStart, uint64_t value) {
#ifdef DEBUG
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
#endif
    content.begin()->setField(fieldStart, value);
}

inline uint64_t RecordChain::getHeadField(uint32_t fieldStart) const {
#ifdef DEBUG
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
#endif
    return content.begin()->getField(fieldStart);
}

deque<keyType> RecordChain::getKeys() const {
    deque<keyType> keys;
    for(const Record &rec : content) {
        keys.push_back(rec.getKey());
    }
    return keys;
}

void RecordChain::addEdge(FieldPosNode which, keyType key, ups_txn_t *tr) {
    unordered_set<indexType> modifiedIndices = hashInsert(which, key);
    for(indexType i : modifiedIndices) {
        content[i].save(db, tr);
    }
}

void RecordChain::stripLeftover() {
    // terminate chain
    content[index].setField(FP_NEXT, KEY_INVALID);
    content.erase(content.begin() + index + 1, content.end());
}

void RecordChain::load(keyType key, ups_txn_t *tr, RCState level) {
    if(level == RCState::EMPTY) {
        throw DebugException("Cannot read no records (requested level = RCState::EMPTY).");
    }
    if(level <= state) {
        // nothing to do
        return;
    }
    if(state == RCState::EMPTY) {
        // we read everything from disk
        content.clear();
    }
    else {
        // we continue on the record we read last
        auto last = content.end();
        last--;
        key = last->getField(FP_NEXT); // cannot be invalid
    }
    if(key == KEY_INVALID) {
        throw DebugException("Trying to read an element for invalid key.");
    }
    indexType desired = numeric_limits<indexType>::max(); // means all
    indexType payloadStart = desired;
    RecordType recType;
    if(level == RCState::HEAD) {
        desired = 1;
    }
    if(state == RCState::HEAD && level == RCState::PARTIAL) {
        recType = static_cast<RecordType>(content[0].getField(FP_RECORDTYPE));
        payloadStart = calcPayloadStart(content[0]);
        desired = payloadStart = payloadStart / Record::getSize() + 1;
    }
    // for PARTIAL we must calculate from head
    while(true) {
        Record record;
        ups_status_t result = record.load(db, key, tr);
        if(result == UPS_KEY_NOT_FOUND) {
            if(content.size() == 0) {
                throw ExistenceException("The element cannot be read might have been deleted meanwhile.");
            }
            else {
                throw CorruptionException("Broken record chain.");
            }
        }
        if(content.size() == 0) {
            recType = static_cast<RecordType>(record.getField(FP_RECORDTYPE));
            if(recType == RT_NODE || recType == RT_ROOT) {
                setHashStart(&record);
            }
            payloadStart = calcPayloadStart(record);
            payloadStart = payloadStart / Record::getSize() + 1;
            if(level == RCState::PARTIAL) {
                if(recType == RT_DEDGE || recType == RT_UEDGE) {
                    // edge
                    desired = 1;
                }
                if(recType == RT_NODE || recType == RT_ROOT) {
                    // node or root
                    desired = payloadStart;
                }
            }
            pType = record.getField(FP_PAYLOADTYPE);
        }
        key = record.getField(FP_NEXT);
        content.push_back(move(record));
        if(key == KEY_INVALID) {
            state = RCState::FULL;
            break;
        }
        if(content.size() == desired) {
            if(desired > 1 || (desired == 1 && payloadStart == 1)) {
                state = RCState::PARTIAL;
            }
            else {
                state = RCState::HEAD;
            }
            break;
        }
    }
    reset();
}

void RecordChain::save(deque<keyType> &oldKeys, keyType key, ups_txn_t *tr) {
    auto itThis = content.begin();
    auto itOther = oldKeys.begin();
    keyType newKey;
    // the head record gets the key
    itThis->setKey(newKey = key);
    // we need iterators for possible deleting
    while (itThis != content.end() && itOther != oldKeys.end()) {
        itThis++;
        itOther++;
    }
    if(itThis != content.end()) {
        // too long for the old list, insert the leftover
        auto newStart = itThis;
        auto itPrev = itThis;
        if(itPrev != content.begin()) {
            itPrev--; // if it equals to itThis there is no previous
        }
        while(itThis != content.end()) {
            itThis->setKey(newKey = keyGen->nextKey());
            if(itThis != content.begin()) {
                itThis->setField(FPC_HEAD, key);
            }
            if(itPrev != itThis) {
                itPrev->setField(FP_NEXT, newKey);
                itPrev++;
            }
            itThis++;
        }
        itPrev->setField(FP_NEXT, KEY_INVALID);
        itThis = newStart;
        while(itThis != content.end()) {
            itThis->save(db, tr); // insert
            itThis++;
        }
    }
    else {
        if(itThis == content.begin()) {
            throw DebugException("Trying to write no records.");
        }
        // terminate the record chain
        itThis--;
        itThis->setField(FP_NEXT, KEY_INVALID);
        // too short for the old list, delete the old leftover
        keyType key;
        ups_key_t upsKey;
        upsKey.flags = upsKey._flags = 0;
        upsKey.data = &key;
        upsKey.size = sizeof(key);
        while(itOther != oldKeys.end()) {
            key = *itOther;
            check(_ups_db_erase(db, tr, &upsKey, 0));
            itOther++;
        }
    }
    itThis = content.begin();
    itOther = oldKeys.begin();
    while (itThis != content.end() && itOther != oldKeys.end()) {
        itThis->save(db, tr); // update
        itThis++;
        itOther++;
    }
}

void RecordChain::write(uint8_t b) {
    if(state == RCState::EMPTY) {
        state = RCState::FULL;
    }
    if(!(content[index] << b)) {
        content.push_back(Record(RT_CONT, pType));
        index++;
        content[index] << b;
    }
}

void RecordChain::write(const char *cp, uint64_t len) {
    do {
        uint64_t written = content[index].write(cp, len);
        if(written < len) {
            content.push_back(Record(RT_CONT, pType));
            index++;
        }
        len -= written;
        cp += written;
    } while(len > 0);
}

void RecordChain::read(uint8_t &b) {
    if(index != content.size()) {
        if(content[index] >> b) {
            return;
        }
        if(++index != content.size()) {
            content[index] >> b;
            return;
        }
    }
    throw DebugException("No more data in RecordChain to read.");
}

void RecordChain::read(char *cp, uint64_t len) {
    do {
        if(index == content.size()) {
            throw DebugException("No more data in RecordChain to read.");
        }
        uint64_t read = content[index].read(cp, len);
        if(read < len) {
            index++;
        }
        len -= read;
        cp += read;
    } while(len > 0);
    *cp = 0;
}

void RecordChain::read(string &s, uint64_t len) {
    char *cp = new char[Record::getSize() + 1];
    do {
        if(index == content.size()) {
            delete[] cp;
            throw DebugException("No more data in RecordChain to read.");
        }
        uint64_t read = content[index].read(cp, len);
        cp[read] = 0;
        if(read < len) {
            index++;
        }
        len -= read;
        s += cp;
    } while(len > 0);
    delete[] cp;
}

indexType RecordChain::calcHashLen(countType buckets) const noexcept {
    // We use the whole record length, not only the space available for hash,
    // because this way we get absolute positions in the record in caller functions.
    indexType keysPerRecordBr = Record::getKeysPerRecord();
    indexType keysPerRecordNet = keysPerRecordBr - Record::hashStarts[RT_CONT];
    indexType firstPrime = primes[0];
    return (buckets - firstPrime + keysPerRecordNet - 1) / keysPerRecordNet *
            keysPerRecordBr + firstPrime;
}

indexType RecordChain::calcPayloadStart(Record &rec) const noexcept {
    RecordType recType = static_cast<RecordType>(rec.getField(FP_RECORDTYPE));
    indexType payloadStart;
    if(recType == RT_NODE || recType == RT_ROOT) {
        payloadStart = Record::hashStarts[recType];
        indexType totalLen =
            calcHashLen(rec.getField(FPN_IN_BUCKETS)) +
            calcHashLen(rec.getField(FPN_OUT_BUCKETS)) +
            calcHashLen(rec.getField(FPN_UN_BUCKETS));
        payloadStart += totalLen;
        payloadStart *= sizeof(keyType);
    }
    else {
        payloadStart = Record::recordVarStarts[recType];
    }
    return payloadStart;
}

void RecordChain::setHashStart(const Record * const rec) {
    RecordType rt;
    countType bucketsIn, bucketsOut, bucketsUn;
    if(rec == nullptr) {
        rt = static_cast<RecordType>(getHeadField(FP_RECORDTYPE));
        bucketsIn = static_cast<countType>(getHeadField(FPN_IN_BUCKETS));
        bucketsOut = static_cast<countType>(getHeadField(FPN_OUT_BUCKETS));
        bucketsUn = static_cast<countType>(getHeadField(FPN_UN_BUCKETS));
    }
    else {
        rt = static_cast<RecordType>(rec->getField(FP_RECORDTYPE));
        bucketsIn = static_cast<countType>(rec->getField(FPN_IN_BUCKETS));
        bucketsOut = static_cast<countType>(rec->getField(FPN_OUT_BUCKETS));
        bucketsUn = static_cast<countType>(rec->getField(FPN_UN_BUCKETS));
    }
    hashStartRecord[RCS_IN] = 0;
    hashStartKey[RCS_IN] = Record::hashStarts[rt];
    indexType nextStart = hashStartKey[RCS_IN] + calcHashLen(bucketsIn);
    // We use the whole record length, not only the space available for hash,
    // because this way we get absolute positions in the record in caller functions.
    countType keysPerRecord = Record::getKeysPerRecord();
    hashStartRecord[RCS_OUT] = nextStart / keysPerRecord;
    hashStartKey[RCS_OUT] = nextStart % keysPerRecord;
    nextStart += calcHashLen(bucketsOut);
    hashStartRecord[RCS_UN] = nextStart / keysPerRecord;
    hashStartKey[RCS_UN] = nextStart % keysPerRecord;
    nextStart += calcHashLen(bucketsUn);
    hashStartRecord[RCS_PAY] = nextStart / keysPerRecord;
    hashStartKey[RCS_PAY] = nextStart % keysPerRecord;
}

inline void RecordChain::calcTableIndices(FieldPosNode which, countType buckets, countType index, indexType &indRecord, countType &indKey) const {
#ifdef DEBUG
    if(which != FPN_IN_BUCKETS && which != FPN_OUT_BUCKETS && which != FPN_UN_BUCKETS) {
        throw DebugException("Invalid hash table specifier.");
    }
    if(index >= buckets) {
        throw DebugException("Hash index out of range.");
    }
#endif
    countType section = (which - FPN_IN_BUCKETS) / FR_SPAN;
    indexType startRecord = hashStartRecord[section];
    countType startKey = hashStartKey[section];
    countType keysPerRecordBr = Record::getKeysPerRecord();
    if(index < keysPerRecordBr - startKey) {
        indRecord = startRecord;
        indKey = startKey + index;
    }
    else {
        countType cntStart = Record::hashStarts[RT_CONT];
        countType keysPerRecordNet = keysPerRecordBr - cntStart;
        countType nom = index + startKey - keysPerRecordBr;
        indRecord = startRecord + 1 + nom / keysPerRecordNet;
        indKey = cntStart + nom % keysPerRecordNet;
    }
#ifdef DEBUG
    if(indKey >= keysPerRecordBr || indRecord >= content.size()) {
        throw DebugException("Calculated table indices are out of range.");
    }
#endif
}

keyType RecordChain::getHashContent(FieldPosNode which, countType buckets, countType index) const {
    indexType indRecord;
    countType indKey;
    calcTableIndices(which, buckets, index, indRecord, indKey);
    return content[indRecord].readKey(indKey);
}

indexType RecordChain::setHashContent(FieldPosNode which, countType buckets, countType index, keyType key) {
    indexType indRecord;
    countType indKey;
    calcTableIndices(which, buckets, index, indRecord, indKey);
    content[indRecord].writeKey(indKey, key);
    return indRecord;
}

countType RecordChain::hash(FieldPosNode which, countType buckets, keyType key, countType disp) {
    // TODO check for consecutive keys
    return static_cast<countType>((key % buckets + disp * (1 + key % (buckets - 1))) % buckets);
}

void RecordChain::hashInit(FieldPosNode which, countType remaining) noexcept {
    int n = (which - FPN_IN_BUCKETS) / FR_SPAN;
    indexType startRecInd = hashStartRecord[n];
    indexType endRecInd = hashStartRecord[n + 1];
    countType startKeyInd = hashStartKey[n];
    countType restKeyInd = Record::hashStarts[RT_CONT];
    for(indexType i = startRecInd; i <= endRecInd; i++) {
        if(i > startRecInd) {
            startKeyInd = restKeyInd;
        }
        countType ready = content[i].hashInit(startKeyInd, remaining);
        remaining -= ready;
    }
}

void RecordChain::hashInit() noexcept {
    hashInit(FPN_IN_BUCKETS, getHeadField((FPN_IN_BUCKETS)));
    hashInit(FPN_OUT_BUCKETS, getHeadField((FPN_OUT_BUCKETS)));
    hashInit(FPN_UN_BUCKETS, getHeadField((FPN_UN_BUCKETS)));
}

countType RecordChain::hashCollect(FieldPosNode which, keyType * array, countType remaining) const noexcept {
    int hashStartInd = (which - FPN_IN_BUCKETS) / FR_SPAN;
    indexType startRecInd = hashStartRecord[hashStartInd];
    indexType endRecInd = hashStartRecord[hashStartInd + 1];
    countType startKeyInd = hashStartKey[hashStartInd];
    countType restKeyInd = Record::hashStarts[RT_CONT];
    keyType *now = array;
    for(indexType i = startRecInd; i <= endRecInd; i++) {
        if(i > startRecInd) {
            startKeyInd = restKeyInd;
        }
        countType examined = content[i].hashCollect(startKeyInd, now, remaining);
        remaining -= examined;
    }
    return now - array;
}

indexType RecordChain::doInsert(FieldPosNode which, countType buckets, keyType key, countType * const deleted) {
    for(countType i = 0; i != buckets; i++) {
        countType ind = hash(which, buckets, key, i);
        keyType hashed = getHashContent(which, buckets, ind);
        if(hashed == HASH_FREE || hashed == HASH_DELETED) {
            if(hashed == HASH_DELETED) {
                if(deleted != nullptr) {
                    (*deleted)--;
                }
                else {
                    throw DebugException("Hash table was thought to be empty.");
                }
            }
            return setHashContent(which, buckets, ind, key);
        }
    }
    throw DebugException("Unable to insert key into hash.");
}

unordered_set<indexType> RecordChain::hashInsert(FieldPosNode which, keyType key) {
    unordered_set<indexType> modifiedIndices;
    int hashStartInd = (which - FPN_IN_BUCKETS) / FR_SPAN;
    countType buckets = getHeadField(which);
    countType used = getHeadField(which + FR_USED);
    countType deleted = getHeadField(which + FR_DELETED);
    if(used + deleted >= double(buckets) * 0.89) {
        if(buckets == primes[primesLen - 1]) {
            // not too likely but who knows
            throw IllegalQuantityException("Too many edges for a node.");
        }
        // save the old contents
        keyType *oldKeys = new keyType[used + 1];
        countType found = hashCollect(which, oldKeys, buckets);
        if(found != used) {
            throw DebugException("Hash content does not match \'used\' count.");
        }
        // and append the new key
        oldKeys[used++] = key;
        // calculate the new bucket count, first try only one more record
        countType keysPerRecordNet = Record::getKeysPerRecord() - Record::hashStarts[RT_CONT];
        countType firstPrime = primes[0];
        countType recordsNow = (buckets - firstPrime + keysPerRecordNet - 1) /
                             keysPerRecordNet;
        countType perhaps = (recordsNow + 1) * keysPerRecordNet + firstPrime;
        countType missingRecords;
        int where;
        for(where = 0; where < primesLen && primes[where] <= perhaps; where++);
        where--;
        if(primes[where] == buckets) {
            // one more record was not enough, calculate how many missing
            where++;
            missingRecords = (primes[where] - firstPrime + keysPerRecordNet - 1) /
                    keysPerRecordNet - recordsNow;
        }
        else {
            // the last one was right
            missingRecords = 1;
        }
        bool wasSingle = buckets == primes[0];
        buckets = primes[where];
        // sets hashStart* as well
        setHeadField(which, buckets);
        indexType firstRecord = hashStartRecord[hashStartInd];
        indexType lastRecord = hashStartRecord[hashStartInd + 1];
        countType firstKey = hashStartKey[hashStartInd];
        // the key after the last key
        countType lastKeyPlus = hashStartKey[hashStartInd + 1];
        for(indexType i = firstRecord; i <= lastRecord; i++) {
            modifiedIndices.insert(i);
        }
        // the index we insert at, pushing its old content and anything beyond
        // it forward
        payloadType pt = getHeadField(FP_PAYLOADTYPE);
        // insert all the missing records in one run to have the existing stuff
        // be moved only once
        auto insertIt = content.begin() + firstRecord + 1;
        deque<Record> newStuff;
        // the new records are initialized to free hash values
        for(countType i = 0; i < missingRecords; i++) {
            Record record(RT_CONT, pt);
            record.setKey(keyGen->nextKey());
            newStuff.push_back(move(record));
        }
        content.insert(insertIt,
                       make_move_iterator(newStuff.begin()),
                       make_move_iterator(newStuff.end()));
        // initialize hash contents to free values and save the remaining record
        // part if needed
        if(wasSingle) {
            // copy the stuff after this hashtable into it
            content[firstRecord + 1].copyContent(content[firstRecord]);
            if(firstRecord == 0) {
                // we copied the head, restore the record type
                content[firstRecord + 1].setField(FP_RECORDTYPE, static_cast<uint8_t>(RT_CONT));
            }
        }
        else {
            for(indexType i = firstRecord + 1 + missingRecords; i < lastRecord; i++) {
                content[i].hashInit(Record::hashStarts[RT_CONT], Record::getKeysPerRecord() - Record::hashStarts[RT_CONT]);
            }
        }
        content[firstRecord].hashInit(firstKey, Record::getKeysPerRecord() - firstKey);
        content[lastRecord].hashInit(Record::hashStarts[RT_CONT], lastKeyPlus - Record::hashStarts[RT_CONT]);
        // link the new records
        keyType oldEnd = content[firstRecord].getField(FP_NEXT);
        for(indexType i = firstRecord + missingRecords; i > firstRecord; i--) {
            Record &rec = content[i];
            rec.setField(FP_NEXT, oldEnd);
            oldEnd = rec.getKey();
        }
        content[firstRecord].setField(FP_NEXT, oldEnd);
        deleted = 0;
        // copy old keys into new table
        for(countType i = 0; i < used; i++) {
            doInsert(which, buckets, oldKeys[i], nullptr);
        }
        delete[] oldKeys;
    }
    else {
        // may decrement deleted
        modifiedIndices.insert(doInsert(which, buckets, key, &deleted));
        used++;
    }
    // we need the head record, too
    modifiedIndices.insert(0);
    setHeadField(which + FR_USED, used);
    setHeadField(which + FR_DELETED, deleted);
    return modifiedIndices;
}

#ifdef DEBUG
void RecordChain::fillHashTable(FieldPosNode which) {
    countType len = getHeadField(which);
    for(countType i = 0; i < len; i++) {
        setHashContent(which, len, i, primes[primesLen - 1] - i);
    }
}

void RecordChain::checkHashTable(FieldPosNode which) {
    countType len = getHeadField(which);
    for(countType i = 0; i < len; i++) {
        if(getHashContent(which, len, i) != primes[primesLen - 1] - i) {
            throw DebugException("Value mismatch during hash table check.");
        }
    }
}

void RecordChain::appendMissingRecords() {
    while(content.size() <= hashStartRecord[RCS_PAY]) {
        content.push_back(Record(RT_CONT, getHeadField(FP_PAYLOADTYPE)));
    }
}
#endif

Converter& Converter::operator>>(int8_t& b) {
    uint8_t t;
    chain.read(t);
    b = int8_t(t);
    return *this;
}

Converter& Converter::operator>>(bool &b) {
    uint8_t t;
    chain.read(t);
    b = bool(t);
    return *this;
}

Converter& Converter::operator>>(char *&p) {
    uint64_t len;
    *this >> len;
    char *tp = p = new char[len + 1];
    chain.read(tp, len);
    return *this;
}

Converter& Converter::operator>>(string &s) {
    uint64_t len;
    *this >> len;
    s.reserve(len);
    chain.read(s, len);
    return *this;
}

Converter& Converter::write(const char * const p) {
    uint64_t len = strlen(p);
    *this << len;
    chain.write(const_cast<const char *>(p), len);
    return *this;
}

