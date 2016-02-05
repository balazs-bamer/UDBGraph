/*
COPYRIGHT COMES HERE
*/

#include<cstring>
#include<algorithm>
#include"serializer.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;


bool Unalignment::allowUnalign = false;

uint32_t FixedFieldIO::pos2sizes[RT_NOMORE][FIELD_VAR_MAX_POS];

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

void FixedFieldIO::setField(uint32_t fieldStart, uint8_t value, uint8_t * const array) {
#ifdef DEBUG
    checkPosition(fieldStart, *array);
#endif
    array[fieldStart] = value;
}

uint64_t FixedFieldIO::getField(uint32_t fieldStart, uint8_t * const array) {
#ifdef DEBUG
    checkPosition(fieldStart, *array);
#endif
    int len = pos2sizes[array[FP_RECORDTYPE]][fieldStart];
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

void FixedFieldIO::checkPosition(uint32_t fieldStart, int recordType) {
    if(recordType >= RT_NOMORE || fieldStart > FIELD_VAR_MAX_POS) {
        throw DebugException(string("Invalid record type (") + to_string(static_cast<int>(recordType)) +
                          string(") or field position (") + to_string(static_cast<int>(fieldStart)) +
                          string(") out of bounds.") );
    }
    uint32_t len = pos2sizes[recordType][fieldStart];
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

size_t RecordChain::Record::size = 0;

constexpr uint32_t RecordChain::Record::recordVarStarts[RT_NOMORE];

void RecordChain::Record::setSize(size_t s) {
#ifndef DEBUG
    if(size == 0) {
#endif
        if(s <= FIELD_VAR_MAX_POS + 2 * sizeof(uint64_t) ||
                s > UDB_MAX_RECORD_SIZE || s % sizeof(uint64_t) != 0) {
            throw DebugException("Invalid record size.");
        }
        size = s;
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
    memset(record, 0, FIELD_VAR_MAX_POS);
    record[FP_RECORDTYPE] = uint8_t(rt);
    setField(FP_PAYLOADTYPE, pType);
}

RecordChain::Record::Record(Record &&p) noexcept :
    record(p.record), index(p.index), key(p.key), upsKey(p.upsKey), upsRecord(p.upsRecord) {
    p.record = nullptr;
    p.upsRecord.data = nullptr;
    upsKey.data = &key;
}

RecordChain::Record& RecordChain::Record::operator=(RecordChain::Record &&p) noexcept {
    upsRecord.data = record = p.record;
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

inline bool RecordChain::Record::operator<<(const uint8_t byte) noexcept {
    if(index == size) {
        return false;
    }
    record[index++] = byte;
    return true;
}

inline uint64_t RecordChain::Record::write(const char *cp, uint64_t len) noexcept {
    uint64_t written = min(len, size - index);
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
    uint64_t read = min(len, size - index);
    strncpy(cp, reinterpret_cast<char*>(record + index), read);
    index += read;
    return read;
}

ups_status_t RecordChain::Record::read(ups_db_t *db, keyType k, ups_txn_t *tr) noexcept {
    key = k;
    ups_status_t result = ups_db_find(db, tr, &upsKey, &upsRecord, 0);
    if(result != UPS_KEY_NOT_FOUND) {
        check(result);
        delete[] record;
        record = new uint8_t[size];
        memcpy(record, upsRecord.data, size);
        index = recordVarStarts[*record];
    }
    return result;
}

void RecordChain::Record::write(ups_db_t *db, ups_txn_t *tr) {
    uint32_t flags = UPS_OVERWRITE;
    upsRecord.size = size;
    upsRecord.data = record;
    check(ups_db_insert(db, tr, &upsKey, &upsRecord, flags));
}

void RecordChain::setRecordSize(size_t s) {
    Record::setSize(s);
}

RecordChain::RecordChain(RecordType rt, payloadType pt) : pType(pt) {
    Record::checkSize();
    state = RCState::EMPTY;
    content.push_back(Record(rt, pt));
    iter = content.begin();
}

void RecordChain::setHead(keyType key, const uint8_t * const rec) {
    content.clear();
    Record record(key, rec);
    pType = record.getField(FP_PAYLOADTYPE);
    content.push_back(move(record));
    iter = content.begin();
    state = RCState::HEAD;
}

void RecordChain::clone(const RecordChain &other) {
    state = other.state;
    pType = other.pType;
    // recordType must remain intact
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
            content.push_back(Record(RT_CONT, pType));
            // clone into the new record
            (--content.end())->clone(*itOther);
            itOther++;
        }
    }
    reset();
}

void RecordChain::reset() {
    iter = content.begin();
    for(Record &rec : content) {
        rec.reset();
    }
}

void RecordChain::clear() {
    RecordType rt = RecordType(getHeadField(FP_RECORDTYPE));
    content.clear();
    content.push_back(Record(rt, pType));
    iter = content.begin();
    state = RCState::EMPTY;
}

void RecordChain::setHeadField(uint32_t fieldStart, uint64_t value) {
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
    content.begin()->setField(fieldStart, value);
}

uint64_t RecordChain::getHeadField(uint32_t fieldStart) {
    if(content.size() == 0) {
        throw DebugException("No record!");
    }
    return content.begin()->getField(fieldStart);
}

deque<keyType> RecordChain::getKeys() const {
    deque<keyType> keys;
    for(const Record &rec : content) {
        keys.push_back(rec.getKey());
    }
    return keys;
}

void RecordChain::addEdge(keyType edgeKey, FieldPosNode where, ups_txn_t *tr) {
// TODO implement
}

void RecordChain::stripLeftover() {
    // terminate chain
    iter->setField(FP_NEXT, KEY_INVALID);
    for(iter++; iter != content.end();) {
        iter = content.erase(iter);
    }
}

void RecordChain::read(keyType key, ups_txn_t *tr, RCState level) {
RCState was = state;
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
    size_t desired = numeric_limits<size_t>::max(); // means all
    size_t payloadStart = desired;
    RecordType recType;
    if(level == RCState::HEAD) {
        desired = 1;
    }
    if(state == RCState::HEAD && level == RCState::PARTIAL) {
        recType = RecordType(content.begin()->getField(FP_RECORDTYPE));
        payloadStart = calcPayloadStart(*(content.begin()));
        desired = payloadStart = payloadStart / Record::getSize() + 1;
    }
    // for PARTIAL we must calculate from head
    while(true) {
        Record record;
        ups_status_t result = record.read(db, key, tr);
        if(result == UPS_KEY_NOT_FOUND) {
            if(content.size() == 0) {
                throw ExistenceException("The element cannot be read, might have been deleted meanwhile.");
            }
            else {
                throw CorruptionException("Broken record chain.");
            }
        }
        if(content.size() == 0) {
            recType = RecordType(record.getField(FP_RECORDTYPE));
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
    iter = content.begin();
}

void RecordChain::write(deque<keyType> &oldKeys, keyType key, ups_txn_t *tr) {
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
        // first construct the double linked list
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
            itThis->write(db, tr); // insert
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
            check(ups_db_erase(db, tr, &upsKey, 0));
            itOther++;
        }
    }
    itThis = content.begin();
    itOther = oldKeys.begin();
    while (itThis != content.end() && itOther != oldKeys.end()) {
        itThis->write(db, tr); // update
        itThis++;
        itOther++;
    }
}

void RecordChain::write(uint8_t b) {
    if(state == RCState::EMPTY) {
        state = RCState::FULL;
    }
    if(!(*iter << b)) {
        content.push_back(Record(RT_CONT, pType));
        iter++;
        *iter << b;
    }
}

void RecordChain::write(const char *cp, uint64_t len) {
    do {
        uint64_t written = iter->write(cp, len);
        if(written < len) {
            content.push_back(Record(RT_CONT, pType));
            iter++;
        }
        len -= written;
        cp += written;
    } while(len > 0);
}

void RecordChain::read(uint8_t &b) {
    if(iter != content.end()) {
        if(*iter >> b) {
            return;
        }
        if(++iter != content.end()) {
            *iter >> b;
            return;
        }
    }
    throw DebugException("No more data in RecordChain to read.");
}

void RecordChain::read(char *cp, uint64_t len) {
    do {
        if(iter == content.end()) {
            throw DebugException("No more data in RecordChain to read.");
        }
        uint64_t read = iter->read(cp, len);
        if(read < len) {
            iter++;
        }
        len -= read;
        cp += read;
    } while(len > 0);
    *cp = 0;
}

void RecordChain::read(string &s, uint64_t len) {
    char *cp = new char[Record::getSize() + 1];
    do {
        if(iter == content.end()) {
            delete[] cp;
            throw DebugException("No more data in RecordChain to read.");
        }
        uint64_t read = iter->read(cp, len);
        cp[read] = 0;
        if(read < len) {
            iter++;
        }
        len -= read;
        s += cp;
    } while(len > 0);
    delete[] cp;
}

size_t RecordChain::calcHashesPad(RecordType recType) {
    return (Record::recordVarStarts[recType] + sizeof(keyType) - 1) /
            sizeof(keyType) * sizeof(keyType);
}

size_t RecordChain::calcHashLen(uint32_t buckets) {
    size_t keysPerRecord = Record::getSize() / sizeof(keyType);
    // TODO change
    unsigned M  = 5;
    return (buckets - M + keysPerRecord - 1) / keysPerRecord * keysPerRecord + M;
}

size_t RecordChain::calcPayloadStart(Record &rec) {
    RecordType recType = RecordType(rec.getField(FP_RECORDTYPE));
    size_t payloadStart;
    if(recType == RT_NODE || recType == RT_ROOT) {
        payloadStart = calcHashesPad(recType);
        size_t totalLen =
            calcHashLen(rec.getField(FPN_IN_BUCKETS)) +
            calcHashLen(rec.getField(FPN_OUT_BUCKETS)) +
            calcHashLen(rec.getField(FPN_UN_BUCKETS));
        payloadStart += totalLen * sizeof(keyType);
    }
    else {
        payloadStart = Record::recordVarStarts[recType];
    }
    return payloadStart;
}

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

