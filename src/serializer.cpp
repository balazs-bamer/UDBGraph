/*
COPYRIGHT COMES HERE
*/

#include<cstring>
#include"serializer.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

using namespace udbgraph;
using namespace std;


uint32_t FixedFieldIO::pos2sizes[RT_NOMORE][FIELD_VAR_MAX_POS];

void FixedFieldIO::initStatic() noexcept {
    for(int i = 0; i < RT_NOMORE; i++) {
        pos2sizes[i][FP_RECORDTYPE] = 1;
        pos2sizes[i][FP_LOCK] = 1;
        pos2sizes[i][FP_RES1] = 1;
        pos2sizes[i][FP_RES2] = 1;
        pos2sizes[i][FP_ACL] = sizeof(keyType);
        pos2sizes[i][FP_NEXT] = sizeof(keyType);
        pos2sizes[i][FP_PAYLOADTYPE] = sizeof(countType);
    }
    for(int i = 0; i < APP_NAME_LENGTH; i++) {
        pos2sizes[RT_ROOT][FPR_APP_NAME + i] = 1;
    }
    pos2sizes[RT_ROOT][FPR_VER_MAJOR] = sizeof(countType);
    pos2sizes[RT_ROOT][FPR_VER_MINOR] = sizeof(countType);
    pos2sizes[RT_ROOT][FPR_CNT_INEDGE] = pos2sizes[RT_NODE][FPN_CNT_INEDGE] = sizeof(countType);
    pos2sizes[RT_ROOT][FPR_CNT_OUTEDGE] = pos2sizes[RT_NODE][FPN_CNT_OUTEDGE] = sizeof(countType);
    pos2sizes[RT_ROOT][FPR_CNT_UNEDGE] = pos2sizes[RT_NODE][FPN_CNT_UNEDGE] = sizeof(countType);
    pos2sizes[RT_ROOT][FPR_CNT_FREDGE] = pos2sizes[RT_NODE][FPN_CNT_FREDGE] = sizeof(countType);
    pos2sizes[RT_DEDGE][FPE_NODE_START] = pos2sizes[RT_UEDGE][FPE_NODE_START] = sizeof(keyType);
    pos2sizes[RT_DEDGE][FPE_NODE_END] = pos2sizes[RT_UEDGE][FPE_NODE_END] = sizeof(keyType);
    pos2sizes[RT_CONT][FPC_HEAD] = sizeof(keyType);
    pos2sizes[RT_CONT][FPC_PREV] = sizeof(keyType);
}

void FixedFieldIO::setField(uint32_t fieldStart, uint64_t value, uint8_t * const array) {
#ifdef DEBUG
    checkPosition(fieldStart, *array);
#endif
    int len = pos2sizes[array[FP_RECORDTYPE]][fieldStart];
    union {
        uint64_t orig;
        uint8_t bytes[sizeof(uint64_t)];
    };
    orig = value;
    if(EndianInfo::isLittle()) {
        for(int i = 0; i < len; i++) {
            array[fieldStart++] = bytes[i];
        }
    }
    else {
        for(int i = 1; i <= len; i++) {
            array[fieldStart++] = bytes[sizeof(uint64_t) - i];
        }
    }
}

uint64_t FixedFieldIO::getField(uint32_t fieldStart, uint8_t * const array) {
#ifdef DEBUG
    checkPosition(fieldStart, *array);
#endif
    int len = pos2sizes[array[FP_RECORDTYPE]][fieldStart];
    union {
        uint64_t orig;
        uint8_t bytes[sizeof(uint64_t)];
    };
    orig = 0;
    if(EndianInfo::isLittle()) {
        for(int i = 0; i < len; i++) {
            bytes[i] = array[fieldStart++];
        }
    }
    else {
        for(int i = 1; i <= len; i++) {
            bytes[sizeof(uint64_t) - i] = array[fieldStart++];
        }
    }
    return orig;
}

void FixedFieldIO::checkPosition(uint32_t fieldStart, int recordType) {
    if(recordType >= RT_NOMORE || fieldStart > FIELD_VAR_MAX_POS) {
        throw logic_error(string("Invalid record type (") + to_string(int(recordType)) +
                          string(") or field position (") + to_string(fieldStart) +
                          string("out of bounds.") );
    }
    uint32_t len = pos2sizes[recordType][fieldStart];
    switch(len) {
    case 1:
    case 2:
    case 4:
    case 8:
        return;
    case 0:
        throw logic_error(string("Invalid position (") + to_string(int(fieldStart)) +
                          string(") required for record type: ") + to_string(int(recordType)));
    default:
        throw logic_error(string("Invalid value in pos2sizes: ") + to_string(len));
    }
}

size_t RecordChain::Record::size = 0;

const uint32_t RecordChain::Record::recordVarStarts[RT_NOMORE];

void RecordChain::Record::setSize(size_t s) {
    if(size == 0) {
        if(s <= FIELD_VAR_MAX_POS || s > UDB_MAX_RECORD_SIZE) {
            throw logic_error("Invalid record size.");
        }
        size = s;
    }
}


inline void RecordChain::Record::checkSize() {
    if(size == 0) {
        throw DatabaseException("Record size was not set.");
    }
}

RecordChain::Record::Record(RecordType rt, payloadType pType) :
    record(new uint8_t[size]), index(recordVarStarts[rt]), key(keyType(KEY_INVALID)) {
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

inline bool RecordChain::Record::operator>>(uint8_t &byte) noexcept {
    if(index == size) {
        return false;
    }
    byte = record[index++];
    return true;
}

void RecordChain::Record::write(ups_db_t *db, ups_txn_t *tr) {
    uint32_t flags = UPS_OVERWRITE;
    // TODO remove/
    check(ups_db_insert(db, tr, &upsKey, &upsRecord, flags));
}

void RecordChain::setRecordSize(size_t s) {
    Record::setSize(s);
}

RecordChain::RecordChain(RecordType rt, payloadType pt) : pType(pt) {
    Record::checkSize();
    content.push_back(Record(rt, pt));
    iter = content.begin();
}

void RecordChain::clone(const RecordChain &other) {
    state = other.state;
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

/*keyType RecordChain::getKey() {
    if(iter == content.cend()) {
        return 0;
    }
    return iter->upsKey;
}

uint8_t * const RecordChain::getRecord() {
    if(iter == content.cend()) {
        return nullptr;
    }
    return iter++->record;
}*/

void RecordChain::reset() {
    iter = content.begin();
    for(Record &rec : content) {
        rec.reset();
    }
}

void RecordChain::clear() {
    content.clear();
    iter = content.begin();
    state = RCState::EMPTY;
}

void RecordChain::setHeadField(uint32_t fieldStart, uint64_t value) {
    if(content.size() == 0) {
        throw logic_error("No record!");
    }
    content.begin()->setField(fieldStart, value);
}

uint64_t RecordChain::getHeadField(uint32_t fieldStart) {
    if(content.size() == 0) {
        throw logic_error("No record!");
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

void RecordChain::stripLeftover() {
    // terminate chain
    iter->setField(FP_NEXT, KEY_INVALID);
    for(iter++; iter != content.end();) {
        iter = content.erase(iter);
    }
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
        auto itNext = itThis;
        itNext++; // definitely exists
        if(itPrev != content.begin()) {
            itPrev--; // if it equals to itThis there is no previous
        }
        while(itThis != content.end()) {
            itThis->setKey(newKey = keyGen->nextKey());
            itThis->setField(FPC_HEAD, key);
            if(itNext != content.end()) {
                itNext->setField(FPC_PREV, newKey);
                itNext++;
            }
            if(itPrev != itNext) {
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
            throw logic_error("Trying to write no records.");
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
    if(iter == content.end()) {
        content.push_back(Record(RT_CONT, pType));
        iter = content.begin();
    }
    if(!(*iter << b)) {
        content.push_back(Record(RT_CONT, pType));
        iter++;
        *iter << b;
    }
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
    throw logic_error("No more data in RecordChain to read.");
}

Converter& Converter::write(const char * const p) {
    uint64_t len = strlen(p);
    *this << len;
    uint64_t i = 0;
    while(p[i]) {
        chain.write(uint8_t(p[i++]));
    }
    return *this;
}

Converter& Converter::operator<<(const string &s) {
    uint64_t len = s.size();
    *this << len;
    const char *p = s.c_str();
    while(*p) {
        chain.write(uint8_t(*p++));
    }
    return *this;
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
    while(len--) {
        uint8_t t;
        chain.read(t);
        *tp++ = char(t);
    }
    *tp = 0;
    return *this;
}

Converter& Converter::operator>>(string &s) {
    uint64_t len;
    *this >> len;
    s.reserve(len);
    while(len--) {
        uint8_t t;
        chain.read(t);
        // TODO check if efficient
        s += char(t);
    }
    return *this;
}
