/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#ifndef UDB_DEBUG_UTIL_H
#define UDB_DEBUG_UTIL_H

#include<vector>
#include<cstring>
#include<fstream>
#include<exception>
#include"udbgraph.h"

#if USE_NVWA == 1
#include"debug_new.h"
#endif

void checkException(std::exception &e, const char * const methodName, const char * const needle);

class ClassicStringPayload : public udbgraph::Payload {
protected:
	static udbgraph::payloadType _staticType;

	char * content = nullptr;

	int maxLen = 0;

public:
    /** Sets type for instance. */
    ClassicStringPayload(udbgraph::payloadType pt) : Payload(pt) {}
    
	~ClassicStringPayload() {
		delete[] content;
	}

	/** Static PayloadType ID for GEFactory. */
	static udbgraph::payloadType id() { return _staticType; }

	/** Sets the static PayloadType ID for GEFactory. */
	static void setID(udbgraph::payloadType pt) { _staticType = pt; }

	/** Used in GEFactory to create a shared_ptr holding a new class instance. */
	static std::shared_ptr<udbgraph::GraphElem> create(std::shared_ptr<udbgraph::Database> &db, udbgraph::payloadType pt) {
		return std::shared_ptr<udbgraph::GraphElem>(new udbgraph::Node(db, std::unique_ptr<udbgraph::Payload>(new ClassicStringPayload(pt))));
	}

	void set(const char * const newContent); 

	void fill(size_t len);

	virtual void serialize(udbgraph::Converter &conv) { conv << content; }

	virtual void deserialize(udbgraph::Converter &conv) { delete[] content; conv >> content; }
};

/** By default looks for equality, but could be extended to use any relation and even change it during its existence. */
class IntPayload : public udbgraph::Payload {
protected:
	static udbgraph::payloadType _staticType;

	int content = 0;

public:
    /** Sets type for instance. */
    IntPayload(udbgraph::payloadType pt) : Payload(pt) {}
    
	~IntPayload() {}

	/** Static PayloadType ID for GEFactory. */
	static udbgraph::payloadType id() { return _staticType; }

	/** Sets the static PayloadType ID for GEFactory. */
	static void setID(udbgraph::payloadType pt) { _staticType = pt; }

	/** Used in GEFactory to create a shared_ptr holding a new class instance. */
	static std::shared_ptr<udbgraph::GraphElem> create(std::shared_ptr<udbgraph::Database> &db, udbgraph::payloadType pt) {
		return std::shared_ptr<udbgraph::GraphElem>(new udbgraph::DirEdge(db, std::unique_ptr<udbgraph::Payload>(new IntPayload(pt))));
	}

	void set(int i) { content = i; }

	int get() { return content; }

	virtual void serialize(udbgraph::Converter &conv) { conv << content; }

	virtual void deserialize(udbgraph::Converter &conv) { conv >> content; }
};

class IntPayloadFilter : public udbgraph::Filter {
protected:
	/** The value to compare to. */
	int value;

public:
	/** Stores the payload type, default is PT_ANY. */
	IntPayloadFilter(int i) noexcept : value(i) {}

	virtual bool match(udbgraph::Payload &pl) noexcept;
};

class StringInFile {
	std::ifstream ifs;
	std::streamsize bufLen;

public:
	StringInFile(const char * const fn, int bufLen = 1 << 20);

	~StringInFile() {}

	/** Performs string search if len is not set, otherwise looks for stuff[0..len-1].
    @return true if found. It  */
	bool find(const char * const stuff, int len = -1);
    
protected:
	// algorithm taken from http://joelverhagen.com/blog/2011/11/three-string-matching-algorithms-in-c/
    std::vector<size_t> knuthMorrisPrattTable(const char * const needle, size_t len);

    bool knuthMorrisPrattSearch(const char * const needle, const size_t lenNee, const char * const haystack, const size_t lenHay);

};

#endif
