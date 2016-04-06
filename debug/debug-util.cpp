/** @file
Main header file.

COPYRIGHT COMES HERE
*/

#include<iostream>
#include"exception.h"
#include"debug-util.h"

using namespace std;
using namespace udbgraph;

void checkException(exception &e, const char * const methodName, const char * const needle) {
	const char * const what = e.what();
	if(strstr(what, needle) == nullptr) {
		cout << methodName << ": " << e.what() << endl;
	}
}

payloadType ClassicStringPayload::_staticType;

void ClassicStringPayload::set(const char * const newContent) {
	int len = strlen(newContent);
	if(maxLen <= len) {
		delete[] content;
		maxLen = len + 1;
		content = new char[maxLen];
	}
	strcpy(content, newContent);
}

void ClassicStringPayload::fill(size_t len) {
	if(len > 9999) {
		len = 9999;
	}
	if(maxLen < len) {
		delete[] content;
		maxLen = len;
		content = new char[maxLen + 1];
	}
	int div10[] = {1000, 100, 10, 1};
	char remain[] = "xxxx abcd\n";
	int i;
	for(i = 0; i < len; i++) {
		int i10 = i % 10;
		if(i10 < 4) {
			content[i] = '0' + ((i - i10) / div10[i10]) % 10;
		}
		else {
			content[i] = remain[i10];
		}
	}
	content[i] = 0;
}

StringInFile::StringInFile(const char * const fn, int bufLen) : ifs(fn, ios::binary) {
	if(bufLen <= 0) {
		throw IllegalQuantityException("StringInFile: Invalid buffer size.");
	}
}

bool StringInFile::find(const char * const stuff, int len) {
	if(len == 0) {
		return true;
	}
	bool isStr = len < 0;
	if(len < 0) {
		len = strlen(stuff);
	}
	if(len > bufLen) {
		throw IllegalQuantityException("find: Invalid buffer size.");
	}
	char *buffer = new char[bufLen * 2 + 1];
	AutoDeleter<char> autoDel(buffer);
	ifs.clear();
	ifs.seekg(0, ios::beg);
	streamsize fill = 0;
	while(ifs) {
		if(fill > bufLen) {
			memcpy(buffer, buffer + bufLen, fill - bufLen);
			fill -= bufLen;
		}
		ifs.read(buffer + fill, bufLen);
		fill += ifs.gcount();
		if(knuthMorrisPrattSearch(stuff, len, buffer, fill)) {
			return true;
		}
	}	
	return false;
}
vector<size_t> StringInFile::knuthMorrisPrattTable(const char * const needle, size_t len) {
	vector<size_t> table(len + 1, -1);
	for(size_t i = 0; i < len; i++) {
		size_t position = table[i];

		while(position != -1 && needle[position] != needle[i])
			position = table[position];

		table[i + 1] = position + 1;
	}
	return table;
}

bool StringInFile::knuthMorrisPrattSearch(const char * const needle, const size_t lenNee, const char * const haystack, const size_t lenHay) {
	vector<size_t> table = knuthMorrisPrattTable(needle, lenNee);
	size_t haystackIndex = 0;
	size_t needleIndex = 0;
	while(haystackIndex < lenHay) {
		while(needleIndex != -1 && (needleIndex == lenNee || needle[needleIndex] != haystack[haystackIndex])) {
			needleIndex = table[needleIndex];
		}
		needleIndex++;
		haystackIndex++;
		if(needleIndex == lenNee) {
			return true;
		}
	}
	return false;
}

