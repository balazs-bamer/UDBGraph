/*
balazs@balazs:~/munka/dgsearch/udbgraph$ ./openaddressing 100 21 20 |less
balazs@balazs:~/munka/dgsearch/udbgraph$ ./openaddressing 100 11 20 |less
used < 80%
deleted sem lehet a maradek
*/

#include<unordered_set>
#include<cmath>
#include<random>
#include<iostream>
#include<cstdlib>

using namespace std;

bool isPrime(unsigned cand) {
	for(unsigned i = unsigned(sqrt(cand)); i > 1; i--) {
		if(cand % i == 0) {
			return false;
		}
	}
	cout << "    " << cand << '\n';
	return true;
}

void prime(unsigned cand) {
	//cout << "--- " << cand << '\n';
	for(unsigned i = 0;; i++) {
		if(isPrime(cand + i)) {
			return;
		}
		if(i == 0) {
			continue;
		}
		if(isPrime(cand - i)) {
			return;
		}
	}
}

void primes(void) {
	double sqrt2 = sqrt(2.0);
	double sqrt22 = sqrt(sqrt2);
	double now = 4 * sqrt22;
	int i;
	for(i = 2; i < 32; i++) {
		cout << "!-- " << (1u << i) << '\n';
		prime(unsigned(round(now)));
		prime(unsigned(round(now *= sqrt2)));
		now *= sqrt2;
	}
	cout << (1 << i) << '\n';
}

#define FREE 0
#define DELETED 1

class Hash {
protected:
	static uint32_t constexpr primes[] = {
		5, 11, 19, 29, 37, 53, 79, 109, 151, 211, 307, 431, 607, 863,
	    1217, 1723, 2437, 3449, 4871, 6883, 9743, 13781, 19483, 27551, 38971,
	    55109, 77933, 110221, 155863, 220447, 311743, 440863, 623477, 881743,
		1246963, 1763491, 2493949, 3526987, 4987901, 7053971, 9975803, 14107889,
		19951579, 28215799, 39903161, 56431601, 79806341, 112863197, 159612679,
		225726419, 319225331, 451452823, 638450719, 902905657, 1276901429,
		1805811263, 2553802819, 3611622607
	};
	static uint32_t constexpr primesLen = sizeof(primes) / sizeof(uint32_t);
	uint32_t whichSize = 0;
	uint32_t buckets;
	uint32_t used;
	uint32_t deleted;
	uint64_t *table;

public:
	Hash(uint32_t size = 5) {
		for(whichSize = 0; primes[whichSize] != size && whichSize < primesLen; whichSize++) {
			if(primes[whichSize] == size) {
				break;
			}
		}
		if(whichSize == primesLen) {
			// TODO exception
			cout << "nem talaltam: " << size << endl;
		}
		buckets = size;
		table = new uint64_t[buckets];
		fill();
		used = deleted = 0;
	}

	~Hash() {
		delete[] table;
	}

	void fill() {
		for(uint32_t i = 0; i < buckets; i++) {
			table[i] = FREE;
		}
	}

	void stat(const char * const cp, uint32_t i) {
		(cout << cp).width(4);
		(cout << buckets << "       u: ").width(4);
		(cout << used << "       d: ").width(4);
		(cout << deleted << "       t: ").width(4);
		cout << i << '\n';
	}

	void insert(uint64_t key) {
        if(used + deleted >= double(buckets) * 0.89) {
            if(whichSize == primesLen) {
				// TODO exception
			}
			cout << "insert: reallocating... " << buckets << '\n';
			uint32_t oldLen = buckets;
			uint64_t *oldStuff = table;
			buckets = primes[++whichSize];
			table = new uint64_t[buckets];
			fill();
			used = deleted = 0;
			// copy content into new table
			for(uint32_t i = 0; i < oldLen; i++) {
				uint64_t key = oldStuff[i];
				if(key != FREE && key != DELETED) {
					insert(key);
				}
			}
			delete[] oldStuff;
			cout << "insert: done. " << buckets << '\n';
        }
		uint32_t i;
        for(i = 0;; i++) {
            uint32_t ind = hash(key, i);
			if(table[ind] == FREE || table[ind] == DELETED) {
				if(table[ind] == DELETED) {
					deleted--;
				}
				table[ind] = key;
				used++;
stat("insert:   ", i);
				break;
			}
        }
    }

	bool find(uint64_t key) {
		uint32_t i;
        for(i = 0; i < buckets; i++) {
            uint32_t ind = hash(key, i);
			if(table[ind] == key) {
stat("  find: + ", i);
				return true;
			}
			if(table[ind] == FREE) {
				break;
			}
        }
stat("  find: - ", i);
		return false;
	}

	void remove(uint64_t key) {
		if(deleted >= used) {
			cout << "remove: reallocating... " << buckets << '\n';
			uint32_t oldLen = buckets;
			uint64_t *oldStuff = table;
			for(whichSize = 0; used >= primes[whichSize]; whichSize++);
			if(whichSize < primesLen && used / double(primes[whichSize]) > 0.72) {
				whichSize++;
			}
			buckets = primes[whichSize];
			if(buckets > oldLen) {
				// TODO exception
				cout << "remove realloc: new size larger than old\n";
			}
			table = new uint64_t[buckets];
			fill();
			used = deleted = 0;
			// copy content into new table
			for(uint32_t i = 0; i < oldLen; i++) {
				uint64_t k = oldStuff[i];
				if(k != FREE && k != DELETED && k != key) {
					insert(k);
				}
			}
			delete[] oldStuff;
			cout << "remove: done. " << buckets << '\n';
			
		}
		else {
			size_t i;
			for(i = 0; i < buckets; i++) {
				size_t ind = hash(key, i);
				if(table[ind] == key) {
					table[ind] = DELETED;
					used--;
					deleted++;
stat("remove: + ", i);
					return;
				}
				if(table[ind] == FREE) {
					break;
				}
			}
stat("remove: - ", i);
		}
	}

protected:
	uint32_t hash(uint64_t key, uint32_t disp) {
		return uint32_t((key % buckets + disp * (1 + key % (buckets - 1))) % buckets);
	}
};

uint32_t constexpr Hash::primes[];
uint32_t constexpr Hash::primesLen;

int main(int argc, char **argv) {
//	primes();
	std::mt19937_64 generator;
	std::uniform_int_distribution<int> distribution(2, numeric_limits<uint64_t>::max());
	size_t l = atol(argv[1]);
	Hash hash(5);
	unordered_set<uint64_t> keys;
	size_t ir = atol(argv[2]);
	size_t j,i;
	for(j = 0; j < l; j++) {
		size_t n = distribution(generator) % ir;
		switch(distribution(generator) % 4) {
		case 0:
			cout << "INSERT: " << n << endl;
			for(i = 0; i < n; i++) {
				uint64_t x = distribution(generator);
				hash.insert(x);
				keys.insert(x);
			}
			break;
		case 1:
			cout << "REMOVE: " << n << endl;
			{
				auto it = keys.begin();
				for(i = 0; i < n && it != keys.end(); i++) {
					uint64_t key = *it;
					it = keys.erase(it);
					hash.remove(key);
				}
			}
			break;
		case 2:
			cout << "- FIND: " << n << endl;
			for(size_t j = 0; j < n; j++) {
				uint64_t x = distribution(generator);
				hash.find(x);
			} 
			break;
		case 3:
			cout << "+ FIND: " << n << endl;
			{
				auto it = keys.begin();
				for(i = 0; i < n && it != keys.end(); i++) {
					uint64_t key = *it++;
					hash.find(key);
				}
			}
			break;
		}
	}
	cout << "REMOVE: " << keys.size() << endl;
	auto it = keys.begin();
	for(i = 0; it != keys.end(); i++) {
		uint64_t key = *it;
		it = keys.erase(it);
		hash.remove(key);
	}
	return 0;
}
