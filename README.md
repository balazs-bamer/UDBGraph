# UDBGraph - a graph database based on UpscaleDB key-value store

**Important!** This is a work-in-progress software with many important features missing. However, the library design presented here and in code comments is stable and detailed enough to let the reader understand the concept and imagine the missing parts. Fully or almost completed parts are:
* Database handling core
* Transaction management
* Payload serializing and deserializing
* Record chain loading and saving together with node's edge lists
* UpscaleDB record management
* Node and edge insert and update

The file src/todo.txt contains planned modifications.


## Motivation

I need a simple, embedded graph database for a future project with C++ API. My design aims were:
* Direct graph traversal using a simple API without network and query language overhead - embedded database library.
* Robust, fail-proof API with custom exception system providing backtrace info (available on some systems for development).
* User-defined class hierarchy of node and edge payloads with arbitrary size and data types and user methods to manipulate them.
* Architecture-independent implementation, should be able to compile on any system with a C++11 compiler.
* Unlimited parallel ACID transactions with serializable isolation.
* Use an existing, efficient storage medium instead of own implementation. For this I choosed UpscaleDB, an efficient crossplatform key-value store with C interface and dual-licensing option with moderate price for commercial application.


## Language

I decided to use C++, because it is a popular language in science and algorithm-oriented tasks, as my future application relying on this library will be. I use C++11 with standard C++ library, which provides most modern language features out of the box and it is already popular and accepted among C++ developers.


## Compiling

Currently I use clang 3.4.2 and cmake 3.4.1 for development with flags
* *CMAKE_CXX_FLAGS* = -std=c++11
* *CMAKE_CXX_FLAGS_DEBUG* = -g3 -gdwarf-3 -O0

Later, when the library is mature enough, I will test it with g++ and Microsoft compilers.

To utilize nested cmake operation, please first start it and make in src (to compile the library) then in the main directory (compiles the debug programs and dumpdb). Later on, make from the main directory makes everything.


## Design

### Graph model and traversal

The database supports nodes, directed and undirected edges (graph elements), each of unlimited count between any two nodes. However, loop edges starting and ending on the same node are not possible. Both nodes and edges may have any set of attributes, or none at all. This "empty functionality" is already implemented in the library.

The graph operations are designed to be simple and fast. They (will) include
* Get all neighbours of a node (using a condition).
* Get the first (or only) neighbour of a node (using a condition).
* Get all edges (incoming, outgoing or undirected) of a node (using a condition).
* Get the first (or only) (incoming, outgoing or undirected) edge of a node (using a condition).
* Get an edge between two nodes (using a condition) or test if it exists.
* Create an independent node. (**ready**)
* Create an edge between two existing nodes.
* Update a node (**ready**) or edge. 
* Remove a node or edge together all adjacent nodes and edges.

The graph has a special node called root, which is in fact the root of the whole graph and cannot be removed. This is the starting point of all traversal, and the application logic must guarantee that all nodes can be reached by starting the traversal from the root. To help this the library will throw an exception signing application logic error if a node would remain orphaned after a removal operation.


### Transactions and concurrency

I did not want to include complex traversal algorithms and indexed search in the first version. These would possibly last long, which would demand a complex synchronisation algorithm instead a simple mutex guarding all database accesses to let other threads access the database during a long query. My targeted application does not require it, and UpscaleDB (version 2.1.12) is also designed using a big mutex guarding every operation, which rises a limit in real concurrency anyway.

Practically all operations happen in a transaction. If none provided, one will be created just before the action and commited right after it.

I have aimed serializable transaction isolation. Any number of transactions may run in parallel, each is either read-only or read-write. A graph elem may be present in any number of read-only tranactions, but only in one read-write one (wothout being involved in any read-only transaction). This model suits applications with many reads but a few writes, or writes occuring on different graph parts. Each UDBGraph tranaction is backed by exactly one UpscaleDB transaction.


### Architecture

Currently the library uses some static fields and methods, which limit the use of databases of different types and record sizes in the same application. Minor changes would eliminate this restrictions. Later during development I will think about it, but currently this design is enough for me.

A main point in architecture design was to separate database management from the attributes stored in edges and nodes. I call the collection of attributes for a particular type of graph element *payload*, which may be an almost arbitrary user-defined object.

As payload types are implemented with classes, each node and edge has a determined type, which affects its attributes, but not the behaviour during graph management. It is independent of the payload.

Traversal conditions are not implemented yet. They are planned to be implemented using an other set of classes, whose common ancestor will be able to be passed as a condition for a traversal step.

This design lets the application easily map its own object hierarchy to the database graph types. Moreover, the payload classes may have an unbounded set of methods, which help achieving easy to understand and efficient application code.

Other advantage is hiding the graph management internals and the UpscaleDB API from the application.


### Database and application versions

The root node of the database stores the application name and version number (major/minor). The name and major version number of an application must match that of the database to be able to open it. This is important, since the database does not maintain payload type information, and reading the wrong binary data would lead to unpredictable results.


### User credentials and permission management

Current version of UDBGraph has no permission management, but it is prepared to use access control lists (ACL) after a future development. Mechanism for user credential handling will be introduced at the same time. The API does not intend to use a connection, so user credentials will be passed in an object at transaction creation. When implemented, the transaction's user credentials will be checked against each accessed element's ACL. The ACL information will be stored in special records unaccessible for the application.

Each graph element has a reference to an ACL. Currently it has a value of *ACL_FREE* meaning no access control restriction on that item.

The UDBGraph versions with and without ACL will be compatible with the following restrictions:
* A database without ACL can be accessed by both software.
* A database with ACL will expose only the free fields to a software without ACL
* A database with ACL will be accessible according to the ACL definied inside when accessed by a software with ACL.


### Graph elements, UpscaleDB records and payloads

UDBGraph elements can have arbitrary size, which may be unknown even during application design. UpscaleDB offers fixed size and variable size records. Smaller fixed size records are stored in the B-tree nodes to increase performance, while variable size records are stored outside the B-tree.

I've decided to use fixed size records, because in many applications most graph elements store only a small amount of data, so these would then fit in the B-tree nodes. Record size is determined during database creation. It must be large enough to contain the largest fixed field set (currently root) together with three smallest-size hash tables and the payload beginning. It may not exceed 1M and be a multiply of the key size. Larger payloads will require more records, which are organized into chain connecting to the head record holding the graph element core. These chains are double linked and all records (except for the head) store the head key in addition. This will help database recovery if it will be implemented.

Records are identified by their key in UpscaleDB, so the key of the head record identify the graph element itself.

Edges have two keys for the (start and end) nodes. Nodes, however, maintain four arrays of keys: incoming directed edges, outgoing directed edges, undirected edges and free key space. This area is used for storing new edge keys without the need of inserting new records every time when adding a new edge. These edge arrays may be continued in subsequent records, if needed.

Each record has a set of fixed field on its beginning. For node records, the edge key arrays follow. After it begins the payload. The library supports the following native data types in the payload:
* bool
* signed and unsigned 8, 16, 32 and 64-bit integers
* float and double
* 0-terminated character string
* std::string object

Everything inside the records is stored as little-endian for database portability. However, the UpscaleDB tree information is stored in host-endian, so an additional UpscaleDB utility is needed to transfer the database between machines with different endian.

The *Converter* class can serialize and deserialize native types only bytewise on big-endian architectures. On little-endian architectures it uses bytewise operation by default, but calling *Converter::enableUnalign()* enables unaligned pointer cast at the actual record index to speedup operation. Note, this generates bus error and program termination on architectures not supporting it.

Record types used in UDBGraph are:

Type		 |Description
:------------|:----------------------
root		 |The singleton root node.
ACL			 |Access control list information, not implemented yet.
Node		 |Graph node with any payload.
Directed edge|A directed edge with any payload.
Undir. edge  |An undirected edge with any payload.
Continuation |Continuation in the record chain.

UpscaleDB records have 64-bit unsigned keys, incremented as new records are inserted. The initial value is determined during database open, so counting continues where it was last time suspended. The extreme bit width assures no overlap in general use.


#### Edge lists in nodes

Nodes store three sets of connecting edge keys:
* Incoming edge
* Outgoing edge
* Undirected edge

Managing these sets must be efficient for just a few edges as well as for thousands of edges. I've implemented a growable open-addressing hash table for each set. The hash table algorithm is designed such that
* It uses space more efficiently than linked-list hash tables.
* It uses double hashing to distribute the keys as evenly as possible - this implies the hash table size to be a prime. These primes are pre-calculated and placed along the geometric series a[n] = 2^(1/4 + n/2).
* If no reallocation occours, adding or removing a key involves at most two UpscaleDB records. This is very important since every edge insertion or deletion involves two node modifications.
* Deleted keys are marked as deleted to speed up average operation.
* Reallocation occurs if
  * the sum of used + deleted entries exceed a limit.
  * if the number of deleted entries exceed the used.
* Reallocation happens by inserting or removing whole records from the hash table such that the begin and end offset inside a record remains the same. This method saves the other hash tables and the payload from expensive relocation. This is even true for the minimal hash table with currently 5 buckets.
* Initially each hash table has 5 buckets only. This lets nodes store a few edges and a short payload in only one record.

The hash algorithm skeleton is implemented in _openaddressing.cpp_.

### Operations and inner status management

All user actions ar designed such that first the library makes sure that every involved graph element is accessible (not locked by other transactions and the user has appropriate permissions). If this check succeeds, only then starts the phase changing inner status of the library and the graph element instances. In this phase only UpscaleDB exceptions may signal fatal errors.

All graph element instances maintain two record chains. One of them holds the original record contents before the transaction, the other the result of modification(s) during the transactions. For better performance, it is possible to read and write these partially, so leaving edge arrays and / or payload off when only the beginning is interesting.

### Classes

The UDBGraph library now consists of these classes:

Class name			|Defined in     |Description
:-------------------|:--------------|:----------
CounterMap    		|util.h			|Associative container template supporting incrementing and decrementing values by key and querying count by key.
KeyGenerator  		|util.h    		|Class template providing a unique key generator by incrementing a counter.
LockGuard2    		|util.h			|Class for simultaneously locking two mutexes and have the destructor for RAII cleanup in case of an exception.
BaseException 		|exception.h	|Common base class for all custom exceptions. It stores the description in a character array, and if enabled, appends a demangled backtrace to it. Backtrace id enabled if DEBUG is defined and we use a glibc and glibcxx.
UpsException 		|exception.h	|Exception class for UpscaleDB BaseException handling, uses its built-in messages.
DebugException 		|exception.h	|Intended for reporting internal errors of the UDBGraph library. Should not occur if mature enough.
GraphException 		|exception.h	|Exception class for graph management related errors, base class for more specific ones.
PermissionException	|exception.h	|Exception for reporting access permission related errors.
TransactionException|exception.h	|Exception for reporting transaction-related errors.
LockedException 	|exception.h	|Exception for reporting locking problems, for example an other transaction locks a graph elem we want to perform an action on.
ExistenceException 	|exception.h	|Exception for reporting problems regarding accessing deleted or not registered elements.
CorruptionException	|exception.h	|Exception for reporting database integrity problems.
DatabaseException	|exception.h	|Exception for reporting database management related problems.
IllegalMethodException|exception.h	|Exception for reporting illegal method use, for example setting end on a node.
IllegalArgumentException|exception.h|Exception for reporting illegal arguments.
CheckUpsCall		|serializer.h	|Common base class for classes performing UpscaleDB operations.
FixedFieldIO		|serializer.h	|Base class to perform fixed field input/output. Used also in Dump.
RecordChain			|serializer.h	|Class to contain serialized native types, 0 delimited char arrays and strings. in a chain of UpscaleDB records. The class Converter and its caller code is responsible of appropriate assembly and extraction as no type information is stored. This class is not thread-safe.
Converter			|serializer.h	|A wrapper class for RecordChain to hide unnecessary parts from Payload providing (de)serialization of some native types.
Database			|udbgraph.h		|Common class for UpscaleDB environments and databases holding all transaction and GraphElem-related status information.
Transaction			|udbgraph.h		|A class encapsulating a transaction handle. This class (being POD) can be freely copied, because the actual UpscalleDB transaction structure resides in the corresponding Database. 
Payload				|udbgraph.h		|An abstract class representing the payload in GraphElem subclasses. Neither this class, nor its subclasses are intended for further dubclassing by the application.
GraphElem			|udbgraph.h		|A common abstract base class for nodes and edges.
AbstractNode		|udbgraph.h		|A common ancestor for Node and Root.
Node				|udbgraph.h		|A general node class represents actual node types in the graph.
Root				|udbgraph.h		|Represents the root node. The root node is a starting point of all graphs in the database and also holds the application name and version information. This class cannot be used in the GEFactory.
Edge				|udbgraph.h		|Abstract edge class, common ancestor of DirEdge and UndirEdge.
DirEdge				|udbgraph.h		|A general directed edge class represents the actual directed edge types in the graph.
UndirEdge			|udbgraph.h		|A general directed edge class represents the actual undirected edge types in the graph.
GEFactory			|udbgraph.h		|A class for producing GraphElem subclasses. It is able to create Node, DirEdge and UndirEdge instances using EmptyNode, EmptyDirEdge and EmptyUndieEdge payloads out-of-the box. User subclasses of Payload must be registered in user modules to be able to created.
EmptyNode			|udbgraph.h		|Empty payload for creating empty nodes, serves as an example.
EmptyDirEdge		|udbgraph.h		|Empty payload for creating empty directed edges, serves as an example.
EmptyUndirEdge		|udbgraph.h		|Empty payload for creating empty nodes, serves as an example.


### Object instantiation

The UDBGraph library is designed with RAII in mind to eliminate pointer ownership and resource leak problems. Instantiation of the user objects:
* Database: *Database::newinstance* static method returns a *shared_ptr* to it. Other objects store only *weak_ptr* to it to prevent circular reference. The instance has to be either opened, or created before actual use.
* Graph elems: *GEFactory::create* static method returns a shared pointer, for user types after registration. Refer to udbgraph.h for more info.
* Transactions: *database->beginTrans* create them.
* Payloads: user subclasses may have their own constructors. The one taking only *payloadType* must be present for instantiation as part of the graph elems. Graph elems' *pl* method return a reference to it, so if the subclass implements *operator=*, it can be overwritten.


### Debugging

When glibc anc glibcxx are used, the class BaseException provides stack trace information. The library can be compiled to use [NVWA](http://sourceforge.net/projects/nvwa/) project, which contains a brilliant memory leak detector working by overloading C++ *new* and *delete* operators. It can be enabled by the *USE_NVWA* option in CMake. By default it finds 4 leaked objects, but these are not originated in this library, since no line number information follows.

Most user operations may throw exceptions for various reasons:
* Incorrect database handling, like open an already open one.
* An other transaction locks the graph element.
* Use of stale Transaction object.
* Insufficient permission for an action.
* Write an element in a read-only transaction.
* Write a previously deleted element.
* etc

Other errors possible are:
* Errors signed by UpscaleDB.
* DebugException signing programmer error in UDBGraph. When it is mature enough, no such exceptions should occur any more.

I'm writing unit tests in the debug subdirectory to make sure each part works correctly. These contain test functions for each major part or milestone in the library. Moreover, the dumpdb utility is provided to dump the database contents (record chains and fixed fields) on standard output.
