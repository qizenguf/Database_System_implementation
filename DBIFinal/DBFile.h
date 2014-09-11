#ifndef DBFILE_H
#define DBFILE_H
#define BUFFER_SIZE 100

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;
typedef enum {read_data, write_data} operationMode;
struct SortInfo{OrderMaker *om; int runLength;};
class GenericDBFile
{
	friend class DBFile;
protected:
	Page page;
	File file;
	char *fileName;
	int recordCounter;
	int pageCounter;
	operationMode mode;
public:
	virtual void MoveFirst() = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &applyme, Record  &literal) = 0;
	virtual int Close () = 0;
	virtual void Load (Schema &mySchema, char *loadpath) = 0;
};


class DBHeap: public GenericDBFile
{
friend class DBFile;
protected:
	DBHeap();
	~DBHeap();

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	int Close ();
	void Load (Schema &mySchema, char *loadpath);
};

class DBSorted: public GenericDBFile
{
friend class DBFile;
private:
	SortInfo *startup;
	// Queue used as differential file.
	BigQ *qFile;
	// Pipe to read records into the queue.
	Pipe *inPipe;
	// Pipe to write records out to queue.
	Pipe *outPipe;
	// Page to keep track of result of binary search after it is performed.
	OrderMaker *searchOrderMaker;
	// Order maker for literal.
	OrderMaker *literalOrderMaker;
	bool binarysearch;
	// Used to merge differential queue into file.
	void Merge();
public:
	DBSorted(void* startup);
	~DBSorted();

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	int Close ();
	void Load (Schema &mySchema, char *loadpath);
};

class DBFile{
private:
	GenericDBFile  *myInternalVar;
public:
	DBFile (); 
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};


#endif
