#ifndef BIGQ_H_
#define BIGQ_H_

#include <pthread.h>
#include "Pipe.h"
#include "File.h"
#include <vector>

// Function for worker thread.
void* DoWork(void *q);

class BigQ {
	
friend void* DoWork(void *q);
friend class SortRecords;
	
private:
	// Gets all the records from the input pipe, sorts them,
	// and puts them in the output pipe.
	pthread_t workerThread;
	// Dictates how records are to be sorted.
	OrderMaker *sortOrder;
	// Source for unsorted records.
	Pipe *inputPipe;
	// Sink for sorted records.
	Pipe *outputPipe;
	// Length of run in pages.
	int runLength;
	
public:
	// Initialize queue and start worker thread.
	BigQ (Pipe &argInputPipe, Pipe &argOutputPipe, OrderMaker
			&argSortOrder, int argRunLength);
	
	virtual ~BigQ();
};

// Wrapper class to attach run index to records.
class RunRecord {
	
private:
	// Index of run this record came from.
	std::vector<Record *>::size_type runIndex;
	// Wrapped record.
	Record *record;

public:
	RunRecord(Record *argRecord, std::vector<Record *>::size_type argRunIndex);
	virtual ~RunRecord();
	// Retrieve the run index the record originated from.
	std::vector<Record *>::size_type GetRunIndex();
	// Retrieve associated record.
	Record *GetRecord();
	
};

#endif /*BIGQ_H_*/
