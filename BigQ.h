#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <stack>
#include <algorithm>
#include <queue>
#include <string.h>

#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"


using namespace std;

class BigQ {

public:
        int runLength;
        OrderMaker *om;
        pthread_t workerThread;
        Pipe *inputPipe;
	// Sink for sorted records.
	Pipe *outputPipe;
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

//wrapper class to attach run index to record
class RunRecord {

private:
public:
	friend class CompareRecords;
	Record *rec;
	OrderMaker *comp;
	int runNum;

	RunRecord(Record *r, OrderMaker *o):rec(r),comp(o){}

	~RunRecord();

	Record record_;

};

class CompareRecords{

private:
	OrderMaker* orderMaker;

public:
	CompareRecords(OrderMaker* sortorder)
    {
		orderMaker=sortorder;
    }

	bool operator()(const Record *left, const Record* right)
	{
		ComparisonEngine comparisonEngine;
		if(comparisonEngine.Compare((Record *)left,(Record *)right,orderMaker)<0)
		                                return false;
		                        else
		                                return true;
		                }

	bool operator() (RunRecord * left, RunRecord * right)
	{
		ComparisonEngine comparisonEngine;
				if(comparisonEngine.Compare(&(left->record_),&(right->record_),orderMaker)<0)
				                                return false;
				                        else
				                                return true;

	}
};


#endif
