#include "BigQ.h"
#include <algorithm>
#include <vector>
#include <queue>
#include <iostream>
#include <unistd.h>
ComparisonEngine comparer;

class SortRecords {
private:
	OrderMaker* orderMaker;
	
public:
	
	SortRecords(OrderMaker* sortOrder):orderMaker(sortOrder) {}
	
	int operator() (const Record *r1, const Record *r2) {
		int result = comparer.Compare((Record *)(r1), (Record *)(r2), orderMaker);
		if (result == -1) {
			return 1;
		} else {
			return 0;
		}
	}
	
	int operator() (RunRecord *r1, RunRecord *r2) {
		int result = comparer.Compare(r1->GetRecord(), r2->GetRecord(), orderMaker);
		if (result == -1) {
			return 0;
		} else {
			return 1;
		}
	}
};


void* DoWork(void *q) {
	BigQ *queue = (BigQ *)q;
	vector <Record *> records;
	vector<int> pagesForRunStarts;
	vector<int> pagesForRunEnds;
	int pageIndex = 0;
	Page currentPage;
	File tempFile;
	char tempFileName[20];
	sprintf(tempFileName, "tempFile%d", (int)queue->workerThread);
	tempFile.Open(0, tempFileName);
	Record nextRecord;
	
	// remove records from the pipe until it is empty
	while (queue->inputPipe->Remove(&nextRecord)) {
		
		
		int pageCount = 0;
		int result = 0;
		// create a run
		do {
			
			// place records in the vector for sorting,
			// as well as adding them to a page
			
			result = currentPage.Append(&nextRecord);
			if (!result) {
				// write all the records in the page to the vector
				Record aRecord;
				while(currentPage.GetFirst(&aRecord)) {
					Record* tempRecord = new Record();
					tempRecord->Consume(&aRecord);
					records.push_back(tempRecord);		
				}
				currentPage.EmptyItOut();
				pageCount++;
				currentPage.Append(&nextRecord);
			}
		} while (pageCount < queue->runLength && queue->inputPipe->Remove(&nextRecord));
		
		
		// above loop doesn't account for records remaining in
		// non full page.
		// write all the records in the page to the vector
		Record nextRecord;
		while(currentPage.GetFirst(&nextRecord)) {
			Record* tempRecord = new Record();
			tempRecord->Consume(&nextRecord);
			records.push_back(tempRecord);		
		}
		pageCount++;
		currentPage.EmptyItOut();

		// sort that run
		sort(records.begin(), records.end(), SortRecords(queue->sortOrder));
		

		// Write to file.
		pagesForRunStarts.push_back(pageIndex); // log the start of the run
		vector<Record *>::const_iterator iter = records.begin();
		while(iter != records.end()){
			// add this record to the page
			int result = currentPage.Append((*iter));
			
			// it might not have fit 
			if(!result) {
				// so ... write full page out
				tempFile.AddPage(&currentPage, pageIndex);
				// empty it out
				currentPage.EmptyItOut();
				// give next page an id
				pageIndex++;
				// add the record to the fresh page
				currentPage.Append((*iter));
			}
			delete (*iter);
			iter++;	
		}
		// always add the page once done.
		tempFile.AddPage(&currentPage, pageIndex);
		pagesForRunEnds.push_back(pageIndex);
		currentPage.EmptyItOut();
		pageIndex++;
		records.clear();
		// start another run
	}
	
	// This is not thread-safe, fix it.
	priority_queue<RunRecord*, vector<RunRecord*>, SortRecords> inQ(SortRecords(queue->sortOrder));
	vector<int>::size_type numRuns = pagesForRunStarts.size();
	int *counts = new int[numRuns];
	int *done = new int[numRuns];
	
	// load up a page from every run
	for (vector<int>::size_type i = 0; i < numRuns; i++) {
		tempFile.GetPage(&currentPage, pagesForRunStarts[i]);
		if (pagesForRunEnds[i] == pagesForRunStarts[i]) {
			done[i] = 1;
		} else {
			done[i] = 0;
		}
		pagesForRunStarts[i]++;
		counts[i] = 0;
		while (currentPage.GetFirst(&nextRecord)) {
			Record* tempRecord = new Record();
			tempRecord->Consume(&nextRecord);
			inQ.push(new RunRecord(tempRecord, i));
			counts[i]++;
		}
	}
	
	while (!inQ.empty()) {
		RunRecord *nextRunRecord = inQ.top();
		inQ.pop();
		Record* tempRecord = nextRunRecord->GetRecord();
		queue->outputPipe->Insert(tempRecord);
		vector<Record *>::size_type i = nextRunRecord->GetRunIndex();
		counts[i]--;
		if (!counts[i]) {
			if (!done[i]) {
				tempFile.GetPage(&currentPage, pagesForRunStarts[i]);
				if (pagesForRunEnds[i] == pagesForRunStarts[i]) {
					done[i] = 1;
				} else {
					done[i] = 0;
				}
				pagesForRunStarts[i]++;
				counts[i] = 0;
				while (currentPage.GetFirst(&nextRecord)) {
					Record* tempRecord = new Record();
					tempRecord->Consume(&nextRecord);
					inQ.push(new RunRecord(tempRecord, i));
					counts[i]++;
				}
			}
		}
		delete tempRecord;
		delete nextRunRecord;
	}
	
	remove(tempFileName);
	
	delete[] counts;
	delete[] done;
	
	queue->outputPipe->ShutDown();
}

BigQ::BigQ (Pipe &argInputPipe, Pipe &argOutputPipe, OrderMaker
			&argSortOrder, int argRunLength) {
	inputPipe = &argInputPipe;
	outputPipe = &argOutputPipe;
	sortOrder = &argSortOrder;
	runLength = argRunLength;
	
	static int counter = 0;
	pthread_create(&workerThread, NULL, DoWork, (void *)this);
}

BigQ::~BigQ() {
}

RunRecord::RunRecord(Record *argRecord, vector<Record *>::size_type argRunIndex) {
	record = argRecord;
	runIndex = argRunIndex;
}

RunRecord::~RunRecord() {	
}

Record *RunRecord::GetRecord() {
	return record;
}

vector<Record *>::size_type RunRecord::GetRunIndex() {
	return runIndex;
}
