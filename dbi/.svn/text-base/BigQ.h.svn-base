#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <list>
#include <algorithm>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"


class BigQ {
struct RunIterator{
	int endPageNum_;//include
	int curPageNum_;
	Page* curPage_;
	File* file_;
	Record curRec_;
	int nextCallTimes;//test information see how many next() called
	RunIterator():curPage_(0){}
	~RunIterator(){
		delete curPage_;
	}
	void init(int startPageNum, int endPageNum, File& file){
		curPageNum_ = startPageNum;
		endPageNum_ = endPageNum;
		file_ = &file;
		curPage_ = new Page();
		//GetPage() cann't fail, since if the error happens exit(1) will be called.
		//Therefore, we don't check if GetPage() fails here.
		file_->GetPage(curPage_, curPageNum_);
		nextCallTimes = 0;
	}
	int next(){
		while((*curPage_).GetFirst(&curRec_) == 0){
			if(curPageNum_ + 1 <= endPageNum_){
				curPageNum_++;
				file_->GetPage(curPage_,curPageNum_);
			}else{
				return 0;
			}
		}
		nextCallTimes++;
		return 1;
	}

};

struct CompareRecord {
	OrderMaker& order;
	ComparisonEngine cEng;
	CompareRecord(OrderMaker& ordermaker):
	order(ordermaker){}
	bool operator() (Record* i,Record* j) { 
		//sort: left < right ascending order
		return (cEng.Compare (i, j, &order)==-1);
	}
	bool operator() (RunIterator* i,RunIterator* j) {
		//priority query: left > right ascending order counterintuitive
		return (cEng.Compare (&i->curRec_,&j->curRec_, &order)==1);
	}
};



Pipe& in_;
Pipe& out_;
int runLen_;
DBFile dbFile_;
CompareRecord cr_;


public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) :
	  in_(in),out_(out), cr_(sortorder)
	  {
		pthread_t workerthread;
		//create dbfile 
		//which store the intermediate result
		//between load data from in pipe (sort intra run) 
		//& store to out pipe(sort inter runs)
		string fileName = generateFileName();
		dbFile_.Create (fileName.c_str(), heap, NULL);
		
		//store all the information about run the worker need to know
		runLen_ = runlen;
		in_ = in;
		out_ = out;
		//run load() which read data from in pipe sort them into runlen pages
		//construct priority queue over sorted runs and dump sorted data 
		//into the out pipe
		pthread_create (&workerthread, NULL, worker, (void *)this);
		//pthread_join (workerthread, NULL);
		//test, put file records to out pipe
		/*
		Record rec;
		runinfo.file.MoveFirst();
		while(runinfo.file.GetNext(rec)){
			out.Insert(&rec);
		}*/
		 
		
		
	}

	//pthread requires the function to be static member function or global function.
	//otherwise pthread do not know which instance's function should be called.
	static void* worker(void* arg){
		//pass the whole BigQ object to thread;
		BigQ& bq = *(BigQ*) arg;
		bq.tpmms();
		// finally shut down the out pipe
		bq.dbFile_.Close();
		//Before ShutDown() is called, 
		//Remove() should not return 0
		//even if there is no record in the out pipe
		bq.out_.ShutDown ();	
		return 0;
	}
	//used by constructor of BigQ
	//generator a random name for each BigQ.h
	//since later there will be multiple BigQ work simultaniously
	string generateFileName(){
		srand (time(NULL));
		int num1 =  rand();
		int num2 =  rand();
		char temp[128];
		sprintf(temp,"data/%d%d",num1,num2);
		return temp;
	}

	//Call by tpmms(Two Phase Multiway Merge Sort)
	//1. write every 1st phase sorted run to the file
	//2. count the end page number of each run (which will be used in the 2nd Phase)
	void writeToFile(vector<Record*>& runvec, vector<Record*>::iterator& it, int& leftspace, vector<int>& endPageNums){
		//sort the record in the vector
		sort (runvec.begin(), runvec.end(), cr_); 
		//store into dbfile
		//int count = 0;
		for (it=runvec.begin(); it!=runvec.end(); ++it){
			Record* rec = *it;
			dbFile_.Add(*rec);
			//count++;
			delete rec;
			*it = 0;
		}
		//if buffer page has left, then write it to file
		dbFile_.writeLastPageToFile();
		//cout << " write records: " << runvec.size()<< " vs file Add: "<< count << endl;
		//empty vector
		runvec.clear();
		File& file = dbFile_.getFile();
		//the end page number of current run is file.GetLength()-2
		//the start page number of next run is file.GetLength()- 1
		//Because if file.GetLength() = 2 which means
		//We have page Page0, Page1 in Chris file. Page0 is empty, We only have Page1.
		//If we want to access Page 1, file_->GetPage(curPage_, 0); We use index 0 instead of index 1. 
		//Therefore we should put the endPageNums to be file.GetLength()-2 = 2-2 = 0
		endPageNums.push_back(file.GetLength()-2);
		//cout << " The end of run page is : " << file.GetLength()-2 << endl;
		
	}
	//Two Phase Multiway Merge Sort
	//Phase 1
	//1.load data from in pipe, 
	//2.sort and store into File with runlen pages (max)
	//3.repeat 1, 2 until in pipe is exhausted
	//Phase 2
	//4.create a iterator for each File
	//5.external sort all the runs through priority queue
	//6.push all the record to out pipe
	//we don't consider the case runlen is too big to fit into memory in the 1st phase of TPMMS
	void tpmms(){
		//1st Phase of Two Phase Multiway Merge Sort
		vector<Record*> runvec;               
		vector<Record*>::iterator it;
		vector<int> endPageNums;
		Record* curRec = new Record();
		int leftspace = runLen_;
		while(in_.Remove(curRec)!=0){
			runvec.push_back(curRec);
			leftspace--;
			curRec = new Record();
			if(leftspace == 0){
				writeToFile(runvec,it,leftspace,endPageNums);
				leftspace = runLen_;
			}
		}
		//if the number of records in the "in pipe" is less than the runLength pages of records
		//which means the loop is terminated before leftspace == 0
		//then we should write the not fully filled runvect(leftspace) to write here.
		if(leftspace != runLen_){
			writeToFile(runvec,it, leftspace,endPageNums);
		}
		//2nd Phase of Two Phase Multiway Merge Sort
		//initialize the run iterators
		int runcount =  endPageNums.size();
		vector<RunIterator> runIteractors;
		runIteractors.resize(runcount);
		int preEnd = -1;
		for(int i = 0; i < runcount; i++){
			int start = preEnd + 1;
			int end = endPageNums[i];
			runIteractors[i].init(start, end,dbFile_.getFile());
			preEnd = end;
		}

		//push to priority queue
		priority_queue<RunIterator*, vector<RunIterator*>, CompareRecord> pq(cr_);
		//int elemCount = 0;
		for(int i = 0; i < runcount; i++){
			if(runIteractors[i].next()==1){
				pq.push(&runIteractors[i]);
				//elemCount++;
			}
		}
		while(!pq.empty()){
			RunIterator* ri = pq.top();
			pq.pop();
			out_.Insert(&ri->curRec_);
			if(ri->next() == 1){
				pq.push(ri);
			//	elemCount++;
			}
		}
		/*
		for(int i = 0; i < runcount; i++){
			//cout << i <<"th iterator: "<< runIteractors[i].nextCallTimes << " times"<<endl;
			//cout << i <<"th iterator: "<< runIteractors[i].endPageNum_ << " p"<<endl;
		}*/
		//cout << " pq push " << elemCount << " times"<<endl;
		
	}

	~BigQ (){
	
	}
	
};

#endif
