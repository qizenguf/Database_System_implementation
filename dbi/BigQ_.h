#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <list>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"

using namespace std;


class PageIter
{
private:
	Page page_;
	int pageIndex_;
	int lastPage_;
	Record head;
	bool headValid;
public:
	PageIter(int firstPage, int lastPage)
		:pageIndex_(firstPage-1),
		lastPage_(lastPage),
		headValid(false)
	{	
	}

	//Get the first item in this run without remove it
	Record* Peek(File& file_)
	{
		if(!headValid)
			if(GetNext(head,file_) == 0)
				return 0;
		headValid = true;
		return &head;
	}
	//Remove the first item in this run
	void MoveToPipe(Pipe &out)
	{
		if(!headValid)
		{
			printf("head is invalid!\n");
			exit(1);
		}
		out.Insert(&head);
		headValid = false;
	
	}

	int GetNext (Record &fetchMe,File& file_)
	{
		//If this is the last record in current page, then do sth. Otherwise, fetch it.
		if(!page_.GetFirst(&fetchMe))
		{
			//if this is the last page in the file
			if( pageIndex_ >= lastPage_ || pageIndex_ == file_.GetLength() - 2)
			{
				//we are done
				return 0;
			}
			//otherwise
			pageIndex_++;
			page_.EmptyItOut();
			//Get next page frome file. This operation will crash the program, if no more page is available
			file_.GetPage(&page_,pageIndex_);
			if(!page_.GetFirst(&fetchMe))
			{
				printf("page_.GetFirst failed twice. New page is empty?\n");
				exit(1);				
			}
		}
		return 1;	
	}


};

struct Myfunctor {
	ComparisonEngine* ceng;
	OrderMaker* sortorder;
	bool operator() (Record* i,Record* j) { return (ceng->Compare(i,j,sortorder)<0);}
} ;



class BigQ {


private:
	Pipe& in_;
	Pipe& out_;
	OrderMaker& order_;
	int runlen_;
	

public:
	static void * worker(void* arg)
	{
		BigQ* bq((BigQ*)arg);
		DBFileHeap dbf_;
		vector<Record*> vr_;
		list<Record> lr_;
		vector<int> runStartPage_;
		vector<PageIter*> runs_;
	// read data from in pipe sort them into runlen pages
		Record r;
		int i(0);
		int curSizeInBytes(0);
		bool done(false);

		//phase 1
		char tempfile [L_tmpnam];
		sprintf (tempfile,"temp.bin");

		dbf_.Create(tempfile,heap,0);

		runStartPage_.push_back(0);

		Myfunctor mf;
		ComparisonEngine ceng;
		mf.ceng = &ceng;
		mf.sortorder = &bq->order_;
		;
		while(!done)
		{
			lr_.push_back(r);
			i++;

			if(bq->in_.Remove(&lr_.back()) == 0)
			{
				lr_.resize(lr_.size() - 1);
				done = true;
			}
			else
			{
				vr_.push_back(&lr_.back());
			}

			// if current run is full
			if ((curSizeInBytes + lr_.back().Size() >= bq->runlen_ * PAGE_SIZE*9/10)||done)
			{
				//internal Sorting...

				sort(vr_.begin(),vr_.end(),mf);

				for(size_t j = 0 ; j < vr_.size() ;  j++)
				{
					dbf_.Add(*vr_[j]);
				}
				dbf_.PersistentPage();//write the last page into file
				//This will not write empty page, because there is at least one record in current page
				//Otherwise, persistentPage in dbf_.Add will not be triggered.
				
			
				runStartPage_.push_back(dbf_.GetFileInstance().GetLength()-1);
				//This is the beginning page index of next run
				//Yes, this page is not in the file yet. -2 is the current last page

				vr_.clear();//clear all records
				lr_.clear();
				curSizeInBytes = 0;
			
			}
			else
			{
				curSizeInBytes += lr_.back().Size();
			}
		}
		//printf("total = %d\n", i );

		//phase 2
		int runNum = runStartPage_.size() - 1;
		for(int i = 0 ; i < runNum ; i++)
		{
			runs_.push_back(new PageIter(runStartPage_[i],runStartPage_[i+1]-1));
		}

		int i0(0);
		while(1)
		{
			done = true;
			Record *min, *tmp;
			min = 0;
			for(int i = 0 ; i < runNum ; i++)
			{
				tmp = runs_[i]->Peek(dbf_.GetFileInstance());
				//if any run is not empty
				if(tmp)
					done = false; //go on
				else
					continue;
				if(min == 0)
				{
					min = tmp;
					i0 = i;
				}
				//Do comparison
				if(ceng.Compare(min,tmp,&bq->order_) > 0)
				{
					min = tmp;
					i0 = i;
				}
			
			}
			if(done) break;
			runs_[i0]->MoveToPipe(bq->out_);
		
		}

	

		for(int i = 0 ; i < runNum ; i++)
		{
			delete runs_[i];
		}

		dbf_.Close();
		remove(tempfile);


    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	bq->out_.ShutDown ();
	

		return 0;
	}

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
		:in_(in),
		out_(out),
		order_(sortorder),
		runlen_(runlen)
	{


	pthread_t thread1;
	pthread_create (&thread1, NULL, BigQ::worker, this);
	
	}
	~BigQ () {};
};

#endif
