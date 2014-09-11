#include "BigQ.h"

void * worker(void * q){
        BigQ *info = (BigQ *)q;
	// read data from in pipe sort them into runlen pages
        char* fileName=new char[50];
        int totalPage=1;
	//TPMMS phase1
        int runlen= info->runLength; 
	Page page;
	Record *record, *copyRecord;
	vector <Record *> recordVector;
	priority_queue<Record *, vector<Record*>, CompareRecords > recordQueue(info->om);
        vector<int> pageForRunNum;
	int pageNum=0;
        sprintf(fileName, "%s%d", "temp", getppid());

        File file;
	    file.Open(0,fileName);
        cout<<"open"<<endl;

	record=new Record;

	//remove records from pipe until it is empty
	while(info->inputPipe->Remove(record))
		{
		//copy record since it will be destroyed after call Append
			copyRecord=new Record;
			copyRecord->Copy(record);
			recordQueue.push(copyRecord);

		//page is full
			if(!page.Append(record))
			{
				pageNum++;

				if(pageNum==runlen)
				{
					Page page;
                                        pageForRunNum.push_back(totalPage);

					while(recordQueue.size()!=0)
                                               {
							if(!page.Append(recordQueue.top()))	// Page is full
							{
								//write page to disk file
								file.AddPage(&page,totalPage);
								totalPage++;
								page.EmptyItOut();
								page.Append(recordQueue.top()) ;
							}
							recordQueue.pop();
						}
						// Write last page
						file.AddPage(&page, totalPage);
						totalPage++;
						page.EmptyItOut();

				     recordQueue.empty();

				}
				page.EmptyItOut();
				page.Append(record);
			}

		}

     delete record;
     if(recordQueue.size()!=0)
     {
    	 Page page;
    	 pageForRunNum.push_back(totalPage);
    	 while(recordQueue.size()!=0)
    	 {
    		 if(!page.Append(recordQueue.top()))	// Page is full
    	 		{
    			 //write page to disk file
    	 		file.AddPage(&page,totalPage);
    	 		totalPage++;
    	 		page.EmptyItOut();
    	 		page.Append(recordQueue.top()) ;
    	 		}
    		 recordQueue.pop();
    	 }
    	 file.AddPage(&page,totalPage);
    	 totalPage++;
    	 page.EmptyItOut();
    	 recordQueue.empty();
     }

     pageForRunNum.push_back(totalPage);
     file.Close();

     //TPMMS phase2
     file.Open(1,fileName);

     int runNum=pageForRunNum.size()-1;
     int pages[runNum];

     Page pageList[runNum]; Record *rec;

     priority_queue<RunRecord *, vector<RunRecord*>, CompareRecords > runRecordQueue(info->om);
     RunRecord *runRecord;

     for(int i=0;i<runNum;i++){
    	 file.GetPage(&pageList[i],pageForRunNum[i]);
    	 pages[i]=pageForRunNum[i+1]-pageForRunNum[i]-1;
    	 pageForRunNum[i]++;

    	 rec=new Record;

    	 if(pageList[i].GetFirst(rec)==0) cout<<"No record reading from page in phase2";

    	 runRecord=new RunRecord(rec,info->om);

    	 runRecord->runNum=i;

    	 runRecordQueue.push(runRecord);

     }

     while(!runRecordQueue.empty())
      {

    	 runRecord = runRecordQueue.top();
  	     runRecordQueue.pop();

          int i = runRecord->runNum;

          info->outputPipe->Insert(runRecord->rec);
  	      rec = new Record;
          if (pageList[i].GetFirst(rec))    // get next rec from that page into imQ
          {
        	  runRecord = new RunRecord(rec,info->om);
              runRecord->runNum = i;
              runRecordQueue.push(runRecord);
          }
          else
          {
              if (pages[i] != 0)
              {

                  int getThisPage = pageForRunNum[i];
                  pageForRunNum[i]++;
                  file.GetPage(&pageList[i], getThisPage);
                  pages[i]--;
                  pageList[i].GetFirst(rec);
                  runRecord = new RunRecord(rec,info->om);
                  runRecord->runNum = i;
                  runRecordQueue.push(runRecord);
              }
          }
      }


    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
    info->outputPipe->ShutDown ();
    //file.Close();
   remove(fileName);
}
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
     this->runLength = runlen;
     this->om = &sortorder;
     inputPipe = &in;
     outputPipe = &out;
     pthread_create(&workerThread, NULL, worker, (void *)this);
}

BigQ::~BigQ () {
}
