#include "RelOp.h"
#include <iostream>
#include <string>
#include<cstring>
#include<cstdlib>


void *SF_work(void* SF){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)SF;
	//workInfo->inFile->MoveFirst();
	CNF* cnf =workInfo->apply_me;
	DBFile *dFile = workInfo->inFile;
	Record * literal =workInfo->literal;
	Record *temp = new Record();
	Pipe *out=workInfo->outPipe;
	ComparisonEngine myCE;
	int result = 1;
//	cout<<dFile->GetNext(temp)<<endl;
	while(dFile->GetNext(*temp)){
	/*	int pointer = ((int *) temp->bits)[4];
		double *myDouble = (double *) &(temp->bits[pointer]);
		cout << *myDouble<<"  ';"<<endl;*/
		if (myCE.Compare(temp,literal,cnf)){
			//cout<<"insert"<<endl;
			//Record *inRecord = new Record();
			//inRecord->Copy(temp);
			//out->Insert(inRecord);
			out->Insert(temp);
			//cout<<"insert"<<endl;
		}
	}
	//workInfo->outPipe->Insert(temp);
	out->ShutDown();
	pthread_exit(NULL);
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	 runInfo *myRunInfo = new runInfo();
	 myRunInfo->inFile=&inFile;
	 myRunInfo->outPipe=&outPipe;
	 myRunInfo->apply_me=&selOp;
	 myRunInfo->literal=&literal;

	pthread_create(&thread,NULL,SF_work,(void *)myRunInfo);
	return;
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
	//pthread_exit(NULL);
	//return;
}

void SelectFile::Use_n_Pages (int runlen) {
     this->runlen=runlen;
}
void *SP_work(void* SP){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)SP;
	CNF* cnf =workInfo->apply_me;
	Record * literal =workInfo->literal;
	Record *temp = new Record();
	Pipe *out=workInfo->outPipe;
	Pipe *in=workInfo->inPipe;
	ComparisonEngine myCE;
	int result = 1;
//	cout<<dFile->GetNext(temp)<<endl;
	while(in->Remove(temp)){
		/*int pointer = ((int *) temp->bits)[4];
		double *myDouble = (double *) &(temp->bits[pointer]);
		cout << *myDouble<<"  ';"<<endl;*/
		//print record for debugging
		if (myCE.Compare(temp,literal,cnf)){
			//cout<<"insert"<<endl;
			//Record *inRecord = new Record();
			//inRecord->Copy(temp);
			//out->Insert(inRecord);
			out->Insert(temp);
			//cout<<"insert"<<endl;
		}
	}
	//workInfo->outPipe->Insert(temp);
	out->ShutDown();
	pthread_exit(NULL);
}
void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	runInfo *myRunInfo = new runInfo();
	myRunInfo->inPipe=&inPipe;
	myRunInfo->outPipe=&outPipe;
	myRunInfo->apply_me=&selOp;
	myRunInfo->literal=&literal;
	pthread_create(&thread, NULL,SP_work,(void *)myRunInfo);
	return;
}
void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
	//pthread_exit(NULL);
	return;
}

void SelectPipe::Use_n_Pages (int runlen) {
     this->runlen=runlen;
}

void sumRecord(Record &temp, double finalRes){
	if (temp.bits != NULL)
	      delete [] temp.bits;
		temp.bits = NULL;
		int currentPosInRec = sizeof (int) * (1 + 1);
		//char *space = new (std::nothrow) char[PAGE_SIZE];
		char *recSpace = new (std::nothrow) char[PAGE_SIZE];
		((int *) recSpace)[0] = 1;
		((int *) recSpace)[1] = currentPosInRec;
		*((double *) &(recSpace[currentPosInRec])) = finalRes;
		currentPosInRec += sizeof (double);
		temp.bits = new (std::nothrow) char[currentPosInRec];
		memcpy (temp.bits, recSpace, currentPosInRec);
	   // delete [] space;
	    delete [] recSpace;
}
void *SUM_work(void* SUM){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)SUM;
	Record *temp = new Record();
	Pipe *out=workInfo->outPipe;
	Pipe *in=workInfo->inPipe;
	Function *computeMe= workInfo->computeMe;
	int intRes = 1;
	double doubleRes=0.0;
	double finalRes=0.0;
//	cout<<dFile->GetNext(temp)<<endl;
	while(in->Remove(temp)){
		/*int pointer = ((int *) temp->bits)[4];
		double *myDouble = (double *) &(temp->bits[pointer]);
		cout << *myDouble<<"  ';"<<endl;*/
		//print record for debugging
			//Record *inRecord = new Record();
			//inRecord->Copy(temp);
			//out->Insert(inRecord);
					//cout<<"insert"<<endl;
		computeMe->Apply(*temp,intRes,doubleRes);
		finalRes+=doubleRes;
		}
	sumRecord(*temp,finalRes);
	out->Insert(temp);
	//workInfo->outPipe->Insert(temp);
	out->ShutDown();
	pthread_exit(NULL);
}
void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	runInfo *myRunInfo = new runInfo();
	myRunInfo->inPipe=&inPipe;
	myRunInfo->outPipe=&outPipe;
	myRunInfo->computeMe = &computeMe;
	pthread_create(&thread, NULL,SUM_work,(void *)myRunInfo);
	return;
}
void Sum::WaitUntilDone () {
	pthread_join (thread, NULL);
	//pthread_exit(NULL);
	return;
}

void Sum::Use_n_Pages (int runlen) {
     this->runlen=runlen;
}

/*void proRecord(Record &temp, Record &pro,int keepMe[], int numAttsInput, int numAttsOutput){
	if (pro.bits != NULL)
			delete [] pro.bits;
		pro.bits = NULL;
		cout<<"keep "<<keepMe[0]<<endl;
		cout<<"keep "<<keepMe[1]<<endl;
		cout<<"keep "<<keepMe[2]<<endl;
		//char *space = new (std::nothrow) char[PAGE_SIZE];
		char *recSpace = new (std::nothrow) char[PAGE_SIZE];
		int currentPosInRec = sizeof (int) * (numAttsOutput+ 1);
		((int *) recSpace)[0] = numAttsOutput;
		cout<<"num = "<<numAttsOutput<<endl;
		for(int i=0; i<numAttsOutput;i++){
		((int *) recSpace)[i+1] = currentPosInRec;
		int targetPos = ((int *)temp.bits)[keepMe[i]+1];
		cout<<"tar "<<targetPos<<endl;
		int tarLen = ((int *)temp.bits)[keepMe[i]+2]-targetPos;
		strncpy(&(recSpace[currentPosInRec]), &temp.bits[targetPos] , tarLen);
		currentPosInRec += tarLen;
		}
		pro.bits = new (std::nothrow) char[currentPosInRec];
		//cout<<recSpace<<endl;
		memcpy (pro.bits, recSpace, currentPosInRec);
	  //  delete [] space;
	    delete [] recSpace;
}*/
void *PRO_work(void* PRO){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)PRO;
	Record *temp = new Record();

	Pipe *out=workInfo->outPipe;
	Pipe *in=workInfo->inPipe;
	int *keepMe = workInfo->keepMe;
	cout<<"keep "<<keepMe[0]<<endl;
	int numAttsInput = workInfo->numAttsInput;
	int numAttsOutput = workInfo->numAttsOutput;
//	cout<<dFile->GetNext(temp)<<endl;
	while(in->Remove(temp)){
		/*int pointer = ((int *) temp->bits)[4];
		double *myDouble = (double *) &(temp->bits[pointer]);
		cout << *myDouble<<"  ';"<<endl;*/
		//print record for debugging
			//Record *inRecord = new Record();
			//inRecord->Copy(temp);
			//out->Insert(inRecord);
			//cout<<"insert"<<endl;
		//Record *pro =new Record();
		temp->Project(keepMe,numAttsOutput,numAttsInput);
		out->Insert(temp);
		}
	//workInfo->outPipe->Insert(temp);
	out->ShutDown();
	pthread_exit(NULL);
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	runInfo *myRunInfo = new runInfo();
	myRunInfo->inPipe=&inPipe;
	myRunInfo->outPipe=&outPipe;
	myRunInfo->keepMe = keepMe;
	//cout<<"keep "<<keepMe[0]<<endl;
	myRunInfo->numAttsInput = numAttsInput;
	myRunInfo->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL,PRO_work,(void *)myRunInfo);
	return;
}
void Project::WaitUntilDone (){
	pthread_join (thread, NULL);     // suspends execution of the calling thread until the target thread terminates
}
void Project::Use_n_Pages (int n){
	this->runlen=n;
}

//void PrintOut (Schema *mySchema,Record temp,FILE *outFile) {

	
//}
void *WO_work(void * WO){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)WO;
	Record temp;
	FILE *outFile= workInfo->outFile;
	Pipe *in =workInfo->inPipe;
	Schema *myS = workInfo->mySchema;
	while(in->Remove(&temp)){
	//temp->Print(myS);
       //	PrintOut(myS,temp,outF);
        int n = myS->GetNumAtts();
	Attribute *atts = myS->GetAtts();
	// loop through all of the attributes
	for (int i = 0; i < n; i++) {

		// print the attribute name
		fputs( atts[i].name,outFile);
		fputs(": ",outFile);

		// use the i^th slot at the head of the record to get the
		// offset to the correct attribute in the record
		int pointer = ((int *) temp.bits)[i + 1];

		// here we determine the type, which given in the schema;
		// depending on the type we then print out the contents
		//cout << "[";
                fputs("[ ",outFile);
		// first is integer
		if (atts[i].myType == Int) {
			int *myInt = (int *) &(temp.bits[pointer]);
			//cout << *myInt;
			fprintf(outFile,"%d", *myInt);

		// then is a double
		} else if (atts[i].myType == Double) {
			double *myDouble = (double *) &(temp.bits[pointer]);
			//fputs(*myDouble,outFile);
			fprintf(outFile,"%f", *myDouble);

		// then is a character string
		} else if (atts[i].myType == String) {
			char *myString = (char *) &(temp.bits[pointer]);
			fputs(myString,outFile);
		}

		fputs( "]",outFile);

		// print out a comma as needed to make things pretty
		if (i != n - 1) {
			fputs( ", ",outFile);
		}
	}

	fputs("\n",outFile);
	//cout<<"write"<<endl;

	}
	if(outFile) fclose(outFile);
	return 0 ;
}
void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema){
	    runInfo *myRunInfo = new runInfo();
		myRunInfo->inPipe=&inPipe;
		myRunInfo->outFile= outFile;
		myRunInfo->mySchema = &mySchema;
		pthread_create(&thread, NULL,WO_work,(void *)myRunInfo);

}
void WriteOut::WaitUntilDone () {
    pthread_join(thread,NULL);
	}
void WriteOut::Use_n_Pages (int n){
        runlen=n;
}


//two input pipes,an output pipe and a CNF and joins all of the records from the
//two pipes according to CNF. using BigQ to store all the tuples
class SimPipe
	{
		list<Record> lrecord;
	public:
		void Insert(Record* prec)
		{
			Record r;
			lrecord.push_back(r);

			lrecord.back().Consume(prec);
		}
		int Remove(Record* prec)
		{
			if(lrecord.size() == 0)
				return 0;
			prec->Consume(&(lrecord.front()));
			lrecord.pop_front();
			return 1;
		}
	};

static void simMerge(Record& rec_l,Record& rec_r,Record& rec_out,int* attrlist)
		{
			int attrnum_l = (((int*)rec_l.bits)[1]/sizeof(int)) - 1;
			int attrnum_r = (((int*)rec_r.bits)[1]/sizeof(int)) - 1;

			if(attrlist[0] == -1)
			{
				for(int i = 0 ; i < attrnum_l; i++)
					attrlist[i] = i;
				for(int i = 0 ; i < attrnum_r; i++)
					attrlist[i + attrnum_l] = i;

			}
			rec_out.MergeRecords(&rec_l,&rec_r,attrnum_l,attrnum_r,attrlist,attrnum_l+attrnum_r,attrnum_l);
		}

void simJoin(SimPipe& left, SimPipe& right, int left_num, int right_num,runInfo *workInfo)
		{
	       // runInfo *workInfo=new runInfo();
		    Pipe *out=workInfo->outPipe;
		    int *attrlist=workInfo->attrList;

	        Record rec_l,rec_r,rec_out;
			for(int j = 0 ; j <left_num  ; j++)
			{
				left.Remove(&rec_l);
				for(int i = 0 ; i < right_num; i++)
				{
					right.Remove(&rec_r);
					simMerge(rec_l,rec_r, rec_out,attrlist);
					if(j < left_num -1)
						right.Insert(&rec_r);
					out->Insert(&rec_out);
				}
			}
		}

void *JOIN_work(void* JN){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)JN;
	Pipe *out=workInfo->outPipe;
	Pipe *inL=workInfo->inPipeL;
	Pipe *inR=workInfo->inPipeR;
	int *attr=workInfo->attrList;
	CNF *apply=workInfo->apply_me;

	int run=workInfo->runLength;


	OrderMaker order_l,order_r;
	int pipe_size = 100;
	Pipe bq_pipe_l(pipe_size), bq_pipe_r(pipe_size);
	Record rec_l,rec_r,rec_out;
	ComparisonEngine ceng;
	SimPipe bigPipe_l;
	SimPipe bigPipe_r;

	attr[0] = -1;
	if(apply->GetSortOrders(order_l,order_r))
				{
					BigQ bq_l(*(inL),bq_pipe_l,order_l,run);
					BigQ bq_r(*(inR),bq_pipe_r,order_r,run);


					int recInPipe = 0;
					int result = 0;
					Record first_l,first_r;
					while(1)
					{
						if(!rec_l.bits || result < 0)
							if(!bq_pipe_l.Remove(&rec_l))
							{
								while(bq_pipe_r.Remove(&rec_r))
								{}
								break;
							}
						if(!rec_r.bits || result > 0)
							if(!bq_pipe_r.Remove(&rec_r))
							{
								while(bq_pipe_l.Remove(&rec_l))
								{}
								break;
							}
						result = ceng.Compare(&rec_l,&order_l,&rec_r,&order_r);
						if(result != 0) //if  don't agree on the attribute
							continue; //keep on fetching
						//otherwise construct two big pipe filled with records having the same attribute
						first_l.Consume(&rec_l);
						first_r.Consume(&rec_r);
						int rec_num_l = 1;
						int rec_num_r = 1;
						while(bq_pipe_l.Remove(&rec_l))
						{
							if(ceng.Compare(&rec_l,&first_l,&order_l))
								break;
							rec_num_l++;
							bigPipe_l.Insert(&rec_l);
						}
						bigPipe_l.Insert(&first_l);


						while(bq_pipe_r.Remove(&rec_r))
						{
							if(ceng.Compare(&rec_r,&first_r,&order_r))
								break;
							rec_num_r++;
							bigPipe_r.Insert(&rec_r);
						}
						bigPipe_r.Insert(&first_r);

						simJoin(bigPipe_l,bigPipe_r,rec_num_l,rec_num_r, workInfo);
				}

				}
				else
				{

						int rec_num_l = 0;
						int rec_num_r = 0;
						while(inL->Remove(&rec_l))
						{
							rec_num_l++;
							bigPipe_l.Insert(&rec_l);
						}


						while(inR->Remove(&rec_r))
						{
							rec_num_r++;
							bigPipe_r.Insert(&rec_r);
						}

						simJoin(bigPipe_l,bigPipe_r,rec_num_l,rec_num_r, workInfo);


				}

				out->ShutDown();
				int i = 0;

	            return 0;
}


void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	runInfo *myRunInfo = new runInfo();
	myRunInfo->inPipeL=&inPipeL;
	myRunInfo->inPipeR=&inPipeR;
	myRunInfo->outPipe=&outPipe;
	myRunInfo->apply_me=&selOp;
	myRunInfo->literal=&literal;
	myRunInfo->runLength=this->runlen;

	pthread_create(&thread, NULL,JOIN_work,(void *)myRunInfo);
	return;
}
void Join::WaitUntilDone (){
	pthread_join (thread, NULL);

}
void Join::Use_n_Pages (int n){
	//runInfo *myRunInfo=new runInfo();
	runlen=n;

}


void *DR_work(void* DR){
	    runInfo *workInfo=new runInfo();
		workInfo = (runInfo *)DR;
		Pipe *out=workInfo->outPipe;
		Pipe *in=workInfo->inPipe;
		Schema *schema=workInfo->mySchema;
		int runlen=workInfo->runLength;

		 OrderMaker om(schema);
		 Pipe bq_pipe(1000);
		 BigQ bq(*(in),bq_pipe,om,runlen);
		 ComparisonEngine ceng;

		 int rec_num = 0;
		 int i = 0;

		 Record rec[2];
		 Record *last = NULL, *prev = NULL;

		 while (bq_pipe.Remove (&rec[i%2])) {

		            prev = last;
		            last = &rec[i%2];
		            if (prev && last) {
		                if (ceng.Compare (prev, last, &om) != 0) {
		                    out->Insert(prev);
				    prev=NULL;
		                    rec_num++;
			            cout<<rec_num<<endl;
		                }
		            }

		            i++;
		        }

		 if(last)
		        {
		            out->Insert(last);
		            rec_num++;
		            cout<<rec_num<<endl;
		        }

		        out->ShutDown();

		        return 0;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
    {
        runInfo *myRunInfo = new runInfo();
    	myRunInfo->inPipe=&inPipe;
    	myRunInfo->outPipe=&outPipe;
    	myRunInfo->mySchema=&mySchema;
    	myRunInfo->runLength= this->runlen;

        pthread_create (&thread, NULL, DR_work, (void *)myRunInfo);
    }
void DuplicateRemoval::WaitUntilDone ()
    {
        pthread_join(thread,NULL);
    }
void DuplicateRemoval::Use_n_Pages (int n) {
	//runInfo *myRunInfo=new runInfo();
	runlen=n;
}


int GetAttrNum(Record& rec)
{
	return (((int*)rec.bits)[1]/sizeof(int)) - 1;
}

int OutputRecord(double val, Record& rec)
{
     	string filepath;

     	start:
     			if(filepath == "")
     			{
     			char tmpfile [L_tmpnam];
     	#ifdef _WIN32
     			tmpnam (tmpfile);
     	#else
     			strcpy(tmpfile, "/tmp/XXXXXX");
     			mkstemp(tmpfile);
     	#endif
     			filepath = tmpfile;
     			}

		FILE* myFile=fopen(filepath.c_str(),"w");

        if (myFile!=NULL)
         {
            fprintf(myFile,"%f|\n",val);
            fclose (myFile);
         }

        //output record into the tmpfile
		Attribute DA = {"double", Double};
        Schema out_sch ("out_sch",1, &DA);

        myFile=fopen(filepath.c_str(),"r");
		int result = rec.SuckNextRecord(&out_sch,myFile);
        fclose(myFile);
        return result;

}

void *GB_work(void* GB){
	    runInfo *workInfo=new runInfo();
		workInfo = (runInfo *)GB;
		Pipe *out=workInfo->outPipe;
		Pipe *in=workInfo->inPipe;
		OrderMaker *order=workInfo->myOrder;
		Function *compute=workInfo->computeMe;
		int runlen=workInfo->runLength;

		Pipe bq_pipe(100);
		BigQ bq(*(in),bq_pipe,*(order),runlen);
		ComparisonEngine ceng;
        int rec_num = 0;
        int i = 0;

        Record rec[2];
		Record *last = NULL, *prev = NULL;

		Record out_rec;
		Record double_rec;

		int int_result=0;
		double double_result=0;
		int int_sum=0;
		double db_sum=0;
		int attrlist[1000];
		attrlist[0] = 0;
		for(int j = 0 ; j < order->numAtts ; j++)
				{
					attrlist[j+1] = order->whichAtts[j];
				}

		while (bq_pipe.Remove (&rec[i%2])) {

		            prev = last;
		            last = &rec[i%2];
		            compute->Apply(*last,int_result,double_result);
		            if (prev && last) {

		                if (ceng.Compare (prev, last, order))
						{
							OutputRecord(db_sum,double_rec);
							out_rec.MergeRecords(&double_rec,prev,1,GetAttrNum(*prev),attrlist,1+order->numAtts,1);

				            out->Insert(&out_rec);
							db_sum = 0;
		                    int_sum = 0;
						}
				    }
					db_sum+=double_result;
		            int_sum+=int_result;
		            i++;
				}
		        if(last)
		        {
		        	OutputRecord(db_sum,double_rec);
					out_rec.MergeRecords(&double_rec,prev,1,GetAttrNum(*prev),attrlist,1+order->numAtts,1);

				    out->Insert(&out_rec);

		        }
		        out->ShutDown();
		        return 0;
}


void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	runInfo *myRunInfo = new runInfo();
	    	myRunInfo->inPipe=&inPipe;
	    	myRunInfo->outPipe=&outPipe;
	    	myRunInfo->myOrder=&groupAtts;
	    	myRunInfo->computeMe=&computeMe;

			myRunInfo->runLength= 100;


    pthread_create(&thread,NULL,GB_work,(void *)myRunInfo);
}
void GroupBy::WaitUntilDone ()
{
    pthread_join(thread,NULL);
}
void GroupBy::Use_n_Pages (int n)
{
	runInfo *myRunInfo=new runInfo();
   myRunInfo->runLength=n;
}


