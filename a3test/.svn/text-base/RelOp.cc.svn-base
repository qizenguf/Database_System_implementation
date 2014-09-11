#include "RelOp.h"
#include <iostream>
#include <string.h>


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
			cout<<"insert"<<endl;
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
			cout<<"insert"<<endl;
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
	pthread_join (thread, NULL);
}
void Project::Use_n_Pages (int n){
	this->runlen=n;
}

void PrintOut (Schema *mySchema,Record temp,FILE *outFile) {

	int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();

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
		cout << "[";

		// first is integer
		if (atts[i].myType == Int) {
			int *myInt = (int *) &(temp.bits[pointer]);
			//cout << *myInt;
			fprintf(outFile,"%d", myInt);

		// then is a double
		} else if (atts[i].myType == Double) {
			double *myDouble = (double *) &(temp.bits[pointer]);
			//fputs(*myDouble,outFile);
			fprintf(outFile,"%f", myDouble);

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
}
void *WO_work(void * WO){
	runInfo *workInfo=new runInfo();
	workInfo = (runInfo *)WO;
	Record *temp = new Record();
	FILE *outF= workInfo->outFile;
	Pipe *in =workInfo->inPipe;
	Schema *myS = workInfo->mySchema;
	while(in->Remove(temp)){
       PrintOut(myS,*temp,outF);
	}
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

}
