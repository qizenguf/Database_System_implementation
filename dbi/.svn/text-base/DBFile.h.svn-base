#pragma once 
//modern way to make sure the file below only be included once;
#define EXIT_MSG(pointer, msg, name) if(pointer == 0) { \
			printf(msg, name); \
			exit(1); };
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


typedef enum {heap, sorted, tree} fType;
typedef enum {read, write} OpenMode;
struct SortInfo{OrderMaker *order; int runlen;};
struct FileInfo{
	string static getMetaFilePath(const char *binfpath){
		string fpath = binfpath;
		fpath += ".meta";
		return fpath;
	}
	void static write(const char *binfpath, fType fileType, SortInfo& sinfo){
		FILE * pFile;
		string fpath = getMetaFilePath(binfpath);
		pFile = fopen(fpath.c_str(), "w");
		//1st line fileType_
		fprintf(pFile, "%d\n", fileType);
		if(sinfo.order!= 0){
			//2nd line number of attribute (numAtts) of order maker
			fprintf(pFile, "%d\n", sinfo.order->numAtts);
			//3rd line print out all the attributes seperated by comma
			//4th line print out all the attribute types seperated by comma
			for(int i = 0; i < sinfo.order->numAtts; i++){
				fprintf(pFile, "%d,", sinfo.order->whichAtts[i]);
			}
			fprintf(pFile, "\n");
			for(int i = 0; i < sinfo.order->numAtts; i++){
				fprintf(pFile, "%d,", sinfo.order->whichTypes[i]);
			}
			fprintf(pFile, "\n");
			//5th line run length
			fprintf(pFile, "%d\n", sinfo.runlen);
		}
		//if the file type is heap, we only write one line
		fclose(pFile);
	}
	void static read(const char *binfpath, fType& fileType, OrderMaker& order, int& runlen){
		string fpath = getMetaFilePath(binfpath);
		FILE * pFile;
		pFile = fopen(fpath.c_str(), "r");
		//1st line fileType_
		fscanf(pFile, "%d", &fileType);
		if(fileType != heap){
			//2nd line number of attribute (numAtts) of order maker
			fscanf(pFile, "%d", &(order.numAtts));
			if(order.numAtts != 0){
				//3rd line print out all the attributes seperated by comma
				//4th line print out all the attribute types seperated by comma
				for(int i = 0; i < order.numAtts; i++){
					fscanf(pFile, "%d,", &(order.whichAtts[i]));
				}
				for(int i = 0; i < order.numAtts; i++){
					fscanf(pFile, "%d,", &(order.whichTypes[i]));
				}
			}
			//5th line run length
			fscanf(pFile, "%d\n", &runlen);
		}
		fclose(pFile);
	}
};
// stub HeapDBFile header..replace it with your own HeapDBFile.h 
class DBFile{
	DBFile* dbFile_;

public:
	DBFile ():dbFile_(0){} 
	virtual ~DBFile (){ 
		delete dbFile_;
	}
	//In java, all the functions are virtual
	//but in c++, we need to explicitly declare a function to be virtual
	//otherwise dbFile_ = new HeapDBFile(); 
	//(*dbFile_).Create(fpath, file_type, startup);
	//Here the Create() method in DBFile will be call, instead of the one in HeadDBFile 

	int virtual Create (const char *fpath, fType file_type, void *startup);
	virtual int Open (char *fpath);
	virtual File&  getFile(){
		return dbFile_->getFile();
	}
	
	
	virtual int Close (){
		return dbFile_->Close();
	}
	virtual void Load (Schema &myschema, char *loadpath){
		dbFile_->Load(myschema,loadpath);
	}
	virtual void writeLastPageToFile(){
		dbFile_->writeLastPageToFile();
	}
	virtual void MoveFirst (){
		dbFile_->MoveFirst();
	}
	virtual void Add (Record &addme){
		dbFile_->Add(addme);
	}
	virtual int GetNext (Record& fetchme, CNF& cnf, Record& literal){
		return dbFile_->GetNext(fetchme, cnf, literal);
	}
	virtual int GetNext (Record &fetchme){
		return dbFile_->GetNext(fetchme);
	}
};




class HeapDBFile : public DBFile{
private:
	File file;
	Page bufferPage;//used by Add(). add the new record to bufferPage first, after bufferPage is full, store it to file.
	int curPageNum; //used by MoveFirst(). curPage represent the page should be visited by getNext().
	//this is an object instead of pointer, new A() return a pointer
	//c++ don't need to be new
	string fpath_;
public:
	HeapDBFile (){} 
	~HeapDBFile (){}
	File& getFile(){
		return file;
	}
	int Create (const char *fpath, fType file_type, void *startup){
        // create file if the first parameter is 0
        fpath_ = fpath;
		file.Open(0, fpath);
        //tempo alway return 1;
        return 1;

	}
	int Open (const char *fpath){

		file.Open(1, fpath);
		//assume we always success for now;
		return 1;
	}
	int Close (){
		SortInfo sinfo = {0,0};
		FileInfo::write(fpath_.c_str(),heap, sinfo);
      	return file.Close();
	}
	//when heap file is used as helper for sorted dbfile, do not change meta file.
	int CloseInHelperMode(){
		//FileInfo::write(fpath_.c_str(),heap,0);
      	return file.Close();
	}
	void Load (Schema &myschema, char *loadpath){
		FILE *tableFile = fopen(loadpath,"r");
		//if fopen fail, then exit
		EXIT_MSG(tableFile, "Can not open %s at func Load in HeapDBFile.h\n ", loadpath);
		Record record;
		//read in all of the records from the text file and add them to file;
		while(record.SuckNextRecord(&myschema,tableFile)==1){
			Add(record);
		}
		writeLastPageToFile();
	}
	
	//This method will make sure every run start from the new page
	void writeLastPageToFile(){
		//if we have some content left in the buffer page, write them page
		if(bufferPage.GetNumRecs()!= 0){
			int length =  file.GetLength();
			//ZTMD Kengdie: 
			//when file.GetLength() = 0, file.curLength = 0 
			//when file.GetLength() = 1, file.curLength = 2
			int nextPageNum = (length == 0 ?length : length - 1);
			file.AddPage(&bufferPage,nextPageNum);
			bufferPage.EmptyItOut();
		}
	}
	
	//used if we want to visit from the beginning.
	void MoveFirst (){
		if(file.GetLength() == 0) return;
		curPageNum = 0;
		//Reload page
		//Everytime when we call GetNext(Record &fetchme)
		//the record holded by fetchme is removed from buffer page physically
		//the more times we call, the buffer page will have less records.
		//If we do it until the buffer page is empty or the buffer page is not the first page, they are fine. (1)
		//But most time we don't use it that way. 
		//If we don't reload the buffer page, after move first, we only got the part of the first page. It is not correct.  
		//Everytime when we call MoveFirst(), we want to restart, the buffer page should be consistent with the first page.
		//therefore we need reload. Even some cases (1) we don't need to reload. It doesn't harm we reload everytime.
		file.GetPage(&bufferPage, curPageNum);
	}
	//the function doesn't consider file isn't empty, while buffer is empty
	//add & getFile
	void Add (Record &addme){
		//if the record can not be fit in the first Page,
		//then 1)copy buffer page back to file
		//     2)empty buffer page
		//     3)re-append record to the page
		//else Append which success in the if statement
		if(!bufferPage.Append(&addme)){
			int length =  file.GetLength();
			//ZTMD Kengdie: 
			//when file.GetLength() = 0, file.curLength = 0 
			//when file.GetLength() = 1, file.curLength = 2
			int nextPageNum = (length == 0 ?length : length - 1);
			file.AddPage(&bufferPage,nextPageNum);
			bufferPage.EmptyItOut();	
			//write record to page buffer
			bufferPage.Append(&addme);
		}
	}
	int GetNext (Record &fetchme){
		while(bufferPage.GetNumRecs()== 0){
			curPageNum++;
			if(curPageNum >= file.GetLength()-1){
				//The last record in the file has already been returned. no more page.
				return 0;
			}else{
				file.GetPage(&bufferPage, curPageNum);
			}
			
		}
		bufferPage.GetFirst(&fetchme);
		return 1;
	}
	int GetNext (Record &fetchme, CNF &cnf, Record &literal){
		ComparisonEngine comp;
		while(GetNext(fetchme) != 0){
			if (comp.Compare (&fetchme, &literal, &cnf)){
				return 1;
			}
		}
        return 0;
	}

};	

#include "BigQ.h"

class SortedDBFile : public DBFile{
private:
	File file_;
	Page bufferPage_;//used by Add(). add the new record to bufferPage first, after bufferPage is full, store it to file.
	int curPageNum_; //used by MoveFirst(). curPage represent the page should be visited by getNext().
	//this is an object instead of pointer, new A() return a pointer
	//c++ don't need to be new
	BigQ* bigq_;
	Pipe in_;
	Pipe out_;
	string fpath_;
	OrderMaker order_;
	int runlen_;
	HeapDBFile helperHeapFile_;
	OpenMode mode_;
	
public:
	SortedDBFile():bigq_(0),in_(10000),out_(10000){}
	//SortedDBFile(SortInfo sortInfo):sortInfo_(&sortInfo),bigq_(0),in_(10000),out_(10000){}
	~SortedDBFile(){ delete bigq_; }
	File& getFile(){
		return file_;
	}
	int Create (const char *fpath, fType file_type, void *startup){
        SortInfo* sortInfo = (SortInfo*)startup;
		order_ = *(sortInfo->order);
		runlen_ = sortInfo->runlen;
		fpath_ = fpath;
        //We did not create the file here, 
		//Since Add() will be after Create() and the true writes happen in the Close() 
		//file_.Open(0, fpath);
		//tempo alway return 1;
		if(bigq_ == 0){
			bigq_ = new BigQ (in_, out_, order_ , runlen_);
		}
		helperHeapFile_.Create (fpath_.c_str(), heap, NULL);
		mode_ = write;
        return 1;
	}
	int Open (char *fpath){
		//assume we always success for now;
		fType fileType;
		fpath_ = fpath;
		FileInfo::read(fpath,fileType,order_,runlen_);
		helperHeapFile_.Open(fpath_.c_str());
		//set mode to be read by default.
		//since now we don't know user wants to read or write
		//we can switch it to write later.
		mode_ = read;
		return 1;
	}
	int Close (){
		if(mode_ == write){
			//write down meta data, it will be read in the open
			SortInfo sinfo = {&order_,runlen_};
			FileInfo::write(fpath_.c_str(),sorted,sinfo);
			//The 1st Phase of TPMMS is completed after the next line called.
			in_.ShutDown ();
			//bigQ will completed the 2nd phase which puts sorted records to out pipe
			Record rec;
			//create a HeapDBFile to help us write everything to File
			//HeapDBFile writeFileHelper;
		
			while (out_.Remove (&rec)){
				helperHeapFile_.Add(rec);
			
			}
			helperHeapFile_.writeLastPageToFile();
			return helperHeapFile_.CloseInHelperMode();
		}else{
			helperHeapFile_.Close();
		}
	}

	void Load (Schema &myschema, char *loadpath){
		FILE *tableFile = fopen(loadpath,"r");
		//if fopen fail, then exit
		EXIT_MSG(tableFile, "Can not open %s at func Load in HeapDBFile.h\n ", loadpath);
		Record record;
		//read in all of the records from the text file and add them to file;
		while(record.SuckNextRecord(&myschema,tableFile)==1){
			Add(record);
		}
	}
	
	//This method will make sure every run start from the new page
	void writeLastPageToFile(){
		//if we have some content left in the buffer page, write them page
		if(bufferPage_.GetNumRecs()!= 0){
			int nextPageNum = file_.GetLength()-1;
			file_.AddPage(&bufferPage_,nextPageNum);
			bufferPage_.EmptyItOut();
		}
	}
	//the function doesn't consider the case
	//when there are some records in the file before
	void Add (Record &addme){
		//add record to in pipe, create a big queue if it hasn't been created
		// pipe cache size, 
		// BigQ has two parts, one part is in the memory, the maxmium size is buffsz
		// When elements are larger than buffsz, the exceeded part is in secordary memory
		// the buffsz can not be too big, eg. larger than the memory size
		// it should not be too small, smaller than page size which will waste lots of space.
		if(mode_== read){
			mode_ = write;
			bigq_ = new BigQ (in_, out_, order_, runlen_);
			//read records from file and push to in pipe
			helperHeapFile_.MoveFirst();
			Record record;
			while(helperHeapFile_.GetNext(record)){
				in_.Insert (&record);
			}
			helperHeapFile_.Close();
			//delete helperHeapFile_ & create a new file with the same name
			if(remove(fpath_.c_str())!=0){
				cerr << "Error deleting file\n";
				exit (1);
			}
			helperHeapFile_.Create (fpath_.c_str(), heap, NULL);
		}
		in_.Insert(&addme);
	}
	void MoveFirst (){
		return helperHeapFile_.MoveFirst();
	}
	
	int GetNext (Record &fetchme){
		return helperHeapFile_.GetNext(fetchme);
	}
	
	int GetNext (Record &fetchme, CNF &cnf, Record &literal){
		return helperHeapFile_.GetNext(fetchme, cnf, literal);
	}
	
};



inline 	int DBFile::Create (const char *fpath, fType file_type, void *startup){
		if(file_type == heap){
			dbFile_ = new HeapDBFile();
		}else{
			dbFile_ = new SortedDBFile();
		}
		return dbFile_->Create(fpath, file_type, startup);
	}

inline int DBFile::Open(char *fpath){
	//read meta data first new right dbFile_
	OrderMaker o;
	fType fileType;
	int runlen = 100;
	FileInfo::read(fpath, fileType, o, runlen);
	if(fileType == heap){
		dbFile_ = new HeapDBFile();
	}else{
		dbFile_ = new SortedDBFile();
		
	}
	return dbFile_->Open(fpath);
}