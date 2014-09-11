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
#include "GenericDBFile.h"

typedef enum {heap, sorted, tree} fType;

class Heap : public GenericDBFile{
private:
	private:
	File file;
	Page bufferPage;//used by Add(). add the new record to bufferPage first, after bufferPage is full, store it to file.
	int curPageNum; //used by MoveFirst(). curPage represent the page should be visited by getNext().
	//this is an object instead of pointer, new A() return a pointer
	//c++ don't need to be new
public:
	Heap (){

	} 
	File& getFile(){
		return file;
	}
	int Create (const char *fpath, fType file_type, void *startup){
        // create file if the first parameter is 0
        file.Open(0, fpath);
        //tempo alway return 1;
        return 1;

	}
	int Open (char *fpath){

		file.Open(1, fpath);
		//assume we always success for now;
		return 1;
	}
	int Close (){
      		 return file.Close();
	}

	void Load (Schema &myschema, char *loadpath){
		FILE *tableFile = fopen(loadpath,"r");
		//if fopen fail, then exit
		EXIT_MSG(tableFile, "Can not open %s at func Load in DBFile.h\n ", loadpath);
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
			int nextPageNum = file.GetLength()-1;
			file.AddPage(&bufferPage,nextPageNum);
			bufferPage.EmptyItOut();
		}
	}
	
	//used if we want to visit from the beginning.
	void MoveFirst (){
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
