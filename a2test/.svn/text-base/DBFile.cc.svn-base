#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include<iostream>
#include<fstream>
#include<string>
#include<stdlib.h>

using namespace std;

DBFile::DBFile () {
	recordCounter=0;
	pageCounter=0;
    mode=read_data;
}

//f_path is bin file storied path
int DBFile::Create (char *f_path, fType f_type, void *startup) {

	mode=read_data;
	pageCounter=0;

	file.Open(0,f_path);
    return 1;

}

// bulk loads the DBFile instance from a text file, appending new data to it using the SuckNextRecord function from Record.h
//loadpath is tblfile path  ; GetPage is read from specified page and AddPage is write to the page

void DBFile::Load (Schema &f_schema, char *loadpath) {
	FILE *loadFile=fopen(loadpath, "r");   //"r" open file for input operations and the file must exist;

	if(loadFile==0){
    	cerr<<"Load failed"<<loadpath<<"\n";
    	exit(1);
    }

     //empty buffer_page
	if(mode==read_data)
		mode=write_data;
    else{
		file.AddPage(&page,pageCounter);
		pageCounter++;
		page.EmptyItOut();
	}


    Record temp;
    pageCounter=0;


    /*
     while(temp.SuckNextRecord(&f_schema,loadFile)==1){

		if(page.Append(&temp)==1){
			//cout<<"Number of records in the page:"<<page.GetNumRecs()<<endl;
		    recordCounter++;
		}else{
			//cout<<"Current size of pages:"<<page.GetCurSize()<<endl;
			//cout<<"Number of records in the page:"<<page.GetNumRecs()<<endl;

			file.AddPage(&page,pageCounter);
			pageCounter++;

			//Page tempPage;
			//file.GetPage(&tempPage,pageCounter);
			//cout<<"Number of records in the temppage:"<<tempPage.GetNumRecs()<<endl;

			page.EmptyItOut();

			if(page.Append(&temp)==1){
				//cout<<"Number of records in the page:"<<page.GetNumRecs()<<endl;
				recordCounter++;
					}
			}
	}
     */


	while(temp.SuckNextRecord(&f_schema,loadFile)==1){

		//if page is filled then write to disk
		if(page.Append(&temp)==0){


			file.AddPage(&page,pageCounter);
			pageCounter++;

			page.EmptyItOut();
			}
	}


    fclose(loadFile);

}

int DBFile::Open (char *f_path) {

	mode=read_data;
	pageCounter=0;

	file.Open(1,f_path);
    return 1;
}

//get the first page in file
void DBFile::MoveFirst () {


	//empty buffer_page
	if(mode==write_data){
		if(file.GetLength()>0||page.GetNumRecs()>0){
			file.AddPage(&page,pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
		mode=read_data;
	}

	    pageCounter=0;

		file.GetPage(&page,0);


}

int DBFile::Close () {
	//empty buffer_page
	if(mode==write_data){
		if(file.GetLength()>0||page.GetNumRecs()>0){
			file.AddPage(&page,pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
		mode=read_data;
	}

	file.Close();
	return 1;
}

//In heap file simply adds the new record to the end of the file
void DBFile::Add (Record &rec) {

	if(mode==read_data){
		page.EmptyItOut();
		mode=write_data;
	}

	if(page.Append(&rec)==0){

			file.AddPage(&page,pageCounter);
	        pageCounter++;
	        page.EmptyItOut();

		page.Append(&rec);
		rec.Consume(&rec); //consume rec
	}

}

//simply gets the next record from the file and returns it to the user,
//where ¡°next¡± is defined to be relative to the current location
//of the pointer. After the function call returns, the pointer into the file is incremented, so a
//subsequent call to GetNext won¡¯t return the same record twice. The return value is an
//integer whose value is zero if and only if there is not a valid record returned from the
//function call (which will be the case, for example, if the last record in the file has already
//been returned).
int DBFile::GetNext (Record &fetchme) {

	if(mode==write_data){
		if(file.GetLength()>0||page.GetNumRecs()>0){
			file.AddPage(&page,pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
		mode=read_data;
	}

    //empty file
	if(file.GetLength()==0&&page.GetNumRecs()==0){
		return 0;
	}

	//why Getlength-2?
	if(page.GetFirst(&fetchme)==0){
		if(pageCounter==file.GetLength()-2){
			return 0;
		}
			pageCounter++;
			page.EmptyItOut();
			file.GetPage(&page, pageCounter);
			page.GetFirst(&fetchme);

	}
	return 1;
}

//also accepts a selection predicate (this is a conjunctive
//normal form expression). It returns the next record in the file that is accepted by the
//selection predicate. The literal record is used to check the selection predicate, and is
//created when the parse tree for the CNF is processed.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	 Record temp;
	 ComparisonEngine comp;
	 while(GetNext(temp) > 0){
	 if (comp.Compare (&temp, &literal, &cnf)){
	      fetchme.Consume(&temp);
	      return 1;
	  }
	 }
	 return 0;
}
