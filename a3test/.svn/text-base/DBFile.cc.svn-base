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

// stub file .. replace it with your own DBFile.cc

DBHeap::DBHeap() {
	recordCounter = 0;
	pageCounter = 0;
	mode = read_data;
}
DBHeap::~DBHeap() {
}
void DBHeap::MoveFirst() {
	//empty buffer_page
	if (mode == write_data) {
		if (file.GetLength() > 0 || page.GetNumRecs() > 0) {
			file.AddPage(&page, pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
		mode = read_data;
	}
	pageCounter = 0;
	file.GetPage(&page, 0);
}
;
void DBHeap::Add(Record &addme) {
	if (mode == read_data) {
		page.EmptyItOut();
		mode = write_data;
	}

	if (page.Append(&addme) == 0) {
		file.AddPage(&page, pageCounter);
		pageCounter++;
		page.EmptyItOut();
		page.Append(&addme);
	}
}
int DBHeap::GetNext(Record &fetchme) {
	if (mode == write_data) {
		if (file.GetLength() > 0 || page.GetNumRecs() > 0) {
			file.AddPage(&page, pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
		mode = read_data;
	}

	//empty file
	if (file.GetLength() == 0 && page.GetNumRecs() == 0) {
		return 0;
	}

	//why Getlength-2?
	if (page.GetFirst(&fetchme) == 0) {
		if (pageCounter == file.GetLength() - 2) {
			return 0;
		}
		pageCounter++;
		page.EmptyItOut();
		file.GetPage(&page, pageCounter);
		page.GetFirst(&fetchme);

	}
	return 1;
}
int DBHeap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	Record temp;
	ComparisonEngine comp;
	while (GetNext(temp) > 0) {
		if (comp.Compare(&temp, &literal, &cnf)) {
			fetchme.Consume(&temp);
			return 1;
		}
	}
	return 0;
}
int DBHeap::Close() {
	//empty buffer_page
	if (mode == write_data) {
		if (file.GetLength() > 0 || page.GetNumRecs() > 0) {
			file.AddPage(&page, pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
		mode = read_data;
	}

	file.Close();
	return 1;
}
void DBHeap::Load(Schema &f_schema, char *loadpath) {
	FILE *loadFile = fopen(loadpath, "r"); //"r" open file for input operations and the file must exist;
	if (loadFile == 0) {
		cerr << "Load failed" << loadpath << "\n";
		exit(1);
	}
	//empty buffer_page
	if (mode == read_data)
		mode = write_data;
	else {
		file.AddPage(&page, pageCounter);
		pageCounter++;
		page.EmptyItOut();
	}
	Record temp;
	pageCounter = 0;

	while (temp.SuckNextRecord(&f_schema, loadFile) == 1) {

		//if page is filled then write to disk
		if (page.Append(&temp) == 0) {
			file.AddPage(&page, pageCounter);
			pageCounter++;
			page.EmptyItOut();
		}
	}

	fclose(loadFile);
}

DBSorted::DBSorted(void* info) {
	inPipe = NULL;
	outPipe = NULL;
	searchOrderMaker = NULL;
	literalOrderMaker = NULL;
	startup = (SortInfo *) info;
	binarysearch = 0;
}

DBSorted::~DBSorted() {
}
void DBSorted::Merge() {
	ComparisonEngine comp;
	inPipe->ShutDown();
	char *tempFileName;
	tempFileName = new char[50];
	File tempFile;
	sprintf(tempFileName, "%s%d", "mergetemp", getppid());
	tempFile.Open(0, tempFileName);
	cout << tempFileName << endl;
	Page tempPage;
	int tempPageIndex = 0;
	tempPage = *new Page();
	tempFile.AddPage(&tempPage, tempPageIndex++);
	tempPage.EmptyItOut();
	pageCounter = 0;
	Page fPage;
	cout << tempFileName << endl;
	Record fRecord, qRecord;
	bool moreFile = 0;
	if (file.GetLength() > 0) {
		//cout<<tempFileName<<"22"<<endl;
		file.GetPage(&fPage, pageCounter);
		moreFile = fPage.GetFirst(&fRecord);
		// cout<<tempFileName<<"22"<<endl;
	}
	//cout<<tempFileName<<"23"<<endl;
	bool morePipe;
	morePipe = (bool) outPipe->Remove(&qRecord);
	//cout<<tempFileName<<"24"<<endl;
	//cout<<tempFileName<<endl;
	while (moreFile || morePipe) {
		if (moreFile
				&& (!morePipe
						|| comp.Compare(&fRecord, &qRecord, startup->om) <= 0)) {
			//frecord is smaller
			int result = tempPage.Append(&fRecord);
			if (!result) {

				tempFile.AddPage(&tempPage, tempPageIndex++);
				tempPage.EmptyItOut();
				tempPage.Append(&fRecord);
			}
			result = fPage.GetFirst(&fRecord);
			if (!result) {
				pageCounter++;
				if (pageCounter < file.GetLength() - 1) {
					file.GetPage(&fPage, pageCounter);
					fPage.GetFirst(&fRecord);
				} else
					moreFile = 0;
			}
		} else {
			int result = tempPage.Append(&qRecord);
			if (!result) {
				//cout<<tempPageIndex<<endl;
				tempFile.AddPage(&tempPage, tempPageIndex++);
				tempPage.EmptyItOut();
				tempPage.Append(&qRecord);
			}
			morePipe = outPipe->Remove(&qRecord);
		}
	}

	cout << tempFileName << endl;
	tempFile.AddPage(&tempPage, tempPageIndex++);
	outPipe->ShutDown();
	tempFile.Close();
	file.Close();
	remove(fileName);
	rename(tempFileName, fileName);
	file.Open(1, fileName);
	//delete qFile;
	//delete tempFileName;

}
void DBSorted::MoveFirst() {
	if (mode == write_data) {
		Merge();
		mode = read_data;
	}
	pageCounter = 0;
	recordCounter = 0;
	file.GetPage(&page, 0);
}
void DBSorted::Add(Record &addme) {
	if (mode == read_data) {
		inPipe = new Pipe(BUFFER_SIZE);
		outPipe = new Pipe(BUFFER_SIZE);
		startup->om->Print();
		qFile = new BigQ(*inPipe, *outPipe, *startup->om, startup->runLength);
		mode = write_data;
	}
	// Add new record to pipe.
	inPipe->Insert(&addme);
	// cout<<"add one"<<endl;

}

void DBSorted::Load(Schema &f_schema, char *loadpath) {
	FILE *loadFile = fopen(loadpath, "r"); //"r" open file for input operations and the file must exist;
	if (mode == read_data) {
		inPipe = new Pipe(BUFFER_SIZE);
		outPipe = new Pipe(BUFFER_SIZE);
		qFile = new BigQ(*inPipe, *outPipe, *(startup->om), startup->runLength);
		mode = write_data;
	}
	Record temp;
	while (temp.SuckNextRecord(&f_schema, loadFile) == 1) {
		inPipe->Insert(&temp);
	}

	fclose(loadFile);
}

int DBSorted::Close() {
	if (mode == write_data) {
		cout << "merge1" << endl;
		Merge();
		cout << "merge2" << endl;
	}
	mode = read_data;
	return file.Close();
}
int DBSorted::GetNext(Record &fetchme) {
	if (mode == write_data) {
		Merge();
		mode = read_data;
	}
	//empty file
	if (file.GetLength() == 0 && page.GetNumRecs() == 0) {
		return 0;
	}
	//cout<<"pagezlengh="<<file.GetLength()<<endl;
	if (page.GetFirst(&fetchme) == 0) {
		if (pageCounter == file.GetLength() - 2) {
			return 0;
		}
		pageCounter++;
		//cout<<"page="<<pageCounter<<endl;
		page.EmptyItOut();   //clean the cache
		file.GetPage(&page, pageCounter);
		page.GetFirst(&fetchme);
	}
	return 1;
}

int DBSorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	if (mode == write_data) {
		Merge();
		mode = read_data;
	}
	ComparisonEngine comp;
	if (!binarysearch) {
		binarysearch = 1;
		OrderMaker *SortOrder = startup->om;
		if (searchOrderMaker != NULL) {
			delete searchOrderMaker;
		}
		searchOrderMaker = new OrderMaker();
		if (literalOrderMaker != NULL) {
			delete literalOrderMaker;
		}
		literalOrderMaker = new OrderMaker();
		for (int i = 0; i < SortOrder->numAtts; i++) {
			int att = SortOrder->GetAttribute(i);
			int literalIndex;
			if ((literalIndex = cnf.hasSimpleEqual(att)) != -1) {
				searchOrderMaker->AddAttribute(att, SortOrder->GetType(i));
				literalOrderMaker->AddAttribute(literalIndex,
						SortOrder->GetType(i));
			} else {
				break;
			}
		}
		int low = 0;
		int high = file.GetLength() - 2;
		cout<<"high"<<high<<endl;
		if(high<=0) high =0;
		int mid = 0;
		int result = 0;
		pageCounter = 0;
		int onepage = -1;

		while (low <= high) {
			mid = (high + low) / 2;
			cout<<"mid"<<endl;
			file.GetPage(&page, mid);
			page.GetFirst(&fetchme);
			result = comp.Compare(&fetchme, searchOrderMaker, &literal,
					literalOrderMaker);
			if (result < 0) {
				page.GetFirst(&fetchme);
				while ((result = comp.Compare(&fetchme, searchOrderMaker,
						&literal, literalOrderMaker)) < 0) {
					if (page.GetFirst(&fetchme) == 0)
						break;
				}
				if (result > 0)
					return 0;
				if (result == 0)
					break;
				low = mid + 1;
			} else if (result == 0) {
				onepage = mid;
				result = 1;
				high = mid - 1;
			} else {
				result = 1;
				high = mid - 1;
			}
		}
		if (result == 0) {

		} else if (onepage > -1) {
			//cout<<"onepage"<<endl;
			file.GetPage(&page, onepage);
			pageCounter = onepage;
			page.GetFirst(&fetchme);
		} else {
			page.EmptyItOut();
			binarysearch = 0;
			return 0;
		}
	} else {
		if (page.GetFirst(&fetchme) == 0) {
			if (pageCounter >= file.GetLength() - 2) {
				return 0;
			}
			pageCounter++;
			page.EmptyItOut();   //clean the cache
			//cout<<"pageCounter"<<endl;
			file.GetPage(&page, pageCounter);
			page.GetFirst(&fetchme);
		}
	}

	while (!comp.Compare(&fetchme, &literal, &cnf)) {
		// If next record is no longer in the range of records that matched the order maker,
		// there are no more records to return.
		if (comp.Compare(&fetchme, searchOrderMaker, &literal,
				literalOrderMaker) != 0) {
			return 0;
		}
		if (page.GetFirst(&fetchme) == 0) {
			if (pageCounter >= file.GetLength() - 2) {
				return 0;
			}
			pageCounter++;
			page.EmptyItOut();		//clean the cache
			//cout<<pageCounter<<endl;
			file.GetPage(&page, pageCounter);
			page.GetFirst(&fetchme);
		}
	}
	return 1;
}

DBFile::DBFile() {
}

//f_path is bin file storied path
int DBFile::Create(char *f_path, fType f_type, void *startup) {
	char *tempName = new char[100];
	// Create name for header file.
	sprintf(tempName, "%s.header", f_path);
	ofstream headerOut(tempName);
	// Save file type.
	headerOut << f_type << endl;

	if (f_type == heap) {
		myInternalVar = new DBHeap();

	} else if (f_type == sorted) {
		myInternalVar = new DBSorted(startup);
		SortInfo* info = (SortInfo *) startup;
		info->om->fWrite(headerOut);
		headerOut << info->runLength << endl;
	}
	headerOut.close();
	myInternalVar->fileName = f_path;
	myInternalVar->file.Open(0, f_path);
	return 1;

}

// bulk loads the DBFile instance from a text file, appending new data to it using the SuckNextRecord function from Record.h
//loadpath is tblfile path  ; GetPage is read from specified page and AddPage is write to the page

void DBFile::Load(Schema &f_schema, char *loadpath) {
	myInternalVar->Load(f_schema, loadpath);
}

int DBFile::Open(char *f_path) {
	char *tempName = new char[100];
	// Create name for header file.
	sprintf(tempName, "%s.header", f_path);
	//cout<<"open0"<<endl;
	ifstream headerInput(tempName);
	delete tempName;
	if (!headerInput.is_open()) {
		return 0;
	}
	// Get file type from file.
	int intType;
	headerInput >> intType;
	if (intType == 0) {
		myInternalVar = new DBHeap();
	} else if (intType == 1) {
		//cout<<"open11"<<endl;
		SortInfo *info = new SortInfo();
		// Read order maker in from file.
		info->om = new OrderMaker(headerInput);
		info->om->Print();

		headerInput >> info->runLength;

		myInternalVar = new DBSorted((void*) info);

	}
	myInternalVar->fileName = f_path;
	headerInput.close();
	myInternalVar->file.Open(1, f_path);

	myInternalVar->mode = read_data;
	myInternalVar->pageCounter = 0;
	return 1;
}

//get the first page in file
void DBFile::MoveFirst() {
	myInternalVar->MoveFirst();
}

int DBFile::Close() {
	//empty buffer_page
	int l = myInternalVar->Close();
	cout << "close" << endl;
	delete myInternalVar;
	return l;
}

//In heap file simply adds the new record to the end of the file
void DBFile::Add(Record &addme) {
	myInternalVar->Add(addme);
}

int DBFile::GetNext(Record &fetchme) {
	return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	return myInternalVar->GetNext(fetchme, cnf, literal);

}
