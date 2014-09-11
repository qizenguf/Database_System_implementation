/*
 * QueryPlan.cpp
 *
 *  Created on: Apr 24, 2013
 *      Author: eric
 */

#include "QueryPlan.h"
#include<iostream>

using namespace std;

int BUFFERSIZE = 100;
QueryPlan::QueryPlan(Node* node, int numPipes){
	tree = node;
	pipeArray = new Pipe*[numPipes];
	relArray = new RelationalOp*[numPipes+1];
}
QueryPlan::~QueryPlan(){
	delete	[]pipeArray;
	delete []relArray;
}
Node* QueryPlan::getRoot() {
	return tree;
}

int QueryPlan::getCost() {
	return cost;
}
//the node put the resulting tuples into outPipe
void QueryPlan::executeNode(Node *node) {

	//Pipe inputPipe1(BUFFERSIZE);


SelectFile *sf;
SelectPipe *sp ;
Project *p;
Join *j;
DuplicateRemoval *dr;
Sum *s;
GroupBy *by;
WriteOut *wo;
DBFile *file;
char *filePath;
	switch(node->nodeType) {
	case SF:
		sf = new SelectFile;
		file = new DBFile();
		filePath = new char[50];
		memset(filePath,'\0',50);
		strcat(filePath,"Tables/");
		strcat(filePath, node->relName);
		strcat(filePath, ".bin");
		cout<<filePath<<endl;
		file->Open(filePath);
		cout<<filePath<<" is open"<<endl;
		sf->Run(*file, *pipeArray[node->outPipeID], *(node->cnf), *(node->literal));
		cout<<filePath<<" is running"<<endl;
		relArray[node->outPipeID] = sf;
		break;

	case SP:
		sp = new SelectPipe;
		if (node->leftChild != 0) {
			executeNode(node->leftChild);
			sp->Run(*pipeArray[node->inputPipe1ID],*pipeArray[node->outPipeID], *(node->cnf), *(node->literal));
		} else {
			executeNode(node->rightChild);
		    sp->Run(*pipeArray[node->inputPipe2ID], *pipeArray[node->outPipeID], *(node->cnf), *(node->literal));
		}
		relArray[node->outPipeID] = sp;
		break;
	case P:
		p = new Project;
		if (node->leftChild != 0) {
			executeNode(node->leftChild);
			p->Run(*pipeArray[node->inputPipe1ID],*pipeArray[node->outPipeID], node->keepMe, node->numAttsInput, node->numAttsOutput);
		} else {
			executeNode(node->rightChild);
			p->Run(*pipeArray[node->inputPipe2ID],*pipeArray[node->outPipeID], node->keepMe, node->numAttsInput, node->numAttsOutput);
		}
		relArray[node->outPipeID] = p;
		break;

	case J:
		j = new Join;
		executeNode(node->leftChild);
		executeNode(node->rightChild);
		cout<<"join 1 ok"<<endl;
		j->Use_n_Pages(200);
		j->Run(*pipeArray[node->inputPipe1ID], *pipeArray[node->inputPipe2ID],*pipeArray[node->outPipeID], *(node->cnf), *(node->literal));
                cout<<"join ok"<<endl;
		relArray[node->outPipeID] = j;
		break;

	case DR:
		dr = new DuplicateRemoval;
		if (node->leftChild != 0) {
			executeNode(node->leftChild);
			dr->Use_n_Pages(200);
			dr->Run(*pipeArray[node->inputPipe1ID],*pipeArray[node->outPipeID], *(node->outputSchema));
		} else {
			executeNode(node->rightChild);
			dr->Use_n_Pages(200);
			dr->Run(*pipeArray[node->inputPipe2ID],*pipeArray[node->outPipeID], *(node->outputSchema));
		}
		relArray[node->outPipeID] = dr;
		break;

	case S:
		s = new Sum;
		if (node->leftChild != 0) {
			executeNode(node->leftChild);
			s->Run(*pipeArray[node->inputPipe1ID],*pipeArray[node->outPipeID], *(node->fun));
		} else {
			executeNode(node->rightChild);
			s->Run(*pipeArray[node->inputPipe2ID],*pipeArray[node->outPipeID], *(node->fun));
		}
		relArray[node->outPipeID] = s;
		break;

	case GB:
		by = new GroupBy;
		if (node->leftChild != 0) {
			executeNode(node->leftChild);
			by->Use_n_Pages(200);
			by->Run(*pipeArray[node->inputPipe1ID],*pipeArray[node->outPipeID],*(node->order), *(node->fun));
		} else {
			executeNode(node->rightChild);
			by->Use_n_Pages(200);
			by->Run(*pipeArray[node->inputPipe2ID],*pipeArray[node->outPipeID],*(node->order), *(node->fun));
		}
		relArray[node->outPipeID] = by;
		break;
	case WO:
		wo = new WriteOut;
		 if (node->leftChild != 0) {
			executeNode(node->leftChild);
			wo->Run(*pipeArray[node->inputPipe1ID],node->file, *(node->outputSchema));
		}
		relArray[node->outPipeID] = wo;
		break;
	}

}
//the query plan put the resulting tuples into outPipe
void QueryPlan::execute() {
	cout<<"pipes = "<<tree->outPipeID<<endl;
	for(int i=0; i<(tree->outPipeID);i++){
		pipeArray[i] = new Pipe(BUFFERSIZE);
}
	executeNode(tree);
	for(int i=0;i<(tree->outPipeID+1);i++){
		relArray[i]->WaitUntilDone();
	}
	cout << " cool! query finished!"<<endl;
}
void inOrderTrav(Node *node) {

	if (node->leftChild != 0) {
		cout<<node->nodeType<<endl;
		inOrderTrav(node->leftChild);
		//cout<<node->nodeType<<endl;
	}
	cout<<"*****************************"<<endl;
	switch (node->nodeType) {
	case SP:
		cout<<"Select pipe Operation:"<<endl;
		break;

	case SF:
		cout<<"Select File Operation:"<<endl;
		break;

	case P:
		cout<<"Project Operation:"<<endl;
		break;

	case J:
		cout<<"Join Operation:"<<endl;
		break;

	case DR:
		cout<<"DuplicateRemoval Operation:"<<endl;
		break;

	case S:
		cout<<"SUM Operation:"<<endl;
		break;
	case GB:
		cout<<"Group By Operation:"<<endl;
		break;
	case WO:
		cout<<"Write Out Operation:"<<endl;
	break;
	}

	if (node->inputPipe1ID != -1) {
		cout<<"Input pipe ID "<<node->inputPipe1ID<<endl;
	}
	if (node->inputPipe2ID != -1) {
		cout<<"Input pipe ID "<<node->inputPipe2ID<<endl;
	}
	cout<<"Output pipe ID "<<node->outPipeID<<endl;
	cout<<"Output Schema:"<<endl;

	Attribute *atts = node->outputSchema->GetAtts();
	int numAtts = node->outputSchema->GetNumAtts();
	for (int i = 0; i < numAtts; i++) {
		cout<<atts[i].name<<": "<<atts[i].myType<<endl;
	}

	switch (node->nodeType) {

	case SP:
		cout<<"Select pipe CNF:"<<endl;
		node->cnf->Print();
		break;

	case SF:
		cout<<"Select file CNF:"<<endl;
		node->cnf->Print();
		break;

	case P:
		cout<<"Attributes to keep:"<<endl;
		for (int i = 0; i < node->numAttsToKeep; i++) {
			cout<<atts[i].name<<": "<<atts[i].myType<<endl;
		}
		break;

	case J:
		cout<<"Join CNF"<<endl;
		node->cnf->Print();
		break;

	case S:
		cout<<"Function:"<<endl;
		node->fun->Print();
		break;


	case GB:
		cout<<"OrderMaker:"<<endl;
		node->order->Print();
		break;

	}
	cout<<"*****************************"<<endl;
	if (node->rightChild != 0) {
		inOrderTrav(node->rightChild);
	}
}


void QueryPlan::printPlan() {
	cout<<"Query Plan:"<<endl;
	inOrderTrav(tree);
}


