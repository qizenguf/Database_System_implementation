#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "Function.h"
#include "Pipe.h"
#include "RelOp.h"
#include <stdio.h>
#include <stdlib.h> 
#include <stdio.h>
#include <string.h> 

enum NodeType{SP, SF, P, J, DR, S, GB, WO};

struct Node {

	NodeType nodeType;
	//if id = -1, there is no pipe associated
	//inputPipe1 corresponds to the pipe connecting to left child
	//inputPipe2 corresponds to the pipe connecting to right child
	//if this node has left child, then inputPipe1 is not equal to -1
	//the same to inputPipe2
	int inputPipe1ID;
	int inputPipe2ID;
	int outPipeID;
	FILE * file;
	Schema *outputSchema;
	CNF *cnf;
	Record *literal;
	Function *fun;
	OrderMaker *order;
	//keepMe, numAttsInput, numAttsOutPut are for projection.
	int *keepMe;
	int numAttsToKeep;
	int numAttsInput;
	int numAttsOutput;
	//only leaf node has relName
	char* relName;


	Node *leftChild;
	Node *rightChild;

};


class QueryPlan {
private:
	int cost;
	Node *tree;
	Pipe **pipeArray;
	RelationalOp ** relArray;
public:
	QueryPlan(Node* node, int numPipes);
	~QueryPlan();
	Node* getRoot();
	int getCost();
	void executeNode(Node *node);
	//execute method always starts in a new thread
	void execute();
	void printPlan();
};

#endif 
