#ifndef QUERYOPTIMIZER_H_
#define QUERYOPTIMIZER_H_

#include <vector>
#include <set>
#include <string.h>
#include <map>
#include <iostream>
#include "Statistics.h"
#include "QueryPlan.h"
#include <stdio.h>
#include <stdlib.h> 
using namespace std;
struct SQL{

	FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
	TableList *tables;
	AndList *boolean;
	NameList *groupingAtts;
	NameList *attsToSelet;
	int distinctAtts;  // 1 if there is a DISTINCT in a non-aggregate query
	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
  FILE * file;
};

struct DPResult{
	set<string> relNames;
	int optIndex[20];
	double cost;
	double estimate;
	Statistics *stat;
	int size;
};
class QueryOptimizer {
private:
	char *statPath;
	char *catalogPath;
	Statistics myStat;
	map<string, Schema*> schemaMap;
	map<string, AndList*> selectAllMap;
	map<string, string> aliasMap;
	int numPipes;
	void DP(AndList *andArray, int numAnds, char **tableNames, char **aliasAss, int numTables, int *optimalIndex);
	Node * produceTree(SQL &mySQL, AndList *optAndArray, int numAnds, Statistics *stats);
	void initialStats(Statistics *stat, char **tableNames, char **aliasAss, int numTables);
	Node *produceSFNode(char *relName, AndList *oneAnd);
	Node *produceSPNode(Node *leftChild, AndList *oneAnd);
	Node *produceJNode(Node *leftChild, Node *rightChild, AndList *andList);
	Node *producePNode(Node *leftChild, NameList *attsToSelet);
	Node *produceDRNode(Node *leftChild);
	Node *produceSNode(Node *leftChild, FuncOperator *finalFunction);
	Node *produceGBNode(Node *leftChild, NameList *a, FuncOperator *finalFunction);
	Node *produceWONode(Node *leftChild,FILE * file);
public:
	QueryOptimizer(char *statPath, char *catalogPath, char **relNames, int numRels);
	QueryOptimizer(char *statPath, char *catalogPath, TableList * Tables);
	~QueryOptimizer();
	//put the optimal plan to bestPlan
	Node* optimize(SQL &mySQL);
	int getNumPipes(){return numPipes;};

};

#endif 
