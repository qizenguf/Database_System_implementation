#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <iostream>
#include <vector>
#include <map>

using namespace std;

struct rel{
	vector<string>relNames;
	int numTuples;
	vector<string>attName;
	int attNum;
	int relNum;
	int ID;
};

class Statistics
{
private:

public:
	map<string, int> rel_m;
	map<string,string> att_m;
	map<string,int> att_DisNum;
	vector<rel*> rel_v;
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	bool CheckParseError(struct AndList *parseTree,char *relNames[],int numToJoin);
	int CheckRelError(char *relNames[],int numToJoin);
};

#endif
