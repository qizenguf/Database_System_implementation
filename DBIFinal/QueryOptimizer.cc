#include <iostream>
#include "Function.h"
#include "ParseTree.h"
#include "QueryOptimizer.h"
using namespace std;
void findRels(AndList *parseTree, map<string, string> *attMap, vector<string> *rels) {

	set<string> relSet;
	for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
		if (parseTree == NULL) {
			break;
		}

		struct OrList *myOr = parseTree->left;

		for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
			if (myOr == NULL) {
				break;
			}

			if (myOr->left->left->code == NAME) {
				set<string>::iterator it = relSet.find(myOr->left->left->value);
				if (it == relSet.end()) {
					relSet.insert(myOr->left->left->value);
					rels->push_back(attMap->find(myOr->left->left->value)->second);
				}
			}
			if (myOr->left->right->code == NAME) {
				set<string>::iterator it = relSet.find(myOr->left->right->value);
				if (it == relSet.end()) {
					relSet.insert(myOr->left->right->value);
					rels->push_back(attMap->find(myOr->left->right->value)->second);
				}
			}

		}


	}
}
bool isJoin(AndList *andList) {

	while (andList != 0) {
		struct OrList *myOr = andList->left;
		if (myOr->rightOr != NULL) {
			return false;
		}
		ComparisonOp *myOp = myOr->left;
		if (myOp->code != EQUALS) {
			return false;
		}
		if ((myOp->left->code != NAME) || (myOp->right->code != NAME)) {
			return false;
		}
		andList = andList->rightAnd;
	}

	return true;

}
bool isSingleSel(AndList andList,char* tempRelName, map<string, string> *attMap){
	vector<string> tempRels;
	findRels(&andList, attMap, &tempRels);
	if(tempRels.size() == 1&&tempRels.at(0).compare(tempRelName)==0) return true;
		return false;
}

void PrintOperand1(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value;
        }
        else
                return;
}

void PrintComparisonOp1(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand1(pCom->left);
                switch(pCom->code)
                {
                        case 5:
                                cout<<" < "; break;
                        case 6:
                                cout<<" > "; break;
                        case 7:
                                cout<<" = "; break;
                }
                PrintOperand1(pCom->right);

        }
        else
        {
                return;
        }
}
void PrintOrList1(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp1(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList1(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList1(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList1(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList1(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}

AndList* combineAnd(AndList *andArray, int *whichAnd, int length, AndList *result) {
//	cout<<endl;
//	cout<<"In combine And"<<endl;
//	cout<<"Before combine"<<endl;
//	PrintAndList1(result);
	for (int i = length - 1; i >= 0; i--) {
		AndList *temp = new AndList();
		temp->rightAnd = result;
		temp->left = andArray[whichAnd[i]].left;
		result = temp;
//		cout<<endl;
	}
//	cout<<"After combine"<<endl;
//	PrintAndList1(result);
//	cout<<endl;
	return result;

}
void showValue(int value, int numAnds) {
	cout<<"value = " <<value;
	cout<<", Set = ";
	for (int i = 0; i < numAnds*numAnds; i++) {
		cout<<value%2 << " ";
		value = value/2;
	}
	cout<<endl;
}
void printMap(map<int, DPResult*> DPRMap) {
	map<int, DPResult*>::iterator it;
	for (it = DPRMap.begin(); it != DPRMap.end(); ++it) {
		cout<<"start*********************************************"<<endl;
		cout<<"value = "<< it->first<<endl;
		cout<<"optIndex = ";
		for (int i = 0; i < it->second->size; i++) {
			cout<<it->second->optIndex[i]<<" ";
		}
		cout<<endl;
		cout<<"cost = "<<it->second->cost<<endl;
		cout<<"estimate = "<<it->second->estimate<<endl;
		cout<<"rel names:";
		for (set<string>::iterator it2 = it->second->relNames.begin(); it2 != it->second->relNames.end(); ++it2) {
			cout<<*it2<<" ";
		}
		cout<<endl;
		cout<<"end***********************************************"<<endl;

	}
}
int MAXPATHLENGTH = 20;
QueryOptimizer::QueryOptimizer(char *statPath, char *catalogPath, char **relNames, int numRels) {

	this->statPath = new char[50];
	strcpy(this->statPath, statPath);
	this->catalogPath = new char[50];
	strcpy(this->catalogPath, catalogPath);
	for (int i = 0; i < numRels; i++) {
		Schema *tempSch = new Schema(catalogPath, relNames[i]);
		schemaMap.insert(pair<string, Schema*>(relNames[i], tempSch));
	}
}
QueryOptimizer::QueryOptimizer(char *statPath, char *catalogPath, TableList * Tables) {

	this->statPath = new char[50];
	strcpy(this->statPath, statPath);
	this->catalogPath = new char[50];
	strcpy(this->catalogPath, catalogPath);
	TableList * i = Tables;
	while(i){
		Schema *tempSch = new Schema(catalogPath, i->tableName);
		schemaMap.insert(pair<string, Schema*>(i->tableName, tempSch));
		i= i->next;
	}
}


QueryOptimizer::~QueryOptimizer() {
}
bool check(int index, int value) {
	for (int i = 0; i < index; i++) {
		value = value/2;
	}
	if (value%2 == 0) {
		return false;
	} else {
		return true;
	}
}
void QueryOptimizer::initialStats(Statistics *stat, char **tableNames, char **aliasAss, int numTables) {
	stat->Read(statPath);
	for (int i = 0; i < numTables; i++) {
		stat->CopyRel(tableNames[i], aliasAss[i]);
	}
}
void QueryOptimizer::DP(AndList *andArray, int numAnds, char **tableNames, char **aliasAss, int numTables, int *optimalIndex) {

	map<int, DPResult*> DPRMap;
	char *relNames[2*numTables];
	int count = 0;
	for (int j = 0; j < numTables; j++) {
		relNames[count] = tableNames[j];
		relNames[count+1] = aliasAss[j];
		count += 2;
	}

	int value = 1;
	for (int i = 0; i < numAnds; i++) {
		if (i != 0) {
			value = value * 2;
		}

		DPResult *dpr = new DPResult();
		dpr->cost = 0;
		dpr->stat = new Statistics();
		initialStats(dpr->stat, tableNames, aliasAss, numTables);
		dpr->estimate = dpr->stat->Estimate(&(andArray[i]), relNames, -1);
		dpr->stat->Apply(&(andArray[i]), relNames, -1);
		dpr->optIndex[0] = i;
		dpr->size = 1;
		vector<string> rels;
		findRels(&(andArray[i]), &(dpr->stat->att_m), &rels);
		for (vector<string>::iterator it = rels.begin(); it != rels.end(); it++) {
			dpr->relNames.insert(*it);
		}
		DPRMap.insert(pair<int, DPResult*>(value, dpr));
	}
//	printMap(DPRMap);

//	cout<<"size = "<< 1 <<endl;
//	printMap(DPRMap);
	for (int size = 2; size <= numAnds; size++) {
		for (int whichAnd = 0; whichAnd < numAnds; whichAnd++) {
			map<int, DPResult*>::iterator it;
			for (it = DPRMap.begin(); it != DPRMap.end(); ++it) {

				if (check(whichAnd, it->first) || it->second->size != size - 1) {
					continue;
				} else {

//					if (size == 3) {
//						cout<<"Not find!"<<endl;
//						cout<<"whichAnd = " << whichAnd <<endl;
//						showValue(it->first, numAnds);
//					}



					DPResult *dpr = new DPResult();
					dpr->size = size;
					dpr->stat = new Statistics();
					initialStats(dpr->stat, tableNames, aliasAss, numTables);

					vector<string> rels;


					findRels(&(andArray[whichAnd]), &(dpr->stat->att_m), &rels);
//					cout<<"rels:";
//					for (vector<string>::iterator i = rels.begin(); i != rels.end(); ++i) {
//						cout<<*i<<" ";
//					}
//					cout<<endl;
					bool checker = false;
					int count = 0;
					for (vector<string>::iterator it2 = rels.begin(); it2 != rels.end(); ++it2) {
						set<string>::iterator it3 = it->second->relNames.find(*it2);
						if (it3 != it->second->relNames.end()) {
							checker = true;
							count++;
//							cout<<"checker = true"<<endl;
						}
					}
					if (!checker) {
						continue;
					}

					if ((rels.size() > 1) && (count < rels.size()) && (!isJoin(&(andArray[whichAnd])))) {
						continue;
					}

					for (set<string>::iterator it4 = it->second->relNames.begin(); it4 != it->second->relNames.end(); ++it4) {
						dpr->relNames.insert(*it4);
					}
					for (vector<string>::iterator it5 = rels.begin(); it5 != rels.end(); ++it5) {
						dpr->relNames.insert(*it5);
					}

					AndList temp;
					temp.left = andArray[whichAnd].left;
					temp.rightAnd = 0;
					AndList *combine = combineAnd(andArray, it->second->optIndex, size - 1, &temp);
//					cout<<"get combine"<<endl;
//					PrintAndList1(combine);
//					cout<<endl;
					dpr->estimate = dpr->stat->Estimate(combine, relNames, -1);
//					cout<<"get estimate = " << dpr->estimate<<endl;
					dpr->stat->Apply(combine, relNames, -1);
					dpr->cost = it->second->cost + it->second->estimate;
//					cout<<"get cost = " << dpr->cost<<endl;
					for (int i = 0; i < size - 1; i++) {
						dpr->optIndex[i] = it->second->optIndex[i];
					}
					dpr->optIndex[size - 1] = whichAnd;

					int newValue = it->first + (1<<whichAnd);
//					cout<<"new value = " << newValue<<endl;
					map<int, DPResult*>::iterator it2 = DPRMap.find(newValue);
					if (it2 != DPRMap.end()) {
						if (dpr->cost < it2->second->cost) {
							DPRMap[newValue] = dpr;
						}
					} else {
						DPRMap[newValue] = dpr;
					}
				}
			}
		}
//		cout<<"size = "<< size <<endl;
//		printMap(DPRMap);
	}

	int finalValue = (1<<numAnds) - 1;
	map<int, DPResult*>::iterator it3 = DPRMap.find(finalValue);
	int *best = it3->second->optIndex;

	for (int i = 0; i < numAnds; i++) {
		optimalIndex[i] = best[i];
	}


}
//sch1 and sch2 have on shared attributes
void combineSchema(Schema *sch1, Schema *sch2, Schema *&combine2) {
	   Schema *combine = new Schema();
     combine->numAtts = sch1->GetNumAtts() + sch2->GetNumAtts();
	 	 combine->myAtts = new Attribute[combine->GetNumAtts()];
     cout<<"combine"<<endl;
	int k = 0;
	for(int j = 0 ; j < sch1->GetNumAtts() ; j++,k++)
	{
		combine->myAtts[k].myType = sch1->GetAtts()[j].myType;
		combine->myAtts[k].name = strdup (sch1->GetAtts()[j].name);    ///Make point to a duplicate of string
	}
   for(int j = 0 ; j < sch2->GetNumAtts() ; j++,k++)
	{
		combine->myAtts[k].myType = sch2->GetAtts()[j].myType;
	    combine->myAtts[k].name = strdup (sch2->GetAtts()[j].name);
	}
		combine2 = combine;

}

void projectSchema(Schema *sch1, Schema *&sch3, NameList *names, int *keepme, int attrnumtokeep)
{
	if(names == 0)
		{
			printf("receives null names\n");
			exit(0);
		}
		Schema * sch2 = new Schema();
		sch2->myAtts = new Attribute[attrnumtokeep];
    sch2->numAtts = attrnumtokeep;
    int k =0;
		for(NameList *i = names ; i; i = i->next,k++ )
		{
			int j = 0;
			for( j = 0 ; j < sch1->GetNumAtts() ; j++)
			{
				if(strcmp(sch1->GetAtts()[j].name,i->name) == 0)     //find attribute
					break;
			}
			if(j == sch1->GetNumAtts())
			{
				printf("Cannot find attribute in schema\n", i->name);
				exit(0);
			}
			sch2->myAtts[k].myType = sch1->GetAtts()[j].myType;
			sch2->myAtts[k].name = strdup (sch1->GetAtts()[j].name);

		}
		sch3 = sch2;

}

Node* QueryOptimizer::produceSFNode(char *relName, AndList *oneAnd) {

	Node *tree = new Node();
	CNF *cnf = new CNF();
	Record *literal = new Record();
	map<string, Schema*>::iterator it = schemaMap.find(relName);
	cnf->GrowFromParseTree(oneAnd, it->second, *literal);
	
	tree->nodeType = SF;
	//tree->inputPipe1ID = numPipes;
	tree->inputPipe1ID = -1;
	tree->inputPipe2ID = -1;
	tree->outPipeID = numPipes;
	numPipes++;
	tree->outputSchema = it->second;
	tree->cnf =cnf;
	tree->literal = literal;
	tree->relName = new char[40];
	string a(relName);
	map<string, string>::iterator it2 = aliasMap.find(relName);
	strcpy(tree->relName,it2->second.c_str());
	tree->leftChild = 0;
	tree->rightChild = 0;
	return tree;
}

Node* QueryOptimizer::produceSPNode(Node *leftChild, AndList *oneAnd) {

	Node *tree = new Node();
	CNF *cnf = new CNF();
	Record *literal = new Record();
	cnf->GrowFromParseTree(oneAnd, leftChild->outputSchema, *literal);
	
	tree->nodeType = SP;
	tree->outPipeID = numPipes;
	numPipes++;
	tree->inputPipe1ID = leftChild->outPipeID;
	tree->inputPipe2ID = -1;
	tree->outputSchema = leftChild->outputSchema;
	tree->cnf =cnf;
	tree->literal = literal;
	tree->leftChild = leftChild;
	tree->rightChild = 0;
	return tree;
}

Node* QueryOptimizer::producePNode(Node *leftChild, struct NameList *attsToSelect) {
	Node *tree = new Node();
	
	tree->nodeType = P;
	tree->inputPipe1ID = leftChild->outPipeID;
	tree->inputPipe2ID = -1;
	tree->outPipeID = numPipes;
	cout<<"proj"<<endl;
	//tree->outputSchema = leftChild->outputSchema;
	int numAttsOutput=0;
	for(NameList *i = attsToSelect; i; i = i->next )
			{
				numAttsOutput++;
			}
	int *keepMe = new int[numAttsOutput];
	NameList *i = attsToSelect;
	int t=0;
	int temp=0;
  while(i)
	{     temp = leftChild->outputSchema->Find(i->name);
		if(temp!=-1){
				keepMe[t] = temp;
				t++;
			}
			i = i->next;
	}						
	tree->numAttsInput = leftChild->outputSchema->GetNumAtts();
	tree->numAttsOutput = numAttsOutput;
  tree->keepMe= keepMe;
	projectSchema(leftChild->outputSchema,tree->outputSchema, attsToSelect, keepMe, numAttsOutput);
	tree->leftChild = leftChild;
	tree->rightChild = 0;
	numPipes++;
	return tree;
}

Node* QueryOptimizer::produceJNode(Node *leftChild, Node *rightChild, AndList *andList) {
	Node *tree = new Node();
	CNF *cnf = new CNF();
	Record *literal = new Record();
	cnf->GrowFromParseTree(andList, leftChild->outputSchema, rightChild->outputSchema, *literal);
	
	tree->nodeType = J;
	tree->inputPipe1ID = leftChild->outPipeID;
	tree->inputPipe2ID = rightChild->outPipeID;
	tree->outPipeID = numPipes;
	cout<<"hah"<<endl;
	combineSchema(leftChild->outputSchema, rightChild->outputSchema, tree->outputSchema);
	tree->cnf =cnf;
	tree->literal = literal;
	tree->leftChild = leftChild;
	tree->rightChild = rightChild;
	numPipes++;
	return tree;
}

Node* QueryOptimizer::produceDRNode(Node *leftChild) {
	Node *tree = new Node();
	
	tree->nodeType = DR;
	tree->inputPipe1ID = leftChild->outPipeID;
	tree->inputPipe2ID = -1;
	tree->outPipeID = numPipes;
	tree->outputSchema = leftChild->outputSchema;
	tree->leftChild = leftChild;
	tree->rightChild = 0;
	numPipes++;
	return tree;
}

Schema *getSSchema() {
	Schema *outputSchema = new Schema();
	outputSchema->numAtts= 1;
	outputSchema->myAtts= new Attribute[1];
	outputSchema->myAtts[0].myType = Double;
	outputSchema->myAtts[0].name = strdup("sum");
	return outputSchema;
}
Schema *getGBSchema(Schema *inputSchema, NameList *groupingAtts, int * keepMe,int attrnumtokeep) {
	Schema *outputSchema = new Schema();
	Schema *sch1;
	sch1->numAtts = 1;
	sch1->myAtts = new Attribute[1];
	sch1->myAtts[0].myType = Double;
	sch1->myAtts[0].name = strdup("sum");

	Schema *sch2;
	projectSchema(inputSchema,sch2,groupingAtts,keepMe,attrnumtokeep);
	combineSchema(sch1,sch2,outputSchema);
	return outputSchema;


}
Node* QueryOptimizer::produceSNode(Node *leftChild, FuncOperator *finalFunction) {
	Node *tree = new Node();
	Function *fun = new Function();
	fun->GrowFromParseTree(finalFunction, *(leftChild->outputSchema));
	cout<<"die in sum"<<endl;
	tree->nodeType = S;
	tree->inputPipe1ID = leftChild->outPipeID;
	tree->inputPipe2ID = -1;
	tree->outPipeID = numPipes;
	numPipes++;
	tree->outputSchema = getSSchema();
	cout<<"die in sum"<<endl;
	tree->fun = fun;
	tree->leftChild = leftChild;
	tree->rightChild = 0;
	return tree;
}
Node* QueryOptimizer::produceGBNode(Node *leftChild, NameList *groupingAtts, FuncOperator *finalFunction) {

	Node *tree = new Node();
	
	tree->nodeType = GB;
	tree->inputPipe1ID = leftChild->outPipeID;
	tree->inputPipe2ID = -1;
	tree->outPipeID = numPipes;
	numPipes++;
	int numAttsOutput=0;
	for(NameList *i = groupingAtts; i; i = i->next )
			{
				numAttsOutput++;
			}
	int* keepMe = new int[numAttsOutput];
	NameList *i = groupingAtts;
	int t=0;
	int temp=0;
  while(i)
	{     
		temp = leftChild->outputSchema->Find(i->name);
		if(temp!=-1){
				keepMe[t] = temp;
				t++;
			}
			i = i->next;
	}	
	OrderMaker *gOrder = new OrderMaker();
	gOrder->numAtts = t;
	Function * func = new Function();
	func->GrowFromParseTree(finalFunction, *leftChild->outputSchema);
	for(int i = 0 ; i < gOrder->numAtts; i++)
			{
				gOrder->whichAtts[i] = keepMe[i];
				gOrder->whichTypes[i] = leftChild->outputSchema->myAtts[keepMe[i]].myType;
			}
	tree->outputSchema = getGBSchema(leftChild->outputSchema,groupingAtts,keepMe,t-1);
	tree->order = gOrder;
	tree->fun = func;
	tree->keepMe = keepMe;
	tree->leftChild = leftChild;
	tree->rightChild = 0;
	return tree;
}
 Node* QueryOptimizer::produceWONode(Node *leftChild,FILE * file){
	Node * tree = new Node();
	//numPipes++;
tree->nodeType = WO;
tree->inputPipe1ID = leftChild->outPipeID;
tree->inputPipe2ID = -1;
tree->outPipeID = numPipes;
tree->outputSchema = leftChild->outputSchema;
tree->leftChild = leftChild;
tree->rightChild = 0;
tree->file=file;
return tree;
}
//we can make sure that there are only two relations in
//andList before calling isJoin
//andList only has one and

Node* QueryOptimizer::produceTree(SQL &mySQL, AndList *optAndArray, int numAnds, Statistics *stat) {
  Node *tree = new Node();
	numPipes = 0;
	set<string> relSet;
	//map<string, AndList*> selectAllMap;
	int i = 0;
	while (i < numAnds) {
		vector<string> tempRels;
		findRels(&(optAndArray[i]), &(stat->att_m), &tempRels);
		if (tempRels.size() == 1) {
			set<string>::iterator tempIt = relSet.find(tempRels.at(0));
			if (tempIt != relSet.end()) {
				//this must be a SP node
				
				Node *node = produceSPNode(tree, &(optAndArray[i]));
				tree = node;
				i++;
				continue;
			} else {
				if (relSet.size() == 0) {
					//this should be the first node in the tree
					//with only one relation added
					//this must be a SF node
					relSet.insert(tempRels.at(0));
					char *tempRelName = new char[30];
					strcpy(tempRelName, tempRels.at(0).c_str());
					//tree = produceSFNode(tempRelName, &(optAndArray[i]));
					tree = produceSFNode(tempRelName, &(optAndArray[i]));
					cout<<tempRels.at(i)<<endl;
					//Node *node = produceSPNode(tree, &(optAndArray[i]));
					//tree = node;
					i++;
					continue;
				} else {
					//followed by a join
					relSet.insert(tempRels.at(0));
					char *tempRelName = new char[30];
					strcpy(tempRelName, tempRels.at(0).c_str());
					Node *rightNode = produceSFNode(tempRelName, &(optAndArray[i]));
					i++;
					Node *temp = produceJNode(tree, rightNode, &(optAndArray[i]));
					i++;
					tree = temp;
					continue;
				}
			}
		}

		if (tempRels.size() == 2) {
			set<string>::iterator tempIt1 = relSet.find(tempRels.at(0));
			set<string>::iterator tempIt2 = relSet.find(tempRels.at(1));
			if ((tempIt1 == relSet.end()) && (tempIt2 == relSet.end())) {
				//this must be the first node in the tree
				//need to contruct two SF node and a join(in default)
				//AndList *selectLeft = selectAllMap.find(tempRels.at(0));
				//AndList *selectRight = selectAllMap.find(tempRels.at(1));
				char *tempRelName1 = new char[30];
				strcpy(tempRelName1, tempRels.at(0).c_str());
				char *tempRelName2 = new char[30];
				strcpy(tempRelName2, tempRels.at(1).c_str());
				//Node *leftNode = produceSFNode(tempRelName1, selectLeft);
				int t=i+1;
				//Node *rightNode = produceSFNode(tempRelName2, selectRight);
				 Node *leftNode = produceSFNode(tempRelName1, NULL);
			   Node * rightNode;
			   cout<<"LOL"<<endl;
				if(t<numAnds&&isSingleSel(optAndArray[t],tempRelName2,&(stat->att_m))){
					rightNode = produceSFNode(tempRelName2, &(optAndArray[t]));
					t++;
				while(t<numAnds && isSingleSel(optAndArray[t],tempRelName2,&(stat->att_m))){
					rightNode = produceSPNode(rightNode, &(optAndArray[t]));
				t++;
				}
				}else rightNode = produceSFNode(tempRelName2, NULL);
				cout<<"LOL bj"<<endl;
				
				tree = produceJNode(leftNode, rightNode, &(optAndArray[i]));
				cout<<"LOLjoin"<<endl;
				i=t;
				relSet.insert(tempRels.at(0));
				relSet.insert(tempRels.at(1));
				//i++;
				continue;
			}

			if ((tempIt1 != relSet.end()) && (tempIt2 != relSet.end())) {
				//this must be a select
				Node *node = produceSPNode(tree, &(optAndArray[i]));
				tree = node;
				i++;
				continue;
			}

			if ((tempIt1 == relSet.end()) && (tempIt2 != relSet.end())) {
				// this should be a join
				char *tempRelName = new char[30];
				strcpy(tempRelName, tempRels.at(0).c_str());
				//AndList *selectRight = selectAllMap.find(tempRels.at(0));
				//Node *rightNode = produceSFNode(tempRelName, selectRight);
				//Node *rightNode = produceSFNode(tempRelName,NULL);
				Node * rightNode;
				int t=i+1;
				if(t<numAnds&&isSingleSel(optAndArray[t],tempRelName,&(stat->att_m))){
					rightNode = produceSFNode(tempRelName, &(optAndArray[t]));
					t++;
				while(t<numAnds && isSingleSel(optAndArray[t],tempRelName,&(stat->att_m))){
					rightNode = produceSPNode(rightNode, &(optAndArray[t]));
				t++;
				}
			}else rightNode = produceSFNode(tempRelName, NULL);
				relSet.insert(tempRels.at(0));
				Node *tempNode = produceJNode(tree, rightNode, &(optAndArray[i]));
				tree = tempNode;
				i=t;
				continue;

			}
			if ((tempIt1 != relSet.end()) && (tempIt2 == relSet.end())) {
				// this should be a join
				char *tempRelName = new char[20];
				strcpy(tempRelName, tempRels.at(1).c_str());
				//AndList *selectRight = selectAllMap.find(tempRels.at(1));
				//Node *rightNode = produceSFNode(tempRelName, selectRight);
				//Node *rightNode = produceSFNode(tempRelName,NULL);
				int t=i+1;
				Node * rightNode;
				if(t<numAnds&&isSingleSel(optAndArray[t],tempRelName,&(stat->att_m))){
					rightNode = produceSFNode(tempRelName, &(optAndArray[t]));
					t++;
				while(t<numAnds && isSingleSel(optAndArray[t],tempRelName,&(stat->att_m))){
					rightNode = produceSPNode(rightNode, &(optAndArray[t]));
				t++;
				}
			}else rightNode = produceSFNode(tempRelName, NULL);
				relSet.insert(tempRels.at(1));
				Node *tempNode = produceJNode(tree, rightNode, &(optAndArray[i]));
				tree = tempNode;
				i=t;
				continue;

			}

		}

//add select pipe node on top of each join node
		//no groupby/sum
		
		}
		if(!mySQL.finalFunction){

			//To do: push project down
			if(mySQL.attsToSelet){
			Node *project = producePNode(tree, mySQL.attsToSelet);
			tree = project;
			}
			if(mySQL.distinctAtts)
			{
				Node *dr= produceDRNode(tree);
				tree=dr;
			}

		}
		else
		{	//if we have group by or sum
		   //Node *project=producePNode(tree, mySQL.attsToSelet);
		  // tree=project;
			if(mySQL.groupingAtts)
			{
				//if we have group by
				Node *gb=produceGBNode(tree,mySQL.groupingAtts,mySQL.finalFunction);
				tree=gb;		
			 
      }else{
      	cout<<"sum here"<<endl;
			  Node *s=produceSNode(tree, mySQL.finalFunction);
        tree=s;
        if(mySQL.distinctFunc)
			 {
				 Node *drf=produceDRNode(tree);
				 tree=drf;
			 }
      }


	}
	Node *w = produceWONode(tree,mySQL.file);
	tree =w;
	return tree;

}
Node* QueryOptimizer::optimize(SQL& mySQL) {

	AndList *iterator = mySQL.boolean;
	int numAnds = 1;
	while (iterator->rightAnd != 0) {
		numAnds++;
		iterator = iterator->rightAnd;
	}
	AndList *list = mySQL.boolean;
	AndList andArray[numAnds];
	for (int i = 0; i < numAnds; i++) {
		OrList *orlist = list->left;
		andArray[i].left = orlist;
		andArray[i].rightAnd = 0;
		list = list->rightAnd;
	}

	int optimalIndex[numAnds];
	TableList *iterator2 = mySQL.tables;
	int numTables = 1;
	while (iterator2->next != 0) {
		numTables++;
		iterator2 = iterator2->next;
	}

	char *tableNames[numTables];
	char *aliasAss[numTables];
	TableList *tbList = mySQL.tables;
	for (int i = 0; i < numTables; i++) {
		tableNames[i] = tbList->tableName;
		aliasAss[i] = tbList->aliasAs;
		Schema *sch_alias = new Schema(catalogPath,tableNames[i],aliasAss[i]);
		schemaMap.insert(pair<string, Schema*>(aliasAss[i], sch_alias));
		aliasMap.insert(pair<string, string> (aliasAss[i],tableNames[i]));
		tbList = tbList->next;
	}

//	cout<<"Before DP:"<<endl;
//	Statistics s;
//	Statistics s2;
//	s.Read(statPath);
//	s2.Read(statPath);
//	for (int i = 0; i < numTables; i++) {
//			s.CopyRel(tableNames[i], aliasAss[i]);
//			s2.CopyRel(tableNames[i], aliasAss[i]);
//	}
//	char *relNames[2*numTables];
//		int count = 0;
//
//	for (int j = 0; j < numTables; j++) {
//		relNames[count] = tableNames[j];
//		relNames[count+1] = aliasAss[j];
//		count += 2;
//	}
//	for (int k = 0; k < numAnds; k++) {
//		cout<<"Sepearte Estimate = " <<s2.Estimate(&(andArray[k]), relNames, -1)<<endl;
//		s2.Apply(&(andArray[k]), relNames, -1);
//	}
//	s2.Apply(mySQL.boolean, relNames, -1);
//	cout<<"Estimate total= " << s.Estimate(mySQL.boolean, relNames, -1)<<endl;
	//cout<<"LOL"<<endl;
	DP(andArray, numAnds, tableNames, aliasAss, numTables, optimalIndex);
	
	AndList optimalAndArray[numAnds];
	for (int i = 0; i < numAnds; i++) {
		optimalAndArray[i] = andArray[optimalIndex[i]];
	}
	cout<<"optimal index:"<<endl;
	for (int i = 0; i < numAnds; i++) {
		cout<<optimalIndex[i]<<endl;
	}
	cout<<"optimal AndList:"<<endl;
	for (int i = 0; i < numAnds; i++) {
		PrintAndList1(&(andArray[optimalIndex[i]]));
		cout<<" ";
	}
	cout<<endl;

	Statistics stat;
	initialStats(&stat, tableNames, aliasAss, numTables);
	//cout<<"LOL"<<endl;
	Node* tree = produceTree(mySQL, optimalAndArray, numAnds, &stat);
	return tree;
}
