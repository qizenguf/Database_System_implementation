#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include "Record.h"
#include <cstdlib>
#include "DBFile.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern "C" struct AndList *final;

void askForCNF(DBFile& db, Schema& lineitem)
{
	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file


	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

    Record temp;
	
	while(db.GetNext(temp,myComparison,literal))
	{
		temp.Print (&lineitem);
	}
}

int main () {

	printf("Hello!\n");
	

	DBFile db;

	db.Create("data/db.db",heap,0);

	Schema lineitem ("data/catalog", "lineitem");

	db.Load(lineitem,"data/lineitem.tbl");

//	db.Open("data/db.db");

    Record temp,temp2;;
	db.MoveFirst();
	int i(0);
	while(db.GetNext(temp))
	{
		temp2.Consume(&temp);
		i++;
		if(i%1000==0)
			temp.Print (&lineitem);
	}
	temp.Print (&lineitem);
//	askForCNF(db,lineitem);
	printf("i=%d\n",i);
	db.Close();


	return 0;


	// now open up the text file and start procesing it

        FILE *tableFile = 0;
#ifdef _WIN32
		tableFile = fopen ("H:/tornado/xiaoke/work/dbi/Chris/data/lineitem.tbl", "r");
		
#else
		tableFile = fopen ("/cise/research57/xiaoke/work/dbi/tpch-dbgen/lineitem.tbl", "r");
#endif


        Schema mySchema ("../data/catalog", "lineitem");

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	//int counter = 0;
	//ComparisonEngine comp;
 //       while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
	//	counter++;
	//	if (counter % 10000 == 0) {
	//		cerr << counter << "\n";
	//	}

	//	if (comp.Compare (&temp, &literal, &myComparison))
 //               	temp.Print (&mySchema);

 //       }

}


