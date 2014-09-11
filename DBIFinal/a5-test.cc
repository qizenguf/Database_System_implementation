#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "SQLParser.h"
#include "QueryPlan.h"
#include "QueryOptimizer.h"

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

using namespace std;

extern "C" {
    int yyparse(void);   // defined in y.tab.c
}


//output format: 0 for printing to the screen
//                 1 for writing result to file
//                 2 for showing query plan
int out_format=0;

//file name of the output file
string out_file="a";


int main () {


    while(1)
    {
        cout<<"SQL>";
        
        string sql_command;
        while(char c=getchar())
        {

            if(c!=';')
            {
                if(c=='\n')
                    c=' ';
                sql_command+=c;
            }
            else
            {
                c=getchar();
                break;
            }
        }
      //  sql_command+=";";

        cout<<sql_command<<endl;

        if (sql_command=="EXIT;")
            break;

        else if (sql_command.find("SET OUTPUT")!=sql_command.npos)
        {
            string out_format_str=sql_command.substr(11,sql_command.length()-11);
            cout<<out_format_str<<endl;
            if(sql_command.find("STDOUT")!=sql_command.npos)
                out_format=0;
            else if(sql_command.find("NONE")!=sql_command.npos)
                out_format=1;
            else if(out_format_str.at(0)=='\''&&out_format_str.at(out_format_str.length()-1)=='\'')
            {
                out_format=2;
                cout<<out_file<<endl;
                out_file=out_format_str.substr(1,out_format_str.length()-2);
                cout<<out_file<<endl;
                
            }
            else
                cout<<"Please give the correct command!";
            
        }

        else if(sql_command.find("SELECT")!=sql_command.npos)
        {
            yy_scan_string(sql_command.c_str());
    				yyparse();
    				
    				struct SQL mySQL;
						mySQL.attsToSelet = attsToSelect;
						mySQL.boolean = boolean;
						mySQL.distinctAtts = distinctAtts;
						mySQL.distinctFunc = distinctFunc;
						mySQL.finalFunction = finalFunction;
						mySQL.groupingAtts = groupingAtts;
						mySQL.tables = tables;
						if(out_format == 0 ){
						mySQL.file = stdout;
						}
						else if(out_format == 2){
						mySQL.file = fopen(out_file.c_str(), "w");
						}
						else{
							QueryOptimizer optimizer("Statistics.txt","catalog",mySQL.tables );
						//optimizer.optimize(mySQL);
						Node * tree = optimizer.optimize(mySQL);
						QueryPlan QP(tree,optimizer.getNumPipes());
						QP.printPlan();
						continue;
						}
						//	PrintAndList(boolean);
						
						QueryOptimizer optimizer("Statistics.txt","catalog",mySQL.tables );
						//optimizer.optimize(mySQL);
						Node * tree = optimizer.optimize(mySQL);
						QueryPlan QP(tree,optimizer.getNumPipes());
						QP.execute();
						//QP.printPlan();
						
        }

        
        else{

            SQLParser sp(sql_command);
            sp.parse();
    
//    SQLParser sp1("CREATE TABLE mytable (att1 INTEGER , att2 DOUBLE , att3 STRING) AS HEAP;");

//    sp1.parse();

//    SQLParser sp2("CREATE TABLE mytable (att1 INTEGER , att2 DOUBLE , att3 STRING) AS SORTED ON att1, att2;");

//    sp2.parse();


//    SQLParser sp3("  DROP TABLE mytable ;");

//    sp3.parse();

//    SQLParser sp4("INSERT 'myfile' INTO mytable;");

//    sp4.parse();
        }


    }


    
    

    return 0;




}



