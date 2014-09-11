#include <iostream>
#include <stdlib.h>
#include "QueryOptimizer.h"
#include "QueryPlan.h"

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


void PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value;
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case 5:
                                cout<<" < "; break;
                        case 6:
                                cout<<" > "; break;
                        case 7:
                                cout<<" = "; break;
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}
void PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}

char *fileName = "Statistics.txt";
char *relName[] = {"supplier",
	"partsupp","lineitem","orders",
	"customer","nation",
	"part", "region"};
void t0() {
	Statistics s;
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);
	s.AddAtt(relName[0], "s_name",10000);
	s.AddAtt(relName[0], "s_address",10000);
	s.AddAtt(relName[0], "s_nationkey",25);
	s.AddAtt(relName[0], "s_phone",10000);
	s.AddAtt(relName[0], "s_acctbal",9955);
	s.AddAtt(relName[0], "s_comment",10000);


	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey", 200000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);
	s.AddAtt(relName[1], "ps_availqty", 9999);
	s.AddAtt(relName[1], "ps_supplycost", 99865);
	s.AddAtt(relName[1], "ps_comment", 799124);



	s.AddRel(relName[2],6001215);
	s.AddAtt(relName[2], "l_orderkey",1500000);
	s.AddAtt(relName[2], "l_partkey",200000);
	s.AddAtt(relName[2], "l_suppkey",10000);
	s.AddAtt(relName[2], "l_linenumber",7);
	s.AddAtt(relName[2], "l_quantity",50);
	s.AddAtt(relName[2], "l_extendedprice",933900);
	s.AddAtt(relName[2], "l_discount",11);
	s.AddAtt(relName[2], "l_tax",9);
	s.AddAtt(relName[2], "l_returnflag",3);
	s.AddAtt(relName[2], "l_linestatus",2);
	s.AddAtt(relName[2], "l_shipdate",2526);
	s.AddAtt(relName[2], "l_commitdate",2466);
	s.AddAtt(relName[2], "l_receiptdate",2554);
	s.AddAtt(relName[2], "l_shipinstruct",4);
	s.AddAtt(relName[2], "l_shipmode",7);
	s.AddAtt(relName[2], "l_comment",4580667);


	s.AddRel(relName[3],1500000);
	s.AddAtt(relName[3], "o_orderkey",1500000);
	s.AddAtt(relName[3], "o_custkey",99996);
	s.AddAtt(relName[3], "o_orderstatus",3);
	s.AddAtt(relName[3], "o_totalprice",1464556);
	s.AddAtt(relName[3], "o_orderdate",2406);
	s.AddAtt(relName[3], "o_orderPriority",5);
	s.AddAtt(relName[3], "o_clerk",1000);
	s.AddAtt(relName[3], "o_shipPriority",1);
	s.AddAtt(relName[3], "o_comment",1482071);



	s.AddRel(relName[4],150000);
	s.AddAtt(relName[4], "c_custkey",150000);
	s.AddAtt(relName[4], "c_name",150000);
	s.AddAtt(relName[4], "c_address",150000);
	s.AddAtt(relName[4], "c_nationkey",25);
	s.AddAtt(relName[4], "c_phone",150000);
	s.AddAtt(relName[4], "c_acctbal",140187);
	s.AddAtt(relName[4], "c_mktsegment",5);
	s.AddAtt(relName[4], "c_comment",149968);


	s.AddRel(relName[5],25);
	s.AddAtt(relName[5], "n_nationkey",25);
	s.AddAtt(relName[5], "n_name",25);
	s.AddAtt(relName[5], "n_regionkey",5);
	s.AddAtt(relName[5], "n_comment",25);

	s.AddRel(relName[6],200000);
	s.AddAtt(relName[6], "p_partkey",200000);
	s.AddAtt(relName[6], "p_name", 199996);
	s.AddAtt(relName[6], "p_mfgr",5);
	s.AddAtt(relName[6], "p_brand",25);
	s.AddAtt(relName[6], "p_type",150);
	s.AddAtt(relName[6], "p_size",50);
	s.AddAtt(relName[6], "p_container",40);
	s.AddAtt(relName[6], "p_retailprice",20899);
	s.AddAtt(relName[6], "p_comment",131753);


	s.AddRel(relName[7],5);
	s.AddAtt(relName[7], "r_name",5);
	s.AddAtt(relName[7], "r_regionkey",5);
	s.AddAtt(relName[7], "r_comment",5);

	s.Write(fileName);
}
void t(char *cnf) {
	yy_scan_string(cnf);
	yyparse();
	struct SQL mySQL;
	mySQL.attsToSelet = attsToSelect;
	mySQL.boolean = boolean;
	mySQL.distinctAtts = distinctAtts;
	mySQL.distinctFunc = distinctFunc;
	mySQL.finalFunction = finalFunction;
	mySQL.groupingAtts = groupingAtts;
	mySQL.tables = tables;
	mySQL.file = stdout;
	cout<<"AndList:"<<endl;
	PrintAndList(boolean);
	cout<<endl;
	QueryOptimizer optimizer("Statistics.txt","catalog",mySQL.tables);
	Node *tree = optimizer.optimize(mySQL);
	QueryPlan QP(tree,optimizer.getNumPipes());
	QP.printPlan();
	QP.execute();

}


int main(int argc, char *argv[]) {
	t0();
	char *cnf1 = "SELECT SUM (ps.ps_supplycost) FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0)";
	//t(cnf1);


	char *cnf2 = "SELECT SUM (c.c_acctbal) FROM customer AS c, orders AS o WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000.0)";
	t(cnf2);

	//PASS
	char *cnf3 = "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey FROM lineitem AS l WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL') AND (l.l_orderkey > 5000) AND (l.l_orderkey < 6000)";
	t(cnf3);

	//PASS
	char *cnf4 = "SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty FROM partsupp AS ps WHERE (ps.ps_partkey < 100) AND (ps.ps_suppkey < 50)";
	t(cnf4);



	char *cnf5 = "SELECT SUM (l.l_discount) FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)";

	char *cnf6 = "SELECT DISTINCT s.s_name FROM supplier AS s, part AS p, partsupp AS ps WHERE (s.s_suppkey = ps.ps_suppkey) AND (p.p_partkey = ps.ps_partkey) AND (p.p_mfgr = 'Manufacturer#4') AND (ps.ps_supplycost < 350.0)";
	t(cnf6);


	char *cnf7 = "SELECT SUM (l.l_extendedprice * (1 - l.l_discount)), l.l_orderkey, o.o_orderdate, o.o_shippriority FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_mktsegment = 'BUILDING') AND (c.c_custkey = o.o_custkey) AND (l.l_orderkey = o.o_orderkey) AND (l_orderkey < 100 OR o_orderkey < 100) GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority";
	t(cnf7);

	//PASS
	char *cnf8 = "SELECT SUM (ps.ps_supplycost), s.suppkey FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) GROUP BY s.s_suppkey";
	t(cnf8);

	//PASS
	char *cnf9 = "SELECT SUM (c.c_acctbal),name FROM customer AS c, orders AS o WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000.0) GROUP BY c.c_name";
	t(cnf9);

	//PASS
	char *cnf10 = "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey FROM lineitem AS l WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')";
	t(cnf10);

	//PASS
	char *cnf11 = "SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal FROM customer AS c1, customer AS c2 WHERE (c1.c_nationkey = c2.c_nationkey) AND (c1.c_name ='Customer#000070919')";
	t(cnf11);


	char *cnf12 = "SELECT SUM(l.l_discount) FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)";
	t(cnf12);

	char *cnf13 = "SELECT l.l_orderkey FROM lineitem AS l WHERE (l.l_quantity > 30.0)";
	t(cnf13);

	char *cnf14 = "SELECT DISTINCT c.c_name FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";
	t(cnf14);

	//seg fault
	char *cnf15 = "SELECT l.l_discount FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey) AND (r.r_regionkey = 1) AND (o.orderkey < 10000)";
	t(cnf15);

	char *cnf16 = "SELECT SUM (l.l_discount) FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)";
	t(cnf16);

	char *cnf17 = "SELECT SUM (l.l_extendedprice * l.l_discount) FROM lineitem AS l WHERE (l.l_discount<0.07) AND (l.l_quantity < 24.0)";
	t(cnf17);

}
