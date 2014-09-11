#include "y.tab.h"
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
int main(int argc, char *argv[]) {
	char *fileName = "Statistics.txt";
	
	char *relName[] = {"supplier","partsupp"};
	s.AddRel("region",5);
	s.AddAtt("region","r_regionkey",5);
	
	s.AddRel("nation" , 25);
	s.AddAtt("nation", "n_nationkey",25);
	s.AddAtt("nation", "n_regionkey",5);
	
	s.AddRel("part",200000);
	s.AddAtt("part", "p_partkey",200000);
	s.AddAtt("part","p_size", 50);
	s.AddAtt("part","p_container",40);
	
	s.AddRel("supplier",10000);
	s.AddAtt("supplier", "s_suppkey",10000);
	s.AddAtt("supplier", "s_nationkey", 25);
	
	s.AddRel("partsupp" ,800000);
	s.AddAtt("partsupp", "ps_suppkey",10000);
	s.AddAtt("part","p_size", 50);
	s.AddAtt("part","p_container",40);
	s.AddRel("part",200000);
	s.AddAtt("part", "p_partkey",200000);
	s.AddAtt("part","p_size", 50);
	s.AddAtt("part","p_container",40);