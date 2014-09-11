#ifndef DBENGINE_H_
#define DBENGINE_H_

#include "QueryOptimizer.h"

enum OutputType{toFile, toScreen, noOutput};

class DBEngine {

private:
	QueryOptimizer optimizer;
	OutputType outputType;
	char *outputFile;
	int runLength;

public:
	DBEngine();
	~DBEngine();
	void createHeapTable(char *tableName, Attribute *atts, int numAtts);
	void createSortTable(char *tableName, Attribute *atts, int numAtts, OrderMaker *order);
	void insertInto(char *fileName, char *tableName);
	void dropTable(char *tableName);
	void setOutPut(OutputType out, char *fileName);
	void executeSQL(SQL &mySQL);
	void start();
};

#endif /* DBENGINE_H_ */
