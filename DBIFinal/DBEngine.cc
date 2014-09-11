/*
 * DBEngine.cpp
 *
 *  Created on: Apr 24, 2013
 *      Author: eric
 */

#include "DBEngine.h"

DBEngine::DBEngine() {
	outputType = toScreen;
	outputFile = 0;
	runLength = 100;
}

DBEngine::~DBEngine() {
}

void DBEngine::createHeapTable(char *tableName, Attribute *atts, int numAtts) {

	DBFile file;
	file.Create(tableName, heap, 0);

}

void DBEngine::insertInto(char *fileName, char *tableName) {

}

void DBEngine::dropTable(char *tableName) {

}

void DBEngine::setOutPut(OutputType out, char *fileName) {

	outputType = out;
	if (outputType == toFile) {
		outputFile = fileName;
	}
}

void DBEngine::executeSQL(SQL &mySQL) {

	QueryPlan optimalPlan;
	Pipe outPipe;
	optimizer(mySQL, optimalPlan);
	optimalPlan.printPlan();
	if (outputType == noOutput) {
		return;
	}

	optimalPlan.execute(outPipe);

	if (outputType == toScreen) {
		Record record;
		while (outPipe.Remove(&record)) {
			record.Print(optimalPlan.getRoot()->outputSchema);
		}
		return;
	}

	if (outputType == toFile) {
		DBFile file;
		Record record;
		file.Open(outputFile);
		while (outPipe.Remove(&record)) {
			file.Add(record);
		}
		file.Close();
	}

}

void DBEngine::start() {

}
