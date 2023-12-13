#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	Status status = OK;
	HeapFileScan *scanner = new HeapFileScan(relation, status);
	if(status != OK) return status;

    if (status != OK) return status;

	if(attrName == ""){
		status = scanner->startScan(0, 0, type, NULL, op);
	}
	else{
		AttrDesc attrDesc;
		status = attrCat->getInfo(relation, attrName, attrDesc);
		if(type == STRING){
			status = scanner->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);
		}
		else if(type == INTEGER){
			int intVal = atoi(attrValue);
			status = scanner->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&intVal, op);
		}
		else if(type == FLOAT){
			float floatVal = atof(attrValue);
			status = scanner->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&floatVal, op);
		}
	}

	if (status != OK) return status;

	RID rid;
	while ((status= scanner->scanNext(rid)) != FILEEOF)
	{
		status = scanner->deleteRecord();
		if (status != OK) return status;
	}
	scanner->endScan();
	delete scanner;

	return OK;
}


