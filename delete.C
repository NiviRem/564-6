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

	AttrDesc attrDesc;
	attrCat->getInfo(relation, attrName, attrDesc);
	int intval;
	float floatval;
	
	switch(type)
	{
		case STRING:
			status = scanner->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);
			break;
		
		case INTEGER:
		 	intval = atoi(attrValue);
			status = scanner->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&intval, op);
			break;
		
		case FLOAT:
			floatval = atof(attrValue);
			status = scanner->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&floatval, op);
			break;
	}

	RID rid;
	while ((status= scanner->scanNext(rid)) == OK)
	{
		if ((status = scanner->deleteRecord()) != OK) return status;
	}

	scanner->endScan();
	
	return OK;
}