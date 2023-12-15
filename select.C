#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    Status status;
    AttrDesc attr1;
    AttrDesc attributes[projCnt];   
    int recordslen = 0;
    const char* filter;
   
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attributes[i]);
        if (status != OK) {
            return status;
        }
        recordslen += attributes[i].attrLen; 
    }

    if (attr != NULL) {
        status = attrCat->getInfo(attr->relName, attr->attrName, attr1);
        if (status != OK) {
            delete attr;
            return status;
        }

        int intval;
        float floatval;

        switch (attr->attrType) {
            case INTEGER:
                intval = atoi(attrValue);
                filter = (char*)&intval;
                break;
            case FLOAT:
                floatval = atof(attrValue);
                filter = (char*)&floatval;
                break;
            case STRING:
                filter = attrValue;
                break;
        }
    }
    else {
        attr1.attrOffset = 0;
        attr1.attrLen = 0;
        attr1.attrType = STRING;
        filter = NULL;
        strcpy(attr1.relName, projNames[0].relName);
        strcpy(attr1.attrName, projNames[0].attrName);
    }

    return ScanSelect(result, projCnt, attributes, &attr1, op, filter, recordslen);
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    
    Status status;
    Record newRecord;
    RID sourceRID, targetRID;

    InsertFileScan targetRelation(result, status);
    if (status != OK) return status;
    
    newRecord.length = reclen;
    newRecord.data = new char[reclen];

    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) return status;

    status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    if (status != OK) {
        return status;
    }

    Record rec;
    while (scan.scanNext(sourceRID) == OK) {
        status = scan.getRecord(rec);
        if (status != OK) return status;
        int offset = 0;

        for (int i = 0; i < projCnt; i++) {
            memcpy((char *)newRecord.data + offset, (char *)rec.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        status = targetRelation.insertRecord(newRecord, targetRID);
        if (status != OK){
            break;
        }
    }

    status = scan.endScan();
    if (status != OK) return status;

    delete[] (char *)newRecord.data;
    return OK;
}