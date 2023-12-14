#include "catalog.h"
#include "stdio.h"
#include "stdlib.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string &result,
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen);

/*
 * Selects records from the specified relation.

 * @param result - result of selection
 * @param projCnt -
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue) {
    cout << "Doing QU_Select " << endl;

    Status status;
    AttrDesc *attr1 = NULL;    
    AttrDesc attributes[projCnt];   
    int recordslen = 0;
   
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attributes[i]);
        if (status != OK) {
            return status;
        }
        recordslen += attributes[i].attrLen; 
    }
    if (attr != NULL) {
        attr1 = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *attr1);
        if (status != OK) {
            delete attr;
            return status;
        }
    }

    return ScanSelect(result, projCnt, attributes, attr1, op, attrValue, recordslen);
}
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen) {
    
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

   
    if (!attrDesc) {
        status = scan.startScan(0, 0, STRING, NULL, EQ);
    } else {
        int toInt;
        float toFloat;
        switch (attrDesc->attrType) {
            case FLOAT:
                toFloat = atof(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char *)&(toFloat), op);
                break;
            case INTEGER:
                toInt = atoi(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char *)&(toInt), op);
                break;
            case STRING:
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
                break;
        }
    }
    if (status != OK) return status;
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
