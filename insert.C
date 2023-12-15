#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
 
    Status status;
    AttrDesc *attributes;
    int actualAttrCnt;
    status = attrCat->getRelInfo(relation, actualAttrCnt, attributes);
    if (status != OK) return status;

    if (actualAttrCnt != attrCnt) return UNIXERR;

    int recordLen = 0;
    for (int i = 0; i < attrCnt; i++){
        recordLen += attributes[i].attrLen;
    }

    char *data = new char[recordLen];

    for (int i = 0; i < attrCnt; i++){
        int offset = 0;
        bool found = false;
        for (int j = 0; j < attrCnt; j++){
            if(strcmp(attributes[i].attrName, attrList[j].attrName) == 0){
                offset = attributes[i].attrOffset;
                if(attrList[j].attrType == STRING){
                    memcpy((char *)data + offset, (char *)attrList[j].attrValue, attributes[i].attrLen);
                }
                else if(attrList[j].attrType == INTEGER){
                    *(int *)(data + offset) = atoi((char *)attrList[j].attrValue);
                }
                else if(attrList[j].attrType == FLOAT){
                    *(float *)(data + offset) = atof((char *)attrList[j].attrValue);
                }
                found = true;
                break;
            }
        }
        if (found == false){
            delete[] data;
            return UNIXERR;
        }
    }

    Record insertRec;
    insertRec.data = (void *)data;
    insertRec.length = recordLen;

    RID insertRID;
    InsertFileScan *scanner = new InsertFileScan(relation, status);
    if (status != OK) return status;
    status = scanner->insertRecord(insertRec, insertRID);

    delete[] data;

    return OK;
}
