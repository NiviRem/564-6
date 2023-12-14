#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

const Status QU_Insert(const string & relation, 
    const int attrCnt, 
    const attrInfo attrList[])
{
 
    Status status;
    AttrDesc *attributes;
    int actualAttrCnt;
    status = attrCat -> getRelInfo(relation, actualAttrCnt, attributes);
    if (status != OK) return status;

    if (actualAttrCnt != attrCnt) return UNIXERR;

    int recordLen = 0;
    for (int i = 0; i < attrCnt; i++){
        recordLen += attributes[i].attrLen;
    }

    InsertFileScan *scanner = new InsertFileScan(relation, status);
    if (status != OK) {
        return status;
    }

    char *insertData = new (std::nothrow) char[recordLen];
    if(!insertData){
        return INSUFMEM;
    }

    for (int i = 0; i < attrCnt; i++){
        int insertOffset = 0;
        bool attrFound = false;

        for (int j = 0; j < attrCnt; j++){
            if(strcmp(attributes[i].attrName, attrList[j].attrName) == 0){
                insertOffset = attributes[i].attrOffset;
                switch (attrList[j].attrType){
                    case STRING:
                        memcpy((char *)insertData + insertOffset, (char *)attrList[j].attrValue, attributes[i].attrLen);
                        break;

                    case INTEGER:
                        *(int *)(insertData + insertOffset) = atoi((char *)attrList[j].attrValue);
                        break;

                    case FLOAT:
                        *(float *)(insertData + insertOffset) = atof((char *)attrList[j].attrValue);
                        break;

                }
                attrFound = true;
                break;
            }
        }
        if (!attrFound){
            delete[] insertData;
            return UNIXERR;
        }
    }

    Record insertRec;
    insertRec.data = (void *)insertData;
    insertRec.length = recordLen;

    RID insertRID;
    status = scanner->insertRecord(insertRec, insertRID);


    delete[] insertData;
    free(attributes);
    return status;
}