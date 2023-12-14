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
 
    Status status = OK;
    AttrDesc *attributes;
    int actualAttrCnt;
    status = attrCat -> getRelInfo(relation, actualAttrCnt, attributes);
    if (status != OK) return status;

    Record insertRec;
    for (int i = 0; i < attrCnt; i++){
        insertRec.length += attributes[i].attrLen;
    }

    char *insertData = new char[insertRec.length];
    if(!insertData) return INSUFMEM;
    

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

   
    insertRec.data = (void *)insertData;

    RID insertRID;
    InsertFileScan *scanner = new InsertFileScan(relation, status);
    if (status != OK) return status;
    status = scanner->insertRecord(insertRec, insertRID);

    delete[] insertData;
    free(attributes);

    return status;
}