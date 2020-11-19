#ifndef STRINGTOIDMAPPING_H
#define STRINGTOIDMAPPING_H

#include "header.h"

class stringToIDMapping{
public:
    stringToIDMapping();
    ~stringToIDMapping();
    
    ID addPredicate(std::string pre);
    ID addUri(std::string uri);
    void deletePredicate(std::string pre);
    void deleteUri(std::string uri);
    
    ID getPredicateID(std::string pre);
    std::string getPredicateString(ID preID);
    ID getUriID(std::string uri);
    std::string getUriString(ID uriID);
    

    int getPredicateTableCount() const {
        return predicateTableCount;
    }

    int getUriTableCount() const{
        return uriTableCount;
    }

    //写入磁盘持久化
    void writeIntoFile();

private:
    int uriTableCount;
    int predicateTableCount;
    std::unordered_map<std::string, int> predicateTable;
    std::unordered_map<int, std::string> id2strPrediacteTable;
    std::unordered_map<std::string, int> uriTable;
    std::unordered_map<int, std::string> id2strUriTable;

};


#endif // STRINGTOIDMAPPING_H
