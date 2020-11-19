#include "stringToIDMapping.h"


stringToIDMapping::stringToIDMapping() : uriTableCount(0), predicateTableCount(0){
    uriTable.clear();
    id2strUriTable.clear();
    predicateTable.clear();
    id2strPrediacteTable.clear();
}

stringToIDMapping::~stringToIDMapping(){

}

ID stringToIDMapping::addPredicate(std::string pre){
    if(predicateTable.find(pre) != predicateTable.end()){
        return predicateTable[pre];
    }
    int tmpCount = predicateTableCount;
    predicateTable[pre] = predicateTableCount++;
    id2strPrediacteTable[tmpCount] = pre;
    return tmpCount;
}

ID stringToIDMapping::addUri(std::string uri){
    if(uriTable.find(uri) != uriTable.end()){
        return uriTable[uri];
    }
    int tmpCount = uriTableCount;
    uriTable[uri] = uriTableCount++;
    id2strUriTable[tmpCount] = uri;
    return tmpCount;
}

void stringToIDMapping::deletePredicate(std::string pre){
    if(predicateTable.find(pre) == predicateTable.end()){
        std::cerr << "delete Predicate not found, key:" << pre << std::endl;
        return;
    }
    int preId = predicateTable[pre];
    predicateTable.erase(pre);
    id2strPrediacteTable.erase(preId);
}

void stringToIDMapping::deleteUri(std::string uri){
    if(uriTable.find(uri) == uriTable.end()){
        std::cerr << "delete uriTable not found, key:" << uri << std::endl;
        return;        
    }
    int uriId = uriTable[uri];
    uriTable.erase(uri);
    id2strUriTable[uriId];
}

ID stringToIDMapping::getPredicateID(std::string pre){
    if(predicateTable.find(pre) == predicateTable.end()){
        std::cerr << "getPredicateID not found, key:" << pre << std::endl;
        return 0;
    }
    return predicateTable[pre];
}

std::string stringToIDMapping::getPredicateString(ID preID){
    // if(preID >= predicateTableCount){
    //     std::cerr << "getpredicateString not found, key:" << preID << std::endl;
    //     return "";
    // }
    // for(auto it = predicateTable.begin(); it != predicateTable.end(); ++it){
    //     if(it->second == preID){
    //         return it->first;
    //     }
    // }
    // return "";

    // 这里先把查找失败的逻辑注释起来
    if(id2strPrediacteTable.find(preID) == id2strPrediacteTable.end()){
        std::cerr << "getPredicateString not found, key:" << preID << std::endl;
        return "";
    }
    return id2strPrediacteTable[preID];
}

ID stringToIDMapping::getUriID(std::string uri){
    // 这里先把查找失败的逻辑注释起来
    if(uriTable.find(uri) == uriTable.end()){
        std::cerr << "getUriID not found, key:" << uri << std::endl;
        return 0;
    }
    return uriTable[uri];
}

std::string stringToIDMapping::getUriString(ID uriID){
    // if(uriID >= uriTableCount){
    //     std::cerr << "getUriString not found, key:" << uriID << std::endl;
    //     return "";
    // }
    // for(auto it = uriTable.begin(); it != uriTable.end(); ++it){
    //     if(it->second == uriID){
    //         return it->first;
    //     }
    // }
    // return "";
    
    if(id2strUriTable.find(uriID) == id2strUriTable.end()){
        std::cerr << "getUriString not found, key:" << uriID << std::endl;
        return "";
    }
    return id2strUriTable[uriID];
}

void stringToIDMapping::writeIntoFile(){
    //默认写入subData/SOMapping, subData/PMapping
    std::fstream outputSO("subData/SOMapping", std::ios::out);
    if(!outputSO){
        std::cout << "writeIntoFile SOMapping error" << std::endl;
        return ;
    }
    for(auto it = uriTable.begin(); it != uriTable.end(); ++it){
        outputSO << it->first << " " << it->second << std::endl;
    }
    outputSO.close();

    std::fstream outputP("subData/PMapping", std::ios::out);
    if(!outputP){
        std::cout << "writeIntoFile PMapping error" << std::endl;
        return ;
    }
    for(auto it = predicateTable.begin(); it != predicateTable.end(); ++it){
        outputP << it->first << " " << it->second << std::endl;
    }
    outputP.close();
}