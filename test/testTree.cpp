// 测试建树方式的正确性
// g++ testTree.cpp ../parallelTree.cpp ../stringToIDMapping.cpp ../header.cpp -o testTree
#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <tuple>
#include <unordered_map>
#include "../parallelTree.h"
#include "../stringToIDMapping.h"

#define ID unsigned int

using namespace std;

string loadData(const string& name, stringToIDMapping& mapping){
    ifstream inputFile(name, ios::in);
    if(!inputFile){
        cerr << "rdfFile open error" << endl;
        return "";
    }
    string content;
    string line;
    int n = 0;
    while(getline(inputFile, line)){
        if(line.size() <= 2) continue;
        auto triple = splitTriple(line, true);
        string s = get<0>(triple), p = get<1>(triple), o = get<2>(triple);
        ID sid = mapping.addUri(s);
        ID pid = mapping.addPredicate(p);
        ID oid = mapping.addUri(o);

        content += to_string(sid) + " " + to_string(pid) + " " + to_string(oid) + "\n";
    }

    inputFile.close();
    return content;
}

int main(int argc, char* argv[]){
    if(argc != 2){
        cout << "arg not correct" << endl;
        return 0;
    }

    string rdfFileName = argv[1];
    stringToIDMapping mapping;
    string content = loadData(rdfFileName, mapping);

    // buildTree(content);
    parallelTree* tree = new parallelTree(content);
    tree->buildTree();
    tree->traverseStatistics("subData/tree");

    for(int i = 0; i < 10; ++i){
        auto res = tree->getSubTree(i);
        cout << i << " subTree's size:" << res.size() << endl;
    }
    
    return 0;
}
