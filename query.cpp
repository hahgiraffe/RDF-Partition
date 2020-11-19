#include "header.h"
#include "queryDecomposition.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <bitset>
#include <cstddef>

using namespace std;

//读取Index文件
void loadIndex(vector<bitset<BITMAPSIZE>>& pIndex, vector<bitset<BITMAPSIZE>>& soIndex){
    //读取bitset二进制文件，当作SO索引，还需读取P索引
    // vector<bitset<BITMAPSIZE>> pIndex;
    // vector<bitset<BITMAPSIZE>> soIndex;
    fstream inputPIndex("subData/Pindex", ios::in);
    if(!inputPIndex){
        cout << "read Pindex file error" << endl;
        return;
    }
    string line;
    while(getline(inputPIndex, line)){
        bitset<BITMAPSIZE> bit(line);
        pIndex.push_back(bit);
    }
    inputPIndex.close();

    fstream inputSOIndex("subData/SOindex", ios::in);
    if(!inputSOIndex){
        cout << "read SOindex file error" << endl;
        return;
    }
    while(getline(inputSOIndex, line)){
        bitset<BITMAPSIZE> bit(line);
        soIndex.push_back(bit);
    }
    inputSOIndex.close();

    //check
    cout << "pIndex's size:" << pIndex.size() << endl;
    cout << "soIndex's size:" << soIndex.size() << endl;
}

//读取Mapping文件
void loadMapping(map<string, int>& soMap, map<string, int>& pMap){
    // map<string, int> soMap;
    fstream inputSOMaping("subData/SOMapping", ios::in);
    if(!inputSOMaping){
        cout << "read SOMapping error" << endl;
        return;
    }
    string line;
    while(getline(inputSOMaping, line)){
        size_t spaceIndex = line.find(' ');
        string uri = line.substr(0, spaceIndex);
        string uriId = line.substr(spaceIndex+1);
        soMap[uri] = atoi(uriId.c_str());
    }
    inputSOMaping.close();

    // map<string, int> pMap;
    fstream inputPMaping("subData/PMapping", ios::in);
    if(!inputPMaping){
        cout << "read PMapping error" << endl;
        return;
    }
    while(getline(inputPMaping, line)){
        size_t spaceIndex = line.find(' ');
        string uri = line.substr(0, spaceIndex);
        string uriId = line.substr(spaceIndex+1);
        pMap[uri] = atoi(uriId.c_str());
    }
    inputPMaping.close();

    //check
    cout << "SOMapping's size:" << soMap.size() << endl;
    int n = 0;
    for(auto it = soMap.begin(); it != soMap.end() && n < 3; ++it, ++n){
        cout << it->first << " " << it->second << endl;
    }

    n = 0;
    cout << "PMapping's size:" << pMap.size() << endl;
    for(auto it = pMap.begin(); it != pMap.end() && n < 3; ++it, ++n){
        cout << it->first << "_" << it->second << endl;
    }

}

int main(int argc, char* argv[]){
    
    if(argc != 2){
        cout << "argc is not correct, usage: ./query <QUERYPATH>" << endl;
        return 1;
    }
    string queryPath = argv[1];

    //load index && mapping
    vector<bitset<BITMAPSIZE>> pIndex;
    vector<bitset<BITMAPSIZE>> soIndex;
    loadIndex(pIndex, soIndex);
    
    map<string, int> soMap;
    map<string, int> pMap;
    loadMapping(soMap, pMap);

    if(NULL == opendir(queryPath.c_str())){
        cout << "queryPath dose not exist" << endl;
        return 1;
    }

    //循环输入查询语句
    while(1){
        cout << ">>>";
        string path = queryPath;
        string queryName;
        cin >> queryName;

        if(queryName == "exit"){
            break;
        } else if(queryName == "\n" || queryName == "\r"){
            continue;
        }
        
        if(path.back() != '/'){
            path += '/' + queryName;
        } else {
            path += queryName;
        }
        // cout << queryPath << endl;

        //读取查询语句
        fstream input(path, ios::in);
        if(!input){
            cout << "open queryFile error" << endl;
            return 1;
        }
        string query;
		string line;
		while (getline(input, line)) {
			query += line + '\n';
		}
        input.close();

        // queryComposeToVec(query);
        queryDecomposition decom(query, pIndex, soIndex, soMap, pMap);
        decom.start();
    }
    return 0;
}