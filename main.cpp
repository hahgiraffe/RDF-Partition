#include <tuple>
#include <bitset>
#include <unistd.h>

#include "time.h"

#include "header.h"
#include "stringToIDMapping.h"
#include "partition.h"

using namespace std;

//加载RDF文件，并做ID映射
// bool loadGraph(string file, stringToIDMapping& mapping, string& IDbuffer){
//     cout << "begin loadGraph" << endl;
//     ifstream inputFile(file, ios::in);
//     if(!inputFile){
//         cerr << "rdfFile open error" << endl;
//         return false;
//     }
//     // ofstream outputFile("subData/StringTOIDFile", ios::out);
//     // if(!outputFile){
//     //     cerr << "outputFile open error" << endl;
//     //     return false;
//     // }
//     string line;
//     int n = 0;
//     while(getline(inputFile, line)){
//         if(line.size() <= 2) continue;
//         auto triple = splitTriple(line);
//         string s = get<0>(triple), p = get<1>(triple), o = get<2>(triple);
//         ID sid = mapping.addUri(s);
//         ID pid = mapping.addPredicate(p);
//         ID oid = mapping.addUri(o);
//         // outputFile << mapping.getUriID(s) << " " << mapping.getPredicateID(p) << " " << mapping.getUriID(o) << endl;
//         IDbuffer += to_string(sid) + " " + to_string(pid) + " " + to_string(oid) + "\n";
//         n++;
//         if(n % 10000 == 0) cout << n << endl;
//     }
//     inputFile.close();
//     // outputFile.close();
//     return true;
// }

//加载ID，转换成string文件
// 这里有一个bug，就是最后的一个文件会多一行
bool transToStringFile(stringToIDMapping& mapping, string& buffer, int index){
    cout << "begin transToStringFile" << endl;
    fstream output("subData/result_str" + to_string(index), ios::out);

    size_t lastIndex = 0;
    size_t location = buffer.find('\n');
    while(location != string::npos){
        
        string line = buffer.substr(lastIndex, location-lastIndex);

        size_t slocation = line.find_first_of(' ');
        string s = line.substr(0, slocation);
        size_t plocation = line.find_first_of(' ', slocation+1);
        string p = line.substr(slocation+1, plocation - slocation);
        string o = line.substr(plocation+1);

        string sstr = mapping.getUriString(atoi(s.c_str()));
        string pstr = mapping.getPredicateString(atoi(p.c_str()));
        string ostr = mapping.getUriString(atoi(o.c_str()));
        //output2最后会加上一个errorMsg
        if(sstr == "" || ostr == "" || pstr == ""){
            cout << "error: " << s << "|" << p << "|" << o << "|" << sstr << "|" << pstr << "|" << ostr << endl;
            cout << "error detailMsg, index:" << index << ",line:" << line << endl;
        } else {
            //单机的TripleBit后面，每一个三元组后面有一个空格和点
            output << sstr << " " << pstr << " " << ostr << " ."<< endl;
        }
        
        lastIndex = location+1;
        location = buffer.find('\n', lastIndex);
    }
    output.close();
    return true;
}

int main(int argc, char* argv[]){
    if(argc != 3){
        cout << "argc number not correct!" << endl;
        return 1;
    }

    int clusterNumber = 0;
    vector<string> clusterNodeIP;
    readConf(clusterNumber, clusterNodeIP);

    //将所有中间结果文件输出到默认路径
    std::string prefixPath = "./subData/";
    if(opendir(prefixPath.c_str()) == NULL)
        mkdir(prefixPath.c_str(),0775);

    string rdfFileName = argv[1];
    string partitionCnt = argv[2];

    if(atoi(partitionCnt.c_str()) != clusterNumber || clusterNumber != clusterNodeIP.size()){
        cout << "readConf error, clusterNumber not correct" << endl;
        return 1;
    }

    stringToIDMapping mapping;
    // string IDbuffer;
    // if(!loadGraph(rdfFileName, mapping, IDbuffer)){
    //     cout << "loadGraph error" << endl;
    //     return 1;
    // }

    cout << "begin partition" << endl;
    
    int cnt = atoi(partitionCnt.c_str());

    //开始计时划分数据的时间
    double startTime = get_time();

    //块划分，同时建立谓词索引
    vector<bitset<BITMAPSIZE>> pIndex(cnt);
    partition p(rdfFileName, cnt);
    p.blockDecomposition(pIndex, mapping);
    
    double middleTime = get_time();
    cout << middleTime - startTime << "s used in build and blockPartition period" << endl;

    //check predicate bitmap
    // cout << "check predicate bitmap" << endl;
    // for(int i = 0; i < cnt; ++i){
    //     for(int j = 0; j < 100; ++j){
    //         if(pIndex[i].test(j)){
    //             cout << "1";
    //         } else {
    //             cout << "0";
    //         }
    //     }
    //     cout << endl;
    // }
    // p.printAll();

    vector<client*> clients(cnt);
    for(int i = 0; i < cnt; ++i){
        clients[i] = new client(clusterNodeIP[i], 7777);
    }
    
    vector<string> partitionContent = p.getPartitionContents();

    //send partition graph
    cout << "begin sendSubGraph" << endl;
    for(int i = 0; i < cnt; ++i){
        //send command
        clients[i]->send("graph");
        char isOk[20];
        clients[i]->receive(isOk);

        //send msg's len
        string msgLen = to_string(partitionContent[i].size());
        cout << "send msg's len:" << msgLen << endl;
        clients[i]->send(msgLen);
        char resp[20];
        clients[i]->receive(resp);

        //send msg
        clients[i]->send(partitionContent[i]);
        char buffer[20];
        clients[i]->receive(buffer);
    }

    cout << "begin find and send intersection" << endl;
    
    //找到不同计算节点中都共同拥有的实体
    set<ID> intersection;
    p.findCrossDomainEntity(intersection);
    //send entityID Intersection
    //这里有一点，就是需要调整set的顺序（可以改为一个vector了），根据每个实体ID在不同的节点上树规模排序
    cout << "intersection's len:" << intersection.size() << endl;

    string interBuffer;
    for(auto it = intersection.begin(); it != intersection.end(); ++it){
        interBuffer += to_string(*it) + "_";
    }
    interBuffer.pop_back();

    int interlen = interBuffer.size();

    for(int i = 0; i < cnt; ++i){
        //send command
        clients[i]->send("intersection");
        char isOk[20];
        clients[i]->receive(isOk);

        //send interBuffer's len
        clients[i]->send(to_string(interlen));
        char resp[20];
        clients[i]->receive(resp);

        //send interBuffer's content
        clients[i]->send(interBuffer);
        char buffer[20];
        clients[i]->receive(buffer);
    }

    //发送指令获取slave之间实体树规模的指令
    cout << "begin send getEntitySize" << endl;
    for(int i = 0; i < cnt; ++i){
        clients[i]->send("getEntitySize");
        char isOk[20];
        clients[i]->receive(isOk);
    }

    //发送指令获取slave之间的实体树内容的指令
    cout << "begin getSubEntityTree" << endl;
    for(int i = 0; i < cnt; ++i){
        clients[i]->send("getSubEntityTree");
        char isOk[20];
        clients[i]->receive(isOk);
    }

    //find && build bitMap index
    //每一个vector纬度对应着一个slave中是否有该实体ID的主实体树，有则置一，可以理解为SO-Index
    //还需要一个P-index，形式和SO-Index是一样的，为了针对S和O全为变量的查询语句
    vector<bitset<BITMAPSIZE>> queryIndex(cnt);
    cout << "begin getQueryParititionIndex" << endl;
    for(int i = 0; i < cnt; ++i){
        clients[i]->send("getIndex");
        char isOk[20];
        clients[i]->receive(isOk);

        clients[i]->send("OK");
        char msgLen[1024];
        clients[i]->receiveMsg(msgLen, 1024);
        
        int len = atoi(msgLen);
        clients[i]->send("OK");
        char* msg = new char[len+1];
        clients[i]->receiveMsg(msg, len);

        string nodeIndex(msg);
        size_t lastIndex = 0;
        auto it = nodeIndex.find('_');
        while(it != string::npos){
            ID targetID = atoi(nodeIndex.substr(lastIndex, it - lastIndex).c_str());
            queryIndex[i].set(targetID);
            lastIndex = it+1;
            it = nodeIndex.find('_', lastIndex);
        }
    }

    //这里只存储了实体树转移的，如果平均的，目前没有index，也没有处理
    for(int i = 0; i < queryIndex.size(); ++i){
        //返回每个slave中bitmap里面“1”的个数
        cout << "queryIndex's len:" << queryIndex[i].count() << endl;
    }
    

    // 这一块其实可以不算在parititon的时间，是节点之间传输结果文件
    for(int i = 0; i < cnt; ++i){
        clients[i]->send("getPartitionGraphLen");
    }

    vector<char*> msgVec(cnt);
    for(int i = 0; i < cnt; ++i){
        char msglen[20];
        clients[i]->receiveMsg(msglen, 20);

        int len = atoi(msglen);
        cout << "getPartitionGraphLen:" << len << endl;
        clients[i]->send("getPartitionGraphContent");
        char* msg = new char[len+1];
        clients[i]->receiveMsg(msg, len);
    
        msgVec[i] = msg;
        // fstream out("subData/output" + to_string(i), ios::out);
        // out << msg;
        // out.close();

        // 这一个步骤很花时间，在LUBM10中多花6s，可以放到划分时间的后面
        // string msgStr(msg);
        // transToStringFile(mapping, msgStr, i);
    }

    double endTime = get_time();
    cout << "总共耗费的时间:" << endTime - startTime << "s" << endl;

    //接下来的持久化特别慢，有什么解决方法吗？

    //划分好的最终数据，先持久化
    for(int i = 0; i < cnt; ++i){
        string msgStr(msgVec[i]);
        transToStringFile(mapping, msgStr, i);
    }

    //Mapping持久化
    mapping.writeIntoFile();

    //持久化SOindex，queryDecomposition需要用
    fstream indexFile("subData/SOindex", ios::out);
    for(int i = 0; i < queryIndex.size(); ++i){
        indexFile << queryIndex[i].to_string() << endl;
    }
    indexFile.close();

    //持久化Pindex，queryDecomposition需要用
    fstream pIndexFile("subData/Pindex", ios::out);
    for(int i = 0; i < pIndex.size(); ++i){
        pIndexFile << pIndex[i].to_string() << endl;
    }
    pIndexFile.close();

    // 这里在关闭的时候还是有bug，先把性能调整好，再来调这里
    // for(int i = 0; i < cnt; ++i){
    //     clients[i]->send("exit");
    //     char isOk[20];
    //     clients[i]->receive(isOk);
    //     cout << "client" << i << " is closed" << endl;
    // }

    for(int i = 0; i < cnt; ++i){
        if(clients[i])
            delete clients[i];
    }

    return 0;
}