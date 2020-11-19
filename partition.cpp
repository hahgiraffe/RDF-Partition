#include <algorithm>

#include "partition.h"

#define DEBUG_INFO 1
using namespace std;

static int threshold = 0;

partition::partition(string name, int cnt) : fileName(name), partitionCnt(cnt){
    // partitionSet.reserve(partitionCnt);
    for(int i = 0; i < partitionCnt; ++i){
        partitionContents.emplace_back("");
        partitionSet.emplace_back(set<ID>());
        predicateSet.emplace_back(set<ID>());
    }
}

partition::~partition(){
    buffer.clear();
}

void partition::blockDecomposition(std::vector<std::bitset<BITMAPSIZE>>& pIndex, stringToIDMapping& mapping){
    cout << "begin blockDecomposition" << endl;

    //检查传入数据文件的正确性
    ifstream inputFile(fileName, ios::in);
    if(!inputFile){
        cerr << "rdfFile open error" << endl;
        return;
    }

    int cnt = loadFileCount(fileName);
    int threshold = cnt / partitionCnt + 1;
    if(DEBUG_INFO){
        cout << "cnt num:" << cnt << endl;
        cout << "threshold num:" << threshold << endl;
    }

    vector<fstream> partitionFiles;
    for(int i = 0; i < partitionCnt; ++i){
        partitionFiles.emplace_back(fstream("subData/FirstSubGraph" + to_string(i), ios::out));
    }

    string line;
    int num = 0;
    int fileIndex = 0;
    while(getline(inputFile, line)){
        if(line.size() <= 2) continue;
        auto triple = splitTriple(line, true);
        string s = get<0>(triple), p = get<1>(triple), o = get<2>(triple);
        ID sid = mapping.addUri(s);
        ID pid = mapping.addPredicate(p);
        ID oid = mapping.addUri(o);

        string content = to_string(sid) + " " + to_string(pid) + " " + to_string(oid) + "\n";
        buffer += content;

        partitionSet[fileIndex].insert(sid);
        predicateSet[fileIndex].insert(pid);
        partitionSet[fileIndex].insert(oid);

        partitionContents[fileIndex] += content;

        partitionFiles[fileIndex] << content;
        
        if(num == threshold){
            cout << "node" << fileIndex << " triples count:" << num << endl;
            num = 0;
            fileIndex++;
        }

        num++;
        if(num % 100000 == 0) cout << num << endl;
    }
    inputFile.close();


    // int num = 0;
    // int fileIndex = 0;
    // size_t tmpIndex = 0;
    // size_t index = buffer.find('\n', tmpIndex);
    // while(index != string::npos){
    //     num++;
    //     string str = buffer.substr(tmpIndex, index-tmpIndex);
    //     if(DEBUG_INFO && num % 10000 == 0){
    //         cout << num << ":" << str << endl;
    //     }

    //     //取出SPO
    //     size_t space_index1 = str.find_first_of(' ');
    //     size_t space_index2 = str.find_last_of(' ');
    //     ID s = atoi(str.substr(0, space_index1).c_str());
    //     ID p = atoi(str.substr(space_index1+1, space_index2-space_index1- 1).c_str());
    //     ID o = atoi(str.substr(space_index2+1).c_str());
    //     partitionSet[fileIndex].insert(s);
    //     partitionSet[fileIndex].insert(o);
    //     predicateSet[fileIndex].insert(p);

    //     partitionContents[fileIndex] += str + "\n";
    //     // partitionFiles[fileIndex] << str << endl;
    //     if(num == threshold){
    //         cout << "node" << fileIndex << " triples count:" << num << endl;
    //         num = 0;
    //         fileIndex++;
    //     }
    //     tmpIndex = index+1;
    //     index = buffer.find('\n', tmpIndex);
    // }


    for(int i = 0; i < partitionCnt; ++i){
        partitionFiles[i].close();
    }

    //将谓词转换成谓词索引（位图）
    for(int i = 0; i < predicateSet.size(); ++i){
        for(auto it = predicateSet[i].begin(); it != predicateSet[i].end(); ++it){
            pIndex[i].set(*it);
        }
    }
    cout << "after blockDecomposition" << endl;
}

//没有用
void partition::treeDecomposition(){
    cout << "begin treeDecomposition" << endl;
    //以subGraph0为例
    string target = partitionContents[0];
    parallelTree tree1(target);
    tree1.buildTree();
    tree1.traverseStatistics("subData/tree1");

    parallelTree tree2(partitionContents[1]);
    tree2.buildTree();
    tree2.traverseStatistics("subData/tree2");

    parallelTree tree3(partitionContents[2]);
    tree3.buildTree();
    tree3.traverseStatistics("subData/tree3");

    //查看同一个实体，在不同计算节点上的规模
    auto set1 = tree1.getCheckSet();
    auto set2 = tree2.getCheckSet();
    auto set3 = tree3.getCheckSet();
    set<ID> twoSetIntersection;
    set_intersection(set1.begin(), set1.end(), set2.begin(), set2.end(), inserter(twoSetIntersection, twoSetIntersection.begin()));
    set<ID> threeSetIntersection;
    set_intersection(set3.begin(), set3.end(), twoSetIntersection.begin(), twoSetIntersection.end(), inserter(threeSetIntersection, threeSetIntersection.begin()));

    fstream intersectionEntity("subData/intersectionEntity", ios::out);
    intersectionEntity << "crossDomainEntity's size: " << threeSetIntersection.size() << endl;
    for(auto it = threeSetIntersection.begin(); it != threeSetIntersection.end(); ++it){
        intersectionEntity << "ID:" << *it << ",totalNumber:" <<  tree1.getTotalNumber(*it) 
                                           << ",totalNumber:" << tree2.getTotalNumber(*it) 
                                           << ",totalNumber:" << tree3.getTotalNumber(*it) << endl;
    }
    intersectionEntity.close();

    //这一部分应该是在master节点上做的事情
    //将相同ID的节点的树进行排序，并将副树合并入主树，因为每次在不同节点上查找主实体树和副实体树都会产生网络开销和数据副本，所以要慎重，并多次比较
    //先按照差值进行整体排序，|entity1_node1 - entity1_node2 ,entity1_node1 - entity1_node3| 要最大，代表实体在不同计算节点上的规模差距越大，优先级应该越高
    
    vector<vector<pair<ID, int>>> transportMsg(3);
    selectExchangeEntity(tree1, tree2, tree3, threeSetIntersection, transportMsg);

    tree1.sendTriple(transportMsg[0]);
    tree2.sendTriple(transportMsg[1]);
    tree3.sendTriple(transportMsg[2]);
}

//找到跨计算节点的相同实体
void partition::findCrossDomainEntity(set<ID>& allSetIntersection){
    for(int i = 0; i < partitionCnt; ++i){
        cout << "node" << i << " entity count:" << partitionSet[i].size() << endl;
    }
    //计算节点数目要大于2
    set_intersection(partitionSet[0].begin(), partitionSet[0].end(), partitionSet[1].begin(), partitionSet[1].end(), inserter(allSetIntersection, allSetIntersection.begin()));
    for(int i = 2; i < partitionSet.size(); ++i){
        set<ID> tmpSetIntersection;
        set_intersection(partitionSet[i].begin(), partitionSet[i].end(), allSetIntersection.begin(), allSetIntersection.end(), inserter(tmpSetIntersection, tmpSetIntersection.begin()));    
        allSetIntersection.swap(tmpSetIntersection);
    }
    
    fstream intersectionEntity("subData/intersectionEntity", ios::out);
    intersectionEntity << "crossDomainEntity's size: " << allSetIntersection.size() << endl;
    for(auto it = allSetIntersection.begin(); it != allSetIntersection.end(); ++it){
        // intersectionEntity << "ID:" << *it << ",totalNumber:" <<  tree1.getTotalNumber(*it) 
        //                                    << ",totalNumber:" << tree2.getTotalNumber(*it) 
        //                                    << ",totalNumber:" << tree3.getTotalNumber(*it) << endl;
        intersectionEntity << *it << endl;
    }
    intersectionEntity.close();
}

void partition::selectExchangeEntity(parallelTree& t1, parallelTree& t2, parallelTree& t3, set<ID>& s, vector<vector<pair<ID, int>>>& transportMsg){
    cout << "begin selectExchangeEntity" << endl;
    vector<pair<ID, int>> vec;  //ID -> 最大与其他计算节点差值
    vector<pair<ID, int>> vecNodeIndex;  //ID -> nodeIndex

    for(auto it = s.begin(); it != s.end(); ++it){
        int node1TotalNumber = t1.getTotalNumber(*it);
        int node2TotalNumber = t2.getTotalNumber(*it);
        int node3TotalNumber = t3.getTotalNumber(*it);
        int maxNumber = max(node1TotalNumber, max(node2TotalNumber, node3TotalNumber));
        if(maxNumber == node1TotalNumber){
            vecNodeIndex.push_back({*it, 0});
        } else if(maxNumber == node2TotalNumber) {
            vecNodeIndex.push_back({*it, 1});
        } else {
            vecNodeIndex.push_back({*it, 2});
        }
        vec.push_back({*it, 3*maxNumber - node1TotalNumber - node2TotalNumber - node3TotalNumber});
    }
    //根据差值，从大到小排序
    sort(vec.begin(), vec.end(), [](pair<ID, int> p1, pair<ID, int> p2){
        return p1.second > p2.second ? true : false;
    });
    cout << "-----实体ID:最大的计算节点与其他计算节点差值和" << endl;
    for(int i = 0; i < 10; ++i){
        cout << vec[i].first << ":" << vec[i].second << endl;
    }

    // 接下来就把需要转移复制的实体ID传递给对应的计算节点。每个计算节点需要的信息：ID ,toNodeIndex（要把实体ID，转移到对应的计算节点Index）
    // vector<vector<pair<ID, int>>> transportMsg(3);  每一个计算节点需要传输的<ID, toNodeIndex>
    int equalCnt = 0;
    for(int i = 0; i < vec.size(); ++i){
        if(vec[i].second > threshold){
            //大于阈值，就表示需要将ID副实体树的传输到主实体树上
            ID targetID = vec[i].first;
            int j = 0;
            for(; j < vecNodeIndex.size(); ++j){
                if(vecNodeIndex[j].first == targetID){
                    break;
                }
            }
            if(j == vecNodeIndex.size()){
                cout << "not found ID:" << targetID << " in vecNodeIndex" << endl;
                continue;
            }
            int mainTreeNode = vecNodeIndex[j].second;
            for(int index = 0; index < 3; ++index){
                if(index != mainTreeNode){
                    transportMsg[index].push_back({targetID, mainTreeNode});
                }
            }
        } else {
            //说明实体在不同计算节点上的出现相同的，这里应该换另一种策略
            equalCnt++;
        }
    }

    //debugInfo
    cout << "每一个计算节点都相等的实体个数:" << equalCnt << endl;
    cout << "-----每一个计算节点需要转移的实体ID" << endl;
    for(int i = 0; i < transportMsg.size(); ++i){
        int num = 0;
        for(int j = 0; j < transportMsg[i].size() && num < 10; ++j, ++num){
            cout << transportMsg[i][j].first << ":" << transportMsg[i][j].second << endl;
        }
        cout << endl;
    }

}

void partition::printAll(){
    cout << "---partition PrintAll" << endl;
    cout << "parittionCnt:" << partitionCnt << endl;
    cout  << "parititonSet:" << endl;
    for(int i = 0; i < partitionSet.size(); ++i){
        cout << "node_" << i << "'s size:" << partitionSet[i].size() << ",";
    }
    cout << endl;
    cout << "predicateSet:" << endl;
    for(int i = 0; i < predicateSet.size(); ++i){
        cout << "node_" << i << "'s size:" << predicateSet[i].size() << ",";
    }
    cout << endl;
}