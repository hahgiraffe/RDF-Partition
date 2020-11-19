#include "parallelTree.h"
#include <sstream>
using namespace std;

parallelTree::parallelTree(std::string c) : content(c){
    nodes.clear();
}

parallelTree::~parallelTree(){
    for(int i = 0; i < nodes.size(); ++i){
        if(nodes[i] != nullptr){
            delete(nodes[i]);
        }
    }
}

//建树并初始化计算节点
void parallelTree::buildTree(){
    cout << "begin buildTree" << endl;
    // 第一个版本的buildTree
    // size_t tmpIndex = 0;
    // size_t index = content.find('\n', tmpIndex);
    // int num = 0;
    
    // while(index != string::npos){
    //     string str = content.substr(tmpIndex, index-tmpIndex);
    //     size_t space_index1 = str.find_first_of(' ');
    //     size_t space_index2 = str.find_last_of(' ');
    //     ID s = atoi(str.substr(0, space_index1).c_str());
    //     ID p = atoi(str.substr(space_index1+1, space_index2 - space_index1 - 1).c_str());
    //     ID o = atoi(str.substr(space_index2+1).c_str());

    //     // cout << s << " : " << p << " : " << o << endl;

    //     //简历SOindexMap
    //     // auto key = make_pair(s, o);
    //     SOindexMap[make_pair(s, o)] = p;

    //     triples.push_back(make_tuple(s,p,o));
    //     TreeNode* sNode = nullptr, *oNode = nullptr;
    //     if(checkMap.find(s) == checkMap.end()){
    //         // checkSet.insert(s);
    //         sNode = new TreeNode(s);
    //         nodes.push_back(sNode);
    //         checkMap.insert({s, sNode});
    //         checkSet.insert(s);
    //     } else {
    //         sNode = checkMap[s];
    //     }
        
    //     if(checkMap.find(o) == checkMap.end()){
    //         // checkSet.insert(o);
    //         oNode = new TreeNode(o);
    //         nodes.push_back(oNode);
    //         checkMap.insert({o, oNode});
    //         checkSet.insert(o);
    //     } else {
    //         oNode = checkMap[o];
    //     }

    //     if(sNode == nullptr || oNode == nullptr){
    //         cout << "sNode or oNode error" << endl;
    //         break;
    //     }
        
    //     //注意，这里有重复的三元组，比如subGraph的第5行和第6698行是完全相同的两个三元组，在addNode内部去重
    //     sNode->addNode(oNode);
        
    //     //check
    //     num++;
    //     if(num % 10000 == 0) cout << num << endl;
        
    //     tmpIndex = index+1;
    //     index = content.find('\n', tmpIndex);
    // }


    //第二个版本的buildTree
    double startTime = get_time();
    int num = 0;
    istringstream iss(content);
    string str;
    while (getline(iss, str, '\n')) {
        // size_t space_index1 = str.find_first_of(' ');
        // size_t space_index2 = str.find_last_of(' ');
        // ID s = atoi(str.substr(0, space_index1).c_str());
        // ID p = atoi(str.substr(space_index1+1, space_index2 - space_index1 - 1).c_str());
        // ID o = atoi(str.substr(space_index2+1).c_str());
        auto triple = splitTriple(str, false);
        ID s = atoi(get<0>(triple).c_str());
        ID p = atoi(get<1>(triple).c_str());
        ID o = atoi(get<2>(triple).c_str());
        /*
        std::tuple<std::string, std::string, std::string> splitTriple(std::string line){
            line.resize(line.size()-2);
            size_t slocation = line.find_first_of(' ');
            std::string s = line.substr(0, slocation);
            size_t plocation = line.find_first_of(' ', slocation+1);
            std::string p = line.substr(slocation+1, plocation - slocation);
            std::string o = line.substr(plocation+1);
            return make_tuple(s, p, o);
        }
        */

        //这一个步骤导致第一份节点的buildTree时间翻十倍
        // SOindexMap[make_pair(s, o)] = p;
        if(SOindexMap.find(s) == SOindexMap.end()){
            unordered_map<ID, ID> mp;
            mp.insert(make_pair(o, p));
            SOindexMap.insert(make_pair(s, mp));
        } else {
            auto& mp = SOindexMap[s];
            mp.insert(make_pair(o, p));
        }
        

        auto idTriple = make_tuple(s, p, o);
        triples.emplace_back(idTriple);
        TreeNode* sNode = nullptr, *oNode = nullptr;
        if(checkMap.find(s) == checkMap.end()){
            sNode = new TreeNode(s);
            nodes.push_back(sNode);
            checkMap.insert({s, sNode});
            checkSet.insert(s);
        } else {
            sNode = checkMap[s];
        }

        if(checkMap.find(o) == checkMap.end()){
            // checkSet.insert(o);
            oNode = new TreeNode(o);
            nodes.push_back(oNode);
            checkMap.insert({o, oNode});
            checkSet.insert(o);
        } else {
            oNode = checkMap[o];
        }

        sNode->addNode(oNode);

        sNode->innerTuple.insert(idTriple);
        oNode->innerTuple.insert(idTriple);

        num++;
        if(num % 10000 == 0) cout << num << endl;
    }

    //这里可以先建立一个索引，根据SO找对应的三元组
    
    // for(int i = 0; i < triples.size(); ++i){
    //     auto key = make_pair(get<0>(triples[i]), get<2>(triples[i]));
    //     SOindexMap[key] = get<1>(triples[i]);
    // }

    double endTime = get_time();
    cout << "buildTree end, time:" << endTime - startTime << " s" << endl;
}

//计算nodes[i]->totalChild，递归性质的所有子节点个数(bfs)
void parallelTree::traverseStatistics(string treeFileName){
    cout << "begin traverseStatistics" << endl;
    // fstream outputFile(treeFileName, ios::out);
    //把每个节点当作根节点，子节点有多少（包括递归性质）
    for(int i = 0; i < nodes.size(); ++i){
        //bfs遍历一下
        int totalNum = 0;
        queue<TreeNode*> q;
        q.push(nodes[i]);
        totalNum++;
        while(!q.empty()){
            TreeNode* tmp = q.front();
            q.pop();
            for(int j = 0; j < tmp->child.size(); ++j){

                ID s = tmp->val;
                ID o = tmp->child[j]->val;
                ID p = SOindexMap[s][o];

                nodes[i]->sonTuple.insert(make_tuple(s, p, o));
                totalNum++;
                q.push(tmp->child[j]);
            }
        }
        // outputFile << nodes[i]->val << ":" << totalNum << endl;
        nodes[i]->totalChild = totalNum;
    }
    // outputFile.close();
    cout << "end traverseStatistics" << endl;

    /*
        指标说明
        （1）scale = child.size() + 1，表示自己节点+第一层子节点个数
        （2）child子数组表示存储第一层子节点
        （3）totalNumber表示，这节点关联的三元组个数（比如这个节点作为S或O都算）。
            可以用totalNumber - child.size()表示该节点作为O的个数
        （4）totalChild存储所有子节点个数（递归性质），在traverseStatistics中生成
    */
    cout << endl;
    cout << "***------show statistic--------***" << endl;
    int num = 0;
    if(1){
        cout << "nodes's size: " << nodes.size() << endl;
        for(int i = 0 ; i < 10; ++i){
            cout << "ID:" << nodes[i]->val << ",";
            cout << "scale:" << nodes[i]->scale << ",";
            cout << "totalNumber:" << nodes[i]->totalNumber << ",";
            cout << "child's size:" << nodes[i]->child.size() << ",";
            cout << "totalChild:" << nodes[i]->totalChild << ",";
            cout << "childs:";
            for(int j = 0; j < nodes[i]->child.size(); ++j){
                cout << nodes[i]->child[j]->val << "|";
            }
            cout << endl;
        }
        int rootNum = 0;
        num = 0;
        cout << "root TreeNode:";
        for(int i = 0; i < nodes.size(); ++i){
            if(nodes[i]->scale != 1 && nodes[i]->father == nullptr){
                rootNum++;
                if(num < 10){
                    num++;
                    cout << nodes[i]->val << "|";
                }
            }
        }
        cout << endl;
        cout << "total TreeNode number:" << rootNum << endl;
    }
    cout << "***------show statistic--------***" << endl;
    cout << endl;
}

//没有用
void parallelTree::sendTriple(std::vector<std::pair<ID, int>> msg){
    for(int i = 0; i < triples.size(); ++i){
        for(int j = 0; j < msg.size(); ++j){
            if(get<0>(triples[i]) == msg[j].first || get<2>(triples[i]) == msg[j].first){
                int target = msg[j].second;
                //发送该三元组
                
            }
        }
    }
}

size_t parallelTree::getTreeScale(ID id){
    if(checkMap.find(id) != checkMap.end())
        return checkMap[id]->scale;
    cout << "not found Entity:" << id << " in getTreeScale" << endl;
    return 0;
}

size_t parallelTree::getTotalNumber(ID id){
    if(checkMap.find(id) != checkMap.end())
        return checkMap[id]->totalNumber;
    cout << "not found Entity:" << id << " in getTotalNumber" << endl;
    return 0;
}

size_t parallelTree::getTotalChild(ID id){
    if(checkMap.find(id) != checkMap.end())
        return checkMap[id]->totalChild;
    cout << "not found Entity:" << id << " in getTotalChild" << endl;
    return 0;
}

vector<tuple<ID, ID, ID>> parallelTree::getSubTree(ID id){
    // vector<tuple<ID, ID, ID>> result;

    if(checkMap.find(id) == checkMap.end()){
        //该节点没有此实体ID，则报错
        cout << "checkMap found ID:" << id << " error" << endl;
        // return {make_tuple(1,2,3)};
        return {};
    }
    TreeNode* target = checkMap[id];
    if(target == nullptr){
        cout << "treeNode not found" << endl;
        return {};
    }
    set<tuple<ID, ID, ID>> result;

    //先把包含id的三元组找到
    // 是这一段费时间
    // for(int i = 0; i < triples.size(); ++i){
    //     if(get<0>(triples[i]) == id || get<2>(triples[i]) == id){
    //         result.insert(triples[i]);
    //     }
    // }
    for(auto it = target->innerTuple.begin(); it != target->innerTuple.end(); ++it){
        result.insert(*it);
    }

    for(auto it = target->sonTuple.begin(); it != target->sonTuple.end(); ++it){
        result.insert(*it);
    }

    //遍历树，bfs
    // queue<TreeNode*> q;
    // q.push(target);
    // while(!q.empty()){
    //     TreeNode* top = q.front();
    //     q.pop();
    //     for(int i = 0; i < top->child.size(); ++i){
    //         //这里可有很多优化
    //         ID s = top->val;
    //         ID o = top->child[i]->val;
    //         // for(int j = 0; j < triples.size(); ++j){
    //         //     if(get<0>(triples[j]) == s && get<2>(triples[j]) == o){
    //         //         result.insert(triples[j]);
    //         //         break;
    //         //     }
    //         // }
    //         // ID p = SOindexMap[{s, o}];
    //         ID p = SOindexMap[s][o];
    //         result.insert(make_tuple(s, p, o));
    //         q.push(top->child[i]);
    //     }
    // }
    
    return vector<tuple<ID, ID, ID>>(result.begin(), result.end());
}