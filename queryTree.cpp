#include "queryTree.h"
using namespace std;

queryTree::queryTree(std::vector<std::tuple<std::string, std::string, std::string>> query)
    :querySentence(query){
}

queryTree::~queryTree(){

}

void queryTree::buildTree(){
    for(int i = 0; i < querySentence.size(); ++i){
        string s = get<0>(querySentence[i]);
        string p = get<1>(querySentence[i]);
        string o = get<2>(querySentence[i]);

        // queryNode* sNode = new queryNode(s);
        // queryNode* oNode = new queryNode(o);
        queryNode** sNode;
        queryNode** oNode;
        bool isSFind = false;
        bool isOFind = false;
        for(int j = 0; j < nodes.size(); ++j){
            if(nodes[j]->val == s){
                isSFind = true;
                sNode = &nodes[j];
            }
            if(nodes[j]->val == o){
                isOFind = true;
                oNode = &nodes[j];
            }
        }
        if(isSFind && isOFind){
            (*sNode)->next.push_back(*oNode);
            continue;
        } else if(isSFind && !isOFind){
            queryNode* tmpoNode = new queryNode(o);
            (*sNode)->next.push_back(tmpoNode);
            nodes.push_back(tmpoNode);
        } else if(!isSFind && isOFind){
            queryNode* tmpsNode = new queryNode(s);
            tmpsNode->next.push_back(*oNode);
            nodes.push_back(tmpsNode);            
        } else {
            queryNode* tmpSNode = new queryNode(s);
            queryNode* tmpONode = new queryNode(o);
            tmpSNode->next.push_back(tmpONode);
            nodes.push_back(tmpSNode);
            nodes.push_back(tmpONode);
        }

    }

    cout << "----After buildTree" << endl;
    for(int i = 0; i < nodes.size(); ++i){
        cout << nodes[i]->val << ",";
    }
    cout << endl;
    
    cout << "----After buildTree(Detail)" << endl;
    for(int i = 0; i < nodes.size(); ++i){
        cout << nodes[i]->val << ":";
        for(int j = 0; j < nodes[i]->next.size(); ++j){
            cout << nodes[i]->next[j]->val << ",";
        }
        cout << endl;
    }
}

//查询树的路径划分，先根据树划分成多个子查询语句
void queryTree::decomposition(vector<vector<tuple<string, string, string>>>& result){
    //find root
    map<pair<string, string>, string> pairMap;
    vector<queryNode*> root(nodes);
    for(int i = 0; i < querySentence.size(); ++i){
        string s = get<0>(querySentence[i]);
        string p = get<1>(querySentence[i]);
        string o = get<2>(querySentence[i]);

        auto soPair = make_pair(s, o);
        pairMap[soPair] = p;
        bool isFind = false;
        int j = 0;
        for(j = 0; j < root.size(); ++j){
            if(root[j]->val == o){
                isFind = true;
                break;
            }
        }
        if(isFind){
            root.erase(root.begin()+j);
        }
    }
    
    cout << "----queryTree::decomposition(root)" << endl;
    for(int i = 0; i < root.size(); ++i){
        cout << root[i]->val << ",";
    }
    cout << endl;

    
    // cout << "----Inside queryTree::decomposition" << endl;
    for(int i = 0; i < root.size(); ++i){
        vector<tuple<string, string, string>> tmpResult;
        queue<queryNode*> q;
        q.push(root[i]);
        cout << root[i]->val << ":" << endl;
        while(!q.empty()){
            queryNode* tmp = q.front();
            q.pop();
            for(int j = 0; j < tmp->next.size(); ++j){
                queryNode* target = tmp->next[j];
                string p = pairMap[make_pair(tmp->val, target->val)];
                tmpResult.push_back(make_tuple(tmp->val, p, target->val));
                // cout << tmp->val << "_" << target->val << ",";
                q.push(target);
            }
        }
        result.push_back(tmpResult);
    }

    cout << "----After queryTree::decomposition" << endl;    
    for(int i = 0; i < result.size(); ++i){
        cout << root[i]->val << ":" << endl;
        for(int j = 0; j < result[i].size(); ++j){
            cout << get<0>(result[i][j]) << endl;
            cout << get<1>(result[i][j]) << endl;
            cout << get<2>(result[i][j]) << endl;
        }
    }

    //判断非叶子节点是否有变量
    // for(int i = 0; i < result.size(); ++i){

    // }
}