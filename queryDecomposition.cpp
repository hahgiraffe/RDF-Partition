#include "queryDecomposition.h"
#include "queryTree.h"

using namespace std;

queryDecomposition::queryDecomposition( std::string query, 
                    std::vector<std::bitset<BITMAPSIZE>> pIdx, 
                    std::vector<std::bitset<BITMAPSIZE>> soIdx,
                    std::map<std::string, int> soMp,
                    std::map<std::string, int> pMp)
                    :querySentence(query), pIndex(pIdx), SOIndex(soIdx), soMap(soMp), pMap(pMp)
                    { }

queryDecomposition::~queryDecomposition(){

}

void queryDecomposition::start(){
    cout << "querySentence:" << endl;
    cout << querySentence << endl;
    parse();
    queryPrefixMapping();
    vector<tuple<string, string, string>> queryID;
    queryStringToID(queryID);

    //首先要构建查询树，建立查询树后，先根据树的路径划分一次
    queryTree tree(queryID);
    tree.buildTree();
    vector<vector<tuple<string, string, string>>> result;
    tree.decomposition(result);

    //将每个根据路径划分好的子查询
    for(int i = 0; i < result.size(); ++i){
        indexDecomposition(result[i]);
    }
}

void queryDecomposition::parse(){
    size_t beginIndex = 0;
    size_t line = querySentence.find('\n', beginIndex);
    while(line != string::npos){
        string subline = querySentence.substr(beginIndex, line-beginIndex);
        if(subline.size() > 5){
            string label = subline.substr(0,6);
            if(label.compare("PREFIX") == 0 || label.compare("prefix") == 0){
                //标签前缀处理
                string content = subline.substr(7);

                size_t middleIndex = content.find_first_of(":");
                string left = content.substr(0, middleIndex);
                string right = content.substr(middleIndex+1);
                
                //去掉首尾空格
                left.erase(0, left.find_first_not_of(" "));
                left.erase(left.find_last_not_of(" ")+1);
                right.erase(0, right.find_first_not_of(" "));
                right.erase(right.find_last_not_of(" ")+1);
                prefixTable[left] = right;
            } else if(label.compare("SELECT") == 0 || label.compare("select") == 0){
                //变量
                string content = subline.substr(7);
                size_t whereIndex = content.find("WHERE");
                if(whereIndex == string::npos){
                    whereIndex = content.find("where");
                }
                string vars = content.substr(0, whereIndex);
                for(int i = 0; i < vars.size()-1; ++i){
                    if(vars[i] == '?'){
                        variables.push_back(vars.substr(i,2));
                    }
                }
            } else {
                //语句
                subline.erase(0, subline.find_first_not_of(" "));
                subline.erase(subline.find_last_not_of(" ")+1);
                
                subline.erase(0, subline.find_first_not_of("\t"));
                subline.erase(subline.find_last_not_of("\t")+1);

                size_t firstSpaceIndex = subline.find_first_of(" ");
                size_t secondSpaceIndex = subline.find_first_of(" ", firstSpaceIndex+1);
                size_t thirdSpaceIndex = subline.find_first_of(" ", secondSpaceIndex+1);
                string s = subline.substr(0, firstSpaceIndex);
                string p = subline.substr(firstSpaceIndex+1, secondSpaceIndex-firstSpaceIndex-1);
                string o = subline.substr(secondSpaceIndex+1, thirdSpaceIndex-secondSpaceIndex-1);
                triples.push_back(make_tuple(s, p, o));
            }
        } else {
            //换行括号等
        }
        beginIndex = line+1;
        line = querySentence.find('\n', beginIndex);
    }

    //check
    cout << "-----variables:" << endl;
    for(auto a : variables){
        cout << a << ",";
    }
    cout << endl;

    cout << "-----prefixTable:" << endl;
    for(auto it = prefixTable.begin(); it != prefixTable.end(); ++it){
        cout << it->first << "," << it->second << endl;
    }

    cout << "-----triples:" << endl;
    for(int i = 0; i < triples.size(); ++i){
        auto triple = triples[i];
        cout << get<0>(triple) << "_" << get<1>(triple) << "_" << get<2>(triple) << endl;
    }

}

void queryDecomposition::queryPrefixMapping(){
    for(int i = 0; i < triples.size(); ++i){
        auto& triple = triples[i];
        string& s = get<0>(triple);
        string& p = get<1>(triple);
        string& o = get<2>(triple);
    
        for(auto it = prefixTable.begin(); it != prefixTable.end(); ++it){
            string pre = it->first;
            string suf = it->second;
            suf.resize(suf.size()-1);
            int len = pre.size();
            if(s.size() > len && s.substr(0, len) == pre){
                string endStr = s.substr(len+1);
                s = suf + endStr + ">";
            }

            if(p.size() > len && p.substr(0, len) == pre){
                string endStr = p.substr(len+1);
                p = suf + endStr + ">";
            }
            
            if(o.size() > len && o.substr(0, len) == pre){
                string endStr = o.substr(len+1);
                o = suf + endStr + ">";
            }
        }
    }

    //check
    cout << "-----After queryPrefixMapping triples:" << endl;
    for(int i = 0; i < triples.size(); ++i){
        auto triple = triples[i];
        cout << get<0>(triple) << "_" << get<1>(triple) << "_" << get<2>(triple) << endl;
    }
}

void queryDecomposition::queryStringToID(vector<tuple<string, string, string>>& queryID){
    for(int i = 0; i < triples.size(); ++i){
        auto triple = triples[i];
        string s = get<0>(triple);
        string p = get<1>(triple);
        string o = get<2>(triple);
        string queryS = s, queryP = p, queryO = o;
        if(s.front() != '?'){
            if(soMap.find(s) == soMap.end()){
                cout << "soMap not found:" << s << endl;
            } else {
                ID sid = soMap[s];        
                queryS = to_string(sid);
            }
        }

        if(p.front() != '?'){
            if(pMap.find(p) == pMap.end()){
                cout << "pMap not found:" << p << endl;
            } else {
                ID pid = pMap[p];
                queryP = to_string(pid);
            }
        }

        if(o.front() != '?'){
            if(soMap.find(o) == soMap.end()){
                cout << "soMap not found:" << o << endl;
            } else {
                ID oid = soMap[o];
                queryO = to_string(oid);
            }
        }
        queryID.push_back(make_tuple(queryS, queryP, queryO));
    }

    cout << "-----After queryStringToID triples:" << endl;
    for(int i = 0; i < queryID.size(); ++i){
        auto triple = queryID[i];
        cout << get<0>(triple) << "_" << get<1>(triple) << "_" << get<2>(triple) << endl;
    }
}

void queryDecomposition::indexDecomposition(const vector<tuple<string, string, string>>& queryID){
    
    //存储
    vector<vector<tuple<string, string, string>>> partitionQuery(SOIndex.size());

    //判断S和O是否全部都是变量
    bool isAllVar = true;
    for(int i = 0; i < queryID.size(); ++i){
        auto triple = queryID[i];
        string s = get<0>(triple);
        string o = get<2>(triple);
        if(s.front() != '?' || o.front() != '?'){
            isAllVar = false;
            break;
        }
    }

    if(isAllVar){
        //每一个子查询语句单独划分

    } else {
        //这里的划分策略不太行，首先应该建立一棵树，先进行树划分

        for(int i = 0; i < queryID.size(); ++i){
            auto triple = queryID[i];
            bool isFlag = false;
            string s = get<0>(triple);
            string p = get<1>(triple);
            string o = get<2>(triple);
            if(s.front() != '?'){
                for(int j = 0; j < SOIndex.size(); ++j){
                    if(SOIndex[j].test(atoi(s.c_str())) == true){
                        partitionQuery[j].push_back(triple);
                        isFlag = true;
                    }
                }
            }

            if(o.front() != '?'){
                for(int j = 0; j < SOIndex.size(); ++j){
                    if(SOIndex[j].test(atoi(o.c_str())) == true){
                        bool checkIsAlive = false;
                        for(int k = 0; k < partitionQuery[j].size(); ++k){
                            if(partitionQuery[j][k] == triple){
                                checkIsAlive = true;
                                break;
                            }
                        }
                        if(checkIsAlive == false){
                            partitionQuery[j].push_back(triple);
                            isFlag = true;
                        }
                    }
                }
            }

            //如果so的Index没有用上
            if(isFlag == false){
                for(int j = 0; j < pIndex.size(); ++j){
                    if(pIndex[j].test(atoi(p.c_str())) == true){
                        partitionQuery[j].push_back(triple);
                    }
                }
            }
        }
    }

    //check
    cout << "-----After decomposition triples:" << endl;
    for(int i = 0; i < partitionQuery.size(); ++i){
        cout << "level:" << i << endl;
        for(int j = 0; j < partitionQuery[i].size(); ++j){
            cout << get<0>(partitionQuery[i][j]) << "_" << get<1>(partitionQuery[i][j]) << "_" << get<2>(partitionQuery[i][j]) << endl;
        }
    }


    return;
}