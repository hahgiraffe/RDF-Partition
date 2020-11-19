#ifndef PARALLELTREE_H
#define PARALLELTREE_H
#include "header.h"

//pair作为unordered_map的key，所需要的hash函数
// struct pair_hash
// {
//     template<class T1, class T2>
//     std::size_t operator() (const std::pair<T1, T2>& p) const
//     {
//         auto h1 = std::hash<T1>{}(p.first);
//         auto h2 = std::hash<T2>{}(p.second);
//         return h1 ^ h2;
//     }
// };

struct TreeNode{
    TreeNode* father;               //父节点  
    std::vector<TreeNode*> child;   //子节点
    int scale;                      //包括本节点的树规模，child.size()+1，自己和第一层的子节点个数
    int totalNumber;                //子数据中的三元组，包含本节点所有次数（包括S和P），也就是包括所有父节点和递归性子节点
    int totalChild;                 //所有子节点个数（包括递归）
    ID val;                         //本节点的ID
    std::set<std::tuple<ID, ID, ID>> sonTuple; //所有子节点中的三元组（递归性质）
    std::set<std::tuple<ID, ID, ID>> innerTuple; //这个节点ID所在S或O的三元组
    TreeNode(ID v) : father(nullptr), scale(1), totalNumber(0), totalChild(0), val(v) {}
    void addNode(TreeNode*& node){
        totalNumber++;
        node->totalNumber++;

        //这里有一个去重工作，如果o已经是s的child，就不用加入了
        //这一个是一定需要的，如果注释的话，后面bfs遍历会遇到很多相同的子节点，效率很低
        for(int i = 0; i < child.size(); ++i){
            if(child[i]->val == node->val){
                return;
            }
        }
        child.push_back(node);
        scale++;
        node->father = this;
    }
};

class parallelTree{
public:
    parallelTree(std::string c);
    ~parallelTree();
    void buildTree();
    void traverseStatistics(std::string treeFileName);
    void sendTriple(std::vector<std::pair<ID, int>> msg);

    size_t getTreeScale(ID id);
    size_t getTotalNumber(ID id);
    size_t getTotalChild(ID id);
    //获取根节点为id的子树的所有三元组
    std::vector<std::tuple<ID, ID, ID>> getSubTree(ID id);

    inline std::vector<TreeNode*> getTreeNodes() const{
        return nodes;
    }
    inline std::unordered_map<ID, TreeNode*> getCheckMap() const{
        return checkMap;
    }
    inline std::string getContent() const{
        return content;
    }
    inline std::unordered_set<ID> getCheckSet() const{
        return checkSet;
    }

private:
    std::string content;                                    //该部分的三元组内容
    std::vector<TreeNode*> nodes;                           //每个实体作为根节点的树
    std::unordered_map<ID, TreeNode*> checkMap;             //该子文件所有的实体ID集合（没有p）
    std::vector<std::tuple<ID, ID, ID>> triples;            //三元组
    std::unordered_set<ID> checkSet;                        //实体ID集合（没有p）
    // std::unordered_map<std::pair<ID, ID>, ID, pair_hash> SOindexMap;   //所有三元组的，SO->P的映射
    std::unordered_map<ID, std::unordered_map<ID, ID>> SOindexMap;
};

#endif