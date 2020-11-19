#ifndef QUERYTREE_H
#define QUERYTREE_H
#include "header.h"


//查询树的结构
struct queryNode{
    std::string val;                        //节点值
    std::vector<queryNode*> next;           //子节点
    queryNode(std::string v): val(v){}
};


class queryTree{
public:    
    queryTree(std::vector<std::tuple<std::string, std::string, std::string>> query);
    ~queryTree();
    
    //根据给的query建立一颗查询树
    void buildTree();

    //对建立的查询树进行划分，根据树的路径划分成多个子查询语句
    void decomposition(std::vector<std::vector<std::tuple<std::string, std::string, std::string>>>& result);
private:

private:
    //查询语句
    std::vector<std::tuple<std::string, std::string, std::string>> querySentence;
    
    //
    std::vector<queryNode*> nodes;
};
#endif //QUERYTREE_H