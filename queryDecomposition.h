#ifndef QUERYDECOMPOSITION_H
#define QUERYDECOMPOSITION_H
#include "header.h"

class queryDecomposition{
public:
    queryDecomposition( std::string query, 
                        std::vector<std::bitset<BITMAPSIZE>> pIdx, 
                        std::vector<std::bitset<BITMAPSIZE>> soIdx,
                        std::map<std::string, int> soMap,
                        std::map<std::string, int> pMap);
    ~queryDecomposition();
    void start();

private:
    //解析SPARQL语句
    void parse();
    //将SPARQL语句的PREXIF替换
    void queryPrefixMapping();
    //将查询triple转换成ID
    void queryStringToID(std::vector<std::tuple<std::string, std::string, std::string>>& queryID);
    //根据Index划分分配到对应不同的节点
    void indexDecomposition(const std::vector<std::tuple<std::string, std::string, std::string>>& queryID);
private:
    std::string querySentence;                      //整条查询语句
    std::vector<std::bitset<BITMAPSIZE>> pIndex;    //P的划分索引
    std::vector<std::bitset<BITMAPSIZE>> SOIndex;   //SO的划分索引
    std::map<std::string, int> soMap;               //SO的String-ID映射
    std::map<std::string, int> pMap;                //P的String-ID映射

    std::map<std::string, std::string> prefixTable; //解析出来的prefix映射
    std::vector<std::string> variables;             //总查询语句变量
    std::vector<std::tuple<std::string, std::string, std::string>> triples; //总查询语句的每个SPO
};

#endif //QUERYDECOMPOSITION_H