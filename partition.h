#ifndef PARTITION_H
#define PARTITION_H

#include "header.h"
#include "parallelTree.h"
#include "stringToIDMapping.h"
#include "client.h"
#include <bitset>

class partition{
public:
    partition(std::string name, int cnt);
    ~partition();
    void blockDecomposition(std::vector<std::bitset<BITMAPSIZE>>& pIndex, stringToIDMapping& mapping);
    void treeDecomposition();
    void findCrossDomainEntity(std::set<ID>& intersection);
    void selectExchangeEntity(parallelTree& t1, parallelTree& t2, parallelTree& t3, std::set<ID>& s, std::vector<std::vector<std::pair<ID, int>>>& transportMsg);
    std::vector<std::string> getPartitionContents() const{
        return partitionContents;
    }

    int getPartitionCnt() const{
        return partitionCnt;
    }
    void printAll();
private:
    void readIDFile();

private:
    std::string fileName;       //数据文件名
    int partitionCnt;           //计算节点数量
    std::string buffer;         //总体内容
    std::vector<std::string> partitionContents; //blockDecomposition划分后每个节点内容
    std::vector<std::set<ID>> partitionSet; //每一个计算节点上实体ID（S和O）
    std::vector<std::set<ID>> predicateSet; //每一个计算节点上谓词
    // std::vector<client> clients;

};

#endif //PARTITION_H