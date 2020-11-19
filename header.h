#ifndef HEADER_H
#define HEADER_H

#include <vector>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <bitset>
#include <tuple>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>

#define ID unsigned int
#define DEBUG 0

//读取配置文件
void readConf(int& clusterNumber, std::vector<std::string>& clusterNodeIP);

//读取数据文件行数
int loadFileCount(const std::string& fileName);

//将RDF文件每一行提取出三元组SPO
std::tuple<std::string, std::string, std::string> splitTriple(std::string line, bool isDeleteEnd);

//
double get_time();

//数据划分索引BITMAP的最大长度
const size_t BITMAPSIZE = 1e9+1;

#endif //HEADER_H