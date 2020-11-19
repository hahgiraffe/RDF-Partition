#include <unordered_map>
#include <string>

#include "server.h"
using namespace std;

// 计算节点上，默认7777端口与master通信， 10001端口和10002端口与slave通信
// 这里每一个计算节点，slave与slave之间通信，开启两个端口
void* serverStart(void* ser) {
    static_cast<server*>(ser)->start_slaves("10001");
    return 0;
}

void* serverStart2(void* ser) {
    static_cast<server*>(ser)->start_slaves("10002");
    return 0;
}

int main(int argc, char* argv[]){
    if(argc != 2){
        std::cout << "argc error, usage: ./bin/main_server 11.11.11.11" << std::endl;
        return 1;
    }
    std::string localIP = argv[1];
    
    std::string prefixPath = "./subData/";         //默认路径
    if(opendir(prefixPath.c_str()) == NULL)
        mkdir(prefixPath.c_str(),0775);

    int clusterNumber = 0;
    std::vector<std::string> clusterNodeIP;
    readConf(clusterNumber, clusterNodeIP);
    if(clusterNumber != clusterNodeIP.size()){
        std::cout << "read conf error" << std::endl;
        return 1;
    }
    server s(localIP, 7777, clusterNodeIP);

    pthread_t t;
    pthread_create(&t, nullptr, serverStart, (void*)&s);
    pthread_detach(t);

    pthread_t t2;
    pthread_create(&t2, nullptr, serverStart2, (void*)&s);
    pthread_detach(t2);

    s.receive_send();
    
    return 0;
}