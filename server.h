#ifndef SERVER_H
#define SERVER_H
#include "header.h"
#include "parallelTree.h"
#include <pthread.h>

struct threadArg;

class server{
public:
    server();
    server(std::string ip, int port, std::vector<std::string> clusterNodeIP);
    ~server();

    //该计算节点上与master通信的服务器端，接受command并执行
    void receive_send();
    //开启该计算节点上与slave通信的服务器端
    void start_slaves(std::string port);
    //成员函数运行多线程遍历实体树
    void runGetTreeContentThread(void* arg);

private:
    //创建slave与slave通信的客户端
    void buildSlaveConnector();
    //终止slave与slave通信的连接
    void stopSlaveConnector();

private:
    std::string serverIP;   //该计算节点的IP
    int serverPort;         //与master通信的port
    std::vector<std::string> clusterIP;
    void* ctx;              //与master通信的context
    void* receiver;         //与master通信的fd
    
    parallelTree* tree; //根据传输来的部分RDF数据生成的tree
    std::vector<ID> intersection;   //传输来的重复性的实体ID
    std::vector<ID> mainEntityTrees; //主实体树在该计算节点，这一类型节点也要作为查询划分的依据

    std::vector<void*> senderContexts;   //与slave通信的context(不包括自己)
    std::vector<void*> senders;          //与slave通信的fd（不包括自己）

    pthread_mutex_t mu;
};

//slave 的 getTreeContent阶段，多线程执行函数
extern void* runThread(void* arg);
#endif //SERVER_H