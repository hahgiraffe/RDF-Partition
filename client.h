#ifndef CLIENT_H
#define CLIENT_H

#include "zmq.h"
#include "header.h"

class client{
public:
    client();
    client(std::string ip, int port);
    ~client();
    void send(std::string msg);
    void receive(char* buf);    //这个指针需要申请内存的
    void receiveMsg(char* buf, size_t s);
private:
    std::string serverIP;
    int serverPort;
    void* sender;
    void* ctx;
};

#endif //CLIENT_H