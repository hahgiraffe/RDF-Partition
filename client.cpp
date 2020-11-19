#include "client.h"
#include <assert.h>
using namespace std;

client::client(){

}
    
client::client(std::string ip, int port) : serverIP(ip), serverPort(port){
    ctx = zmq_ctx_new();
    sender = zmq_socket(ctx, ZMQ_REQ);
    string serverAddr = "tcp://" + serverIP + ":" + to_string(serverPort);
    // cout << serverAddr << endl;
    zmq_connect(sender, serverAddr.c_str());
}

client::~client(){
    zmq_close(sender);
    zmq_ctx_destroy(&ctx);
}

void client::send(std::string msg){
    int err = zmq_send(sender, msg.c_str(), msg.size()+1, 0);
    assert(err != -1);
}
    
void client::receive(char* buf){
    int recvbyte = zmq_recv(sender, buf, 20, 0);
    assert(recvbyte != -1);
    // std::cout << "recvBuf:" << buf << std::endl;
    assert(std::string(buf).compare("OK") == 0);
}
void client::receiveMsg(char* buf, size_t s){
    int recvbyte = zmq_recv(sender, buf, s, 0);
    assert(recvbyte != -1);
}