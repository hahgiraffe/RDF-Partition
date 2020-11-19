#include "server.h"
#include "zmq.h"
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <atomic>
#include <algorithm>
#include <iostream>
#include <sstream>
using namespace std;

#define MAXCOMMANDSIZE 1024
#define MAXBUFSIZE 102400

multimap<string, pair<string, string>> portMap{
    {"11.11.11.4", {"11.11.11.11", "10001"}},
    {"11.11.11.4", {"11.11.11.12", "10001"}},
    {"11.11.11.11", {"11.11.11.4", "10001"}},
    {"11.11.11.11", {"11.11.11.12", "10002"}},
    {"11.11.11.12", {"11.11.11.4", "10002"}},
    {"11.11.11.12", {"11.11.11.11", "10002"}},
};

server::server(){
    pthread_mutex_init(&mu, NULL);
}
    
server::server(std::string ip, int port, vector<string> clusterNodeIP) : 
                                                                serverIP(ip), 
                                                                serverPort(port), 
                                                                clusterIP(clusterNodeIP){
    ctx = zmq_ctx_new();
    receiver = zmq_socket(ctx, ZMQ_REP);
    std::string serverAddr = "tcp://*:" + to_string(serverPort);
    int rc = zmq_bind(receiver, serverAddr.c_str());
    assert(rc == 0);
    pthread_mutex_init(&mu, NULL);
}
    
server::~server(){
    zmq_close(receiver);
    zmq_ctx_destroy(ctx);
    pthread_mutex_destroy(&mu);
}

//对于master通信，接收指令并执行
void server::receive_send(){
    cout << "begin receive_send" << endl;
    while(1){
        char commandBuf[MAXCOMMANDSIZE];
        int recvbyte = zmq_recv(receiver, commandBuf, MAXCOMMANDSIZE, 0);
        assert(recvbyte != -1);

        string command(commandBuf);
        cout << "command is:" << command << " size: " << command.size() << endl;
        if(command.compare("heartBeat") == 0){
            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
        } else if(command.compare("exit") == 0){

            //先退出与其他slave连接
            for(int i = 0; i < senders.size(); ++i){
                string msg = "exit";
                int err = zmq_send(senders[i], msg.c_str(), msg.size()+1, 0);
                assert(err != -1);
                char isOk[20];
                int recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);
            }

            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
            break; 
        } else if(command.compare("graph") == 0){
            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
            
            //msg's len
            char msgLen[MAXCOMMANDSIZE];
            int recvbyte = zmq_recv(receiver, msgLen, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);

            int len = atoi(msgLen);
            cout << "msg's len: " << len << endl;
            //msg
            char* msg = new char[len];
            recvbyte = zmq_recv(receiver, msg, len+1, 0);
            assert(recvbyte != -1);
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);

            //接收到子图信息，先持久化检验
            // fstream ouputFile("subData/SecondSubGraph" + to_string(serverPort), ios::out);
            // ouputFile << msg;
            // ouputFile.close();

            //接下来根据接收到子图数据建树
            string target(msg);
            tree = new parallelTree(target);
            tree->buildTree();
            tree->traverseStatistics("subData/tree" + to_string(serverPort));

        } else if(command.compare("intersection") == 0){
            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);

            //msg's len
            char msgLen[MAXCOMMANDSIZE];
            int recvbyte = zmq_recv(receiver, msgLen, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
            
            int len = atoi(msgLen);

            //msg
            char *buf = new char[len+1];
            recvbyte = zmq_recv(receiver, buf, len, 0);
            assert(recvbyte != -1);
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
            string interStr(buf);
            
            istringstream iss(interStr);
            string temp;
            while (getline(iss, temp, '_')) {
                intersection.push_back(atoi(temp.c_str()));
            }

            // cout << "intersection's actual len is:" << intersection.size() << endl;

        } else if(command.compare("getEntitySize") == 0){
            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);

            double startTime = get_time();
            //创建与slave之间的连接
            buildSlaveConnector();

            //接下来对每个其他的slave节点通信，问他们ID的树规模，然后再判断是否为主实体树
            int entityIsLocal = 0;
            
            vector<vector<int>> allCheckResult;
            for(int i = 0; i < senders.size(); ++i){
                //先发送command
                string msg = "getTreeSize";
                int err = zmq_send(senders[i], msg.c_str(), msg.size()+1, 0);
                assert(err != -1);
                char isOk[20];
                int recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);

                //将interseciton进行编码
                string interStr;
                for(int i = 0; i < intersection.size(); ++i){
                    interStr += to_string(intersection[i]) + "_";
                }
                interStr.pop_back();

                //发送长度，返回ok
                string interStrlen = to_string(interStr.size());
                err = zmq_send(senders[i], interStrlen.c_str(), interStrlen.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);

                //发送信息，返回ok
                err = zmq_send(senders[i], interStr.c_str(), interStr.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);

                //发送ok，返回结果长度
                char resultLen[MAXCOMMANDSIZE];
                err = zmq_send(senders[i], resp.c_str(), resp.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], resultLen, MAXCOMMANDSIZE, 0);
                assert(recvbyte != -1);

                //发送ok，返回结果内容
                char *result = new char[atoi(resultLen)+1];
                err = zmq_send(senders[i], resp.c_str(), resp.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], result, atoi(resultLen), 0);
                assert(recvbyte != -1);

                //解析result
                vector<int> entitySizeArray;
                istringstream iss(result);
                string temp;

                while (getline(iss, temp, '_')) {
                    entitySizeArray.push_back(atoi(temp.c_str()));
                }

                allCheckResult.push_back(entitySizeArray);
            }

            for(int i = 0; i < allCheckResult[0].size(); ++i){
                int scale = tree->getTotalNumber(intersection[i]);
                bool flag = true;
                for(int j = 0; j < allCheckResult.size(); ++j){
                    if(allCheckResult[j][i] >= scale){
                        flag = false;
                        break;
                    }
                }
                if(flag){
                    mainEntityTrees.push_back(intersection[i]);
                    entityIsLocal++;
                }
            }

            double endTime = get_time();
            cout << "getEntitySize end, entityIsLocal:" << entityIsLocal << ", time:" << endTime-startTime << "s" << endl;

        } else if(command.compare("getSubEntityTree") == 0){
            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);

            double startTime = get_time();
            //这里首先是给所有sender发送command
            for(int i = 0; i < senders.size(); ++i){
                //先发送命令
                string msg = "getTreeContent";
                int err = zmq_send(senders[i], msg.c_str(), msg.size()+1, 0);
                assert(err != -1);
                char isOk[20];
                int recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);
            }

            //mainEntityTrees编码
            string mainEntityTreeStr;
            for(int i = 0; i < mainEntityTrees.size(); ++i){
                mainEntityTreeStr += to_string(mainEntityTrees[i]) + "_";
            }
            
            mainEntityTreeStr.pop_back();

            for(int i = 0; i < senders.size(); ++i){
                //发送长度,返回OK
                string mainEntityTreeStrLen = to_string(mainEntityTreeStr.size());
                int err = zmq_send(senders[i], mainEntityTreeStrLen.c_str(), mainEntityTreeStrLen.size()+1, 0);
                assert(err != -1);
                char isOk[20];
                recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);

                //发送内容，返回OK
                err = zmq_send(senders[i], mainEntityTreeStr.c_str(), mainEntityTreeStr.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], isOk, 20, 0);
                assert(recvbyte != -1);
                assert(std::string(isOk).compare("OK") == 0);
            }

            //存储所有节点的结果
            vector<vector<string>> allNodeResult;

            for(int i = 0; i < senders.size(); ++i){
                //发送OK，返回长度
                char resultLen[MAXCOMMANDSIZE];
                int err = zmq_send(senders[i], resp.c_str(), resp.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], resultLen, MAXCOMMANDSIZE, 0);
                assert(recvbyte != -1);
                
                // cout << "resultLen:" << atoi(resultLen) << endl;
                
                //发送OK，返回内容
                char* result = new char[atoi(resultLen)+1];
                err = zmq_send(senders[i], resp.c_str(), resp.size()+1, 0);
                assert(err != -1);
                recvbyte = zmq_recv(senders[i], result, atoi(resultLen), 0);
                assert(recvbyte != -1);

                //解析resultStr
                istringstream iss(result);
                string temp;
                vector<string> resultVec;
                while (getline(iss, temp, '$')) {
                    resultVec.push_back(temp);
                }
                allNodeResult.push_back(resultVec);
                cout << "slave" << i << " get TreeContent end, content's number:" << resultVec.size() << endl;
            }

            //check,只打印十个值
            // cout << "----getSubEntityTree" << endl;
            // for(int i = 0; i < 5 && i < allNodeResult[0].size(); ++i){
            //     cout << "ID:" << mainEntityTrees[i] << endl;
            //     for(int j = 0; j < allNodeResult.size(); ++j){
            //         cout << "node" << j << ":" << allNodeResult[j][i] << endl;
            //     }
            //     cout << endl;
            // }

            string originContent = tree->getContent();
            set<string> check;
            for(int i = 0; i < allNodeResult.size(); ++i){
                for(int j = 0; j < allNodeResult[i].size(); ++j){
                    string tranMsg = allNodeResult[i][j];
                    replace(tranMsg.begin(), tranMsg.end(), '_', ' ');
                    replace(tranMsg.begin(), tranMsg.end(), '|', '\n');
                    
                    size_t beginIndex = 0;
                    auto spaceIndex = tranMsg.find('\n', beginIndex);
                    while(spaceIndex != string::npos){
                        string subTriple = tranMsg.substr(beginIndex, spaceIndex-beginIndex);
                        
                        if(check.find(subTriple) == check.end()){
                            check.insert(subTriple);
                            originContent += subTriple + "\n";
                        }
                        
                        beginIndex = spaceIndex+1;
                        spaceIndex = tranMsg.find('\n', beginIndex);
                    }
                }
            }

            //持久化
            fstream outputFile("subData/result", ios::out);
            outputFile << originContent;
            outputFile.close();

            double endTime = get_time();
            cout << "getSubEntityTree end!!!time:" << endTime-startTime << "s" << endl;
         
        } else if(command.compare("getIndex") == 0){
            //这里直接返回该节点上的主实体树实体ID
            string resp = "OK";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
            
            char isOK[MAXCOMMANDSIZE];
            recvbyte = zmq_recv(receiver, isOK, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);

            string result;
            for(auto entity : mainEntityTrees){
                result += to_string(entity);
                result += "_";
            }

            string resultLen = to_string(result.size()+1);

            zmq_send(receiver, resultLen.c_str(), resultLen.size()+1, 0);
            
            recvbyte = zmq_recv(receiver, isOK, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            zmq_send(receiver, result.c_str(), result.size()+1, 0);

        } else if(command.compare("getPartitionGraphLen") == 0){
            //先传输长度
            string result;
            string line;
            fstream inputFile("subData/result", ios::in);
            if(!inputFile){
                cout << "open subData/result error" << endl;
            }
            while(getline(inputFile, line)){
                result += line;
                result += '\n';
            }
            inputFile.close();

            string resultLen = to_string(result.size());
            zmq_send(receiver, resultLen.c_str(), resultLen.size()+1, 0);
            cout << "send parititonGraphLen is:" << resultLen << endl;

            char graphCommand[MAXCOMMANDSIZE];
            recvbyte = zmq_recv(receiver, graphCommand, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);

            //再传输内容
            zmq_send(receiver, result.c_str(), result.size()+1, 0);
            cout << "end getPartitionGraphLen" << endl;
        } else {
            //Invaild Command
            string resp = "Invaild Command";
            zmq_send(receiver, resp.c_str(), resp.size()+1, 0);
            exit(1);
        }
    }
}

//服务器2 * 14 = 28虚拟cpu
const int PTHREAD_NUM = 5;

struct threadArg{
    server* ser;
    vector<string> tasks;
    // int index;
    int beginIndex;
    int interval;
    string threadResult;
};

//与slave进行通信的服务器端
void server::start_slaves(string port){
    cout << "start listen to slave" << endl;
    void* slave_ctx = zmq_ctx_new();
    void* slave_receiver = zmq_socket(ctx, ZMQ_REP);

    std::string serverAddr = "tcp://*:" + port;
    int rc = zmq_bind(slave_receiver, serverAddr.c_str());
    assert(rc == 0);
    
    while(1){
        char commandBuf[MAXCOMMANDSIZE];
        int recvbyte = zmq_recv(slave_receiver, commandBuf, MAXCOMMANDSIZE, 0);
        assert(recvbyte != -1);
        string command(commandBuf);
        cout << "slave command is:" << command << endl;
        if(command.compare("heartBeat") == 0){
            string resp = "OK";
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
        } else if(command.compare("getTreeSize") == 0){
            //获得该计算节点，实体树的规模大小（目前是整体的实体出现次数）
            string resp = "OK";
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);

            //接受长度，返回OK
            char recvLen[MAXCOMMANDSIZE];
            recvbyte = zmq_recv(slave_receiver, recvLen, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            int len = atoi(recvLen);
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
            
            cout << "slave getTreeSize'len: " << len << endl;
            //接受内存，返回OK
            char *buf = new char[len+1];
            recvbyte = zmq_recv(slave_receiver, buf, len, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
            string target(buf);

            //解码内容
            string result;
            istringstream iss(target);
            string temp;
            vector<string> tmpResult;
            while (getline(iss, temp, '_')) {
                tmpResult.push_back(temp);
            }

            for(int i = 0; i < tmpResult.size(); ++i){
                int scale = tree->getTotalNumber(atoi(tmpResult[i].c_str()));
                result += to_string(scale) + "_";
            }

            result.pop_back();

            //发送结果长度
            string resultLen = to_string(result.size());
            recvbyte = zmq_recv(slave_receiver, recvLen, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, resultLen.c_str(), resultLen.size()+1, 0);

            //发送结果内容
            recvbyte = zmq_recv(slave_receiver, recvLen, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, result.c_str(), result.size()+1, 0);

            delete buf;

        } else if(command.compare("getTreeContent") == 0) {

            double startTime = get_time();

            //获得该计算节点，实体树的所有子树（以三元组的形式传回去）
            string resp = "OK";
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
            
            //接受字符串长度，返回OK
            char recvLen[MAXCOMMANDSIZE];
            recvbyte = zmq_recv(slave_receiver, recvLen, MAXCOMMANDSIZE, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
            int len = atoi(recvLen);

            //接受字符串，返回OK
            char* buf = new char[len+1];
            recvbyte = zmq_recv(slave_receiver, buf, len, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);

            //解码内容
            string target(buf);
            istringstream iss(target);
            string temp;
            vector<string> taskArr;
            while (getline(iss, temp, '_')) {
                taskArr.push_back(temp);
            }
            
            int interval = taskArr.size() / PTHREAD_NUM + 1;
            cout << "interval:" << interval << endl;

            //开多个线程，并行遍历子树
            std::vector<pthread_t> pidArr(PTHREAD_NUM);
            std::vector<threadArg> pidArg;

            for(int i = 0; i < PTHREAD_NUM; ++i){
                threadArg arg;
                // arg.index = i;
                arg.ser = this;
                arg.beginIndex = i * interval;
                arg.interval = interval;
                arg.tasks = taskArr;
                pidArg.push_back(arg);
            }
            
            for(int i = 0; i < PTHREAD_NUM; ++i){
                pthread_create(&pidArr[i], NULL, runThread, (void*)&pidArg[i]);
            }

            for(int i = 0; i < PTHREAD_NUM; ++i){
                pthread_join(pidArr[i], NULL);
            }

            // check thread return value
            string lastResult;
            for(int i = 0; i < pidArg.size(); ++i){
                lastResult += pidArg[i].threadResult;
            }

            //因为最后多了一个'$'，则去除
            lastResult.pop_back();

            // 这里有问题            
            // for(int i = 0; i < PTHREAD_NUM; ++i){
            //     pthread_cancel(pidArr[i]);
            // }

            cout << "slave "<< port <<" thread end" << endl;

            //接受OK，返回字符串长度
            char isOK[20];
            string lastResultLen = to_string(lastResult.size());
            recvbyte = zmq_recv(slave_receiver, isOK, 20, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, lastResultLen.c_str(), lastResultLen.size()+1, 0);

            //接受OK，返回字符串
            recvbyte = zmq_recv(slave_receiver, isOK, 20, 0);
            assert(recvbyte != -1);
            zmq_send(slave_receiver, lastResult.c_str(), lastResult.size()+1, 0);

            double endTime = get_time();
            cout << "slave use " << endTime - startTime << "s do getTreeContent for " << port << endl;

        } else if(command.compare("exit") == 0){
            string resp = "OK";
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
            break;
        } else {
            string resp = "Invaild Command";
            zmq_send(slave_receiver, resp.c_str(), resp.size()+1, 0);
            exit(1);
        }
    }

    zmq_close(slave_receiver);
    zmq_ctx_destroy(slave_ctx);
}

void server::buildSlaveConnector(){
    for(int i = 0; i < clusterIP.size(); ++i){
        if(clusterIP[i] == serverIP){
            continue;
        }
        void* sender_ctx = zmq_ctx_new();
        void *sender = zmq_socket(ctx, ZMQ_REQ);
        string desPort;
        auto it = portMap.find(serverIP);
        for(int k = 0; k != portMap.count(serverIP); k++, it++){
            if(it->second.first == clusterIP[i]){
                desPort = it->second.second;
            }
        }
        if(desPort == ""){
            cout << "find portMap error" << endl;
            continue;
        }
        
        string serverAddr = "tcp://" + clusterIP[i] + ":" + desPort;
        cout << serverAddr << endl;
        zmq_connect(sender, serverAddr.c_str());
        senderContexts.push_back(sender_ctx);
        senders.push_back(sender);
    }
}

void server::stopSlaveConnector(){
    for(int i = 0; i < senders.size(); ++i){
        zmq_close(senders[i]);
    }
    for(int i = 0; i < senderContexts.size(); ++i){
        zmq_ctx_destroy(senderContexts[i]);
    }
}

void server::runGetTreeContentThread(void* arg){
    threadArg* localArg = (threadArg*)arg;
    int limit = localArg->beginIndex + localArg->interval;
    
    // pthread_mutex_lock(&mu);
    int n = 0;
    string& result = ((threadArg*)arg)->threadResult;
    for(int i = localArg->beginIndex; i < limit && i < localArg->tasks.size(); ++i){
        auto tempResult = tree->getSubTree(atoi(localArg->tasks[i].c_str()));
        if(tempResult.empty()){
            continue;
        }
        string response;
        for(auto r : tempResult){
            response += to_string(get<0>(r));
            response += '_';
            response += to_string(get<1>(r));
            response += '_';
            response += to_string(get<2>(r));
            response += '|';
        }
        result += response + "$";
        // n++;
        // if(n % 10 == 0) cout << n << endl;
    }

    // pthread_mutex_unlock(&mu);
}

//多开线程，非成员函数调用成员函数
void* runThread(void* arg){
    ((threadArg*)arg)->ser->runGetTreeContentThread(arg);
    return 0;
}