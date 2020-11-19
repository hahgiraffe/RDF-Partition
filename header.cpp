#include "header.h"

//读取配置文件
void readConf(int& clusterNumber, std::vector<std::string>& clusterNodeIP){
    std::fstream ifs("./cluster.conf");
    std::string line;
    if(!ifs){
        std::cout << "open cluster.conf error" << std::endl;
        exit(1);
    }

    while(getline(ifs, line)){
        // std::cout << line << std::endl;
        if(line.find("clusterNumber") != line.npos){
            auto tmp = line.find(":") + line.begin() + 1;
            while(tmp != line.end()){
                if(isdigit(*tmp)){
                    int sum = 0;
                    while(tmp != line.end() && isdigit(*tmp)){
                        sum = sum * 10 + *tmp - '0';
                        tmp++;
                    }
                    // std::cout << "mysum" << sum;
                    clusterNumber = sum;
                    break;
                }
                tmp++;
            }
        } else if(line.find("clusterNodeIP") != line.npos){
            auto it = line.find(":") + line.begin() + 1;
            while(it != line.end()){
                if(*it == '\"'){
                    it++;
                    std::string tmp;
                    while(it != line.end() && *it != '\"'){
                        tmp += *it;
                        it++;
                    }
                    clusterNodeIP.push_back(tmp);
                }
                it++;
            }
        }
    }
}

//读取数据文件行数
int loadFileCount(const std::string& fileName){
    FILE* fstream = NULL;
    char buf[1024];
    memset(buf, 0, sizeof(buf));

    std::string command = "wc -l " + fileName;
    if(NULL == (fstream = popen(command.c_str(), "r"))){
        std::cerr << "execute command failed: " << errno << std::endl;
        exit(1);
    }

    while(NULL != fgets(buf, sizeof(buf), fstream)){
        std::cout << buf;
    }

    std::string result(buf);
    pclose(fstream);
    return atoi(result.substr(0, result.find_first_of(' ')).c_str());
}

//将RDF文件每一行提取出三元组SPO
std::tuple<std::string, std::string, std::string> splitTriple(std::string line, bool isDeleteEnd){
    if(isDeleteEnd)
        line.resize(line.size()-2);
    size_t slocation = line.find_first_of(' ');
    std::string s = line.substr(0, slocation);
    size_t plocation = line.find_first_of(' ', slocation+1);
    std::string p = line.substr(slocation+1, plocation - slocation);
    std::string o = line.substr(plocation+1);
    return make_tuple(s, p, o);
}

double get_time(){
	struct timeval time;
	if (gettimeofday(&time,NULL)) return 0;
	return (double)time.tv_sec + (double)time.tv_usec * .0000001;
}