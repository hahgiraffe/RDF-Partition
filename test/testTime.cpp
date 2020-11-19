// 测试加载文件的时间
// g++ testTime.cpp -o testTime
#include <tuple>
#include <bitset>
#include <sys/time.h>
#include <fstream>
#include <iostream>
#include <unordered_map>

using namespace std;

double get_time(){
	struct timeval time;
	if (gettimeofday(&time,NULL)) return 0;
	return (double)time.tv_sec + (double)time.tv_usec * .0000001;
}

//将RDF文件每一行提取出三元组SPO
tuple<string, string, string> splitTriple(string line){
    line.resize(line.size()-2);
    size_t slocation = line.find_first_of(' ');
    string s = line.substr(0, slocation);
    size_t plocation = line.find_first_of(' ', slocation+1);
    string p = line.substr(slocation+1, plocation - slocation);
    string o = line.substr(plocation+1);
    return make_tuple(s, p, o);
}

//加载RDF文件，并做ID映射
bool loadGraph(string file, string& IDbuffer){
    cout << "begin loadGraph" << endl;
    ifstream inputFile(file, ios::in);
    if(!inputFile){
        cerr << "rdfFile open error" << endl;
        return false;
    }
    unordered_map<string, int> uriTable;
    unordered_map<string, int> preTable;
    int uriCount = 1;
    int preCount = 1;
    // ofstream outputFile("subData/StringTOIDFile", ios::out);
    // if(!outputFile){
    //     cerr << "outputFile open error" << endl;
    //     return false;
    // }
    string line;
    int n = 0;
    while(getline(inputFile, line)){
        if(line.size() <= 2) continue;
        auto triple = splitTriple(line);
        string s = get<0>(triple), p = get<1>(triple), o = get<2>(triple);
        int sid = uriTable[s];
        if(sid == 0){
            sid = uriCount;
            uriTable[s] = uriCount++;
        }

        int pid = preTable[p];
        if(pid == 0){
            pid = preCount;
            preTable[p] = preCount++;
        }

        int oid = uriTable[o];
        if(oid == 0){
            oid = uriCount;
            uriTable[o] = uriCount++;
        }
        // int sid = mapping.addUri(s);
        // int pid = mapping.addPredicate(p);
        // int oid = mapping.addUri(o);
        // outputFile << mapping.getUriID(s) << " " << mapping.getPredicateID(p) << " " << mapping.getUriID(o) << endl;
        IDbuffer += to_string(sid) + " " + to_string(pid) + " " + to_string(oid) + "\n";
        // IDbuffer = IDbuffer + to_string(sid) + " " + to_string(pid) + " " + to_string(oid) + "\n";
        n++;
        if(n % 10000 == 0){
            cout << n << endl;
            cout << "preTable's size" << preTable.size() << endl;
            cout << "uriTable's size" << uriTable.size() << endl;
        }
    }
    inputFile.close();

    cout << "preTable's size" << preTable.size() << endl;
    cout << "uriTable's size" << uriTable.size() << endl;
    // outputFile.close();
    return true;
}

int main(int argc, char* argv[]){
    if(argc != 2){
        cout << "arg error" << endl;
        return 0;
    }
    double startTime = get_time();
    string buf;
    loadGraph(argv[1], buf);
    double endTime = get_time();
    cout << endTime - startTime << "s" << endl;
    return 0;
}