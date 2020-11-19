TARGET = main main_server query

CC =  g++ 
CCFLAGS =  -g -ggdb -Wall 

#依赖库文件
LIB =  -std=c++11 -lpthread -lzmq

#当前目录和CPPINC目录下查找原文件
CPPINC = /usr/include
INC = -I. -I$(CPPINC)

#创建执行文件路径
BIN_DIR = ./bin/
# OBJ_DIR = ./obj/
$(shell mkdir -p ${BIN_DIR})
# $(shell mkdir -p ${OBJ_DIR})

all : $(TARGET)

main_server: parallelTree.cpp server.cpp main_server.cpp header.cpp
	$(CC) $(CCFLAGS) -o $@ $^ $(INC) $(LIB) 
	mv main_server ${BIN_DIR}
	# mv *.o $(OBJ_DIR)

main: main.cpp client.cpp partition.cpp stringToIDMapping.cpp parallelTree.cpp header.cpp
	$(CC) $(CCFLAGS) -o $@ $^ $(INC) $(LIB) 
	mv main $(BIN_DIR)
	# mv *.o $(OBJ_DIR)

query: query.cpp queryDecomposition.cpp queryTree.cpp
	$(CC) $(CCFLAGS) -o $@ $^ $(INC) $(LIB) 
	mv query ${BIN_DIR}

#编译所有.cpp文件为.o文件
# .cpp.o:
# 	@echo $<
# 	$(CC) $(CCFLAGS) -c -o $@ $< 
    #$(CC) $(CCFLAGS)  -c -o $*.o $*.cpp $(INC) $(LIB)

#编译所有.c文件为.o文件
# .c.o:
# 	@echo $<
# 	$(CC) $(CCFLAGS) -c -o $@ $< 
    #$(CC)  $(CCFLAGS) -c -o $*.o $*.c  $(INC) $(LIB)
clean: 
	- rm -f $(OBJ_DIR)*.o $(BIN_DIR)$(TARGET)
rmData:
	- rm -rf ./subData/