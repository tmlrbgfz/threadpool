CXXFLAGS=-std=c++17 -g -O0 -DTHREADPOOL_USE_GSL -DSTDTHREADPOOL_STANDALONE
LDFLAGS=-pthread -lboost_thread -lboost_system
CXX=g++

ThreadPoolTest.o: ThreadPoolTest.cpp ThreadPool.hpp
	$(CXX) -c $(CXXFLAGS) $<

tests: ThreadPoolTest.o
	$(CXX) -o $@ $(CXXFLAGS) $^ $(LDFLAGS)

.PHONY: all clean
all: tests

clean:
	rm -f ThreadPoolTest.o ThreadPool.o tests threadpool.a
