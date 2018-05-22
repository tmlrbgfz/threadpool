CXXFLAGS=-std=c++11 -g -O0 -DSTDTHREADPOOL_STANDALONE
LDFLAGS=-pthread -lboost_thread -lboost_system
CXX=g++

ThreadPoolTest.o: ThreadPoolTest.cpp ThreadPool.hpp
	$(CXX) -c $(CXXFLAGS) $<

ThreadPool.o: ThreadPool.cpp ThreadPool.hpp
	$(CXX) -c $(CXXFLAGS) $<

threadpool.a: ThreadPool.o
	ar q $@ $^

tests: ThreadPoolTest.o
	$(CXX) -o $@ $(CXXFLAGS) $^ $(LDFLAGS)

.PHONY: all clean
all: tests

clean:
	rm -f ThreadPoolTest.o ThreadPool.o tests threadpool.a
