CXXFLAGS=-std=c++17 -g -O0 -DTHREADPOOL_USE_GSL
LDFLAGS=-pthread
CXX=g++

.PHONY: all clean
all: tests

ThreadPoolTest.o: ThreadPoolTest.cpp ThreadPool.hpp
	$(CXX) -c $(CXXFLAGS) $<

tests: ThreadPoolTest.o
	$(CXX) -o $@ $(CXXFLAGS) $^ $(LDFLAGS)

clean:
	rm -f ThreadPoolTest.o ThreadPool.o tests threadpool.a
