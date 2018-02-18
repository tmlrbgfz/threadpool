CXXFLAGS=-std=c++11 -g
LDFLAGS=-pthread -lboost_thread -lboost_system
CXX=g++

StdThreadPoolTest.o: StdThreadPoolTest.cpp StdThreadPool.h
	$(CXX) -c $(CXXFLAGS) $<

StdThreadPool.o: StdThreadPool.cpp StdThreadPool.h
	$(CXX) -c $(CXXFLAGS) $<

stdthreadpool.a: StdThreadPool.o
	ar q $@ $^

tests: StdThreadPoolTest.o stdthreadpool.a
	$(CXX) -o $@ $(CXXFLAGS) $^ $(LDFLAGS)

.PHONY: all clean
all: tests

clean:
	rm -f StdThreadPoolTest.o StdThreadPool.o
