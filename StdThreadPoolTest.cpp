/*
 * StdThreadPoolTest.cpp
 *
 *  Created on: Jun 15, 2016
 *      Author: niklas
 */

#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "StdThreadPool.h"
#include <atomic>
#include <cmath>

//Creates 2^limit tasks
unsigned char exponentialTaskRecursion(StdThreadPool *ptr, std::atomic_ullong &var, unsigned char limit) {
    ++var;
    if(limit > 0) {
        limit -= 1;
        ptr->addTask(std::bind(exponentialTaskRecursion, ptr, std::ref(var), limit));
        ptr->addTask(std::bind(exponentialTaskRecursion, ptr, std::ref(var), limit));
    }
    return limit;
}

//Calculates a lower bound for the number of working threads in a thread pool while trying to not assume anything about the behaviour of the thread pool.
//Threads that are locked up somewhere will not be detected and thus would not contribute to the lower bound.
//If there are more threads running than the thread pool size, this will be detected.
//Precondition: pool is empty
unsigned long detectNumberOfWorkingThreadsLowerBound(StdThreadPool &pool) {
  std::set<std::thread::id> threadIDs;
  std::mutex setMutex, runMutex;
  std::condition_variable runNotification;
  std::atomic_ulong numTasksExecuted;
  numTasksExecuted.store(0);
  bool run = false;
  unsigned long const numTasksToCreate = pool.getMaxNumThreads() + 1;
  for(unsigned long i = 0; i < numTasksToCreate; ++i) {
    pool.addTask([&]()->int {
      std::unique_lock<std::mutex> lock(runMutex);
      //Wait for non-spurious wake up
      runNotification.wait(lock, [&](){ return run; });
      std::unique_lock<std::mutex> setLock(setMutex);
      threadIDs.insert(std::this_thread::get_id());
      setLock.unlock();
      ++numTasksExecuted;
      return numTasksExecuted;
    });
  }
  //Lock the runMutex. This will be used with the condition variable to make
  //all threads wait for all tasks to be assigned.
  sleep(1);
  runMutex.lock();
  run = true;
  runNotification.notify_all();
  runMutex.unlock();
  pool.wait();
  if(numTasksExecuted.load() != numTasksToCreate) {
    throw "Not all tasks were executed.";
  }
  return threadIDs.size();
}

SCENARIO("StdThreadPool basic tests") {
    GIVEN("A thread pool.") {
        unsigned int const numThreads = 10;
        StdThreadPool pool(numThreads);

        WHEN("running threads inserting their threads ID in a set") {
          auto numThreadsDetected = detectNumberOfWorkingThreadsLowerBound(pool);
          THEN("the number of thread IDs in the set should be equal to the size of the thread pool") {
            REQUIRE(numThreadsDetected == numThreads);
            REQUIRE(numThreadsDetected == pool.getMaxNumThreads());
            REQUIRE(pool.getNumTasksRunning() == 0);
            REQUIRE(pool.empty());
          }
        }

        WHEN("running a function increasing a counter multiple times, creating all tasks in this thread") {
          std::atomic_ulong var;
          unsigned long limit = 100;
          var.store(0);
          for(unsigned long i = 0; i < limit; ++i) {
            pool.addTask([&var]()->void{
              ++var;
            });
          }
          pool.wait();
          THEN("the counter should be increased by the same amount of times") {
            REQUIRE(var.load() == limit);
          }
          AND_WHEN("reusing the pool") {
            auto numThreadsDetected = detectNumberOfWorkingThreadsLowerBound(pool);
            THEN("all threads are still alive") {
              REQUIRE(numThreadsDetected == numThreads);
            }
          }
        }

        WHEN("running a function increasing a counter multiple times, creating only one task in this thread") {
            std::atomic_ullong var;
            unsigned long limit = 15;
            var.store(0);
            pool.addTask(std::bind(exponentialTaskRecursion, &pool, std::ref(var), limit));
            pool.wait();
            THEN("the counter should be increased by the same amount of times") {
              REQUIRE(var.load() == static_cast<unsigned long long>((1.0 - std::pow(2, limit+1))/(1.0 - 2.0)));
            }
        }
    }
}

SCENARIO("StdThreadPool dependency management tests") {
  GIVEN("A thread pool") {
    auto &pool = StdThreadPool::getDefaultInstance();
    WHEN("running two functions, one depending on the other") {
      unsigned long var = 0;
      auto t1 = pool.addTask([&](){
        sleep(1);
        var = 1;
      });
      auto t2 = pool.addTask([&](){
        var += 1;
      }, {t1.TaskID});
      THEN("the second task shall not run before the first") {
        REQUIRE(pool.getNumTasks() == 2);
        pool.wait();
        REQUIRE(var == 2);
      }
      AND_WHEN("running a third function, depending on both") {
        std::set<StdThreadPool::TaskID> dependencies;
        dependencies.insert(t1.TaskID);
        dependencies.insert(t2.TaskID);
        auto t3 = pool.addTask([&](){
          var *= 2;
        }, dependencies);
        THEN("the third function should be executed after the first two ran in order") {
          REQUIRE(pool.getNumTasks() == 3);
          pool.wait();
          REQUIRE(var == 4);
        }
      }
    }
  }
}

