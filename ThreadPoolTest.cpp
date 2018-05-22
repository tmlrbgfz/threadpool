/*
 * ThreadPoolTest.cpp
 *
 *  Created on: Jun 15, 2016
 *      Author: niklas
 */

#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "ThreadPool.hpp"
#include "ThreadPoolPolicies.hpp"
#include <atomic>
#include <cmath>
#include <random>
#include <chrono>
#include <ratio>

//Creates 2^limit tasks
template<typename DependencyPolicy>
unsigned char exponentialTaskRecursion(ThreadPool<DependencyPolicy> *ptr, std::atomic_ullong &var, unsigned char limit) {
    ++var;
    if(limit > 0) {
        limit -= 1;
        ptr->addTask(std::bind(exponentialTaskRecursion<DependencyPolicy>, ptr, std::ref(var), limit));
        ptr->addTask(std::bind(exponentialTaskRecursion<DependencyPolicy>, ptr, std::ref(var), limit));
    }
    return limit;
}

//Calculates a lower bound for the number of working threads in a thread pool while trying to not assume anything about the behaviour of the thread pool.
//Threads that are locked up somewhere will not be detected and thus would not contribute to the lower bound.
//If there are more threads running than the thread pool size, this will be detected.
//Precondition: pool is empty
template<typename DependencyPolicy>
unsigned long detectNumberOfWorkingThreadsLowerBound(ThreadPool<DependencyPolicy> &pool) {
  std::set<std::thread::id> threadIDs;
  std::mutex setMutex, runMutex;
  std::condition_variable runNotification;
  std::atomic_ulong numTasksExecuted;
  numTasksExecuted.store(0);
  bool run = false;
  unsigned long const numTasksToCreate = pool.getNumberOfThreads() + 1;
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

SCENARIO("ThreadPool basic tests") {
    GIVEN("A thread pool.") {
        unsigned int const numThreads = 10;
        ThreadPool<policies::DependenciesRespected> pool(numThreads);

        WHEN("running threads inserting their threads ID in a set") {
          auto numThreadsDetected = detectNumberOfWorkingThreadsLowerBound(pool);
          THEN("the number of thread IDs in the set should be equal to the size of the thread pool") {
            REQUIRE(numThreadsDetected == numThreads);
            REQUIRE(numThreadsDetected == pool.getNumberOfThreads());
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
            pool.addTask(std::bind(exponentialTaskRecursion<policies::DependenciesRespected>, &pool, std::ref(var), limit));
            pool.wait();
            THEN("the counter should be increased by the same amount of times") {
              REQUIRE(var.load() == static_cast<unsigned long long>((1.0 - std::pow(2, limit+1))/(1.0 - 2.0)));
            }
        }

        WHEN("running a task returning an integer") {
          std::random_device rd;
          std::mt19937 random_number_engine(rd());
          std::uniform_int_distribution<int> distribution;
          unsigned long value = distribution(random_number_engine);
          auto task = pool.addTask([value]()->int{
            return value;
          });
          pool.wait(task.TaskID);
          THEN("the integer should be available through the returned future object") {
            REQUIRE(task.future.get() == value);
          }
        }
    }
}

SCENARIO("ThreadPool dependency management tests") {
  GIVEN("A thread pool") {
    auto &pool = ThreadPool<policies::DependenciesRespected>::getDefaultInstance();
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
        std::set<ThreadPool<policies::DependenciesRespected>::TaskID> dependencies;
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

SCENARIO("Thread pool size modification tests") {
  GIVEN("A thread pool") {
    ThreadPool<policies::DependenciesRespected> pool(1);
    unsigned int numTasksToCreate = 12;
    WHEN("Adding tasks") {
      auto startTime = std::chrono::steady_clock::now();
      for(unsigned int i = 0; i < numTasksToCreate; ++i) {
        pool.addTask([&](){
          sleep(1);
        });
      }
      AND_WHEN("the thread pool has one thread") {
        THEN("the time needed should be the number of tasks in seconds") {
          pool.wait();
          auto endTime = std::chrono::steady_clock::now();
          std::chrono::nanoseconds duration = endTime - startTime;
          REQUIRE(duration.count()/std::nano::den >= numTasksToCreate);
        }
      }
      AND_WHEN("the thread pool size is increased to two") {
        pool.setNumberOfThreads(2);
        THEN("the time needed should be half the number of tasks in seconds") {
          pool.wait();
          auto endTime = std::chrono::steady_clock::now();
          std::chrono::nanoseconds duration = endTime - startTime;
          REQUIRE(duration.count()/std::nano::den < numTasksToCreate);
          REQUIRE(duration.count()/std::nano::den >= numTasksToCreate/2);
        }
      }
      AND_WHEN("the thread pool size is increased to three") {
        pool.setNumberOfThreads(3);
        THEN("the time needed should be one third the number of tasks in seconds") {
          pool.wait();
          auto endTime = std::chrono::steady_clock::now();
          std::chrono::nanoseconds duration = endTime - startTime;
          REQUIRE(duration.count()/std::nano::den < numTasksToCreate/2);
          REQUIRE(duration.count()/std::nano::den >= numTasksToCreate/3);
        }
      }
      AND_WHEN("the thread pool size is increased to four") {
        pool.setNumberOfThreads(4);
        THEN("the time needed should be one fourth the number of tasks in seconds") {
          pool.wait();
          auto endTime = std::chrono::steady_clock::now();
          std::chrono::nanoseconds duration = endTime - startTime;
          REQUIRE(duration.count()/std::nano::den < numTasksToCreate/3);
          REQUIRE(duration.count()/std::nano::den >= numTasksToCreate/4);
        }
        AND_WHEN("reducing the number of threads to one again after one second") {
          sleep(1);
          pool.setNumberOfThreads(1);
          THEN("The number of joinable threads should be three") {
            sleep(1);
            REQUIRE(pool.joinStoppedThreads() == 3);
            AND_THEN("The time needed should be between two thirds and one third the number of tasks in seconds") {
              pool.wait();
              auto endTime = std::chrono::steady_clock::now();
              std::chrono::nanoseconds duration = endTime - startTime;
              REQUIRE(duration.count()/std::nano::den <= (numTasksToCreate - numTasksToCreate/3 + 1));
              REQUIRE(duration.count()/std::nano::den >= numTasksToCreate/3);
            }
          }
        }
      }
    }
  }
}

SCENARIO("Performance tests", "[performance]") {
  GIVEN("A big thread pool") {
    ThreadPool<policies::DependenciesNotRespected> pool(8);
    auto startTime1 = std::chrono::steady_clock::now();
    pool.addTask([]()->unsigned int{
      unsigned int x = 0;
      while(x + 1 > x) {
        x += 1;
      }
      return x;
    });
    pool.wait();
    auto endTime1 = std::chrono::steady_clock::now();
    std::chrono::nanoseconds duration1 = endTime1 - startTime1;
    std::cout << "Base duration: " << duration1.count()/std::nano::den << std::endl;

    WHEN("Running a number of tasks less or equal than the number of threads") {
      for(unsigned int i = 1; i <= 8; ++i) {
        auto startTime2 = std::chrono::steady_clock::now();
        for(unsigned int j = 1; j <= i; ++j) {
          pool.addTask([]()->unsigned int{
            unsigned int x = 0;
            while(x + 1 > x) {
              x += 1;
            }
            return x;
          });
        }
        pool.wait();
        auto endTime2 = std::chrono::steady_clock::now();
        std::chrono::nanoseconds duration2 = endTime2 - startTime2;
        std::cout << "Measured duration for n = " << i << ": " << duration2.count()/std::nano::den << std::endl;
        REQUIRE((((double)duration1.count()) * 0.8 < duration2.count() && ((double)duration1.count()) * 2 > duration2.count()));
        sleep(1);
      }
    }
    WHEN("Running a number of tasks greater than the number of threads") {
      for(unsigned int i = 9; i <= 24; ++i) {
        auto startTime2 = std::chrono::steady_clock::now();
        for(unsigned int j = 1; j <= i; ++j) {
          pool.addTask([]()->unsigned int{
            unsigned int x = 0;
            while(x + 1 > x) {
              x += 1;
            }
            return x;
          });
        }
        pool.wait();
        auto endTime2 = std::chrono::steady_clock::now();
        std::chrono::nanoseconds duration2 = endTime2 - startTime2;
        std::cout << "Measured duration for n = " << i << ": " << duration2.count()/std::nano::den << std::endl;
        REQUIRE((((double)duration1.count()) * 0.8 * ((i/8)+1) < duration2.count() && ((double)duration1.count()) * 2 * ((i/8)+1) > duration2.count()));
        sleep(1);
      }
    }
  }
}

