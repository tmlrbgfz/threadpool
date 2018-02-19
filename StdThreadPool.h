/*
 * StdThreadPool.h
 *
 *  Created on: May 24, 2016
 *      Author: niklas
 */

#ifndef EXECUTABLES_RTT_MBT_ECPT_STDTHREADPOOL_H_
#define EXECUTABLES_RTT_MBT_ECPT_STDTHREADPOOL_H_

#include <thread>
#include <mutex>
#include <condition_variable>
#include <forward_list>
#include <vector>
#include <atomic>
#include <memory>
#include <map>
#include <set>
#include <list>
#include <deque>
#include <future>
#include <type_traits>
#include <utility>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/optional.hpp>
#include <stdint.h>
#include "copyismove.h"

//#define CONGESTION_ANALYSIS

/*
 * List of known flaws:
 * 
 * 1. After std::numeric_limits<uint64_t>::max() tasks, the TaskIDs wrap
 * around.  While it's not a problem that there is a point where a returned
 * task id is lower than the previously returned task id, it is possible that
 * one of the subsequently returned task ids corresponds to a task still in the
 * list of active tasks.
 * Example:
 * Start a task doing some computation and then rescheduling itself.
 * Start std::numeric_limits<uint64_t>::max()-1 tasks.
 * The next task added is assigned the id still belonging to the task rescheduling itself.
 */

class StdThreadPool {
public:
    typedef std::vector<std::thread>::size_type size_type;
    typedef uint64_t TaskID;

    typedef std::function<void(void)> TaskDefinition;

    template<typename T>
    struct TaskHandle_t {
      //TODO: Make const
      StdThreadPool::TaskID TaskID;
      std::future<T> future;
    };
    template<typename T>
    using TaskHandle = struct TaskHandle_t<T>;

    class TaskPackage {
    private:
    	friend class StdThreadPool;
        std::mutex mutex;
        std::deque<TaskID> tasks;
        StdThreadPool *correspondingPool;

        TaskPackage();
    public:
        template<typename T, typename ...Args>
        StdThreadPool::TaskHandle<T> addTask(std::function<T(Args...)> &&task, Args...args, const std::set<TaskID> &dependencies) {
          auto taskHandle = correspondingPool->addTask(std::move(task), std::forward(args...), dependencies);
          std::unique_lock<std::mutex> lock(this->mutex);
          this->tasks.push_back(taskHandle.TaskID);
        }
        template<typename T, typename ...Args>
        StdThreadPool::TaskHandle<T> addTask(std::function<T(Args...)> &&task, Args...args, const TaskID dependency) {
          auto taskHandle = correspondingPool->addTask(std::move(task), std::forward(args...), dependency);
          std::unique_lock<std::mutex> lock(this->mutex);
          this->tasks.push_back(taskHandle.TaskID);
        }
        //Returns true if the tasks which were in this package when calling this function
        //  are all finished. There might be further tasks which were added while this
        //  function was executed
        bool finished();
        void wait();
        void wait(TaskID);

        std::set<TaskID> getAsDependency() const;
    };
    typedef std::shared_ptr<TaskPackage> TaskPackagePtr;
private:
    struct Task {
    	TaskDefinition fn;
    	std::atomic_ulong dependencyCount;
    	std::vector<TaskID> dependants;
    	std::mutex objMutex;
    	std::condition_variable cv_done;
    	bool done;
    };
private:
    static std::shared_ptr<StdThreadPool> defaultInstancePtr;
    static std::map<std::thread::id, StdThreadPool*> threadAssociation;
    static std::mutex globalMutex;

    typedef std::list<TaskID> TaskList;
    typedef std::map<TaskID, std::shared_ptr<Task>> TaskContainer;

    std::atomic<TaskID> tIdRegistry;
    std::atomic<size_type> numTasksRunning;
    std::atomic<size_type> numTasksTotal;
    std::vector<std::thread> threads;
    std::mutex instanceMutex;
    std::condition_variable workAvailable;
    TaskList inactiveTasks;
    TaskList activeTasks;
    std::map<std::thread::id, TaskContainer::iterator> threadWork;
    TaskContainer taskDefinitions;
    boost::shared_mutex taskDefAccessMutex;
    bool stop;

#ifdef CONGESTION_ANALYSIS
    std::atomic_ullong instanceLockCongestion, instanceLockTries;
    std::atomic_ullong taskDefLockCongestion, taskDefLockTries;
    std::atomic_ullong taskDefExLockCongestion, taskDefExLockTries;

    void tryLockInstanceLock() { if(this->instanceMutex.try_lock() == false) ++instanceLockCongestion; else this->instanceMutex.unlock(); ++instanceLockTries; }
    void tryLockTaskDefLockShared() { if(this->taskDefAccessMutex.try_lock_shared() == false) ++taskDefLockCongestion; else this->taskDefAccessMutex.unlock_shared(); ++taskDefLockTries; }
    void tryLockTaskDefLock() { if(this->taskDefAccessMutex.try_lock() == false) ++taskDefExLockCongestion; else this->taskDefAccessMutex.unlock(); ++taskDefExLockTries; }
#endif

    size_type maxNumThreads;

    void work(void);
    TaskContainer::iterator getTaskDefinition(const TaskID id, bool lockingRequired);
    void addDependencies(const TaskID id, const std::set<TaskID> &dependencies);
    TaskID addTaskDetail(TaskDefinition &&task, const std::set<TaskID> &dependencies);
    void removeTask(const TaskID id);

    TaskID findNextUnusedTaskId(TaskID id) const;

    static StdThreadPool *threadPoolAssociation();
    static TaskContainer::iterator getOwnTaskIterator();
    static TaskID getOwnTaskID();

    StdThreadPool();
public:
    StdThreadPool(size_type size);
    StdThreadPool(const StdThreadPool &/*other*/) = delete;
    ~StdThreadPool();

    static StdThreadPool &getDefaultInstance();
    static StdThreadPool *getDefaultInstancePtr();

    size_type getMaxNumThreads() const;
    size_type getNumTasksRunning() const;
    size_type getNumTasks() const;
    bool empty();

    template<typename Fn>
    auto addTask(Fn task, std::set<TaskID> const &dependencies = std::set<TaskID>())
      -> StdThreadPool::TaskHandle<typename std::result_of<Fn()>::type> {
      typedef typename std::result_of<Fn()>::type ReturnType;
      StdThreadPool::TaskHandle<ReturnType> returnValue;
      std::packaged_task<ReturnType()> packagedTask(task);
      returnValue.future = std::move(packagedTask.get_future());
      CopyIsMove<std::packaged_task<ReturnType()>> packagedTaskWrapper(std::move(packagedTask));
      //Is it possible to add the packaged task as a unique pointer to the lambda object?
      returnValue.TaskID = this->addTaskDetail([packagedTaskWrapper]()mutable->void{
        (packagedTaskWrapper.getData()());
      }, dependencies);
      return returnValue;
    }
    template<typename Fn, typename ...Args>
    auto addTask(Fn task, Args...args, std::set<TaskID> const &dependencies = std::set<TaskID>())
      -> StdThreadPool::TaskHandle<typename std::result_of<Fn(Args...)>::type> {
      return this->addTask(std::bind(task, std::forward(args...)), dependencies);
    }

    void wait();
    void wait(TaskID id);
    void waitOne();
    bool finished(TaskID id);

    TaskPackagePtr createTaskPackage();
};

#endif /* EXECUTABLES_RTT_MBT_ECPT_STDTHREADPOOL_H_ */
