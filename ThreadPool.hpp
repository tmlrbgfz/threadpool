/*
 * ThreadPool.hpp
 *
 *  Created on: May 24, 2016
 *      Author: niklas
 *
 * Copyright (c) 2018 Niklas Krafczyk
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * The permission granted above is not granted to persons developing software for
 * military or paramilitary uses.
 */

#ifndef __THREADPOOL_THREADPOOL_H__
#define __THREADPOOL_THREADPOOL_H__

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
#include <functional>
#include <algorithm>
#include <optional>
#ifndef THREADPOOL_STANDALONE
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#endif
#ifdef THREADPOOL_USE_GSL
#include <gsl/gsl_assert>
#else
#define GSL_LIKELY(x) (!!(x))
#define Expects(x) static_cast<void>(0)
#endif
#include <stdint.h>
#include "ThreadPoolPolicies.hpp"
#include "AutoCleanedForwardList.hpp"

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
 * Start a task doing some computation forever.
 * Start std::numeric_limits<uint64_t>::max()-1 tasks.
 * The next task added is assigned the id still belonging to the task rescheduling itself.
 */

template<class DependencyPolicy = policies::DependenciesNotRespected>
class ThreadPool {
public:
    typedef std::vector<std::thread>::size_type size_type;
    typedef uint64_t TaskID;

    typedef std::function<void(void)> TaskDefinition;

    template<typename T>
    struct TaskHandle_t {
      //TODO: Make const
      ThreadPool<DependencyPolicy>::TaskID TaskID;
      std::future<T> future;
    };
    template<typename T>
    using TaskHandle = struct TaskHandle_t<T>;

    class TaskPackage {
    private:
    	friend class ThreadPool;
        std::mutex mutex;
        std::deque<TaskID> tasks;
        ThreadPool<DependencyPolicy> *correspondingPool;

        TaskPackage();
    public:
        template<typename T, typename ...Args>
        typename ThreadPool<DependencyPolicy>::template TaskHandle<T> addTask(std::function<T(Args...)> &&task, const std::set<TaskID> &dependencies, Args...args);
        template<typename T, typename ...Args>
        typename ThreadPool<DependencyPolicy>::template TaskHandle<T> addTask(std::function<T(Args...)> &&task, const TaskID dependency, Args...args);
        template<typename T, typename ...Args>
        typename ThreadPool<DependencyPolicy>::template TaskHandle<T> addTask(std::function<T(Args...)> &&task, Args...args);
        //Returns true if the tasks which were in this package when calling this function
        //  are all finished. There might be further tasks which were added while this
        //  function was executed
        bool finished();
        void wait();
        void wait(TaskID id);

        std::set<TaskID> getAsDependency() const;
    };
    typedef std::shared_ptr<TaskPackage> TaskPackagePtr;
private:
    template<typename T, typename Enable = void>
    struct TaskStruct {
    	TaskDefinition fn;
    	std::mutex objMutex;
    	std::condition_variable cv_done;
    	bool done;

        void addDependant(TaskID) {}
        void setNumberOfDependencies(unsigned long) {}
        unsigned long getNumberOfDependencies() {
            return 0;
        }
        std::vector<TaskID> getDependants() const { return {}; }
    };

    template<typename T>
    struct TaskStruct<T, typename std::enable_if<T::respectDependencies>::type> {
    	TaskDefinition fn;
    	std::atomic_ulong dependencyCount;
    	std::vector<TaskID> dependants;
    	std::mutex objMutex;
    	std::condition_variable cv_done;
    	bool done;

        void addDependant(TaskID id) {
            this->dependants.push_back(id);
        }
        void setNumberOfDependencies(unsigned long n) {
            this->dependencyCount = n;
        }
        unsigned long getNumberOfDependencies() {
            return this->dependencyCount.load();
        }
        std::vector<TaskID> getDependants() const { return dependants; }
    };
    /*template<typename T>
    struct TaskStruct<typename T, typename std::enable_if<!T::respectDependencies>::type> {
    	TaskDefinition fn;
    	std::mutex objMutex;
    	std::condition_variable cv_done;
    	bool done;
    };*/

    typedef TaskStruct<DependencyPolicy> Task;
private:
    static std::shared_ptr<std::map<std::thread::id, ThreadPool<DependencyPolicy>*>> threadAssociation;
    static std::unique_ptr<ThreadPool<DependencyPolicy>> defaultInstancePtr;
    static std::mutex globalMutex;

    typedef std::list<TaskID> TaskList;
    typedef std::map<TaskID, std::shared_ptr<ThreadPool<DependencyPolicy>::Task>> TaskContainer;

    std::atomic<TaskID> tIdRegistry;
    std::atomic<size_type> numTasksRunning;
    std::atomic<size_type> numTasksTotal;
    std::list<std::thread> threads;
    std::mutex instanceMutex;
    std::condition_variable workAvailable;
    std::condition_variable taskDone;

    std::mutex completedTasksMutex;
    AutoCleanedForwardList<TaskID> completedTasks;

    TaskList inactiveTasks;
    TaskList activeTasks;
    std::map<std::thread::id, typename TaskContainer::iterator> threadWork;
    TaskContainer taskDefinitions;
#ifndef THREADPOOL_STANDALONE
    boost::shared_mutex taskDefAccessMutex;
#else
    std::mutex taskDefAccessMutex;
#endif
    bool stop;
    unsigned int numThreadsToStop;
    std::vector<std::thread::id> joinableThreads;
    size_type maxNumThreads;

#ifdef CONGESTION_ANALYSIS
    std::atomic_ullong instanceLockCongestion, instanceLockTries;
    std::atomic_ullong taskDefLockCongestion, taskDefLockTries;
    std::atomic_ullong taskDefExLockCongestion, taskDefExLockTries;

    void tryLockInstanceLock() { if(this->instanceMutex.try_lock() == false) ++instanceLockCongestion; else this->instanceMutex.unlock(); ++instanceLockTries; }
    void tryLockTaskDefLockShared() { if(this->taskDefAccessMutex.try_lock_shared() == false) ++taskDefLockCongestion; else this->taskDefAccessMutex.unlock_shared(); ++taskDefLockTries; }
    void tryLockTaskDefLock() { if(this->taskDefAccessMutex.try_lock() == false) ++taskDefExLockCongestion; else this->taskDefAccessMutex.unlock(); ++taskDefExLockTries; }
#endif

    bool checkDependencies(typename TaskContainer::iterator const &task);
    void notifyDependencies(Task const *task);
    void addDependencies(Task *task, TaskID id, const std::set<TaskID> *dependencies);

    void work(void);
    typename TaskContainer::iterator getTaskDefinition(const TaskID id, bool lockingRequired);
    TaskID addTaskDetail(TaskDefinition&& task, const std::set<TaskID> *dependencies = nullptr);
    void removeTask(const TaskID id);

    TaskID findNextUnusedTaskId(TaskID id) const;

    static ThreadPool<DependencyPolicy> *threadPoolAssociation();
    static typename TaskContainer::iterator getOwnTaskIterator();
    static TaskID getOwnTaskID();

    ThreadPool();
public:
    ThreadPool(size_type size);
    ThreadPool(const ThreadPool &/*other*/) = delete;
    ~ThreadPool();

    static ThreadPool<DependencyPolicy>& getDefaultInstance();
    static ThreadPool<DependencyPolicy>* getDefaultInstancePtr();

    void setNumberOfThreads(size_type n);
    size_type getNumberOfThreads();
    //Cleanup function joining threads that were stopped to reduce the thread
    //pool size. Returns the number of joined threads.
    size_type joinStoppedThreads();

    size_type getNumTasksRunning() const;
    size_type getNumTasks() const;
    bool empty();

    template<typename Fn>
    auto addTask(Fn task, std::set<TaskID> const &dependencies) -> ThreadPool::TaskHandle<typename std::result_of<Fn()>::type>;
    template<typename Fn, typename ...Args>
    auto addTask(Fn task, std::set<TaskID> const &dependencies, Args...args) -> ThreadPool::TaskHandle<typename std::result_of<Fn(Args...)>::type>;
    template<typename Fn>
    auto addTask(Fn task) -> ThreadPool::TaskHandle<typename std::result_of<Fn()>::type>;
    template<typename Fn, typename ...Args>
    auto addTask(Fn task, Args...args) -> ThreadPool::TaskHandle<typename std::result_of<Fn(Args...)>::type>;


    /*
    * Principle of waiting for all tasks to finish:
    * 1. Lock the instanceMutex. This assures that the task lists are not modified
    *    while we examine them.
    * 2. If the (probably longer) list of inactive tasks is not empty, the last
    *    task is select. As the lists are processed sequentially from front to back,
    *    this is the task that will be selected last for execution. Note that other
    *    tasks may be inserted after selecting it.
    * 3. If the list of inactive tasks is empty and the list of active tasks is not empty,
    *    the last task of the list of active tasks is selected. Assuming all tasks to
    *    take about the same amount of time, this is the last one to finish.
    * 4. If the list of inactive tasks is empty and the list of active tasks is empty,
    *    all queues are empty for the time being, thus all tasks which were in any list when
    *    this function was called finished. Then, this function shall return and no task is selected.
    * 5. Unlock the instanceMutex. As both lists have been examined, they may be modified again.
    * 6. If a task was selected, wait for it. Afterwards, goto 1.
    * 7. If no task was selected, return.
    */
    void wait();

    /*
    * Principle of waiting for one task to finish:
    * 1. Lock the task definition access mutex shared. Thus, the container mapping
    *    task IDs to task definitions is locked and can still be read but not modified.
    *    This means that the task definition can not be removed before the mutex is
    *    unlocked by all readers.
    * 2. Get the task definition. If the task does not exist in the container, it already finished.
    *    Therefore we may return after unlocking the mutex again.
    * 3. If the task definition exists in the container, lock it's internal mutex.
    *    This lock is needed for the condition variable.
    * 4. When the lock is aquired, the task definition access mutex is unlocked. Thus,
    *    the task definition can be removed from the container if no other thread is currently
    *    locking the mutex shared.
    * 5. With the task definition mutex locked, this thread can wait on the task definition
    *    condition variable and will be notified when the task is done.
    * 6. EITHER the task definition was removed from the container before the task
    *    definition access mutex was locked,
    *    OR the task definition was not removed and cannot be removed until the task definition
    *    mutex is held.
    */
    void wait(TaskID id);
    std::optional<TaskID> waitOne();
    bool finished(TaskID id);

    TaskPackagePtr createTaskPackage();
};


template<class DependencyPolicy>
ThreadPool<DependencyPolicy>::TaskPackage::TaskPackage() : correspondingPool(0) {
}

template<class DependencyPolicy>
template<typename T, typename ...Args>
typename ThreadPool<DependencyPolicy>::template TaskHandle<T> ThreadPool<DependencyPolicy>::TaskPackage::addTask(std::function<T(Args...)> &&task, const std::set<TaskID> &dependencies, Args...args) {
    auto taskHandle = correspondingPool->addTask(std::move(task), std::forward<Args>(args)..., dependencies);
    std::unique_lock<std::mutex> lock(this->mutex);
    this->tasks.push_back(taskHandle.TaskID);
}

template<class DependencyPolicy>
template<typename T, typename ...Args>
typename ThreadPool<DependencyPolicy>::template TaskHandle<T> ThreadPool<DependencyPolicy>::TaskPackage::addTask(std::function<T(Args...)> &&task, const TaskID dependency, Args...args) {
    auto taskHandle = correspondingPool->addTask(std::move(task), std::forward<Args>(args)..., dependency);
    std::unique_lock<std::mutex> lock(this->mutex);
    this->tasks.push_back(taskHandle.TaskID);
}

template<class DependencyPolicy>
template<typename T, typename ...Args>
typename ThreadPool<DependencyPolicy>::template TaskHandle<T> ThreadPool<DependencyPolicy>::TaskPackage::addTask(std::function<T(Args...)> &&task, Args...args) {
    auto taskHandle = correspondingPool->addTask(std::move(task), std::forward<Args>(args)...);
    std::unique_lock<std::mutex> lock(this->mutex);
    this->tasks.push_back(taskHandle.TaskID);
}

//Returns true if the tasks which were in this package when calling this function
//  are all finished. There might be further tasks which were added while this
//  function was executed
template<class DependencyPolicy>
bool ThreadPool<DependencyPolicy>::TaskPackage::finished() {
    std::deque<ThreadPool<DependencyPolicy>::TaskID> ids;
    bool allDone = true;
    this->mutex.lock();
    ids = this->tasks;
    this->mutex.unlock();
    for(const ThreadPool<DependencyPolicy>::TaskID id : ids) {
        allDone &= this->correspondingPool->finished(id);
    }
    return allDone;
}

template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::TaskPackage::wait() {
    while(this->tasks.empty() == false) {
        TaskID id;
        this->mutex.lock();
        if(this->tasks.empty() == false) {
            id = this->tasks.front();
            this->tasks.pop_front();
        } else {
            //Last task finished while locking the mutex, return
            return;
        }
        this->mutex.unlock();
        this->correspondingPool->wait(id);
    }
}

template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::TaskPackage::wait(TaskID id) {
    this->correspondingPool->wait(id);
}

template<class DependencyPolicy>
std::set<typename ThreadPool<DependencyPolicy>::TaskID> ThreadPool<DependencyPolicy>::TaskPackage::getAsDependency() const {
    return std::set<TaskID>(tasks.begin(), tasks.end());
}

template<class DependencyPolicy>
bool ThreadPool<DependencyPolicy>::checkDependencies(typename ThreadPool<DependencyPolicy>::TaskContainer::iterator const &task) {
    return task->second->getNumberOfDependencies() == 0;
}

//TODO: Unused
template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::notifyDependencies(ThreadPool<DependencyPolicy>::Task const *task) {
    if(DependencyPolicy::respectDependencies) {
        for(const ThreadPool<DependencyPolicy>::TaskID depID : task->getDependants()) {
            typename ThreadPool<DependencyPolicy>::TaskContainer::iterator dep = this->getTaskDefinition(depID, true);
            //NOTE: Reasoning for assert: The tasks in dependants were added to this pool
            //          The tasks in dependants can't run before this loop finishes
            //          Therefore, the tasks must be in the set of task definitions
            //BOOST_ASSERT(dep != this->taskDefinitions.end());
            dep->second->setNumberOfDependencies(dep->second->getNumberOfDependencies() - 1);
        }
    }
}

template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::addDependencies(ThreadPool<DependencyPolicy>::Task *task, ThreadPool<DependencyPolicy>::TaskID id, const std::set<ThreadPool<DependencyPolicy>::TaskID> *dependencies) {
    if(DependencyPolicy::respectDependencies && dependencies != nullptr) {
        std::set<ThreadPool<DependencyPolicy>::TaskID>::size_type satisfiedDependencies = 0;
        //After obtaining this shared lock, all dependencies which are still in the task definition container
        // will not be removed before the dependencies were added
    #ifdef CONGESTION_ANALYSIS
            tryLockTaskDefLockShared();
    #endif
#ifndef THREADPOOL_STANDALONE
        boost::shared_lock_guard<boost::shared_mutex> tskDefLock(this->taskDefAccessMutex);
#else
        std::lock_guard<std::mutex> tskDefLock(this->taskDefAccessMutex);
#endif
        for(const ThreadPool<DependencyPolicy>::TaskID dep : *dependencies) {
            typename ThreadPool<DependencyPolicy>::TaskContainer::iterator dependency = this->getTaskDefinition(dep, false);
            if(dependency != this->taskDefinitions.end()) {
                dependency->second->addDependant(id);
            } else {
                //Task already finished, reduce dependency count
                ++satisfiedDependencies;
            }
        }
        task->setNumberOfDependencies(dependencies->size() - satisfiedDependencies);
    } else {
        task->setNumberOfDependencies(0);
    }
}

template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::work(void) {
    std::thread::id threadID = std::this_thread::get_id();
    std::weak_ptr<std::map<std::thread::id, ThreadPool<DependencyPolicy>*>> poolAssociation = ThreadPool<DependencyPolicy>::threadAssociation;
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
    this->instanceMutex.lock();
    this->threadWork[threadID] = this->taskDefinitions.end();
    this->instanceMutex.unlock();
    if(poolAssociation.expired() == false) {
        ThreadPool<DependencyPolicy>::globalMutex.lock();
        (*(poolAssociation.lock()))[threadID] = this;
        ThreadPool<DependencyPolicy>::globalMutex.unlock();
    }
    while(true) {
        ThreadPool<DependencyPolicy>::TaskList::iterator front;
        typename ThreadPool<DependencyPolicy>::TaskContainer::iterator taskIter;
        std::shared_ptr<ThreadPool<DependencyPolicy>::Task> task;
        {
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
            std::unique_lock<std::mutex> lock(this->instanceMutex);
            //Annahme: Warten auf ConditionVariable findet nicht statt,
            //  wenn das Prädikat true ist
            //  Jeder Thread wartet also darauf, dass inactiveTasks nicht leer ist oder stop true
            this->workAvailable.wait(lock, [&]() -> bool {
                return this->inactiveTasks.empty() == false || this->stop == true || this->numThreadsToStop > 0;
            });
            if(this->numThreadsToStop > 0) {
                this->numThreadsToStop -= 1;
                break;
            }
            if(this->stop == true) {
                break;
            }
            front = this->inactiveTasks.begin();
            this->activeTasks.splice(this->activeTasks.end(),
                                        this->inactiveTasks,
                                        front);
        }
        //Get task definition
        taskIter = this->getTaskDefinition(*front, true);
        //BOOST_ASSERT(taskIter != this->taskDefinitions.end());
        //We made sure that only one thread gets this task, thus we don't need locking for the Task struct
        //If the dependencyCount is greater than zero, this task has unfulfilled dependencies and has to be put back
        if(DependencyPolicy::respectDependencies && checkDependencies(taskIter) == false) {
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
            std::unique_lock<std::mutex> lock(this->instanceMutex);
            this->inactiveTasks.splice(this->inactiveTasks.end(), this->activeTasks, front);
            continue;
        }
        this->threadWork[threadID] = taskIter;
        task = taskIter->second;
        ++(this->numTasksRunning);
        task->fn();
        --(this->numTasksRunning);
        this->removeTask(taskIter->first);
        {
            std::unique_lock<std::mutex> lock(this->completedTasksMutex);
            this->completedTasks.push_back(taskIter->first);
        }
        this->taskDone.notify_all();
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        this->activeTasks.erase(front);
        task.reset();
        this->threadWork[threadID] = this->taskDefinitions.end();
    }
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    this->joinableThreads.push_back(threadID);
    this->threadWork.erase(threadID);
    if(poolAssociation.expired() == false) {
        ThreadPool<DependencyPolicy>::globalMutex.lock();
        poolAssociation.lock()->erase(threadID);
        ThreadPool<DependencyPolicy>::globalMutex.unlock();
    }
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::TaskContainer::iterator ThreadPool<DependencyPolicy>::getTaskDefinition(const ThreadPool<DependencyPolicy>::TaskID id, bool lockingRequired) {
    typename ThreadPool<DependencyPolicy>::TaskContainer::iterator result;
    if(lockingRequired) {
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
        //NOTE: Replace with std::shared_lock as soon as C++17 is state-of-the-art
#ifndef THREADPOOL_STANDALONE
        boost::shared_lock_guard<boost::shared_mutex> lock(this->taskDefAccessMutex);
#else
        std::lock_guard<std::mutex> lock(this->taskDefAccessMutex);
#endif
        result = this->taskDefinitions.find(id);
    } else {
        result = this->taskDefinitions.find(id);
    }
    return result;
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::TaskID ThreadPool<DependencyPolicy>::addTaskDetail(typename ThreadPool<DependencyPolicy>::TaskDefinition&& task,
                                                                                        const std::set<typename ThreadPool<DependencyPolicy>::TaskID> *dependencies) {
    std::shared_ptr<typename ThreadPool<DependencyPolicy>::Task> t = std::shared_ptr<typename ThreadPool<DependencyPolicy>::Task>(new typename ThreadPool<DependencyPolicy>::Task);
    t->fn = std::move(task);
    t->done = false;
    TaskID id = ++(this->tIdRegistry);
    ++(this->numTasksTotal);

    //Emplace Task definition
    #ifdef CONGESTION_ANALYSIS
    tryLockTaskDefLock();
    #endif
    this->taskDefAccessMutex.lock();
    this->taskDefinitions.emplace(id, t);
    this->taskDefAccessMutex.unlock();

    //Add task dependencies
    this->addDependencies(t.get(), id, dependencies);

    //Enqueue in work queue
    #ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
    #endif
    this->instanceMutex.lock();
    this->inactiveTasks.emplace_back(id);
    this->instanceMutex.unlock();

    //Notify a waiting thread (if there is any)
    this->workAvailable.notify_one();
    return id;
}

template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::removeTask(const ThreadPool<DependencyPolicy>::TaskID id) {
    typename ThreadPool<DependencyPolicy>::TaskContainer::iterator taskIter = this->getTaskDefinition(id, true);
    if(taskIter == this->taskDefinitions.end()) {
        return;
    }
    std::shared_ptr<ThreadPool<DependencyPolicy>::Task> task = taskIter->second;

    //First, remove task from task definition container. This marks the task as finished for
    // all threads which try to obtain it's definition afterwards.
    //Then, all threads which already have the task definition (by pointer) are notified by the condition variable.
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLock();
#endif
    this->taskDefAccessMutex.lock();
    //When this is locked, all threads which might be interested in this task either did not call wait() yet
    // or they aquired the task definition lock.
    this->taskDefinitions.erase(taskIter);
    this->taskDefAccessMutex.unlock();

    //From now on, only threads already having a pointer to the object may access it
    task->objMutex.lock();
    //Now, all threads are either not in the process of accessing the task definition object or are waiting
    // on the condition variable
    task->done = true;
    task->objMutex.unlock();
    task->cv_done.notify_all();

    //Now, notify all dependencies
    if(DependencyPolicy::respectDependencies) {
        this->notifyDependencies(task.get());
    }
    --(this->numTasksTotal);
}

/*TODO: template<class DependencyPolicy>
ThreadPool<DependencyPolicy>::TaskID ThreadPool<DependencyPolicy>::findNextUnusedTaskId(TaskID id) const;*/

template<class DependencyPolicy>
ThreadPool<DependencyPolicy> *ThreadPool<DependencyPolicy>::threadPoolAssociation() {
    std::thread::id tid = std::this_thread::get_id();
    ThreadPool<DependencyPolicy> *pool = 0;
    ThreadPool<DependencyPolicy>::globalMutex.lock();
    typename std::map<std::thread::id, ThreadPool<DependencyPolicy>*>::iterator thread = ThreadPool<DependencyPolicy>::threadAssociation->find(tid);
    if(thread != ThreadPool<DependencyPolicy>::threadAssociation->end()) {
        pool = thread->second;
    }
    ThreadPool<DependencyPolicy>::globalMutex.unlock();
    return pool;
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::TaskContainer::iterator ThreadPool<DependencyPolicy>::getOwnTaskIterator() {
    std::thread::id tid = std::this_thread::get_id();
    ThreadPool<DependencyPolicy> *pool = ThreadPool<DependencyPolicy>::threadPoolAssociation();
    //BOOST_ASSERT(pool != 0); //TODO: Replace with exception or errx
    //No lock required, this tasks thread will not change
    typename ThreadPool<DependencyPolicy>::TaskContainer::iterator task = pool->threadWork.at(tid);
    /*
    * Rationale:
    * EITHER the calling thread is not part of any thread pool.
    * Then, pool == 0 and this point is not reached.
    * OR the calling thread is part of a thread pool.
    * Then, it either has a task or is in the work()-function.
    * The work()-function does not call this function, thus the
    * thread must have a task assigned.
    */
    //BOOST_ASSERT(task != pool->taskDefinitions.end());
    return task;
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::TaskID ThreadPool<DependencyPolicy>::getOwnTaskID() {
    return ThreadPool<DependencyPolicy>::getOwnTaskIterator()->first;
}

template<class DependencyPolicy>
ThreadPool<DependencyPolicy>::ThreadPool() : ThreadPool(std::thread::hardware_concurrency() - 1) {
}

template<class DependencyPolicy>
ThreadPool<DependencyPolicy>::ThreadPool(size_type size) 
: tIdRegistry(0),
numTasksRunning(0),
numTasksTotal(0),
completedTasks(static_cast<uint64_t>(static_cast<int64_t>(-1))),
stop(false),
numThreadsToStop(0),
maxNumThreads(0) {
#ifdef CONGESTION_ANALYSIS
    this->instanceLockCongestion.store(0);
    this->instanceLockTries.store(0);
    this->taskDefLockCongestion.store(0);
    this->taskDefLockTries.store(0);
    this->taskDefExLockCongestion.store(0);
    this->taskDefExLockTries.store(0);
#endif
    this->setNumberOfThreads(size);
}

template<class DependencyPolicy>
ThreadPool<DependencyPolicy>::~ThreadPool() {
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
    this->instanceMutex.lock();
    this->stop = true;
    this->instanceMutex.unlock();
    this->workAvailable.notify_all();
    for(std::thread &thr : this->threads) {
        thr.join();
    }
#ifdef CONGESTION_ANALYSIS
    std::cout << this->instanceLockCongestion.load() << "/" << this->instanceLockTries.load() << std::endl;
    std::cout << this->taskDefLockCongestion.load() << "/" << this->taskDefLockTries.load() << std::endl;
    std::cout << this->taskDefExLockCongestion.load() << "/" << this->taskDefExLockTries.load() << std::endl;
#endif
}

template<class DependencyPolicy>
ThreadPool<DependencyPolicy>& ThreadPool<DependencyPolicy>::getDefaultInstance() {
    return *ThreadPool<DependencyPolicy>::getDefaultInstancePtr();
}

template<class DependencyPolicy>
ThreadPool<DependencyPolicy>* ThreadPool<DependencyPolicy>::getDefaultInstancePtr() {
    if(ThreadPool<DependencyPolicy>::defaultInstancePtr.get() == nullptr) {
        ThreadPool<DependencyPolicy>::defaultInstancePtr.reset(new ThreadPool<DependencyPolicy>);
    }
    return ThreadPool<DependencyPolicy>::defaultInstancePtr.get();
}

template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::setNumberOfThreads(size_type n) {
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    if(n < this->maxNumThreads) {
        this->numThreadsToStop = (this->maxNumThreads - n);
        this->maxNumThreads = n;
    }
    while(n > this->maxNumThreads) {
        this->threads.emplace_back(std::bind(&ThreadPool<DependencyPolicy>::work, this));
        ++this->maxNumThreads;
    }
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::size_type ThreadPool<DependencyPolicy>::getNumberOfThreads() {
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    return this->maxNumThreads;
}

//Cleanup function joining threads that were stopped to reduce the thread
//pool size. Returns the number of joined threads.
template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::size_type ThreadPool<DependencyPolicy>::joinStoppedThreads() {
    std::list<std::thread> threadList;
    std::vector<std::list<std::thread>::iterator> listItemsToRemove;
    listItemsToRemove.reserve(this->threads.size());
    {
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        for(auto iter = this->threads.begin(); iter != this->threads.end(); ++iter) {
            if(std::any_of(this->joinableThreads.begin(), this->joinableThreads.end(), [&iter](std::thread::id id)->bool{
                return id == iter->get_id();
            })) {
                listItemsToRemove.push_back(iter);
            }
        }
        //Splicing invalidates the iterator in question thus we cannot do this in the loop above
        for(auto &iter : listItemsToRemove) {
            threadList.splice(threadList.end(), this->threads, iter);
        }
    }
    for(auto &thread : threadList) {
        thread.join();
    }
    return threadList.size();
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::size_type ThreadPool<DependencyPolicy>::getNumTasksRunning() const {
    return this->numTasksRunning.load();
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::size_type ThreadPool<DependencyPolicy>::getNumTasks() const {
    return this->numTasksTotal.load();
}

template<class DependencyPolicy>
bool ThreadPool<DependencyPolicy>::empty() {
    if(this->activeTasks.empty() and this->inactiveTasks.empty()) {
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        return this->activeTasks.empty() and this->inactiveTasks.empty();
    }
    return false;
}

template<class DependencyPolicy>
template<typename Fn>
auto ThreadPool<DependencyPolicy>::addTask(Fn task, std::set<typename ThreadPool<DependencyPolicy>::TaskID> const &dependencies)
    -> ThreadPool<DependencyPolicy>::TaskHandle<typename std::result_of<Fn()>::type> {
    typedef typename std::result_of<Fn()>::type ReturnType;
    ThreadPool<DependencyPolicy>::TaskHandle<ReturnType> returnValue;
    std::packaged_task<ReturnType()> *packagedTask = new std::packaged_task<ReturnType()>(task);
    returnValue.future = std::move(packagedTask->get_future());
    returnValue.TaskID = this->addTaskDetail([packagedTask]()->void{
        (*packagedTask)();
        delete packagedTask;
    }, &dependencies);
    return returnValue;
}

template<class DependencyPolicy>
template<typename Fn, typename ...Args>
auto ThreadPool<DependencyPolicy>::addTask(Fn task, std::set<ThreadPool<DependencyPolicy>::TaskID> const &dependencies, Args...args)
    -> ThreadPool<DependencyPolicy>::TaskHandle<typename std::result_of<Fn(Args...)>::type> {
    return this->addTask(std::bind(task, std::forward<Args>(args)...), dependencies);
}

template<class DependencyPolicy>
template<typename Fn>
auto ThreadPool<DependencyPolicy>::addTask(Fn task)
    -> ThreadPool<DependencyPolicy>::TaskHandle<typename std::result_of<Fn()>::type> {
    typedef typename std::result_of<Fn()>::type ReturnType;
    ThreadPool<DependencyPolicy>::TaskHandle<ReturnType> returnValue;
    std::packaged_task<ReturnType()> *packagedTask = new std::packaged_task<ReturnType()>(task);
    returnValue.future = std::move(packagedTask->get_future());
    returnValue.TaskID = this->addTaskDetail([packagedTask]()->void{
        (*packagedTask)();
        delete packagedTask;
    });
    return returnValue;
}

template<class DependencyPolicy>
template<typename Fn, typename ...Args>
auto ThreadPool<DependencyPolicy>::addTask(Fn task, Args...args)
    -> ThreadPool<DependencyPolicy>::TaskHandle<typename std::result_of<Fn(Args...)>::type> {
    return this->addTask(std::bind(task, std::forward<Args>(args)...));
}


/*
* Principle of waiting for all tasks to finish:
* 1. Lock the instanceMutex. This assures that the task lists are not modified
*    while we examine them.
* 2. If the (probably longer) list of inactive tasks is not empty, the last
*    task is select. As the lists are processed sequentially from front to back,
*    this is the task that will be selected last for execution. Note that other
*    tasks may be inserted after selecting it.
* 3. If the list of inactive tasks is empty and the list of active tasks is not empty,
*    the last task of the list of active tasks is selected. Assuming all tasks to
*    take about the same amount of time, this is the last one to finish.
* 4. If the list of inactive tasks is empty and the list of active tasks is empty,
*    all queues are empty for the time being, thus all tasks which were in any list when
*    this function was called finished. Then, this function shall return and no task is selected.
* 5. Unlock the instanceMutex. As both lists have been examined, they may be modified again.
* 6. If a task was selected, wait for it. Afterwards, goto 1.
* 7. If no task was selected, return.
*/
template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::wait() {
    ThreadPool<DependencyPolicy>::TaskID lastTask;
    bool allQueuesEmpty = false;
    do {
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
        this->instanceMutex.lock();
        if(this->inactiveTasks.empty() == false) {
            lastTask = this->inactiveTasks.back();
        } else if(this->activeTasks.empty() == false) {
            lastTask = this->activeTasks.back();
        } else {
            allQueuesEmpty = true;
        }
        this->instanceMutex.unlock();
        if(allQueuesEmpty == false) {
            this->wait(lastTask);
        }
    }while(allQueuesEmpty == false);
}

/*
* Principle of waiting for one task to finish:
* 1. Lock the task definition access mutex shared. Thus, the container mapping
*    task IDs to task definitions is locked and can still be read but not modified.
*    This means that the task definition can not be removed before the mutex is
*    unlocked by all readers.
* 2. Get the task definition. If the task does not exist in the container, it already finished.
*    Therefore we may return after unlocking the mutex again.
* 3. If the task definition exists in the container, lock it's internal mutex.
*    This lock is needed for the condition variable.
* 4. When the lock is aquired, the task definition access mutex is unlocked. Thus,
*    the task definition can be removed from the container if no other thread is currently
*    locking the mutex shared.
* 5. With the task definition mutex locked, this thread can wait on the task definition
*    condition variable and will be notified when the task is done.
* 6. EITHER the task definition was removed from the container before the task
*    definition access mutex was locked,
*    OR the task definition was not removed and cannot be removed until the task definition
*    mutex is held.
*/
template<class DependencyPolicy>
void ThreadPool<DependencyPolicy>::wait(TaskID id) {
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
#ifndef THREADPOOL_STANDALONE
    this->taskDefAccessMutex.lock_shared();
#else
    this->taskDefAccessMutex.lock();
#endif
    typename ThreadPool<DependencyPolicy>::TaskContainer::iterator task = this->getTaskDefinition(id, false);
    if(task == this->taskDefinitions.end()) {
#ifndef THREADPOOL_STANDALONE
        this->taskDefAccessMutex.unlock_shared();
#else
        this->taskDefAccessMutex.unlock();
#endif
        return;
    }
    std::shared_ptr<ThreadPool<DependencyPolicy>::Task> taskPtr = task->second;
    std::unique_lock<std::mutex> lock(taskPtr->objMutex);
#ifndef THREADPOOL_STANDALONE
    this->taskDefAccessMutex.unlock_shared();
#else
    this->taskDefAccessMutex.unlock();
#endif
    taskPtr->cv_done.wait(lock, [&]() -> bool { return taskPtr->done; });
}

template<class DependencyPolicy>
std::optional<typename ThreadPool<DependencyPolicy>::TaskID> ThreadPool<DependencyPolicy>::waitOne() {
    if(this->getNumTasksRunning() == 0) {
        return {};
    }
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    if(this->getNumTasksRunning() == 0) {
        return *this->completedTasks.cbegin();
    }
    auto lastIter = this->completedTasks.cbegin();
    this->taskDone.wait(lock, [&lastIter,this]() -> bool { return lastIter != this->completedTasks.cbegin(); });
    if(GSL_LIKELY(lastIter != this->completedTasks.cend())) { //If the list was not empty when lastIter was initialized
        return *(++lastIter);
    } else {
        return *this->completedTasks.cbegin();
    }
}

template<class DependencyPolicy>
bool ThreadPool<DependencyPolicy>::finished(TaskID id) {
    return this->getTaskDefinition(id, true) == this->taskDefinitions.end();
}

template<class DependencyPolicy>
typename ThreadPool<DependencyPolicy>::TaskPackagePtr ThreadPool<DependencyPolicy>::createTaskPackage() {
    ThreadPool<DependencyPolicy>::TaskPackage *pkg = new ThreadPool<DependencyPolicy>::TaskPackage;
    //TODO: Maybe use a weak ptr and initialise it from a private shared_ptr of the Pool?
    pkg->correspondingPool = this;
    return std::shared_ptr<ThreadPool<DependencyPolicy>::TaskPackage>(pkg);
}

template<typename DependencyPolicy>
std::shared_ptr<std::map<std::thread::id, ThreadPool<DependencyPolicy>*>> ThreadPool<DependencyPolicy>::threadAssociation;
template<typename DependencyPolicy>
std::unique_ptr<ThreadPool<DependencyPolicy>> ThreadPool<DependencyPolicy>::defaultInstancePtr = nullptr;
template<typename DependencyPolicy>
std::mutex ThreadPool<DependencyPolicy>::globalMutex;


/*template<class Iterator, class R, class DependencyPolicy>
std::tuple<typename ThreadPool<DependencyPolicy>::TaskPackagePtr, std::vector<typename ThreadPool<DependencyPolicy>::TaskHandle<typename R>>>
distributeContainerOperationOnPool(ThreadPool<DependencyPolicy> &pool, Iterator begin, Iterator end, std::function<R(Iterator,Iterator)> fn) {
  auto const poolsize = pool.getNumberOfThreads();
  auto const numElements = std::distance(begin, end);
  auto const numElementsPerThread = std::max(1, numElements/poolsize);
  Iterator intermediate = begin;
  typename ThreadPool<DependencyPolicy>::TaskPackagePtr package = pool.createTaskPackage();
  std::vector<typename ThreadPool<DependencyPolicy>::TaskHandle<R>> handles;
  for(unsigned int i = 0; i < (numElements/numElementsPerThread); ++i) {
    Iterator intermediateEnd = intermediate;
    std::advance(intermediateEnd, numElementsPerThread);
    handles.push_back(package->addTask(fn, intermediate, intermediateEnd));
  }
  handles.push_back(package->addTask(fn, intermediateEnd, end));
  return std::make_tuple<typename ThreadPool<DependencyPolicy>::TaskPackagePtr, std::vector<typename ThreadPool<DependencyPolicy>::TaskHandle<R>>>(package, handles);
}*/

#endif /* __THREADPOOL_THREADPOOL_H__ */

