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
#include <shared_mutex>
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
 * The next task added is assigned the id still belonging to the task started first.
 */

template<class Policies = policies::PolicyCollection<policies::DependenciesNotRespected,
                                                     policies::PassiveWaiting>>
class ThreadPool {
public:
    typedef std::vector<std::thread>::size_type size_type;
    typedef uint64_t TaskID;

    typedef std::function<void(void)> TaskDefinition;

    template<typename T>
    struct TaskHandle_t {
      //TODO: Make const
      ThreadPool<Policies>::TaskID const TaskID;
      std::future<T> future;
      TaskHandle_t(ThreadPool<Policies>::TaskID id, std::future<T> &&future) : TaskID(id), future(std::move(future)) { }
      TaskHandle_t(TaskHandle_t const &) = delete;
      TaskHandle_t(TaskHandle_t &&) = default;
      TaskHandle_t &operator=(TaskHandle_t const&) = delete;
      TaskHandle_t &operator=(TaskHandle_t &&) noexcept = default;
    };
    template<typename T>
    using TaskHandle = struct TaskHandle_t<T>;

    typedef AutoCleanedForwardList<TaskID>::const_iterator Snapshot;

    class TaskPackage {
    private:
    	friend class ThreadPool;
        std::mutex mutex;
        std::deque<TaskID> tasks;
        ThreadPool<Policies> *correspondingPool;

        TaskPackage();
    public:
        template<typename T, typename ...Args>
        typename ThreadPool<Policies>::template TaskHandle<T> addTask(std::function<T(Args...)> &&task, const std::set<TaskID> &dependencies, Args...args);
        template<typename T, typename ...Args>
        typename ThreadPool<Policies>::template TaskHandle<T> addTask(std::function<T(Args...)> &&task, const TaskID dependency, Args...args);
        template<typename T, typename ...Args>
        typename ThreadPool<Policies>::template TaskHandle<T> addTask(std::function<T(Args...)> &&task, Args...args);
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

        ~TaskStruct() {
            std::unique_lock<std::mutex> lock(this->objMutex);
        }

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

        ~TaskStruct() {
            std::unique_lock<std::mutex> lock(this->objMutex);
        }

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

    typedef TaskStruct<Policies> Task;
private:
    static std::shared_ptr<std::map<std::thread::id, ThreadPool<Policies>*>> threadAssociation;
    static std::unique_ptr<ThreadPool<Policies>> defaultInstancePtr;
    static std::mutex globalMutex;

    typedef std::list<TaskID> TaskList;
    typedef std::map<TaskID, std::shared_ptr<ThreadPool<Policies>::Task>> TaskContainer;

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
    std::shared_mutex taskDefAccessMutex;
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
    bool workOnce(void);
    typename TaskContainer::iterator getTaskDefinition(const TaskID id, bool lockingRequired);
    TaskID addTaskDetail(TaskDefinition&& task, const std::set<TaskID> *dependencies = nullptr);
    void removeTask(const TaskID id);

    TaskID findNextUnusedTaskId(TaskID id) const;

    static ThreadPool<Policies> *threadPoolAssociation();
    static typename TaskContainer::iterator getOwnTaskIterator();
    static TaskID getOwnTaskID();

    bool callingThreadBelongsToPool() const;

    ThreadPool();
public:
    ThreadPool(size_type size);
    ThreadPool(const ThreadPool &/*other*/) = delete;
    ~ThreadPool();

    static ThreadPool<Policies>& getDefaultInstance();
    static ThreadPool<Policies>* getDefaultInstancePtr();

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
    void waitOne();
    bool finished(TaskID id);

    Snapshot getSnapshot();
    std::tuple<std::vector<TaskID>, Snapshot> getCompletedTasksSinceSnapshot(Snapshot const &snapshot);

    TaskPackagePtr createTaskPackage();
};


template<class Policies>
ThreadPool<Policies>::TaskPackage::TaskPackage() : correspondingPool(0) {
}

template<class Policies>
template<typename T, typename ...Args>
typename ThreadPool<Policies>::template TaskHandle<T> ThreadPool<Policies>::TaskPackage::addTask(std::function<T(Args...)> &&task, const std::set<TaskID> &dependencies, Args...args) {
    auto taskHandle = correspondingPool->addTask(std::move(task), std::forward<Args>(args)..., dependencies);
    std::unique_lock<std::mutex> lock(this->mutex);
    this->tasks.push_back(taskHandle.TaskID);
    return taskHandle;
}

template<class Policies>
template<typename T, typename ...Args>
typename ThreadPool<Policies>::template TaskHandle<T> ThreadPool<Policies>::TaskPackage::addTask(std::function<T(Args...)> &&task, const TaskID dependency, Args...args) {
    auto taskHandle = correspondingPool->addTask(std::move(task), std::forward<Args>(args)..., dependency);
    std::unique_lock<std::mutex> lock(this->mutex);
    this->tasks.push_back(taskHandle.TaskID);
    return taskHandle;
}

template<class Policies>
template<typename T, typename ...Args>
typename ThreadPool<Policies>::template TaskHandle<T> ThreadPool<Policies>::TaskPackage::addTask(std::function<T(Args...)> &&task, Args...args) {
    auto taskHandle = correspondingPool->addTask(std::move(task), std::forward<Args>(args)...);
    std::unique_lock<std::mutex> lock(this->mutex);
    this->tasks.push_back(taskHandle.TaskID);
    return taskHandle;
}

//Returns true if the tasks which were in this package when calling this function
//  are all finished. There might be further tasks which were added while this
//  function was executed
template<class Policies>
bool ThreadPool<Policies>::TaskPackage::finished() {
    std::deque<ThreadPool<Policies>::TaskID> ids;
    bool allDone = true;
    this->mutex.lock();
    ids = this->tasks;
    this->mutex.unlock();
    for(const ThreadPool<Policies>::TaskID id : ids) {
        allDone &= this->correspondingPool->finished(id);
    }
    return allDone;
}

template<class Policies>
typename ThreadPool<Policies>::Snapshot ThreadPool<Policies>::getSnapshot() {
    std::unique_lock<std::mutex> completedTasksLock(this->completedTasksMutex);
    return this->completedTasks.cbegin();
}

template<class Policies>
std::tuple<std::vector<typename ThreadPool<Policies>::TaskID>, typename ThreadPool<Policies>::Snapshot>
ThreadPool<Policies>::getCompletedTasksSinceSnapshot(typename ThreadPool<Policies>::Snapshot const &snapshot) {
    Snapshot newSnapshot = snapshot;
    std::vector<TaskID> result;
    std::unique_lock<std::mutex> completedTasksLock(this->completedTasksMutex);
    while(newSnapshot + 1 != this->completedTasks.cend()) {
        ++newSnapshot;
        result.push_back(*newSnapshot);
    }
    return std::make_tuple(result, newSnapshot);
}

template<class Policies>
void ThreadPool<Policies>::TaskPackage::wait() {
    while(this->tasks.empty() == false) {
        TaskID id;
        this->mutex.lock();
        //TODO: Check whether tasks should be popped or spliced to different list
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

template<class Policies>
void ThreadPool<Policies>::TaskPackage::wait(TaskID id) {
    this->correspondingPool->wait(id);
}

template<class Policies>
std::set<typename ThreadPool<Policies>::TaskID> ThreadPool<Policies>::TaskPackage::getAsDependency() const {
    return std::set<TaskID>(tasks.begin(), tasks.end());
}

template<class Policies>
bool ThreadPool<Policies>::checkDependencies(typename ThreadPool<Policies>::TaskContainer::iterator const &task) {
    return task->second->getNumberOfDependencies() == 0;
}

//TODO: Unused
template<class Policies>
void ThreadPool<Policies>::notifyDependencies(ThreadPool<Policies>::Task const *task) {
    if(Policies::respectDependencies) {
        for(const ThreadPool<Policies>::TaskID depID : task->getDependants()) {
            typename ThreadPool<Policies>::TaskContainer::iterator dep = this->getTaskDefinition(depID, true);
            //NOTE: Reasoning for assert: The tasks in dependants were added to this pool
            //          The tasks in dependants can't run before this loop finishes
            //          Therefore, the tasks must be in the set of task definitions
            //BOOST_ASSERT(dep != this->taskDefinitions.end());
            dep->second->setNumberOfDependencies(dep->second->getNumberOfDependencies() - 1);
        }
    }
}

template<class Policies>
void ThreadPool<Policies>::addDependencies(ThreadPool<Policies>::Task *task, ThreadPool<Policies>::TaskID id, const std::set<ThreadPool<Policies>::TaskID> *dependencies) {
    if(Policies::respectDependencies && dependencies != nullptr) {
        std::set<ThreadPool<Policies>::TaskID>::size_type satisfiedDependencies = 0;
        //After obtaining this shared lock, all dependencies which are still in the task definition container
        // will not be removed before the dependencies were added
    #ifdef CONGESTION_ANALYSIS
            tryLockTaskDefLockShared();
    #endif
        std::shared_lock<std::shared_mutex> tskDefLock(this->taskDefAccessMutex);
        for(const ThreadPool<Policies>::TaskID dep : *dependencies) {
            typename ThreadPool<Policies>::TaskContainer::iterator dependency = this->getTaskDefinition(dep, false);
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

template<class Policies>
void ThreadPool<Policies>::work(void) {
    //Setup
    std::thread::id threadID = std::this_thread::get_id();
    std::weak_ptr<std::map<std::thread::id, ThreadPool<Policies>*>> poolAssociation = ThreadPool<Policies>::threadAssociation;
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
    {
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        this->threadWork[threadID] = this->taskDefinitions.end();
    }
    if(poolAssociation.expired() == false) {
        std::unique_lock<std::mutex> lock(ThreadPool<Policies>::globalMutex);
        (*(poolAssociation.lock()))[threadID] = this;
    }
    bool continueWorking = true;
    do {
        {
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
        }
        continueWorking = workOnce();
    } while(continueWorking);
    //Cleanup
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    this->joinableThreads.push_back(threadID);
    this->threadWork.erase(threadID);
    if(poolAssociation.expired() == false) {
        std::unique_lock<std::mutex> lock(ThreadPool<Policies>::globalMutex);
        poolAssociation.lock()->erase(threadID);
    }
}

template<class Policies>
bool ThreadPool<Policies>::workOnce(void) {
    std::thread::id threadID = std::this_thread::get_id();
    ThreadPool<Policies>::TaskList::iterator front;
    typename ThreadPool<Policies>::TaskContainer::iterator taskIter;
    std::shared_ptr<ThreadPool<Policies>::Task> task;
    TaskID taskID;
    {
#ifdef CONGESTION_ANALYSIS
        tryLockInstanceLock();
#endif
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        if(this->inactiveTasks.empty()) {
            return true;
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
    if(Policies::respectDependencies && checkDependencies(taskIter) == false) {
#ifdef CONGESTION_ANALYSIS
tryLockInstanceLock();
#endif
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        this->inactiveTasks.splice(this->inactiveTasks.end(), this->activeTasks, front);
        return true;
    }
    {
        //Lock not necessary (in theory)
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        this->threadWork[threadID] = taskIter;
    }
    taskID = taskIter->first;
    task = taskIter->second;
    ++(this->numTasksRunning);
    task->fn();
    --(this->numTasksRunning);
    this->removeTask(taskID);
    this->taskDone.notify_all();
#ifdef CONGESTION_ANALYSIS
tryLockInstanceLock();
#endif
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    this->activeTasks.erase(front);
    task.reset();
    this->threadWork[threadID] = this->taskDefinitions.end();
    return true;
}

template<class Policies>
typename ThreadPool<Policies>::TaskContainer::iterator ThreadPool<Policies>::getTaskDefinition(const ThreadPool<Policies>::TaskID id, bool lockingRequired) {
    typename ThreadPool<Policies>::TaskContainer::iterator result;
    if(lockingRequired) {
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
        //NOTE: Replace with std::shared_lock as soon as C++17 is state-of-the-art
        std::shared_lock<std::shared_mutex> lock(this->taskDefAccessMutex);
        result = this->taskDefinitions.find(id);
    } else {
        result = this->taskDefinitions.find(id);
    }
    return result;
}

template<class Policies>
typename ThreadPool<Policies>::TaskID ThreadPool<Policies>::addTaskDetail(typename ThreadPool<Policies>::TaskDefinition&& task,
                                                                                        const std::set<typename ThreadPool<Policies>::TaskID> *dependencies) {
    std::shared_ptr<typename ThreadPool<Policies>::Task> t = std::shared_ptr<typename ThreadPool<Policies>::Task>(new typename ThreadPool<Policies>::Task);
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

template<class Policies>
void ThreadPool<Policies>::removeTask(const ThreadPool<Policies>::TaskID id) {
    typename ThreadPool<Policies>::TaskContainer::iterator taskIter = this->getTaskDefinition(id, true);
    if(taskIter == this->taskDefinitions.end()) {
        return;
    }
    TaskID taskID = taskIter->first;
    std::shared_ptr<ThreadPool<Policies>::Task> task = taskIter->second;

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
    {
        std::unique_lock<std::mutex> lock(this->completedTasksMutex);
        this->completedTasks.push_back(taskID);
    }

    //From now on, only threads already having a pointer to the object may access it
    task->objMutex.lock();
    //Now, all threads are either not in the process of accessing the task definition object or are waiting
    // on the condition variable
    task->done = true;
    task->objMutex.unlock();
    task->cv_done.notify_all();

    //Now, notify all dependencies
    if(Policies::respectDependencies) {
        this->notifyDependencies(task.get());
    }
    --(this->numTasksTotal);
}

/*TODO: template<class Policies>
ThreadPool<Policies>::TaskID ThreadPool<Policies>::findNextUnusedTaskId(TaskID id) const;*/

template<class Policies>
ThreadPool<Policies> *ThreadPool<Policies>::threadPoolAssociation() {
    std::thread::id tid = std::this_thread::get_id();
    ThreadPool<Policies> *pool = nullptr;
    {
        std::unique_lock<std::mutex> lock(ThreadPool<Policies>::globalMutex);
        typename std::map<std::thread::id, ThreadPool<Policies>*>::iterator thread = ThreadPool<Policies>::threadAssociation->find(tid);
        if(thread != ThreadPool<Policies>::threadAssociation->end()) {
            pool = thread->second;
        }
    }
    return pool;
}

template<class Policies>
typename ThreadPool<Policies>::TaskContainer::iterator ThreadPool<Policies>::getOwnTaskIterator() {
    std::thread::id tid = std::this_thread::get_id();
    ThreadPool<Policies> *pool = ThreadPool<Policies>::threadPoolAssociation();
    Expects(pool != nullptr);
    //No lock required, this tasks thread will not change
    typename ThreadPool<Policies>::TaskContainer::iterator task = pool->threadWork.at(tid);
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

template<class Policies>
typename ThreadPool<Policies>::TaskID ThreadPool<Policies>::getOwnTaskID() {
    return ThreadPool<Policies>::getOwnTaskIterator()->first;
}

template<class Policies>
bool ThreadPool<Policies>::callingThreadBelongsToPool() const {
    return ThreadPool<Policies>::threadPoolAssociation() == this;
}

template<class Policies>
ThreadPool<Policies>::ThreadPool() : ThreadPool(std::thread::hardware_concurrency() - 1) {
}

template<class Policies>
ThreadPool<Policies>::ThreadPool(size_type size) 
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

template<class Policies>
ThreadPool<Policies>::~ThreadPool() {
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
    this->instanceMutex.lock();
    this->stop = true;
    this->instanceMutex.unlock();
    this->workAvailable.notify_all();
    //Wait for all tasks to finish, then join them
    this->wait();
    for(std::thread &thr : this->threads) {
        thr.join();
    }
#ifdef CONGESTION_ANALYSIS
    std::cout << this->instanceLockCongestion.load() << "/" << this->instanceLockTries.load() << std::endl;
    std::cout << this->taskDefLockCongestion.load() << "/" << this->taskDefLockTries.load() << std::endl;
    std::cout << this->taskDefExLockCongestion.load() << "/" << this->taskDefExLockTries.load() << std::endl;
#endif
}

template<class Policies>
ThreadPool<Policies>& ThreadPool<Policies>::getDefaultInstance() {
    return *ThreadPool<Policies>::getDefaultInstancePtr();
}

template<class Policies>
ThreadPool<Policies>* ThreadPool<Policies>::getDefaultInstancePtr() {
    if(ThreadPool<Policies>::defaultInstancePtr.get() == nullptr) {
        ThreadPool<Policies>::defaultInstancePtr.reset(new ThreadPool<Policies>);
    }
    return ThreadPool<Policies>::defaultInstancePtr.get();
}

template<class Policies>
void ThreadPool<Policies>::setNumberOfThreads(size_type n) {
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    if(n < this->maxNumThreads) {
        this->numThreadsToStop = (this->maxNumThreads - n);
        this->maxNumThreads = n;
    }
    while(n > this->maxNumThreads) {
        this->threads.emplace_back(std::bind(&ThreadPool<Policies>::work, this));
        ++this->maxNumThreads;
    }
}

template<class Policies>
typename ThreadPool<Policies>::size_type ThreadPool<Policies>::getNumberOfThreads() {
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    return this->maxNumThreads;
}

//Cleanup function joining threads that were stopped to reduce the thread
//pool size. Returns the number of joined threads.
template<class Policies>
typename ThreadPool<Policies>::size_type ThreadPool<Policies>::joinStoppedThreads() {
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

template<class Policies>
typename ThreadPool<Policies>::size_type ThreadPool<Policies>::getNumTasksRunning() const {
    return this->numTasksRunning.load();
}

template<class Policies>
typename ThreadPool<Policies>::size_type ThreadPool<Policies>::getNumTasks() const {
    return this->numTasksTotal.load();
}

template<class Policies>
bool ThreadPool<Policies>::empty() {
    if(this->activeTasks.empty() and this->inactiveTasks.empty()) {
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        return this->activeTasks.empty() and this->inactiveTasks.empty();
    }
    return false;
}

template<class Policies>
template<typename Fn>
auto ThreadPool<Policies>::addTask(Fn task, std::set<typename ThreadPool<Policies>::TaskID> const &dependencies)
    -> ThreadPool<Policies>::TaskHandle<typename std::result_of<Fn()>::type> {
    typedef typename std::result_of<Fn()>::type ReturnType;
    std::packaged_task<ReturnType()> *packagedTask = new std::packaged_task<ReturnType()>(task);
    auto future = std::move(packagedTask->get_future());
    auto TaskID = this->addTaskDetail([packagedTask]()->void{
        (*packagedTask)();
        delete packagedTask;
    }, &dependencies);
    return ThreadPool<Policies>::TaskHandle<ReturnType>{TaskID, std::move(future)};
}

template<class Policies>
template<typename Fn, typename ...Args>
auto ThreadPool<Policies>::addTask(Fn task, std::set<ThreadPool<Policies>::TaskID> const &dependencies, Args...args)
    -> ThreadPool<Policies>::TaskHandle<typename std::result_of<Fn(Args...)>::type> {
    return this->addTask(std::bind(task, std::forward<Args>(args)...), dependencies);
}

template<class Policies>
template<typename Fn>
auto ThreadPool<Policies>::addTask(Fn task)
    -> ThreadPool<Policies>::TaskHandle<typename std::result_of<Fn()>::type> {
    typedef typename std::result_of<Fn()>::type ReturnType;
    std::packaged_task<ReturnType()> *packagedTask = new std::packaged_task<ReturnType()>(task);
    auto future = std::move(packagedTask->get_future());
    auto TaskID = this->addTaskDetail([packagedTask]()->void{
        (*packagedTask)();
        delete packagedTask;
    });
    return ThreadPool<Policies>::TaskHandle<ReturnType>{TaskID, std::move(future)};
}

template<class Policies>
template<typename Fn, typename ...Args>
auto ThreadPool<Policies>::addTask(Fn task, Args...args)
    -> ThreadPool<Policies>::TaskHandle<typename std::result_of<Fn(Args...)>::type> {
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
template<class Policies>
void ThreadPool<Policies>::wait() {
    ThreadPool<Policies>::TaskID lastTask;
    bool allQueuesEmpty = false;
    bool inactiveTasksEmpty = true;
    while(not allQueuesEmpty) {
        {
#ifdef CONGESTION_ANALYSIS
            tryLockInstanceLock();
#endif
            std::unique_lock<std::mutex> lock(this->instanceMutex);
            inactiveTasksEmpty = this->inactiveTasks.empty();
            allQueuesEmpty = inactiveTasksEmpty && this->activeTasks.empty();
        }
        if(Policies::activeWaiting and not inactiveTasksEmpty) {
            this->workOnce();
        } else if(not allQueuesEmpty) {
            TaskID lastTask;
            {
                std::unique_lock<std::mutex> lock(this->instanceMutex);
                //instance mutex was unlocked in between, empty status could
                // have changed, needs to be checked again
                if(not this->activeTasks.empty()) {
                    lastTask = this->activeTasks.back();
                } else {
                    //activeTasks now empty, do another loop to check again
                    continue;
                }
            }
            //Wait for the last task to finish. Then check again as other tasks
            // could have been added/started in the meantime
            this->wait(lastTask);
        }
    }
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
template<class Policies>
void ThreadPool<Policies>::wait(TaskID id) {
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
    this->taskDefAccessMutex.lock_shared();
    typename ThreadPool<Policies>::TaskContainer::iterator task = this->getTaskDefinition(id, false);
    if(task == this->taskDefinitions.end()) {
        this->taskDefAccessMutex.unlock_shared();
        return;
    }
    std::shared_ptr<ThreadPool<Policies>::Task> taskPtr = task->second;
    this->taskDefAccessMutex.unlock_shared();
    /*
     * In wait(), we simply used the calling thread to do some work to speed up
     * the work.  Here, this is not possible as using a thread which is not
     * part of the pool to wait for a task inside the pool to finish to work on
     * a different task could make the waiting thread miss the end of the task
     * it's waiting for. Thus, tasks inside the pool can work on other tasks
     * inside the same pool while waiting and tasks outside the pool simply
     * wait.
     */
    /*unsafe for now (workOnce could start working on an infinite task): if(Policies::activeWaiting) {
        while([&]()->bool{ std::unique_lock<std::mutex> lock(taskPtr->objMutex); return not taskPtr->done; }()) {
            workOnce();
        }
    } else*/ {
        std::unique_lock<std::mutex> lock(taskPtr->objMutex);
        taskPtr->cv_done.wait(lock, [&]() -> bool { return taskPtr->done; });
    }
}

template<class Policies>
void ThreadPool<Policies>::waitOne() {
    if(this->getNumTasks() == 0) {
        return;
    }
    auto snapshot = this->getSnapshot();
    if(not this->callingThreadBelongsToPool()) {
        std::unique_lock<std::mutex> lock(this->instanceMutex);
        this->taskDone.wait(lock, [&snapshot,this]() -> bool { return snapshot != this->getSnapshot(); });
    } else {
        workOnce();
    }
}

template<class Policies>
bool ThreadPool<Policies>::finished(TaskID id) {
    return this->getTaskDefinition(id, true) == this->taskDefinitions.end();
}

template<class Policies>
typename ThreadPool<Policies>::TaskPackagePtr ThreadPool<Policies>::createTaskPackage() {
    ThreadPool<Policies>::TaskPackage *pkg = new ThreadPool<Policies>::TaskPackage;
    //TODO: Maybe use a weak ptr and initialise it from a private shared_ptr of the Pool?
    pkg->correspondingPool = this;
    return std::shared_ptr<ThreadPool<Policies>::TaskPackage>(pkg);
}

template<typename Policies>
std::shared_ptr<std::map<std::thread::id, ThreadPool<Policies>*>> ThreadPool<Policies>::threadAssociation { new std::map<std::thread::id, ThreadPool<Policies>*> };
template<typename Policies>
std::unique_ptr<ThreadPool<Policies>> ThreadPool<Policies>::defaultInstancePtr = nullptr;
template<typename Policies>
std::mutex ThreadPool<Policies>::globalMutex;


/*template<class Iterator, class R, class Policies>
std::tuple<typename ThreadPool<Policies>::TaskPackagePtr, std::vector<typename ThreadPool<Policies>::TaskHandle<typename R>>>
distributeContainerOperationOnPool(ThreadPool<Policies> &pool, Iterator begin, Iterator end, std::function<R(Iterator,Iterator)> fn) {
  auto const poolsize = pool.getNumberOfThreads();
  auto const numElements = std::distance(begin, end);
  auto const numElementsPerThread = std::max(1, numElements/poolsize);
  Iterator intermediate = begin;
  typename ThreadPool<Policies>::TaskPackagePtr package = pool.createTaskPackage();
  std::vector<typename ThreadPool<Policies>::TaskHandle<R>> handles;
  for(unsigned int i = 0; i < (numElements/numElementsPerThread); ++i) {
    Iterator intermediateEnd = intermediate;
    std::advance(intermediateEnd, numElementsPerThread);
    handles.push_back(package->addTask(fn, intermediate, intermediateEnd));
  }
  handles.push_back(package->addTask(fn, intermediateEnd, end));
  return std::make_tuple<typename ThreadPool<Policies>::TaskPackagePtr, std::vector<typename ThreadPool<Policies>::TaskHandle<R>>>(package, handles);
}*/

#endif /* __THREADPOOL_THREADPOOL_H__ */

