/*
 * StdThreadPool.cpp
 *
 *  Created on: May 24, 2016
 *      Author: niklas
 */

#include "StdThreadPool.h"
#include <boost/assert.hpp>

std::map<std::thread::id, StdThreadPool*> StdThreadPool::threadAssociation;
std::shared_ptr<StdThreadPool> StdThreadPool::defaultInstancePtr = nullptr;
std::mutex StdThreadPool::globalMutex;

void StdThreadPool::work(void) {
    std::thread::id threadID = std::this_thread::get_id();
#ifdef CONGESTION_ANALYSIS
    tryLockInstanceLock();
#endif
    this->instanceMutex.lock();
    this->threadWork[threadID] = this->taskDefinitions.end();
    this->instanceMutex.unlock();
    StdThreadPool::globalMutex.lock();
    StdThreadPool::threadAssociation[threadID] = this;
    StdThreadPool::globalMutex.unlock();
    while(true) {
        TaskList::iterator front;
        TaskContainer::iterator taskIter;
        std::shared_ptr<Task> task;
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
        BOOST_ASSERT(taskIter != this->taskDefinitions.end());
        //We made sure that only one thread gets this task, thus we don't need locking for the Task struct
        //If the dependencyCount is greater than zero, this task has unfulfilled dependencies and has to be put back
        if(taskIter->second->dependencyCount.load() > 0) {
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
    StdThreadPool::globalMutex.lock();
    StdThreadPool::threadAssociation.erase(threadID);
    StdThreadPool::globalMutex.unlock();
}

StdThreadPool::TaskContainer::iterator StdThreadPool::getTaskDefinition(const TaskID id, bool lockingRequired) {
    TaskContainer::iterator result;
    if(lockingRequired) {
        //NOTE: Replace with std::shared_lock as soon as C++17 is state-of-the-art
        //NOTE: Get permission from the robot overlords which are ruling you by that time
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
        boost::shared_lock_guard<boost::shared_mutex> lock(this->taskDefAccessMutex);
        result = this->taskDefinitions.find(id);
    } else {
        result = this->taskDefinitions.find(id);
    }
    return result;
}

void StdThreadPool::addDependencies(const TaskID id, const std::set<TaskID> &dependencies) {
	std::set<TaskID>::size_type satisfiedDependencies = 0;
	TaskContainer::iterator task = this->getTaskDefinition(id, true);
	if(task == this->taskDefinitions.end()) {
		return;
	}

	//After obtaining this shared lock, all dependencies which are still in the task definition container
	// will not be removed before the dependencies were added
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
	boost::shared_lock_guard<boost::shared_mutex> tskDefLock(this->taskDefAccessMutex);
	for(const TaskID dep : dependencies) {
		TaskContainer::iterator dependency = this->getTaskDefinition(dep, false);
		if(dependency != this->taskDefinitions.end()) {
			dependency->second->dependants.push_back(id);
		} else {
			//Task already finished, reduce dependency count
			++satisfiedDependencies;
		}
	}
	task->second->dependencyCount = dependencies.size() - satisfiedDependencies;
}

void StdThreadPool::removeTask(const TaskID id) {
	TaskContainer::iterator taskIter = this->getTaskDefinition(id, true);
	if(taskIter == this->taskDefinitions.end()) {
		return;
	}
	std::shared_ptr<Task> task = taskIter->second;

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
	for(const TaskID depID : task->dependants) {
		TaskContainer::iterator dep = this->getTaskDefinition(depID, true);
		//NOTE: Reasoning for assert: The tasks in dependants were added to this pool
		//          The tasks in dependants can't run before this loop finishes
		//          Therefore, the tasks must be in the set of task definitions
		BOOST_ASSERT(dep != this->taskDefinitions.end());
		dep->second->dependencyCount -= 1;
	}
    --(this->numTasksTotal);
}

StdThreadPool *StdThreadPool::threadPoolAssociation() {
    std::thread::id tid = std::this_thread::get_id();
    StdThreadPool *pool = 0;
    StdThreadPool::globalMutex.lock();
    std::map<std::thread::id, StdThreadPool*>::iterator thread = StdThreadPool::threadAssociation.find(tid);
    if(thread != StdThreadPool::threadAssociation.end()) {
    	pool = thread->second;
    }
    StdThreadPool::globalMutex.unlock();
    return pool;
}

StdThreadPool::TaskContainer::iterator StdThreadPool::getOwnTaskIterator() {
    std::thread::id tid = std::this_thread::get_id();
    StdThreadPool *pool = StdThreadPool::threadPoolAssociation();
    BOOST_ASSERT(pool != 0); //TODO: Replace with exception or errx
    //No lock required, this tasks thread will not change
    TaskContainer::iterator task = pool->threadWork.at(tid);
    /*
     * Rationale:
     * EITHER the calling thread is not part of any thread pool.
     * Then, pool == 0 and this point is not reached.
     * OR the calling thread is part of a thread pool.
     * Then, it either has a task or is in the work()-function.
     * The work()-function does not call this function, thus the
     * thread must have a task assigned.
     */
    BOOST_ASSERT(task != pool->taskDefinitions.end());
    return task;
}

StdThreadPool::TaskID StdThreadPool::getOwnTaskID() {
	return StdThreadPool::getOwnTaskIterator()->first;
}

void StdThreadPool::setNumberOfThreads(size_type n) {
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    if(n < this->maxNumThreads) {
        this->numThreadsToStop = (this->maxNumThreads - n);
        this->maxNumThreads = n;
    }
    while(n > this->maxNumThreads) {
        this->threads.emplace_back(std::bind(&StdThreadPool::work, this));
        ++this->maxNumThreads;
    }
}

StdThreadPool::size_type StdThreadPool::getNumberOfThreads() {
    std::unique_lock<std::mutex> lock(this->instanceMutex);
    return this->maxNumThreads;
}

StdThreadPool::size_type StdThreadPool::joinStoppedThreads() {
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

StdThreadPool::StdThreadPool() : StdThreadPool(std::thread::hardware_concurrency() - 1) {
}

StdThreadPool::StdThreadPool(size_type size) 
: tIdRegistry(0),
  numTasksRunning(0),
  numTasksTotal(0),
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

StdThreadPool::~StdThreadPool() {
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

StdThreadPool& StdThreadPool::getDefaultInstance() {
    return *StdThreadPool::getDefaultInstancePtr();
}

StdThreadPool* StdThreadPool::getDefaultInstancePtr() {
	if(StdThreadPool::defaultInstancePtr.get() == nullptr) {
		StdThreadPool::defaultInstancePtr.reset(new StdThreadPool);
	}
    return StdThreadPool::defaultInstancePtr.get();
}

StdThreadPool::size_type StdThreadPool::getNumTasksRunning() const {
	return this->numTasksRunning.load();
}

StdThreadPool::size_type StdThreadPool::getNumTasks() const {
	return this->numTasksTotal.load();
}

bool StdThreadPool::empty() {
	if(this->activeTasks.empty() and this->inactiveTasks.empty()) {
		std::unique_lock<std::mutex> lock(this->instanceMutex);
		return this->activeTasks.empty() and this->inactiveTasks.empty();
	}
	return false;
}

StdThreadPool::TaskID StdThreadPool::addTaskDetail(StdThreadPool::TaskDefinition&& task, const std::set<StdThreadPool::TaskID> &dependencies) {
  std::shared_ptr<Task> t = std::shared_ptr<Task>(new Task);
  t->fn = std::move(task);
  t->dependencyCount.store(0);
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
  this->addDependencies(id, dependencies);

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
void StdThreadPool::wait() {
    TaskID lastTask;
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
void StdThreadPool::wait(StdThreadPool::TaskID id) {
#ifdef CONGESTION_ANALYSIS
        tryLockTaskDefLockShared();
#endif
    this->taskDefAccessMutex.lock_shared();
    TaskContainer::iterator task = this->getTaskDefinition(id, false);
    if(task == this->taskDefinitions.end()) {
        this->taskDefAccessMutex.unlock_shared();
        return;
    }
    std::shared_ptr<Task> taskPtr = task->second;
    std::unique_lock<std::mutex> lock(taskPtr->objMutex);
    this->taskDefAccessMutex.unlock_shared();
    taskPtr->cv_done.wait(lock, [&]() -> bool { return taskPtr->done; });
}

 void StdThreadPool::waitOne() {
	 if(this->getNumTasksRunning() == 0) {
		 return;
	 }
	 this->instanceMutex.lock();
	 if(this->getNumTasksRunning() == 0) {
		 this->instanceMutex.unlock();
		 return;
	 }
	 TaskList::iterator iter = this->activeTasks.begin();
	 this->instanceMutex.unlock();
	 this->wait(*iter);
 }

bool StdThreadPool::finished(StdThreadPool::TaskID id) {
    return this->getTaskDefinition(id, true) == this->taskDefinitions.end();
}

std::shared_ptr<StdThreadPool::TaskPackage> StdThreadPool::createTaskPackage() {
    TaskPackage *pkg = new TaskPackage;
    //TODO: Maybe use a weak ptr and initialise it from a private shared_ptr of the Pool?
    pkg->correspondingPool = this;
    return std::shared_ptr<StdThreadPool::TaskPackage>(pkg);
}

StdThreadPool::TaskPackage::TaskPackage() : correspondingPool(0) {
}

bool StdThreadPool::TaskPackage::finished() {
    std::deque<TaskID> ids;
    bool allDone = true;
    this->mutex.lock();
    ids = this->tasks;
    this->mutex.unlock();
    for(const TaskID id : ids) {
        allDone &= this->correspondingPool->finished(id);
    }
    return allDone;
}

void StdThreadPool::TaskPackage::wait() {
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

void StdThreadPool::TaskPackage::wait(StdThreadPool::TaskID id) {
	this->correspondingPool->wait(id);
}

