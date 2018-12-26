#ifndef __THREADPOOL_AUTOCLEANED_FORWARD_LIST_HPP__
#define __THREADPOOL_AUTOCLEANED_FORWARD_LIST_HPP__

/**
 * AutoCleanedForwardList.hpp
 * Author: Niklas Krafczyk
 * This file describes a container class which implements a singly linked list.
 * That singly linked list object only maintains a reference to the object
 * which was last inserted. That object is referenced by the operator returned
 * by begin()/cbegin(). Each list node is managed by a shared_ptr and iterators
 * of the described list class reference the object they are currently pointing
 * to as shared_ptrs as well. This way only those list elements which may be
 * accessed in the future are still available as all others will be deleted by
 * the shared_ptr mechanism. The performance overhead incurred by the
 * shared_ptrs is acknowledged. This data structure is not for you if you are
 * looking for a container for performance critical components.
 */

#include <memory>

template<class T>
class AutoCleanedForwardListNode {
    T element;
    std::shared_ptr<AutoCleanedForwardListNode<T>> next;
public:
    AutoCleanedForwardListNode(T &&data) : element(data) { }
    AutoCleanedForwardListIterator(AutoCleanedForwardListNode const &other) : element(other.element), next(other.next) { }

    T const &data() const {
        return element;
    }
    T &data() {
        return element;
    }
    std::shared_ptr<AutoCleanedForwardListNode<T>> const &getNext() const { return next; }
    void setNext(std::shared_ptr<AutoCleanedForwardListNode<T> const &node) { this->next = node; }
};

template<class T>
class AutoCleanedForwardListIterator {
    std::shared_ptr<AutoCleanedForwardListNode<T>> node;
public:
    AutoCleanedForwardListIterator(std::shared_ptr<AutoCleanedForwardListNode<T>> &ptr) : node(ptr) { }
    AutoCleanedForwardListIterator(AutoCleanedForwardListIterator<T> &other) : node(other.node) { }
    T& operator*() {
        return (this->node->data());
    }
    T const & operator*() const {
        return (this->node->data());
    }
    T* operator->() {
        return &(this->node->data());
    }
    T const * operator->() const {
        return &(this->node->data());
    }
    AutoCleanedForwardListIterator<T> operator++(int) {
        *this = AutoCleanedForwardListIterator<T>(this->node->getNext());
        return *this;
    }
    AutoCleanedForwardListIterator<T> operator++() {
        auto returnValue = AutoCleanedForwardListIterator<T>(*this);
        *this = AutoCleanedForwardListIterator<T>(this->node->getNext());
        return returnValue;
    }
    AutoCleanedForwardListIterator<T> operator+(unsigned int offset) const {
        std::shared_ptr<AutoCleanedForwardListNode<T>> ptr = this->node;
        while(offset > 0 && ptr != nullptr) {
            ptr = ptr->getNext();
            --offset;
        }
        return { ptr };
    }

    bool operator==(AutoCleanedForwardListIterator<T> const &other) const {
        return this->node == other.node;
    }
    bool operator!=(AutoCleanedForwardListIterator<T> const &other) const {
        return this->node != other.node;
    }
};

template<class T>
class AutoCleanedForwardList {
    std::shared_ptr<AutoCleanedForwardListNode<T>> last;
public: //types
    typedef T value_type;
    typedef AutoCleanedForwardListIterator<T> iterator;
    typedef AutoCleanedForwardListIterator<T const> const_iterator;
public:
    AutoCleanedForwardList() = default;
    ~InifinteForwardList() = default;

    iterator begin() { return last; }
    const_iterator begin() const { return last; }
    const_iterator cbegin() { return last; }

    iterator end() { return AutoCleanedForwardListIterator(nullptr); }
    const_iterator end() const { return AutoCleanedForwardListIterator(nullptr); }
    const_iterator cend() const { return AutoCleanedForwardListIterator(nullptr); }


    void push_back(T &&element) {
        this->insert_after(this->begin(), std::forward<T>(element));
        this->last = this->last->getNext();
    }

    void insert_after(iterator pos, T &&element) {
        AutoCleanedForwardListNode<T> *newNode = new AutoCleanedForwardListNode<T>(std::forward<T>(element));
        newNode->setNext(pos->getNext());
        pos->setNext(newNode);
    }
};

#endif //__THREADPOOL_AUTOCLEANED_FORWARD_LIST_HPP__
