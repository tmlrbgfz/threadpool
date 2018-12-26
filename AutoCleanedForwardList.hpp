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
#include <type_traits>
#include <mutex>

template<class T>
class AutoCleanedForwardList;

template<class T>
class AutoCleanedForwardListNode {
    T element;
    std::shared_ptr<AutoCleanedForwardListNode<T>> next;
public:
    AutoCleanedForwardListNode(T &&data) : element(data) { }
    AutoCleanedForwardListNode(T const &data) : element(data) { }
    AutoCleanedForwardListNode(AutoCleanedForwardListNode<T> const &other) : element(other.element), next(other.next) { }

    T const &data() const {
        return element;
    }
    T &data() {
        return element;
    }
    std::shared_ptr<AutoCleanedForwardListNode<T>> const &getNext() const { return next; }
    void setNext(std::shared_ptr<AutoCleanedForwardListNode<T>> const &node) { this->next = node; }
};

template<class T>
class AutoCleanedForwardListIterator;
template<class T>
class AutoCleanedForwardListConstIterator;

template<class T>
class AutoCleanedForwardListIterator {
public:
    typedef T value_type;
private:
    friend class AutoCleanedForwardList<value_type>;
    friend class AutoCleanedForwardListConstIterator<T>;
    std::shared_ptr<AutoCleanedForwardListNode<value_type>> node;
public:
    AutoCleanedForwardListIterator(std::shared_ptr<AutoCleanedForwardListNode<value_type>> const &ptr) : node(ptr) { }
    AutoCleanedForwardListIterator() = default;
    AutoCleanedForwardListIterator(AutoCleanedForwardListIterator<T> const &other) : node(other.node) { }
    value_type& operator*() {
        return (this->node->data());
    }
    value_type const & operator*() const {
        return (this->node->data());
    }
    value_type* operator->() {
        return &(this->node->data());
    }
    value_type const * operator->() const {
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
class AutoCleanedForwardListConstIterator {
public:
    typedef T value_type;
private:
    friend class AutoCleanedForwardList<value_type>;
    std::shared_ptr<AutoCleanedForwardListNode<value_type>> node;
public:
    AutoCleanedForwardListConstIterator(std::shared_ptr<AutoCleanedForwardListNode<value_type>> const &ptr) : node(ptr) { }
    AutoCleanedForwardListConstIterator() = default;
    AutoCleanedForwardListConstIterator(AutoCleanedForwardListConstIterator<T> const &other) : node(other.node) { }
    AutoCleanedForwardListConstIterator(AutoCleanedForwardListIterator<T> const &other) : node(other.node) { }
    T const & operator*() const {
        return (this->node->data());
    }
    T const * operator->() const {
        return &(this->node->data());
    }
    AutoCleanedForwardListConstIterator<T> operator++(int) {
        *this = AutoCleanedForwardListConstIterator<T>(this->node->getNext());
        return *this;
    }
    AutoCleanedForwardListConstIterator<T> operator++() {
        auto returnValue = AutoCleanedForwardListConstIterator<T>(*this);
        *this = AutoCleanedForwardListConstIterator<T>(this->node->getNext());
        return returnValue;
    }
    AutoCleanedForwardListConstIterator<T> operator+(unsigned int offset) const {
        std::shared_ptr<AutoCleanedForwardListNode<T>> ptr = this->node;
        while(offset > 0 && ptr != nullptr) {
            ptr = ptr->getNext();
            --offset;
        }
        return { ptr };
    }

    bool operator==(AutoCleanedForwardListConstIterator<T> const &other) const {
        return this->node == other.node;
    }
    bool operator!=(AutoCleanedForwardListConstIterator<T> const &other) const {
        return this->node != other.node;
    }
};

template<class T, class U>
bool operator==(AutoCleanedForwardListIterator<T> &&iter1, AutoCleanedForwardListConstIterator<U> &&iter2) {
    AutoCleanedForwardListConstIterator<U> casted(iter1);
    return casted == iter2;
}
template<class T, class U>
bool operator==(AutoCleanedForwardListConstIterator<T> &&iter1, AutoCleanedForwardListIterator<U> &&iter2) {
    return iter2 == iter1;
}

template<class T>
class AutoCleanedForwardList {
    std::shared_ptr<AutoCleanedForwardListNode<T>> last;
public: //types
    typedef T value_type;
    typedef AutoCleanedForwardListIterator<T> iterator;
    typedef AutoCleanedForwardListConstIterator<T> const_iterator;
private:
    void _M_insert_after(iterator pos, AutoCleanedForwardListNode<T> *newNode) {
        if(pos != this->end()) {
            newNode->setNext(pos.node->getNext());
            pos.node->setNext(std::shared_ptr<AutoCleanedForwardListNode<T>>(newNode));
            if(pos == this->begin()) {
                this->last = pos.node->getNext();
            }
        } else {
            if(this->empty()) {
                this->last = std::shared_ptr<AutoCleanedForwardListNode<T>>(newNode);
            } else {
                this->begin().node->setNext(std::shared_ptr<AutoCleanedForwardListNode<T>>(newNode));
                this->last = pos.node->getNext();
            }
        }
    }
public:
    AutoCleanedForwardList() = default;
    ~AutoCleanedForwardList() = default;

    iterator begin() { return last; }
    const_iterator begin() const { return last; }
    const_iterator cbegin() const { return last; }

    iterator end() { return AutoCleanedForwardListIterator<T>(); }
    const_iterator end() const { return AutoCleanedForwardListIterator<T>(); }
    const_iterator cend() const { return AutoCleanedForwardListIterator<T>(); }

    bool empty() const { return this->begin() == this->end(); }

    void push_back(T &&element) {
        this->insert_after(this->begin(), element);
    }
    void push_back(T const &element) {
        this->insert_after(this->begin(), element);
    }

    void insert_after(iterator pos, T &&element) {
        this->_M_insert_after(pos, new AutoCleanedForwardListNode<T>(element));
    }
    void insert_after(iterator pos, T const &element) {
        this->_M_insert_after(pos, new AutoCleanedForwardListNode<T>(element));
    }
};

#endif //__THREADPOOL_AUTOCLEANED_FORWARD_LIST_HPP__
