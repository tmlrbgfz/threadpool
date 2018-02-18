#ifndef STDTHREADPOOL_COPYISMOVE_H_
#define STDTHREADPOOL_COPYISMOVE_H_

#include <utility>

//Helper class to move objects into lambdas
template<class T>
class CopyIsMove {
private:
  T data;
public:
  CopyIsMove(T &&init) : data(std::move(init)) {}
  CopyIsMove(CopyIsMove &other) : data(std::move(other.data)) {}
  CopyIsMove(CopyIsMove &&other) = default;

  T& getData() { return data; }
  void setData(T &&value) {
    data = std::move(value);
  }
};

#endif //STDTHREADPOOL_COPYISMOVE_H_