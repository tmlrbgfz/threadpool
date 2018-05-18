#ifndef __THREADPOOL_POLICIES_H__
#define __THREADPOOL_POLICIES_H__

namespace policies {
    typedef struct DependenciesRespected_t {
        static constexpr bool respectDependencies = true;
    } DependenciesRespected;
    typedef struct DependenciesNotRespected_t {
        static constexpr bool respectDependencies = false;
    } DependenciesNotRespected;
}

#endif //__THREADPOOL_POLICIES_H__