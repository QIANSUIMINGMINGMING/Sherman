#include <iostream>
#include <cmath>

#include <time.h>
#include <chrono>

struct myClock
{
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    static constexpr bool is_steady = false;

    static uint64_t get_ts()
    {
        timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts))
            throw 1;
        uint64_t timestamp = (uint64_t)(ts.tv_sec * 1000000000) + (uint64_t)(ts.tv_nsec); 
        return timestamp;
    }
};

int main() {
    myClock clock;
    uint64_t ts = clock.get_ts();
    std::cout << "timestamp" << ts;
    return 0;
}