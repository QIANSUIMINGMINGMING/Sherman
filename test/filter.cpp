#include <iostream>
#include <cmath>

#include <time.h>
#include <chrono>
#include "Common.h"


int main() {
    // myClock clock;
    // uint64_t ts = clock.get_ts();
    // std::cout << "timestamp" << ts;
    size_t ptrs[16]{kKeyMax};
    for (int i = 0; i < 16; i++) {
        printf("%lu\n", ptrs[i]);
    }
    return 0;
}