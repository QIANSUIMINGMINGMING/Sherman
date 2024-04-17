#include "KVCache.h"

#include <iostream>

int main() {
    int a = 0;
    std::atomic<int> *b;
    b = reinterpret_cast<std::atomic<int> *>(&a);
    return 0;
}