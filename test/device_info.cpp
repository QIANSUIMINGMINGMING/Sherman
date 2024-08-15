#include "Common.h"
#include "Rdma.h"

int main() {
    RdmaContext ctx;
    createContext(&ctx);
    checkDMSupported(ctx.ctx);    
    return 0;
}