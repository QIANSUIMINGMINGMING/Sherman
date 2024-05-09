import config
import os
from distexprunner import *

NUMBER_NODES = 5

parameter_grid = ParameterGrid(
    worker=[4,8,16,32,64],
)

@reg_exp(servers=config.server_list[:NUMBER_NODES])
def compile(servers):
    servers.cd("/home/muxi/ccpro/Sherman")
    git_cmd1 = f'git checkout bench'
    procs = [s.run_cmd(git_cmd1) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    git_cmd2 = f'git pull'
    procs = [s.run_cmd(git_cmd2) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    cmake_cmd = f'rm -rf build && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_C_COMPILER=/bin/x86_64-linux-gnu-gcc-9 -DCMAKE_CXX_COMPILER=/bin/x86_64-linux-gnu-g++-9 .. && make -j'
    procs = [s.run_cmd(cmake_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

@reg_exp(servers=config.server_list[:NUMBER_NODES])
def restart_memcached(servers):
    # read memcached.conf in this machine, first line is ip, second line is port
    # servers.cd("/home/muxi/ccpro/DM-cooperation/scripts")
    # with open('memcached.conf', 'r') as file:
    #     data = file.read()
    #     memcache_ip = data.split('\n')[0]
    #     memcache_port = data.split('\n')[1]
    
    # for s in servers:
    #     if s.ip == memcache_ip:
    #         s.run_cmd("bash ./restartMemc.sh")

    # allocate huge pages
    huge_page_cmd = "sudo sh -c 'echo 36864 > /proc/sys/vm/nr_hugepages && ulimit -l unlimited'" 
    procs = [s.run_cmd(huge_page_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

@reg_exp(servers=config.server_list[:NUMBER_NODES], max_restarts=1)
def get_current_time(servers):
    servers.cd("/home/muxi/ccpro/Sherman/build")
    procs = [s.run_cmd("./filter") for s in servers]
    assert(all(p.wait() == 0 for p in procs))

# @reg_exp(servers=config.server_list[:NUMBER_NODES], max_restarts=1)
# def test_btree(servers):
#     pass
#     # servers.cd("/home/muxi/ccpro/Sherman/build")
#     # for i in range(1, NUMBER_NODES):
#     #     cmd = f'numactl --membind=1 ./benchmark -ownIp={servers[i].ibIp} -computeNodes=4 -nodeId={i}'        
#     #     servers[i].run_cmd(cmd).wait()
#     # servers.cd("/home/muxi/ccpro/DM-cooperation/build")
#     # cmds = []
#     # for i in range(1, NUMBER_NODES):
#     #     cmd = f'numactl --membind=1 ./testmCM -ownIp={servers[i].ibIp} -computeNodes=4 -nodeId={i}'        
#     #     cmds += [servers[i].run_cmd(cmd)]
    
#     # if not all(cmd.wait() == 0 for cmd in cmds):
#     #     return Action.RESTART

#     # cmds = []
#     # cmd = f'numactl --membind=1 ./testmCM -ownIp={servers[0].ibIp} -port=20886 -worker={worker}'
#     # cmds += [servers[0].run_cmd(cmd)]

#     # work = worker
#     # numberNodes=1
#     # if worker >=4:
#     #     work = int(worker/4)
#     #     numberNodes=4
    
#     # for i in range(1, numberNodes+1):
#     #     cmd = f'numactl --membind=1 ./testCM -ownIp={servers[i].ibIp} -port=20886 -worker={work} -run_for_seconds=30'
#     #     cmds += [servers[i].run_cmd(cmd)]
        
#     # if not all(cmd.wait() == 0 for cmd in cmds):
#     #     return Action.RESTART