import config
import os
from distexprunner import *

NUMBER_NODES = 5

parameter_grid = ParameterGrid(
    worker=[4,8,16,32,64],
)

@reg_exp(servers=config.server_list[:NUMBER_NODES], max_restarts=1)
def check_latency(servers):
    servers.cd("/home/muxi/ccpro/Sherman/build")
    procs = [servers[i].run_cmd("./rate_limit_validate -computeNodes 5 -psn_numbers 1000000 -rate_limit_node_id "+str(i)) for i in range(0,5)]
    assert(all(p.wait() == 0 for p in procs))

# @reg_exp(servers=config.server_list[:NUMBER_NODES], max_restarts=1)
# def get_current_time(servers):
#     servers.cd("/home/muxi/ccpro/Sherman/build")
#     procs = [s.run_cmd("./filter") for s in servers]
#     assert(all(p.wait() == 0 for p in procs))
