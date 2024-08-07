from distexprunner import ServerList, Server


SERVER_PORT = 20418

# server_list = ServerList(
#     # fill in
#     Server('gpu01', '10.16.70.46', SERVER_PORT, ibIp="192.168.1.2"),
#     Server('gpu02', '10.16.81.26', SERVER_PORT, ibIp="192.168.1.3"),
#     Server('gpu03', '10.16.70.16', SERVER_PORT, ibIp="192.168.1.4"),
#     Server('gpu04', '10.16.71.35', SERVER_PORT, ibIp="192.168.1.5"),
#     Server('gpu05', '10.16.81.54', SERVER_PORT, ibIp="192.168.1.6"),
#     Server('dbg21', '10.16.22.253', SERVER_PORT),
# )

server_list = ServerList(
    # fill in
    Server('gpu01', '10.16.70.46', SERVER_PORT, ibIp="10.16.70.46"),
    Server('gpu02', '10.16.81.26', SERVER_PORT, ibIp="10.16.81.26"),
    Server('gpu03', '10.16.94.136', SERVER_PORT, ibIp="10.16.94.136"),
    Server('gpu04', '10.16.71.35', SERVER_PORT, ibIp="10.16.71.35"),
    Server('gpu05', '10.16.81.54', SERVER_PORT, ibIp="10.16.81.54"),
    Server('dbg21', '10.16.22.253', SERVER_PORT),
)

# numa_info = {

# }

# file = open('check_nic.log', 'r')
# for i in range(server_list.__len__()):
#     ipline = file.readline()
#     ipline = ipline.strip('\n')
#     numaline = file.readline()
#     numaline = numaline.strip('\n')
#     numa_info[ipline] = numaline

# print(numa_info)