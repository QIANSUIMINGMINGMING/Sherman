import fabric
from fabric import Connection, Group, Task

# Define your servers
master_servers = [
    "ymx@10.16.22.253"
]

servers = [
    'muxi@10.16.70.46',
    'muxi@10.16.81.26',
    'muxi@10.16.94.136',
    'muxi@10.16.71.35',
    'muxi@10.16.81.54'
]

@Task
def check_and_disable_service(c, service_name):
    result = c.run(f'systemctl is-active {service_name}', warn=True)
    print(result)
    # if result.ok:
    #     print(f"Service {service_name} is active. Stopping and disabling it...")
    #     c.sudo(f'systemctl stop {service_name}')
    #     c.sudo(f'systemctl disable {service_name}')
    #     print(f"Service {service_name} has been stopped and disabled.")
    # else:
    #     print(f"Service {service_name} is not active or does not exist.")


# # Define the command you want to execute on all servers
# command = 'uptime'

# # Create a Group object with your servers
# group = Group(*servers)

# # Execute the command on all servers
# for conn in group:
#     result = conn.run(command, hide=True)
#     print(f'{conn.host}:\n{result.stdout.strip()}')

# check ntp service status
def check_ntp_status():
    pass

# stop ntp service

# start master phc2sys and ptp4l

# start slave phc2sys and ptp4l

# stop master phc2sys and ptp4l

# stop slave phc2sys and ptp4l

# start ntp service

