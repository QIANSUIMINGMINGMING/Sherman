# README
1. Turn off ntp service of slave machines. Check  ``sudo systemctl status`` of ``systemd-timesyncd``, ``ntp``, and ``chronyd``. Then turn off all of them via ``sudo systemctl stop``
2. Put "ptp4l.service" and "master_ptp4l.service" to "/lib/systemd/system" and start service
3. Put "phc2sys.service" to all machines and start the service