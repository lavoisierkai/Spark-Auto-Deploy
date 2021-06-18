from Class.Server import Server
from Class.Master import Master
from invoke import UnexpectedExit
from typing import List
from datetime import datetime
from os import mkdir, remove
import numpy as np
import pandas as pd
import sys


def main(job: str, folder_name: str):
    # connect to master
    with open("conf/master-port", "r") as f:
        master = Master(int(f.read()))

    # connect to slaves
    with open("conf/slave-ports", "r") as f:
        ports: List[int] = [int(line) for line in f.readlines()]
        slaves: List[Server] = []
        for port in ports:
            print("")
            slave = Server(port)
            slaves.append(slave)

    master.start_monitor(interval=0.01)
    # start monitor on each slave
    for slave in slaves:
        slave.start_monitor(interval=0.01)

    # send command to master to start the job
    try:
        master.get_connection().run("source /etc/profile && cd $SPARK_HOME && " + job)
    except UnexpectedExit as exp:
        pass

    # job is done, stop monitors, the slaves will write data to their own disks
    master.stop_monitor()
    for slave in slaves:
        slave.stop_monitor()

    # collect data to "./monitor_data/"
    current_time = str(datetime.now())[0:10] + "_" +str(datetime.now())[-15:-7]
    print(current_time)
    # folder_name = input("Please input the folder name (default: {current_time}):\n".format(current_time=current_time))
    # folder_name = folder_name if folder_name else current_time
    folder_path = "./monitor_data/" + current_time + "_" + folder_name
    # master.get_connection().run("mkdir {folder_path}".format(folder_path=folder_path))
    mkdir(folder_path)

    i: int = 1
    lst_i = list()
    lst_mean_cpu_usage = list()
    lst_max_cpu_usage = list()
    for slave in slaves:
        file_path = "{folder_path}/slave{i}.csv".format(folder_path=folder_path, i=i)
        slave.get_connection().get(slave.get_log_path(), file_path)
        array = np.genfromtxt(file_path, delimiter=',')
        mean_cpu_usage = np.mean(array[:, 1])
        max_cpu_usage = np.max(array[:, 1])
        print("slave{}: {}, {}".format(i, mean_cpu_usage, max_cpu_usage))
        if max_cpu_usage >= 90:
            lst_i.append(i)
            lst_mean_cpu_usage.append(mean_cpu_usage)
            lst_max_cpu_usage.append(max_cpu_usage)
        if max_cpu_usage <= 90:
            remove(file_path)
        i += 1
    c={"slave":lst_i,"mean_cpu_usage":lst_mean_cpu_usage,"max_cpu_usage":lst_max_cpu_usage}
    df = pd.DataFrame(c,columns=["slave","mean_cpu_usage","max_cpu_usage"])
    df.to_csv("{folder_path}/slave_total_{folder_name}.csv".format(folder_path=folder_path,folder_name=folder_name), index=False)
    master.get_connection().get(master.get_log_path(), "{folder_path}/master.csv".format(folder_path=folder_path,
                                                                                         i=i))
    print("The data files have been put into monitor_data/" + folder_name)

if __name__ == '__main__':
    # excuate_list = [0, 1, 0, 2, 0, 3] * 5

    # LRmethod = ["LR_O",
    #             "LR_H",
    #             "LR_T",
    #             "LR_R"]
                
    data_length = list()
    for i in range(54,61):
        data_length.append(i*300000)
        i=i+1
    print(data_length)

    for i in range(len(data_length)):
        job: str = ("bin/spark-submit " \
                "--master spark://192.168.122.51:7077 " \
                "--conf spark.cores.max=40 " \
                "--conf spark.executor.cores=4 " \
                "--executor-memory 6g " \
                "--name {name} " \
                "/home/spark/Downloads/LogisticRegression/target/scala-2.11/logisticregression_2.11-1.0.jar " \
                "hdfs://192.168.122.51:9000/LR_data/LR_data_original{file}.csv " \
                "10 10 10 10 " \
                "100 ").format(name = i, file = data_length[i])
        main(job, folder_name = "LR_data_original{a}".format(a=i))
