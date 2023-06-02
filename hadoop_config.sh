#scp -P 22000 $HADOOP_HOME/conf/* root@skv-node5:$HADOOP_HOME/conf
scp -P 22000 -r /host/debug      root@skv-node5:/host
scp -P 22000 *.cfg               root@skv-node5:/host
scp -P 22000 grasper-conf.ini    root@skv-node5:/host
