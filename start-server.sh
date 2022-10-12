# # $1 PRO_NUM   $2 MACHINE.CFG    $3 IB.CONF
# mpiexec -n $1 -f $2 ./release/server $3
# $1 PRO_NUM   $2 IB.CONF
# mpiexec -n $1 ./debug/server $2
/usr/local/mpich/bin/mpiexec -np $1 \
    -env UCX_NET_DEVICES=mlx5_1:1 \
    ./debug/server $2
