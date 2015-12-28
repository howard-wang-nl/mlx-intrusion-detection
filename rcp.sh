HOST=lab5
scp target/scala-2.10/mlx-intrusion-detection-assembly-0.1.0.jar $HOST:hw/spark/
scp submit.sh $HOST:hw/spark/
ssh $HOST "( chmod a+x hw/spark/*.sh; chmod a+r hw/spark/* )"
