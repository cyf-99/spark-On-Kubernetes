#!/bin/bash

code=$1
data=$2
memory_size=$3
cores_num=$4
pod_name=$5
instance=$6
#num_features=$7
#algorithm_name=$8


/opt/spark/bin/spark-submit \
  --master k8s://https://192.168.10.4:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.driver.pod.name=$pod_name \
  --conf spark.executor.instances=$instance \
  --conf spark.executor.cores=$cores_num \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.volumes.hostPath.tmp.mount.path=/tmp  \
  --conf spark.kubernetes.driver.volumes.hostPath.tmp.options.path=/tmp \
  --conf spark.kubernetes.executor.volumes.hostPath.tmp.mount.path=/tmp  \
  --conf spark.kubernetes.executor.volumes.hostPath.tmp.options.path=/tmp \
  --conf spark.kubernetes.container.image=myregistry.local:5000/cyf_spark_py_test1:3.1.1 \
	$code $data

  #--conf spark.kubernetes.container.image=myregistry.local:5000/spark_kube2:test3 \
  #$code $data
  # cyf_spark_py_test1:3.1.1
	#--conf spark.kubernetes.pyspark.pythonVersion=3 \/spark_user/
	#--conf spark.kubernetes.driver.volumes.hostPath.tmp.mount.path=hdfs://192.168.10.6:9000/spark_user  \
  #--conf spark.kubernetes.driver.volumes.hostPath.tmp.options.path=hdfs://192.168.10.6:9000/spark_user \
  #--conf spark.kubernetes.executor.volumes.hostPath.tmp.mount.path=hdfs://192.168.10.6:9000/spark_user  \
  #--conf spark.kubernetes.executor.volumes.hostPath.tmp.options.path=hdfs://192.168.10.6:9000/spark_user \
#  --conf spark.driver.cores=1 \
#  --conf spark.driver.memory=$memory_size \
#  --conf spark.executor.cores=$cores_num \
#  --conf spark.executor.memory=$memory_size \

