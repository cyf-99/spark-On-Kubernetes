# from kubernetes import client, config
#
# config.load_kube_config()
# v1 = client.CoreV1Api()
# print(v1.read_namespaced_pod_status(name="11701080205", namespace="default"))
# # print(v1.read_namespaced_endpoints(name="11701080205", namespace="default"))
# v1.read_namespaced_pod_status()
# v1.get
# # print(v1.patch_namespace_status(name="11701080205", namespace="default"))
# print(v1.e)


from pyhive import hive

conn = hive.Connection(host='192.168.10.201', port=10000, username='root', database='spark')
cursor = conn.cursor()
cursor.execute("select * from spark.tmp_ads_class_sucess_faile_num")
tmp = list(cursor)
print(tmp)
chart_class = []
if len(tmp) == 0:
    chart_class.append(('117010801', 0, 0))
    chart_class.append(('117010802', 0, 0))
elif len(tmp) == 1 and tmp[0][0] == '117010801':
    chart_class.append(chart_class[0])
    chart_class.append(('117010802', 0, 0))
elif len(tmp) == 1 and tmp[0][0] == '117010802':
    chart_class.append(('117010801', 0, 0))
    chart_class.append(chart_class[1])
else:
    chart_class = tmp
print(tmp)
print(chart_class)
