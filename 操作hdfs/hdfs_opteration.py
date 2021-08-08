from hdfs.client import Client
from kubernetes import client, config

config.load_kube_config()
v1 = client.CoreV1Api()

hdfs = Client("http://192.168.10.201:50070", root='root')
namespace = "default"


# print(hdfs.list())

# 显示目录下的文件名
def list_dir_file(user_name, file_flag):
    # 用户代码文件夹路径
    list_path = '/spark_user/' + user_name + file_flag
    try:
        files = hdfs.list(list_path)
        return files
    except Exception as e:
        print(e)
        return e


# 将文件上传到hdfs
def uploading_file(local_path, user_name, file_flag):
    # 文件上传到指定目录下
    target_path = '/spark_user/' + user_name + file_flag
    try:
        hdfs.upload(target_path, local_path)
        return 1
    except Exception as e:
        print(e)
        return e


# 查看文件的内容
def see_file(file_name, user_name, file_flag):
    file_path = "/spark_user/" + user_name + file_flag + file_name

    try:
        data = []
        with hdfs.read(file_path, encoding='utf-8') as fp:
            for line in fp:
                data.append(line.strip())
        print(data)
        return 1, data
    except Exception as e:
        print(e)
        return -1, e


# 删除文件
def delete_file(file_name, user_name, file_flag):
    target_file = "/spark_user/" + user_name + file_flag + file_name

    try:
        hdfs.delete(target_file)
        return 1
    except Exception as e:
        print(e)
        return e


# 重命名文件
def rename_file(file_name, rename, user_name, file_flag):
    path = "/spark_user/" + user_name + file_flag
    print(path)
    try:
        hdfs.rename(path + file_name, path + rename)
        return 1
    except Exception as e:
        return e


# 下载文件到本地
def load_local(file_name, local_path, user_name, file_flag):
    path = "/spark_user/" + user_name + file_flag + file_name

    try:
        hdfs.download(path, local_path)
        return 1
    except Exception as e:
        return e


# 运行代码
def run_code(user_name, code_name, data_name, memory_size=2, cores_num=2, instance=1):
    from subprocess import Popen
    code_path = "hdfs://192.168.10.201:9000/spark_user/" + user_name + '/code/' + code_name
    data_path = "hdfs://192.168.10.201:9000/spark_user/" + user_name + '/data/' + data_name
    print(code_name, data_name, memory_size, cores_num, instance)
    try:
        pods = []
        for e in v1.list_namespaced_pod(namespace).items:
            pods.append(e.metadata.name)
        if user_name in pods:
            v1.delete_namespaced_pod(name=user_name, namespace=namespace)
        Process = Popen(
            r'/home/cyf/flask_spark/shell/run.sh %s %s %d %d %s %d' % (code_path, data_path, memory_size, cores_num, user_name, instance),
            shell=True)
        p = Process.wait()
        return p
    except Exception as e:
        return e


# 获取pod中的日志
def get_result(user_name, code_name):
    logs_tmp = '/home/cyf/flask_spark/tmp/' + user_name + '-' + code_name.split('.')[0] + '-result.logs'
    status_tmp = '/home/cyf/flask_spark/status/' + user_name + '-' + code_name.split('.')[0] + '-status.logs'
    save_path = "/spark_user/" + user_name + '/result/'

    try:
        logs = v1.read_namespaced_pod_log(name=user_name, namespace=namespace)
        status = v1.read_namespaced_pod_status(name=user_name, namespace=namespace)
        status = str(status).split('\n')[-5].strip(",").replace("'", "").split(':')[-1].strip()
        print(len(status), status)
        if status == 'Succeeded':
            status = 1
        else:
            status = -1
        v1.delete_namespaced_pod(user_name, namespace)

        with open(str(logs_tmp), 'w+') as fp:
            fp.write(logs)
        hdfs.upload(save_path, logs_tmp, overwrite=True)
        return 1, list(logs.split('\n')), status
    except Exception as e:
        return -1, e, -1


def delete_pod(user_name):
    try:
        v1.delete_namespaced_pod(user_name, namespace)
        return 1
    except Exception as e:
        print(e)
        return -1


