from flask import *

app = Flask(__name__)
from pyhive import hive

conn = hive.Connection(host='192.168.10.201', port=10000, username='root', database='spark')
cursor = conn.cursor()


@app.route('/', methods=['GET', 'POST'])
def home_page():
    cursor.execute("select success, faile from spark.ads_sucess_faile_num")
    chart1_data = list(cursor)
    print(chart1_data)

    cursor.execute("select days, pv, uv from spark.ads_pv_uv_num")
    chart4_data = [[], [], []]
    for line in list(cursor):
        chart4_data[0].append(line[0])
        chart4_data[1].append(line[1])
        chart4_data[2].append(line[2])
    print(chart4_data)

    cursor.execute("select * from spark.ads_class_sucess_faile_num")
    tmp = list(cursor)
    # print(tmp)
    chart_class = []
    if len(tmp) == 0:
        chart_class.append(('117010801', 0, 0))
        chart_class.append(('117010802', 0, 0))
    elif len(tmp) == 1 and tmp[0][0] == '117010801':
        chart_class.append(tmp[0])
        chart_class.append(('117010802', 0, 0))
    elif len(tmp) == 1 and tmp[0][0] == '117010802':
        chart_class.append(('117010801', 0, 0))
        chart_class.append(tmp[0])
    else:
        chart_class = tmp
    # chart_class = list(cursor)
    # tmp = []
    # if chart_class[0][0] == '117010801' and chart_class[0][1] != '117010802':
    #     tmp.append(chart_class[0])
    #     tmp.append(('117010802', 0, 0))
    # elif chart_class[0][0] != '117010801' and chart_class[0][1] != '117010802':
    #     tmp.append(('117010801', 0, 0))
    #     tmp.append(('117010802', 0, 0))
    # elif chart_class[0][0] == '117010802':
    #     tmp.append(('117010801', 0, 0))
    #     tmp.append(chart_class[1])
    print(chart_class)

    cursor.execute("select * from spark.ads_code_sucess_faile_num")
    chart3_data = [[], [], []]
    for line in list(cursor):
        chart3_data[0].append(line[0])
        chart3_data[1].append(line[1])
        chart3_data[2].append(line[2])
    print(chart3_data)

    cursor.execute(
        "select userid,student_name,class_id,code_number,job_success_distinct_number from spark.ads_student_sort_chart1_table1 limit 5")
    tmp = list(cursor)
    table_left = []
    if len(tmp) == 0:
        table_left.append(('-', '-', '-', 0, 0))
    else:
        table_left = tmp
    print(table_left)

    cursor.execute(
        "select userid,student_name,class_id,code_number,job_success_distinct_number from spark.ads_student_sort_table2 limit 5")
    table_right = list(cursor)
    print(table_right)

    return render_template('home_page.html', chart1_data=chart1_data, chart4_data=chart4_data, chart_class=chart_class,
                           chart3_data=chart3_data, table_left=table_left, table_right=table_right)


@app.route('/chart1', methods=['GET', 'POST'])
def chart1():
    cursor.execute("select * from spark.ads_student_sort_chart1_table1")
    data = list(cursor)

    return render_template("chart1.html", data=data)


@app.route('/chart2', methods=['GET', 'POST'])
def chart2():
    cursor.execute("select userid,student_name,class_id,academy,profession,submit_number,code_number,day "
                   "from spark.dws_student_summary_f where class_id='117010801'")
    data = list(cursor)

    return render_template("chart2.html", data=data)


@app.route('/chart3', methods=['GET', 'POST'])
def chart3():
    cursor.execute("select * from spark.ads_code_chart3")
    data = list(cursor)

    return render_template("chart3.html", data=data)


@app.route('/chart4', methods=['GET', 'POST'])
def chart4():
    cursor.execute("select * from spark.ads_pv_uv_num")
    data = list(cursor)

    return render_template("chart4.html", data=data)


@app.route('/chart5', methods=['GET', 'POST'])
def chart5():
    cursor.execute("select userid,student_name,class_id,academy,profession,submit_number,code_number,day "
                   "from spark.dws_student_summary_f where class_id='117010802'")
    data = list(cursor)

    return render_template("chart5.html", data=data)


@app.route('/chart6', methods=['GET', 'POST'])
def chart6():
    cursor.execute("select * from spark.ads_student_sort_table2")
    data = list(cursor)

    return render_template("chart6.html", data=data)


if __name__ == "__main__":
    app.run(debug=True, port=30001, host='192.168.10.4')
