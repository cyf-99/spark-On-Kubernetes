/* 将昨天新增的提交记录提交到spark.ods_student_submit_job */
load data local inpath '/tmp/submit.txt' into table spark.ods_student_submit_job;


/*更新dwd_student_submit_job_record_d�?*/
insert overwrite table spark.dwd_student_submit_job_record_d 
select 
userid,
code_name,
data_name,
int(memory),
int(core_num),
int(instance),
if(status='1',"Success","Failue"),
start_time,
finish_time,
bigint(finish_time)-bigint(start_time),
cast(from_unixtime(int(finish_time),'yyyy-MM-dd') as string),
cast(from_unixtime(int(finish_time),'yyyy-MM') as string)
from spark.ods_student_submit_job;


/*更新学生每日汇总表spark.dws_student_day_summary_d*/
drop table if exists spark.dws_student_day_summary_d;
create table if not exists spark.dws_student_day_summary_d as
with a as(
select
userid,
count(job.userid) submit_number,
count(distinct code_name) code_number,
count(if(status="Success",job.code_name,null)) job_success_number,
count(distinct if(status="Success",job.code_name,null)) job_success_distinct_number,
count(if(status="Failue",job.code_name,null)) job_faile_number,
count(distinct if(status="Failue",job.code_name,null)) job_faile_distinct_number,
sum(run_time) run_time,
min(run_time) min_run_time,
max(run_time) max_run_time,
max(days) `day` 
from spark.dwd_student_submit_job_record_d job
where days = date_sub(current_date,1)
group by job.userid
)
select
a.userid,
student_name,
class_id,
academy,
profession,
submit_number,
code_number,
job_success_number,
job_success_distinct_number,
job_faile_number,
job_faile_distinct_number,
run_time,
min_run_time,
max_run_time,
`day` 
from a
join spark.dim_student_info s on a.userid=s.userid;

/*更新学生累积汇总表spark.dws_student_day_summary_d*/
drop table if exists spark.dws_student_summary_f;
create table if not exists spark.dws_student_summary_f as
with a as(
select
userid,
count(job.userid) submit_number,
count(distinct code_name) code_number,
count(if(status="Success",job.userid,null)) job_success_number,
count(distinct if(status="Success",job.code_name,null)) job_success_distinct_number,
count(if(status="Failue",job.userid,null)) job_faile_number,
count(distinct if(status="Failue",job.code_name,null)) job_faile_distinct_number,
sum(run_time) run_time,
min(run_time) min_run_time,
max(run_time) max_run_time,
max(days) `day` 
from spark.dwd_student_submit_job_record_d job
group by job.userid
)
select
a.userid,
student_name,
class_id,
academy,
profession,
submit_number,
code_number,
job_success_number,
job_success_distinct_number,
job_faile_number,
job_faile_distinct_number,
run_time,
min_run_time,
max_run_time,
`day` 
from a
join spark.dim_student_info s on a.userid=s.userid;


/*学生每日提交次数排序*/
drop table if exists spark.ads_student_sort_day;
create table if not exists spark.ads_student_sort_day as
select 
userid,
student_name,
class_id,
profession,
submit_number,
code_number,
job_success_distinct_number,
job_faile_distinct_number
from spark.dws_student_day_summary_d
order by job_success_number desc, job_faile_distinct_number;


/*��ҳ-�ۻ��ɹ���ʧ�ܴ�����ߵ�ѧ����Ϣ*/
drop table if exists spark.ads_student_sort_total;
create table if not exists spark.ads_student_sort_total as
select 
userid,
student_name,
class_id,
profession,
submit_number,
code_number,
job_success_distinct_number,
job_faile_distinct_number
from spark.dws_student_summary_f
order by job_success_number desc, job_faile_distinct_number;


/*��ҳ-�ɹ���ʧ�ܴ���*/
drop table if exists spark.ads_sucess_faile_num;
create table if not exists spark.ads_sucess_faile_num as
select if(count(job_success_number)=0, 0, sum(job_success_number)) success,
if(count(job_faile_number)=0,0,sum(job_faile_number)) faile 
from spark.dws_student_day_summary_d;


/*��ҳ-pv��uv����*/
drop table if exists spark.ads_pv_uv_num;
create table if not exists spark.ads_pv_uv_num as
select days, count(userid) pv, count(distinct userid) uv
from spark.dwd_student_submit_job_record_d
where days>cast(add_months(current_date, -1) as string)
group by days
order by days;


/*��ҳ-�༶�ɹ���ʧ�ܴ���*/
drop table if exists spark.ads_class_sucess_faile_num;
create table if not exists spark.ads_class_sucess_faile_num as
select class_id,
count(if(job_success_number>0,userid,null)) success_user,
count(if(job_success_number=0,userid,null)) fail_user
from spark.dws_student_day_summary_d
where class_id in ('117010801', '117010802')
group by class_id
order by class_id;


/*��ҳ-����ɹ���ʧ�ܴ���*/
drop table if exists spark.ads_code_sucess_faile_num;
create table if not exists spark.ads_code_sucess_faile_num as
select code_name,
count(if(status='Success',code_name,null)) success_num,
count(if(status='Failue',code_name,null)) faile_num
from spark.dwd_student_submit_job_record_d
where days=cast(date_sub(current_date, 1) as string)
group by code_name;


/*ͼ1�ͱ�1����ϸ��Ϣ*/
drop table if exists spark.ads_student_sort_chart1_table1;
create table if not exists spark.ads_student_sort_chart1_table1 as
select 
*,
dense_rank() over(order by job_success_distinct_number desc, job_faile_distinct_number) r
from spark.dws_student_day_summary_d;


/*ͼ3����ϸ��Ϣ*/
drop table if exists spark.ads_code_chart3;
create table if not exists spark.ads_code_chart3 as
select code_name,d.userid,student_name,class_id,academy,profession,data_name,memory,core_num,instance,status,start_time,finish_time,run_time,days
from spark.dwd_student_submit_job_record_d d
join spark.dim_student_info s on d.userid=s.userid
where days=cast(date_sub(current_date, 1) as string)
order by code_name, userid;

/*��2����ϸ��Ϣ*/
drop table if exists spark.ads_student_sort_table2;
create table if not exists spark.ads_student_sort_table2 as
select 
*,
dense_rank() over(order by job_success_distinct_number desc, job_faile_distinct_number) r
from spark.dws_student_summary_f
order by r;
