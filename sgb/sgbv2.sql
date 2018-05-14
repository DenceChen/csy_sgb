
--订单outcall分配时间  7782504
use tmp_htlbidb;
drop table if exists tmp_sgb_processtime;
create table tmp_sgb_processtime as
select 
	a.orderid, a.operatetime as processtime, a.datachange_lasttime
from
(
  	select 
		*, 
		row_number() over(partition by orderid order by operatetime) as rk
	from ods_htl_orderdb.ord_processlog
  	where d >= '2017-12-01' and upper(reason) like '%OUTCALL分配%'
)a
where a.rk = 1
==========================================================================================================

--为了支持spark2，从olap_htlmaindb.o_htl_007取出需要的数据，这样下面所有的耗时操作都可以在spark2中跑了 67325740
set hive.support.quoted.identifiers=None;
use tmp_htlbidb;
drop table if exists tmp_sgb_ordinfo;
create table tmp_sgb_ordinfo as
select 
	`(hotelcountry|ipid|commentdate|isdirectorder|businessgroup)?+.+`
from olap_htlmaindb.o_htl_007 
where confirmdate2 >= '2017-12-01'--只取2018年的数据

==========================================================================================================

--订单确认客户时间  这样处理之后该操作支持spark2了  67325740
use tmp_htlbidb;
drop table if exists tmp_sgb_confirmtime;
create table tmp_sgb_confirmtime as
select
	d.orderid, d.hotel, d.confirmdate2 as confirmdate, d.canceltime, d.ord_amount, d.arrival,
	d.isholdroom, d.freesale, d.submitfrom, d.ord_roomstatus, d.orderdate, d.ordertype, d.iscorp,
	(case when datediff(d.arrival, d.orderdate) = 0 then 1 else 0 end) as istodayord, d.isdelay,
	--统计入住日与订单日相差的时间（小时）
	(unix_timestamp(d.arrival)-unix_timestamp(d.orderdate))/60 as arr2ord_time,
	d.cityid, d.isonline,
	d.actualmasterhotelid, --母酒店ID 
	d.masterstar,          --母酒店星级
	d.star,                --政府给的评级
	d.goldstar             --携程给的评级
from
(
	select
		c.*,
		row_number() over(partition by c.orderid order by c.confirmdate2) as rk
	from
	(
		--取出订单对应子酒店相关信息
		select
			a.*, b.actualmasterhotelid, b.masterstar, b.star, b.goldstar
		from
		tmp_htlbidb.tmp_sgb_ordinfo as a
		left join
		(select * from dim_hoteldb.dimhotel where d = '${zdt.format("yyyy-MM-dd")}')b
		on a.hotel = b.hotel
	)c
)d
where d.rk = 1

==========================================================================================================

--订单对上供应商ID  supplierid 67325740
use tmp_htlbidb;
drop table if exists tmp_sgb_confirmtime_suppid;
create table tmp_sgb_confirmtime_suppid as
select 
	a.*, b.supplierid
from
tmp_htlbidb.tmp_sgb_confirmtime as a
left join
(select * from ods_htl_hotelpubdb.supplierhotel where d = '${zdt.format("yyyy-MM-dd")}') as b
on a.hotel = b.hotelid

==========================================================================================================
select count(*) from tmp_htlbidb.tmp_sgb_duration where duration<0
--订单确认客户时间差值 6064598  2133936小于零
use tmp_htlbidb;
drop table if exists tmp_sgb_duration;
create table tmp_sgb_duration as
select 
	b.*, a.processtime,
	hour(a.processtime) as process_hour, hour(b.orderdate) as ord_hour,
	(unix_timestamp(b.confirmdate)-unix_timestamp(a.processtime))/60 as duration, -- 回复时间间隔(min)
	(unix_timestamp(a.processtime)-unix_timestamp(b.orderdate))/60/60 as process2ord_time, -- outcall分配时间到、
	a.datachange_lasttime
from 
tmp_htlbidb.tmp_sgb_processtime as a
join
tmp_htlbidb.tmp_sgb_confirmtime_suppid as b
on a.orderid = b.orderid

==========================================================================================================

--过滤确认日期大于入住日期的数据 confirmdate > arrivaldate 5778316
use tmp_htlbidb;
drop table if exists tmp_sgb_duration_true;
create table tmp_sgb_duration_true as
select *
from tmp_htlbidb.tmp_sgb_duration
--where to_date(arrival) >= to_date(confirmdate)
where arrival >= confirmdate 



--过滤时间差为正数的样本标签以及时间间隔大于3天的（4500分钟左右） 3650004
use tmp_htlbidb;
drop table if exists tmp_sgb_duration_positive;
create table tmp_sgb_duration_positive as
select *
from tmp_htlbidb.tmp_sgb_duration_true
where duration >= 0 and duration < 4500

==========================================================================================================

--提取历史数据特征(过去一个月，过去一个星期)  559437304(5亿)  子酒店
--不过滤时间
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_full;
create table tmp_sgb_htl_full as
select
    a.hotel, 
    a.processtime as processtime_1, a.orderdate as orderdate_1,
    b.processtime as processtime_2, b.orderdate as orderdate_2,
    b.process_hour, b.ord_hour, b.duration
from
(select hotel, processtime, orderdate from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select hotel, processtime, orderdate, process_hour, ord_hour, duration from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.hotel = b.hotel

==================================================================================================================
--=========================================计算一个星期===========================================================
==================================================================================================================

--一个星期 basetime = processtime  36059580, 子酒店 
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_process_full_7;
create table tmp_sgb_htl_process_full_7 as
select * from tmp_htlbidb.tmp_sgb_htl_full
--where processtime_2 >= date_sub(processtime_1, 7) and processtime_2 < to_date(processtime_1) --精确到天，提高准确性是不是应该精确到小时
-- from_unixtime(unix_timestamp("2018-03-21 15:11:14.10"),'yyyy-MM-dd HH:00:00')  2018-03-21 15:00:00
where processtime_2 >= from_unixtime(unix_timestamp(processtime_1) - 7*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and processtime_2 < from_unixtime(unix_timestamp(processtime_1),'yyyy-MM-dd HH:00:00')

==================================================================================================================

--过去一个星期子酒店分小时订单回复统计(processtime)  1057396
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_process_hour_7;
create table tmp_sgb_htl_process_hour_7 as
select 
    hotel, processtime_1 as processtime,
    avg(duration) as duration_htl_process_hour_7_pre,                       --过去一个星期酒店分小时订单回复时长平均值
    stddev(duration) as duration_htl_process_hour_7_stddev,                 --过去一个星期酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_process_hour_7_per75, --过去一个星期酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_process_hour_7_per50, --过去一个星期酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_process_hour_7_per25  --过去一个星期酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_process_full_7 where process_hour = hour(processtime_1)
group by hotel, processtime_1

==================================================================================================================

--过去一个星期子酒店订单回复时长统计  2650866  
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_process_week_7;
create table tmp_sgb_htl_process_week_7 as
select
    hotel, processtime_1 as processtime,
    avg(duration) as duration_htl_process_week_7_pre,                       --过去一个星期酒店订单回复时长平均值
    stddev(duration) as duration_htl_process_week_7_stddev,                 --过去一个星期酒店订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_process_week_7_per75, --过去一个星期酒店订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_process_week_7_per50, --过去一个星期酒店订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_process_week_7_per25  --过去一个星期酒店订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_process_full_7
group by hotel, processtime_1

==================================================================================================================
--================================================分割线==========================================================
==================================================================================================================

--一个星期 basetime = ordertime  36167886  子酒店 
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_ord_full_7;
create table tmp_sgb_htl_ord_full_7 as
select * from tmp_htlbidb.tmp_sgb_htl_full
--where orderdate_2 >= date_sub(orderdate_1, 7) and orderdate_2 < to_date(orderdate_1)
where orderdate_2 >= from_unixtime(unix_timestamp(orderdate_1) - 7*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and orderdate_2 < from_unixtime(unix_timestamp(orderdate_1),'yyyy-MM-dd HH:00:00')

==================================================================================================================

--过去一个月子酒店分小时订单回复统计(ordertime) 1068117
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_ord_hour_7;
create table tmp_sgb_htl_ord_hour_7 as
select 
    hotel, orderdate_1 as orderdate,
    avg(duration) as duration_htl_ord_hour_7_pre,                       	--过去一个星期子酒店分小时订单回复时长平均值
    stddev(duration) as duration_htl_ord_hour_7_stddev,                 	--过去一个星期子酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_ord_hour_7_per75, 	--过去一个星期子酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_ord_hour_7_per50, 	--过去一个星期子酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_ord_hour_7_per25  	--过去一个星期子酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_ord_full_7 where ord_hour = hour(orderdate_1)
group by hotel, orderdate_1

==================================================================================================================

--过去一个星期子酒店订单回复时长统计  2787054
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_ord_week_7;
create table tmp_sgb_htl_ord_week_7 as
select
    hotel, orderdate_1 as orderdate,
    avg(duration) as duration_htl_ord_week_7_pre,                       	--过去一个星期子酒店订单回复时长平均值
    stddev(duration) as duration_htl_ord_week_7_stddev,                 	--过去一个星期子酒店订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_ord_week_7_per75, 	--过去一个星期子酒店订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_ord_week_7_per50, 	--过去一个星期子酒店订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_ord_week_7_per25  	--过去一个星期子酒店订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_ord_full_7
group by hotel, orderdate_1


==================================================================================================================
--=========================================计算一个月=============================================================
==================================================================================================================

--一个月 basetime = processtime  121832990 子酒店 
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_process_full_30;
create table tmp_sgb_htl_process_full_30 as
select * from tmp_htlbidb.tmp_sgb_htl_full
--where processtime_2 >= date_sub(processtime_1, 30) and processtime_2 < to_date(processtime_1)
where processtime_2 >= from_unixtime(unix_timestamp(processtime_1) - 30*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and processtime_2 < from_unixtime(unix_timestamp(processtime_1),'yyyy-MM-dd HH:00:00')

==============================================================================================================

--过去一个月子酒店分小时订单回复统计(processtime) 1742841
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_process_hour_30;
create table tmp_sgb_htl_process_hour_30 as
select 
    hotel, processtime_1 as processtime,
    avg(duration) as duration_htl_process_hour_30_pre,                       --过去一个月子酒店分小时订单回复时长平均值
    stddev(duration) as duration_htl_process_hour_30_stddev,                 --过去一个月子酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_process_hour_30_per75, --过去一个月子酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_process_hour_30_per50, --过去一个月子酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_process_hour_30_per25  --过去一个月子酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_process_full_30 where process_hour = hour(processtime_1)
group by hotel, processtime_1

==============================================================================================================

--过去一个月子酒店订单回复时长统计 3027399
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_process_month_30;
create table tmp_sgb_htl_process_month_30 as
select
    hotel, processtime_1 as processtime,
    avg(duration) as duration_htl_process_month_30_pre,                       		--过去一个月子酒店订单回复时长平均值
    stddev(duration) as duration_htl_process_month_30_stddev,                 		--过去一个月子酒店订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_process_month_30_per75, 		--过去一个月子酒店订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_process_month_30_per50, 		--过去一个月子酒店订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_process_month_30_per25  		--过去一个月子酒店订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_process_full_30
group by hotel, processtime_1

==============================================================================================================
--=========================================分割线=============================================================
==============================================================================================================

--一个月 basetime = ordertime   121950619  子酒店 
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_ord_full_30;  
create table tmp_sgb_htl_ord_full_30 as
select * from tmp_htlbidb.tmp_sgb_htl_full
--where orderdate_2 >= date_sub(orderdate_1, 30) and orderdate_2 < to_date(orderdate_1)
where orderdate_2 >= from_unixtime(unix_timestamp(orderdate_1) - 30*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and orderdate_2 < from_unixtime(unix_timestamp(orderdate_1),'yyyy-MM-dd HH:00:00')

==============================================================================================================

--过去一个月子酒店分小时订单回复统计(ordertime)  1776440
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_ord_hour_30;
create table tmp_sgb_htl_ord_hour_30 as
select 
    hotel, orderdate_1 as orderdate,
    avg(duration) as duration_htl_ord_hour_30_pre,                       	--过去一个月子酒店分小时订单回复时长平均值
    stddev(duration) as duration_htl_ord_hour_30_stddev,                 	--过去一个月子酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_ord_hour_30_per75, 	--过去一个月子酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_ord_hour_30_per50, 	--过去一个月子酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_ord_hour_30_per25  	--过去一个月子酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_ord_full_30 where ord_hour = hour(orderdate_1)
group by hotel, orderdate_1

=============================================================================================================

--过去一个月子酒店订单回复时长统计  3169672
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_ord_month_30;
create table tmp_sgb_htl_ord_month_30 as
select
    hotel, orderdate_1 as orderdate,
    avg(duration) as duration_htl_ord_month_30_pre,                       		--过去一个月子酒店订单回复时长平均值
    stddev(duration) as duration_htl_ord_month_30_stddev,                 		--过去一个月子酒店订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_htl_ord_month_30_per75, 		--过去一个月子酒店订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_htl_ord_month_30_per50, 		--过去一个月子酒店订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_htl_ord_month_30_per25  		--过去一个月子酒店订单回复时长25分位
from tmp_htlbidb.tmp_sgb_htl_ord_full_30
group by hotel, orderdate_1

==============================================================================================================
--============================================================================================================
==============================================================================================================

--拼接多张特征表  3650004
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_f7;
create table tmp_sgb_htl_f7 as
select 
	g.*, h.duration_htl_ord_hour_7_pre,
	h.duration_htl_ord_hour_7_stddev, h.duration_htl_ord_hour_7_per75,
	h.duration_htl_ord_hour_7_per50, h.duration_htl_ord_hour_7_per25
from
(
	select 
		e.*, f.duration_htl_ord_week_7_pre,
		f.duration_htl_ord_week_7_stddev, f.duration_htl_ord_week_7_per75,
		f.duration_htl_ord_week_7_per50, f.duration_htl_ord_week_7_per25
	from
	(
		select 
			c.*, d.duration_htl_process_hour_7_pre,
			d.duration_htl_process_hour_7_stddev, d.duration_htl_process_hour_7_per75,
			d.duration_htl_process_hour_7_per50, d.duration_htl_process_hour_7_per25
		from
		(
			select 
				a.*, b.duration_htl_process_week_7_pre,
				b.duration_htl_process_week_7_stddev, b.duration_htl_process_week_7_per75,
				b.duration_htl_process_week_7_per50, b.duration_htl_process_week_7_per25
			from
				tmp_htlbidb.tmp_sgb_duration_positive as a
			left join
				tmp_htlbidb.tmp_sgb_htl_process_week_7 as b
			on a.hotel = b.hotel and a.processtime = b.processtime
		)c
		left join
		tmp_htlbidb.tmp_sgb_htl_process_hour_7 as d
		on c.hotel = d.hotel and c.processtime = d.processtime
	)e
	left join
	tmp_htlbidb.tmp_sgb_htl_ord_week_7 as f
	on e.hotel = f.hotel and e.orderdate = f.orderdate
)g
left join
tmp_htlbidb.tmp_sgb_htl_ord_hour_7 as h
on g.hotel = h.hotel and g.orderdate = h.orderdate

--拼接多张特征表  3650004
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_f30;
create table tmp_sgb_htl_f30 as
select 
	g.*, h.duration_htl_ord_hour_30_pre,
	h.duration_htl_ord_hour_30_stddev, h.duration_htl_ord_hour_30_per75,
	h.duration_htl_ord_hour_30_per50, h.duration_htl_ord_hour_30_per25
from
(
	select 
		e.*, f.duration_htl_ord_month_30_pre,
		f.duration_htl_ord_month_30_stddev, f.duration_htl_ord_month_30_per75,
		f.duration_htl_ord_month_30_per50, f.duration_htl_ord_month_30_per25
	from
	(
		select 
			c.*, d.duration_htl_process_hour_30_pre,
			d.duration_htl_process_hour_30_stddev, d.duration_htl_process_hour_30_per75,
			d.duration_htl_process_hour_30_per50, d.duration_htl_process_hour_30_per25
		from
		(
			select 
				a.*, b.duration_htl_process_month_30_pre,
				b.duration_htl_process_month_30_stddev, b.duration_htl_process_month_30_per75,
				b.duration_htl_process_month_30_per50, b.duration_htl_process_month_30_per25
			from
				tmp_htlbidb.tmp_sgb_duration_positive as a
			left join
				tmp_htlbidb.tmp_sgb_htl_process_month_30 as b
			on a.hotel = b.hotel and a.processtime = b.processtime
		)c
		left join
		tmp_htlbidb.tmp_sgb_htl_process_hour_30 as d
		on c.hotel = d.hotel and c.processtime = d.processtime
	)e
	left join
	tmp_htlbidb.tmp_sgb_htl_ord_month_30 as f
	on e.hotel = f.hotel and e.orderdate = f.orderdate
)g
left join
tmp_htlbidb.tmp_sgb_htl_ord_hour_30 as h
on g.hotel = h.hotel and g.orderdate = h.orderdate


--set hive.support.quoted.identifiers=None;
--select `(orderid|name)?+.+` from tableName; --输出排除orderid与name以外的所有字段
--tmp_htlbidb.tmp_sgb_htl_fall 
--hive -e 'select * from tmp_htlbidb.tmp_sgb_htl_fall where processtime >= '2018-02-01' and processtime < '2018-04-01'' > tmp_sgb_htl_fall4.csv
-- 3650004
use tmp_htlbidb;
drop table if exists tmp_sgb_htl_fall;
create table tmp_sgb_htl_fall as
select 
	a.*,
	b.duration_htl_ord_hour_30_pre,
	b.duration_htl_ord_hour_30_stddev, b.duration_htl_ord_hour_30_per75,
	b.duration_htl_ord_hour_30_per50, b.duration_htl_ord_hour_30_per25,
	b.duration_htl_ord_month_30_pre,
	b.duration_htl_ord_month_30_stddev, b.duration_htl_ord_month_30_per75,
	b.duration_htl_ord_month_30_per50, b.duration_htl_ord_month_30_per25,
	b.duration_htl_process_hour_30_pre,
	b.duration_htl_process_hour_30_stddev, b.duration_htl_process_hour_30_per75,
	b.duration_htl_process_hour_30_per50, b.duration_htl_process_hour_30_per25,
	b.duration_htl_process_month_30_pre,
	b.duration_htl_process_month_30_stddev, b.duration_htl_process_month_30_per75,
	b.duration_htl_process_month_30_per50, b.duration_htl_process_month_30_per25
from 
tmp_htlbidb.tmp_sgb_htl_f7 as a
left join
tmp_htlbidb.tmp_sgb_htl_f30 as b
on a.orderid = b.orderid


=============================================================================================================
=============================================================================================================
=============================================================================================================

set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapreduce.map.memory.mb=20000;
set mapreduce.reduce.memory.mb=20000;
set mapreduce.map.java.opts=-Xmx20000m -Xmx20000m;
set mapreduce.reduce.java.opts=-Xmx20000m -Xmx20000m;
set mapreduce.task.io.sort.mb=512;
set mapreduce.task.io.sort.factor=100; 
set mapred.reduce.tasks = 2000;
set hive.exec.reducers.bytes.per.reducer = 10000;
--房态表中取数
use tmp_htlbidb;
drop table if exists tmp_sgb_feature;
create table tmp_sgb_feature as
select f.*
from
(
	select
		e.*,
		row_number() over(partition by e.roomid, e.effectdate order by e.dur_time) as rk
	from 
	(
	  	select 
	        c.*, d.processtime, d.orderid,
	        (
	            case when c.createtime <= d.processtime 
	            then (unix_timestamp(d.processtime) - unix_timestamp(c.createtime))
	            else (unix_timestamp(c.createtime) - unix_timestamp(d.processtime)) + 1000000
	            end
	        )as dur_time
	    from
	    (select * from dw_htlapidb.htl_model_roomstatus where d >= '2018-01-01')c
	    join
	    (
	        select
          		a.orderid, a.room, a.arrival, b.processtime as processtime
	        from
	        (select orderid, room, arrival from olap_htlmaindb.o_htl_007 where orderdate >= '2017-12-01')a
	        join
	        (select * from tmp_htlbidb.tmp_sgb_processtime where processtime >= '2017-12-01')b
	        on a.orderid = b.orderid
	    )d
	    on c.roomid = d.room and from_unixtime(unix_timestamp(c.effectdate,'yyyyMMdd'), 'yyyy-MM-dd') = to_date(d.arrival)
	)e
)f
WHERE rk = 1
