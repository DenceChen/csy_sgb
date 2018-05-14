select orderid, count(*) as nums 
from olap_htlmaindb.o_htl_019 
where calltype='O' and d = '${zdt.format("yyyy-MM-dd")}' 
group by orderid
having count(*) > 5


select * 
from olap_htlmaindb.o_htl_019
where calltype='O' and d = '${zdt.format("yyyy-MM-dd")}' and orderid = '1439635880'

==========================================================================================================
--订单outcall分配时间
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
--订单确认客户时间
use tmp_htlbidb;
drop table if exists tmp_sgb_confirmtime;
create table tmp_sgb_confirmtime as
select
	d.orderid, d.hotel, d.confirmdate2 as confirmdate, d.canceltime, d.ord_amount, d.arrival,
	d.isholdroom, d.freesale, d.submitfrom, d.ord_roomstatus, d.orderdate, d.ordertype, d.iscorp,
	(case when datediff(d.arrival, d.orderdate) = 0 then 1 else 0 end) as istodayord, d.isdelay,
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
		(select * from olap_htlmaindb.o_htl_007 where confirmdate2 >= '2017-12-01')a --只取2018年的数据
		left join
		(select * from dim_hoteldb.dimhotel where d = '${zdt.format("yyyy-MM-dd")}')b
		on a.hotel = b.hotel
	)c
)d
where d.rk = 1

==========================================================================================================
--订单对上供应商ID  supplierid
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

--订单确认客户时间差值
use tmp_htlbidb;
drop table if exists tmp_sgb_duration;
create table tmp_sgb_duration as
select 
	b.*, a.processtime,
	hour(a.processtime) as process_hour, hour(b.orderdate) as ord_hour,
	(unix_timestamp(b.confirmdate)-unix_timestamp(a.processtime))/60 as duration, -- 回复时间间隔(min)
	a.datachange_lasttime
from 
tmp_htlbidb.tmp_sgb_processtime as a
join
tmp_htlbidb.tmp_sgb_confirmtime_suppid as b
on a.orderid = b.orderid

==========================================================================================================

--过滤确认日期大于入住日期的数据
use tmp_htlbidb;
drop table if exists tmp_sgb_duration_true;
create table tmp_sgb_duration_true as
select *
from tmp_htlbidb.tmp_sgb_duration
where to_date(arrival) > to_date(confirmdate)



--过滤时间差为正数的样本标签以及时间间隔大于7天的（10000分钟左右）
use tmp_htlbidb;
drop table if exists tmp_sgb_duration_positive;
create table tmp_sgb_duration_positive as
select *
from tmp_htlbidb.tmp_sgb_duration_true
where duration >= 0 and duration < 10000

==========================================================================================================

--判断，统计
select count(*) from tmp_htlbidb.tmp_sgb_duration --5869115
select count(*) from tmp_htlbidb.tmp_sgb_duration where duration <= 0 --2066547
select * from tmp_htlbidb.tmp_sgb_duration where duration <= 0

==========================================================================================================

--提取历史数据特征(过去一个月，过去一个星期)
--tmp_htlbidb.tmp_sgb_duration_positive
--不过滤时间
use tmp_htlbidb;
drop table if exists tmp_sgb_full;
create table tmp_sgb_full as
select
    a.hotel, a.processtime as processtime_1, a.orderdate as orderdate_1,
    b.duration, b.processtime as processtime_2, b.orderdate as orderdate_1,
    b.process_hour, b.ord_hour
from
(select hotel, processtime, orderdate from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select hotel, processtime, orderdate, process_hour, ord_hour, duration from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.hotel = b.hotel

==========================================================================================================

--一个星期
use tmp_htlbidb;
drop table if exists tmp_sgb_full_7;
create table tmp_sgb_full_7 as
select
	a.hotel, a.processtime as processtime_1, 
	b.duration, b.processtime as processtime_2, 
	b.process_hour, b.ord_hour
from
(select hotel, processtime from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select hotel, processtime, duration, orderdate, process_hour, ord_hour from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.hotel = b.hotel
where b.processtime >= date_sub(a.processtime, 7) and b.processtime < to_date(a.processtime)

==================================================================================================================

--过去一个月子酒店分小时订单回复统计(processtime)
use tmp_htlbidb;
drop table if exists tmp_sgb_hour_process_7;
create table tmp_sgb_hour_process_7 as
select 
    hotel, process_hour,
    avg(duration) as duration_hour_process_7_pre,                       --过去一个星期酒店分小时订单回复时长平均值
    stddev(duration) as duration_hour_process_7_stddev,                 --过去一个星期酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_hour_process_7_per75, --过去一个星期酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_hour_process_7_per50, --过去一个星期酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_hour_process_7_per25  --过去一个星期酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_full_7
group by hotel, process_hour

==================================================================================================================

--过去一个月子酒店分小时订单回复统计(processtime)
use tmp_htlbidb;
drop table if exists tmp_sgb_hour_ord_7;
create table tmp_sgb_hour_ord_7 as
select 
    hotel, ord_hour,
    avg(duration) as duration_hour_ord_7_pre,                       	--过去一个星期酒店分小时订单回复时长平均值
    stddev(duration) as duration_hour_ord_7_stddev,                 	--过去一个星期酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_hour_ord_7_per75, 	--过去一个星期酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_hour_ord_7_per50, 	--过去一个星期酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_hour_ord_7_per25  	--过去一个星期酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_full_7
group by hotel, ord_hour


==================================================================================================================

--过去一个月子酒店订单回复时长统计
use tmp_htlbidb;
drop table if exists tmp_sgb_month_7;
create table tmp_sgb_month_7 as
select
    hotel, processtime_1 as processtime,
    avg(duration) as duration_month_7_pre,                       		--过去一个星期酒店订单回复时长平均值
    stddev(duration) as duration_month_7_stddev,                 		--过去一个星期酒店订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_month_7_per75, 		--过去一个星期酒店订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_month_7_per50, 		--过去一个星期酒店订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_month_7_per25  		--过去一个星期酒店订单回复时长25分位
from tmp_htlbidb.tmp_sgb_full_7
group by hotel, processtime_1

==================================================================================================================
==================================================================================================================

--一个月
use tmp_htlbidb;
drop table if exists tmp_sgb_full_30;
create table tmp_sgb_full_30 as
select
    a.hotel, a.processtime as processtime_1, 
    b.duration, b.processtime as processtime_2, 
    b.process_hour, b.ord_hour
from
(select hotel, processtime from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select hotel, processtime, duration, orderdate, process_hour, ord_hour from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.hotel = b.hotel
where b.processtime >= date_sub(a.processtime, 30) and b.processtime < to_date(a.processtime)

==========================================================================================================

--过去一个月子酒店分小时订单回复统计(processtime)
use tmp_htlbidb;
drop table if exists tmp_sgb_hour_process_30;
create table tmp_sgb_hour_process_30 as
select 
    hotel, process_hour,
    avg(duration) as duration_hour_process_30_pre,                       --过去一个月子酒店分小时订单回复时长平均值
    stddev(duration) as duration_hour_process_30_stddev,                 --过去一个月子酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_hour_process_30_per75, --过去一个月子酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_hour_process_30_per50, --过去一个月子酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_hour_process_30_per25  --过去一个月子酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_full_30
group by hotel, process_hour

==========================================================================================================

--过去一个月子酒店分小时订单回复统计(ordertime)
use tmp_htlbidb;
drop table if exists tmp_sgb_hour_ord_30;
create table tmp_sgb_hour_ord_30 as
select 
    hotel, ord_hour,
    avg(duration) as duration_hour_ord_30_pre,                       	--过去一个月子酒店分小时订单回复时长平均值
    stddev(duration) as duration_hour_ord_30_stddev,                 	--过去一个月子酒店分小时订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_hour_ord_30_per75, 	--过去一个月子酒店分小时订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_hour_ord_30_per50, 	--过去一个月子酒店分小时订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_hour_ord_30_per25  	--过去一个月子酒店分小时订单回复时长25分位
from tmp_htlbidb.tmp_sgb_full_30
group by hotel, ord_hour

==========================================================================================================

--过去一个月子酒店订单回复时长统计
use tmp_htlbidb;
drop table if exists tmp_sgb_month_30;
create table tmp_sgb_month_30 as
select
    hotel, processtime_1 as processtime,
    avg(duration) as duration_month_30_pre,                       		--过去一个月子酒店订单回复时长平均值
    stddev(duration) as duration_month_30_stddev,                 		--过去一个月子酒店订单回复时长标准差
    percentile_approx(duration, 0.75) as duration_month_30_per75, 		--过去一个月子酒店订单回复时长75分位
    percentile_approx(duration, 0.50) as duration_month_30_per50, 		--过去一个月子酒店订单回复时长中位数
    percentile_approx(duration, 0.25) as duration_month_30_per25  		--过去一个月子酒店订单回复时长25分位
from tmp_htlbidb.tmp_sgb_full_30
group by hotel, processtime_1

=============================================================================================================

--拼接多张特征表
--tmp_htlbidb.tmp_sgb_hour_process_7
--tmp_htlbidb.tmp_sgb_hour_ord_7
--tmp_htlbidb.tmp_sgb_month_7
use tmp_htlbidb;
drop table if exists tmp_sgb_f7;
create table tmp_sgb_f7 as
select 
	e.*, f.duration_hour_process_7_pre,
	f.duration_hour_process_7_stddev, f.duration_hour_process_7_per75,
	f.duration_hour_process_7_per50, f.duration_hour_process_7_per25
from
(
	select 
		c.*, d.duration_hour_ord_7_pre,
		d.duration_hour_ord_7_stddev, d.duration_hour_ord_7_per75,
		d.duration_hour_ord_7_per50, d.duration_hour_ord_7_per25
	from
	(
		select 
			a.*, b.duration_month_7_pre,
			b.duration_month_7_stddev, b.duration_month_7_per75,
			b.duration_month_7_per50, b.duration_month_7_per25
		from
			tmp_htlbidb.tmp_sgb_duration_positive as a
		join
			tmp_htlbidb.tmp_sgb_month_7 as b
		on a.hotel = b.hotel and a.processtime = b.processtime
	)c
	join
	tmp_htlbidb.tmp_sgb_hour_ord_7 as d
	on c.hotel = d.hotel and c.ord_hour = d.ord_hour
)e
join
tmp_htlbidb.tmp_sgb_hour_process_7 as f
on e.hotel = f.hotel and e.process_hour = f.process_hour

--拼接多张特征表
--tmp_htlbidb.tmp_sgb_hour_process_30
--tmp_htlbidb.tmp_sgb_hour_ord_30
--tmp_htlbidb.tmp_sgb_month_30  
use tmp_htlbidb;
drop table if exists tmp_sgb_f30;
create table tmp_sgb_f30 as
select 
	e.*, f.duration_hour_process_30_pre,
	f.duration_hour_process_30_stddev, f.duration_hour_process_30_per75,
	f.duration_hour_process_30_per50, f.duration_hour_process_30_per25
from
(
	select 
		c.*, d.duration_hour_ord_30_pre,
		d.duration_hour_ord_30_stddev, d.duration_hour_ord_30_per75,
		d.duration_hour_ord_30_per50, d.duration_hour_ord_30_per25
	from
	(
		select 
			a.*, b.duration_month_30_pre,
			b.duration_month_30_stddev, b.duration_month_30_per75,
			b.duration_month_30_per50, b.duration_month_30_per25
		from
			tmp_htlbidb.tmp_sgb_duration_positive as a
		join
			tmp_htlbidb.tmp_sgb_month_30 as b
		on a.hotel = b.hotel and a.processtime = b.processtime
	)c
	join
	tmp_htlbidb.tmp_sgb_hour_ord_30 as d
	on c.hotel = d.hotel and c.ord_hour = d.ord_hour
)e
join
tmp_htlbidb.tmp_sgb_hour_process_30 as f
on e.hotel = f.hotel and e.process_hour = f.process_hour


--tmp_htlbidb.tmp_sgb_f30
--tmp_htlbidb.tmp_sgb_f7
--select `(orderid|name)?+.+` from tableName; --输出排除orderid与name以外的所有字段
set hive.support.quoted.identifiers=None;
use tmp_htlbidb;
drop table if exists tmp_sgb_fall;
create table tmp_sgb_fall as
select 
	--a.*, `(b.orderid)?+.+` --b除了orderid其他的都输出
	a.*,
	b.duration_hour_process_30_pre,
	b.duration_hour_process_30_stddev, b.duration_hour_process_30_per75,
	b.duration_hour_process_30_per50, b.duration_hour_process_30_per25,
	b.duration_hour_ord_30_pre,
	b.duration_hour_ord_30_stddev, b.duration_hour_ord_30_per75,
	b.duration_hour_ord_30_per50, b.duration_hour_ord_30_per25,
	b.duration_month_30_pre,
	b.duration_month_30_stddev, b.duration_month_30_per75,
	b.duration_month_30_per50, b.duration_month_30_per25	
from 
tmp_htlbidb.tmp_sgb_f7 as a
join
tmp_htlbidb.tmp_sgb_f30 as b
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
