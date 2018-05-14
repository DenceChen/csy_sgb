==========================================================================================================
--为了支持spark2，从olap_htlmaindb.o_htl_007取出需要的数据，这样下面所有的耗时操作都可以在spark2中跑了 67325740
set hive.support.quoted.identifiers=None;
use tmp_htlbidb;
drop table if exists tmp_csy_ordinfo;
create table tmp_csy_ordinfo as
select 
	`(hotelcountry|ipid|commentdate|isdirectorder|businessgroup)?+.+`
from olap_htlmaindb.o_htl_007 
where confirmdate2 >= '2017-12-01'--只取2018年的数据

==========================================================================================================

--取出海外订单    3225663
use tmp_htlbidb;
drop table if exists tmp_csy_oversea_ord;
create table tmp_csy_oversea_ord as
select 
    c.*, d.actualmasterhotelid, d.masterstar, d.star, d.goldstar
from
(
    select 
        a.orderid, a.hotel, a.cityid, b.cityname, b.country, b.countryname,
        a.confirmdate2 as confirmdate, a.orderdate, hour(a.orderdate) as ord_hour,
        a.arrival, hour(a.arrival) as arr_hour, 
        (case when datediff(a.arrival, a.orderdate) = 0 then 1 else 0 end) as istodayord,
        (unix_timestamp(a.arrival)-unix_timestamp(a.orderdate))/60 as arr2ord_time, --入住日到下单日的时间差（单位分钟）
        pmod(datediff(a.arrival, '1920-01-01') - 3, 7) as arr_week, -- 入住日是星期几
        pmod(datediff(a.orderdate, '1920-01-01') - 3, 7) as ord_week, -- 下单日是星期几
        a.freesale, a.submitfrom, a.ord_roomstatus, a.iscorp, a.isdelay, a.isholdroom, 
        a.ordertype, a.canceltime, a.ord_amount, a.isonline, a.recommendlevel, a.holdroomtype,
        a.cost, a.ave_price, a.cii_amount, a.cii_amount_valid, a.ord_persons,
        a.ismaskedorder, a.frompackageorder, a.ord_days, a.ord_roomnum, a.remarktype,
        from_unixtime(unix_timestamp(a.confirmdate2),'yyyy-MM-dd HH:mm:ss') as confirmdate_format, 
        from_unixtime(unix_timestamp(a.orderdate),'yyyy-MM-dd HH:mm:ss') as orderdate_format, 
        (unix_timestamp(a.confirmdate2)-unix_timestamp(a.orderdate))/60 as duration1 -- 确认客户时间间隔
    from
    tmp_htlbidb.tmp_csy_ordinfo as a  --订单主表（2017年以后），orderid主键没有重复的
    join
    (select * from dim_hoteldb.dimcity where country <> 1)b   --取出海外城市数据
    on a.cityid = b.cityid
)c
join
(select * from dim_hoteldb.dimhotel where d = '${zdt.format("yyyy-MM-dd")}')d
on c.hotel = d.hotel

==========================================================================================================

-- 取出收到供应商确认的数据 3179222
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ordercall;
create table tmp_csy_htl_ordercall as
--增加一个海外订单酒店回复时间的字段
select
    a.*, (unix_timestamp(b.operatetime)-unix_timestamp(a.orderdate))/60 as duration2, -- 酒店（供应商）回复时间间隔(min)
    b.operatetime                   -- 订单的真实回复时间
    
from
tmp_htlbidb.tmp_csy_oversea_ord as a
join
(
    -- 获取酒店回复时间(如果有两条回复记录的取最早的)
    select orderid, min(from_unixtime(unix_timestamp(operatetime),'yyyy-MM-dd HH:mm:ss')) as operatetime
    from ods_htl_orderdb.ord_operatetime
    where upper(trim(operatetype)) = 'P' and d = '${zdt.format("yyyy-MM-dd")}' -- P收到供应商确认
    group by orderid
)b
on a.orderid = b.orderid

==========================================================================================================

--订单对上供应商ID  supplierid 3179222
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ordercall_suppid;
create table tmp_csy_htl_ordercall_suppid as
select 
	a.*, b.supplierid
from
tmp_htlbidb.tmp_csy_htl_ordercall as a
left join
(select * from ods_htl_hotelpubdb.supplierhotel where d = '${zdt.format("yyyy-MM-dd")}') as b
on a.hotel = b.hotelid


==========================================================================================================

--过滤确认日期大于入住日期的数据 confirmdate > arrivaldate 3150893
use tmp_htlbidb;
drop table if exists tmp_csy_duration_true;
create table tmp_csy_duration_true as
select *
from tmp_htlbidb.tmp_csy_htl_ordercall_suppid
where arrival >= confirmdate -- 5728485

==========================================================================================================

--过滤时间差为负数的样本标签以及时间间隔大于7天的（10000分钟左右） 461817
use tmp_htlbidb;
drop table if exists tmp_csy_duration_positive;
create table tmp_csy_duration_positive as
select *
from tmp_htlbidb.tmp_csy_duration_true
where duration1 >= 3 and (duration2 >=0 and duration2 < 10000)


==========================================================================================================


-- 构建一张全交表,基于子酒店   52680033
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ordercall_full;
create table tmp_csy_htl_ordercall_full as
select
    a.hotel, a.orderdate as orderdate_1, 
    b.orderdate as orderdate_2, b.ord_hour, b.duration1, b.duration2
from
(select hotel, orderdate from tmp_htlbidb.tmp_csy_duration_positive)a
join
(select hotel, orderdate, ord_hour, duration1, duration2 from tmp_htlbidb.tmp_csy_duration_positive)b
on a.hotel = b.hotel

==================================================================================================================
--=========================================计算一个星期===========================================================
==================================================================================================================

-- 一个星期 basetime = orderdate  3695351, 
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_full_7;
create table tmp_csy_htl_ord_full_7 as
select * from tmp_htlbidb.tmp_csy_htl_ordercall_full
--where orderdate_2 >= date_sub(orderdate_1, 7) and orderdate_2 < to_date(orderdate_1)
where orderdate_2 >= from_unixtime(unix_timestamp(orderdate_1) - 7*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and orderdate_2 < from_unixtime(unix_timestamp(orderdate_1),'yyyy-MM-dd HH:00:00')


==========================================================================================================
--根据hotel，orderdate_1分组之后统计每个组的数量
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_full_7_geo;
create table tmp_csy_htl_ord_full_7_geo as
select
    a.*, b.nums
from
tmp_htlbidb.tmp_csy_htl_ord_full_7 as a
join
(
    select
        hotel, orderdate_1, count(*) as nums
    from tmp_htlbidb.tmp_csy_htl_ord_full_7
    group by hotel, orderdate_1
)b
on a.hotel = b.hotel and a.orderdate_1 = b.orderdate_1

==========================================================================================================

--过去一个星期子酒店分小时订单回复统计(ordtime)  101385
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_hour_7;
create table tmp_csy_htl_ord_hour_7 as
select 
    --hotel, orderdate_1 as orderdate, ord_hour,
    hotel, orderdate_1 as orderdate,
    avg(duration2) as duration_htl_ord_hour_7_pre,                              --过去一个星期子酒店分小时订单回复时长平均值
    stddev(duration2) as duration_htl_ord_hour_7_stddev,                        --过去一个星期子酒店分小时订单回复时长标准差
    percentile_approx(duration2, 0.75) as duration_htl_ord_hour_7_per75,        --过去一个星期子酒店分小时订单回复时长75分位
    percentile_approx(duration2, 0.50) as duration_htl_ord_hour_7_per50,        --过去一个星期子酒店分小时订单回复时长中位数
    percentile_approx(duration2, 0.25) as duration_htl_ord_hour_7_per25,        --过去一个星期子酒店分小时订单回复时长25分位
    count(*) as duration_htl_ord_hour_7_numsord,                                --过去一个星期子酒店分小时订单总数
    count(*)/7 as duration_htl_ord_hour_7_preord,                               --过去一个星期子酒店分小时订单平均数(每天)
    exp(sum(log(power(duration2, 1.0/nums)))) as duration_htl_ord_hour_7_geomean
from tmp_htlbidb.tmp_csy_htl_ord_full_7_geo where ord_hour = hour(orderdate_1)
--group by hotel, orderdate_1, ord_hour
group by hotel, orderdate_1

==========================================================================================================

--过去一个星期子酒店订单回复时长统计    317109
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_week_7;
create table tmp_csy_htl_ord_week_7 as
select
    hotel, orderdate_1 as orderdate,
    avg(duration2) as duration_htl_ord_week_7_pre,                              --过去一个星期子酒店订单回复时长平均值
    stddev(duration2) as duration_htl_ord_week_7_stddev,                        --过去一个星期子酒店订单回复时长标准差
    percentile_approx(duration2, 0.75) as duration_htl_ord_week_7_per75,        --过去一个星期子酒店订单回复时长75分位
    percentile_approx(duration2, 0.50) as duration_htl_ord_week_7_per50,        --过去一个星期子酒店订单回复时长中位数
    percentile_approx(duration2, 0.25) as duration_htl_ord_week_7_per25,        --过去一个星期子酒店订单回复时长25分位
    count(*) as duration_htl_ord_week_7_numsord,                                --过去一个星期子酒店订单订单总数
    count(*)/7 as duration_htl_ord_week_7_preord,                               --过去一个星期子酒店订单平均数(每天)
    exp(sum(log(power(duration2, 1.0/nums)))) as duration_htl_ord_week_7_geomean
from tmp_htlbidb.tmp_csy_htl_ord_full_7_geo
group by hotel, orderdate_1


==================================================================================================================
--=========================================计算一个月=============================================================
==================================================================================================================

--一个月 basetime = ordertime   11979046
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_full_30;
create table tmp_csy_htl_ord_full_30 as
select * from tmp_htlbidb.tmp_csy_htl_ordercall_full
-- where orderdate_2 >= date_sub(orderdate_1, 30) and orderdate_2 < to_date(orderdate_1)
where orderdate_2 >= from_unixtime(unix_timestamp(orderdate_1) - 30*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and orderdate_2 < from_unixtime(unix_timestamp(orderdate_1), 'yyyy-MM-dd HH:00:00')

==========================================================================================================
--根据hotel，orderdate_1分组之后统计每个组的数量
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_full_30_geo;
create table tmp_csy_htl_ord_full_30_geo as
select
    a.*, b.nums
from
tmp_htlbidb.tmp_csy_htl_ord_full_30 as a
join
(
    select
        hotel, orderdate_1, count(*) as nums
    from tmp_htlbidb.tmp_csy_htl_ord_full_30
    group by hotel, orderdate_1
)b
on a.hotel = b.hotel and a.orderdate_1 = b.orderdate_1

==========================================================================================================

--过去一个月子酒店分小时订单回复统计(ordtime) 179096
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_hour_30;
create table tmp_csy_htl_ord_hour_30 as
select 
    --hotel, ord_hour,
    hotel, orderdate_1 as orderdate,
    avg(duration2) as duration_htl_ord_hour_30_pre,                              --过去一个月子酒店分小时订单回复时长平均值
    stddev(duration2) as duration_htl_ord_hour_30_stddev,                        --过去一个月子酒店分小时订单回复时长标准差
    percentile_approx(duration2, 0.75) as duration_htl_ord_hour_30_per75,        --过去一个月子酒店分小时订单回复时长75分位
    percentile_approx(duration2, 0.50) as duration_htl_ord_hour_30_per50,        --过去一个月子酒店分小时订单回复时长中位数
    percentile_approx(duration2, 0.25) as duration_htl_ord_hour_30_per25,        --过去一个月子酒店分小时订单回复时长25分位
    count(*) as duration_htl_ord_hour_30_numsord,                                --过去一个月子酒店分小时订单总数
    count(*)/30 as duration_htl_ord_hour_30_preord,                              --过去一个月子酒店分小时订单平均数(每天)
    exp(sum(log(power(duration2, 1.0/nums)))) as duration_htl_ord_hour_30_geomean
from tmp_htlbidb.tmp_csy_htl_ord_full_30_geo where ord_hour = hour(orderdate_1)
--group by hotel, ord_hour
group by hotel, orderdate_1

==========================================================================================================

--过去一个月子酒店订单回复时长统计 368359
use tmp_htlbidb;
drop table if exists tmp_csy_htl_ord_month_30;
create table tmp_csy_htl_ord_month_30 as
select
    hotel, orderdate_1 as orderdate,
    avg(duration2) as duration_htl_ord_month_30_pre,                       		  --过去一个月子酒店订单回复时长平均值
    stddev(duration2) as duration_htl_ord_month_30_stddev,                 		  --过去一个月子酒店订单回复时长标准差
    percentile_approx(duration2, 0.75) as duration_htl_ord_month_30_per75, 		  --过去一个月子酒店订单回复时长75分位
    percentile_approx(duration2, 0.50) as duration_htl_ord_month_30_per50, 		  --过去一个月子酒店订单回复时长中位数
    percentile_approx(duration2, 0.25) as duration_htl_ord_month_30_per25,  	  --过去一个月子酒店订单回复时长25分位
    count(*) as duration_htl_ord_month_30_numsord,                                --过去一个月子酒店订单总数
    count(*)/30 as duration_htl_ord_month_30_preord,                              --过去一个月子酒店订单平均数(每天)
    exp(sum(log(power(duration2, 1.0/nums)))) as duration_htl_ord_month_30_geomean
from tmp_htlbidb.tmp_csy_htl_ord_full_30_geo
group by hotel, orderdate_1


==========================================================================================================

--多张表交集 105955  加了前两个left join 179707， 四个连续的left join 461817
use tmp_htlbidb;
drop table if exists tmp_csy_ordercall_finally;
create table tmp_csy_ordercall_finally as
select
    g.*, h.duration_htl_ord_month_30_pre,
    h.duration_htl_ord_month_30_stddev, h.duration_htl_ord_month_30_per75,
    h.duration_htl_ord_month_30_per50, h.duration_htl_ord_month_30_per25,
    h.duration_htl_ord_month_30_numsord, h.duration_htl_ord_month_30_preord,
    h.duration_htl_ord_month_30_geomean 
from
(
    select
        e.*,f.duration_htl_ord_hour_30_pre,
        f.duration_htl_ord_hour_30_stddev, f.duration_htl_ord_hour_30_per75,
        f.duration_htl_ord_hour_30_per50, f.duration_htl_ord_hour_30_per25,
        f.duration_htl_ord_hour_30_numsord, f.duration_htl_ord_hour_30_preord,
        f.duration_htl_ord_hour_30_geomean 
    from
    (
        select 
            c.*, d.duration_htl_ord_week_7_pre,
            d.duration_htl_ord_week_7_stddev, d.duration_htl_ord_week_7_per75,
            d.duration_htl_ord_week_7_per50, d.duration_htl_ord_week_7_per25,
            d.duration_htl_ord_week_7_numsord, d.duration_htl_ord_week_7_preord,
            d.duration_htl_ord_week_7_geomean 
        from
        (
            select 
                a.*, b.duration_htl_ord_hour_7_pre,
                b.duration_htl_ord_hour_7_stddev, b.duration_htl_ord_hour_7_per75,
                b.duration_htl_ord_hour_7_per50, b.duration_htl_ord_hour_7_per25,
                b.duration_htl_ord_hour_7_numsord, b.duration_htl_ord_hour_7_preord,
                b.duration_htl_ord_hour_7_geomean 
            from
            tmp_htlbidb.tmp_csy_duration_positive as a
            left join -- 防止数据丢失太多
            tmp_htlbidb.tmp_csy_htl_ord_hour_7 as b
            on a.hotel = b.hotel and a.orderdate = b.orderdate
        )c
        left join -- 防止数据丢失太多
        tmp_htlbidb.tmp_csy_htl_ord_week_7 as d
        on c.hotel = d.hotel and c.orderdate = d.orderdate
    )e
    join
    tmp_htlbidb.tmp_csy_htl_ord_hour_30 as f
    on e.hotel = f.hotel and e.orderdate = f.orderdate
)g
join
tmp_htlbidb.tmp_csy_htl_ord_month_30 as h
on g.hotel = h.hotel and g.orderdate = h.orderdate

==========================================================================================================