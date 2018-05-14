-- 构建一张全交表,基于供应商   52680033
use tmp_htlbidb;
drop table if exists tmp_csy_supp_ordercall_full;
create table tmp_csy_supp_ordercall_full as
select
    a.supplierid, a.orderdate as orderdate_1, 
    b.orderdate as orderdate_2, b.ord_hour, b.duration1, b.duration2
from
(select supplierid, orderdate from tmp_htlbidb.tmp_csy_duration_positive)a
join
(select supplierid, orderdate, ord_hour, duration1, duration2 from tmp_htlbidb.tmp_csy_duration_positive)b
on a.supplierid = b.supplierid


==================================================================================================================
--=========================================计算一个月=============================================================
==================================================================================================================

--一个月 basetime = ordertime   11979046
use tmp_htlbidb;
drop table if exists tmp_csy_supp_ord_full_30;
create table tmp_csy_supp_ord_full_30 as
select * from tmp_htlbidb.tmp_csy_supp_ordercall_full
-- where orderdate_2 >= date_sub(orderdate_1, 30) and orderdate_2 < to_date(orderdate_1)
where orderdate_2 >= from_unixtime(unix_timestamp(orderdate_1) - 30*24*60*60 ,'yyyy-MM-dd HH:mm:ss') and orderdate_2 < from_unixtime(unix_timestamp(orderdate_1),'yyyy-MM-dd HH:00:00')

==========================================================================================================

--过去一个月子酒店分小时订单回复统计(ordtime) 179096
use tmp_htlbidb;
drop table if exists tmp_csy_supp_ord_hour_30;
create table tmp_csy_supp_ord_hour_30 as
select 
    supplierid, orderdate_1 as orderdate,
    avg(duration2) as duration_supp_ord_hour_30_pre,                       --过去一个月供应商分小时订单回复时长平均值
    stddev(duration2) as duration_supp_ord_hour_30_stddev,                 --过去一个月供应商分小时订单回复时长标准差
    percentile_approx(duration2, 0.75) as duration_supp_ord_hour_30_per75, --过去一个月供应商分小时订单回复时长75分位
    percentile_approx(duration2, 0.50) as duration_supp_ord_hour_30_per50, --过去一个月供应商分小时订单回复时长中位数
    percentile_approx(duration2, 0.25) as duration_supp_ord_hour_30_per25  --过去一个月供应商分小时订单回复时长25分位
    count(*) as duration_supp_ord_hour_30_numsord,                         --过去一个月供应商分小时订单总数
    count(*)/30 as duration_supp_ord_hour_30_preord                        --过去一个月供应商分小时订单平均数
from tmp_htlbidb.tmp_csy_supp_ord_full_30 where ord_hour = hour(orderdate_1)
group by supplierid, orderdate_1

==========================================================================================================

--过去一个月子酒店订单回复时长统计 368359
use tmp_htlbidb;
drop table if exists tmp_csy_supp_ord_month_30;
create table tmp_csy_supp_ord_month_30 as
select
    supplierid, orderdate_1 as orderdate,
    avg(duration2) as duration_supp_ord_month_30_pre,                       		--过去一个月供应商订单回复时长平均值
    stddev(duration2) as duration_supp_ord_month_30_stddev,                 		--过去一个月供应商订单回复时长标准差
    percentile_approx(duration2, 0.75) as duration_supp_ord_month_30_per75, 		--过去一个月供应商订单回复时长75分位
    percentile_approx(duration2, 0.50) as duration_supp_ord_month_30_per50, 		--过去一个月供应商订单回复时长中位数
    percentile_approx(duration2, 0.25) as duration_supp_ord_month_30_per25,  	--过去一个月供应商订单回复时长25分位
    count(*) as duration_supp_ord_month_30_numsord,                              --过去一个月供应商订单总数
    count(*)/30 as duration_supp_ord_month_30_preord                             --过去一个月供应商订单平均数
from tmp_htlbidb.tmp_csy_supp_ord_full_30
group by supplierid, orderdate_1

==========================================================================================================

--多张表交集 105955  加了前两个left join 179707， 四个连续的left join 461817
use tmp_htlbidb;
drop table if exists tmp_csy_ordercall_finally;
create table tmp_csy_ordercall_finally as
select
    k.*, l.duration_supp_ord_month_30_pre,
    l.duration_supp_ord_month_30_stddev, l.duration_supp_ord_hour_30_per75, 
    l.duration_supp_ord_hour_30_per50, l.duration_supp_ord_hour_30_per25, 
    l.duration_supp_ord_hour_30_numsord, l.duration_supp_ord_hour_30_preord
(
    select 
        i.*, j.duration_supp_ord_hour_30_pre, 
        j.duration_supp_ord_hour_30_stddev, j.duration_supp_ord_hour_30_per75, 
        j.duration_supp_ord_hour_30_per50, j.duration_supp_ord_hour_30_per25,
        j.duration_supp_ord_hour_30_numsord, j.duration_supp_ord_hour_30_preord

    (
        select
            g.*, h.duration_htl_ord_month_30_pre,
            h.duration_htl_ord_month_30_stddev, h.duration_htl_ord_month_30_per75,
            h.duration_htl_ord_month_30_per50, h.duration_htl_ord_month_30_per25,
            h.duration_htl_ord_month_30_numsord, h.duration_htl_ord_month_30_preord 
        from
        (
            select
                e.*,f.duration_htl_ord_hour_30_pre,
                f.duration_htl_ord_hour_30_stddev, f.duration_htl_ord_hour_30_per75,
                f.duration_htl_ord_hour_30_per50, f.duration_htl_ord_hour_30_per25,
                f.duration_htl_ord_hour_30_numsord, f.duration_htl_ord_hour_30_preord 
            from
            (
                select 
                    c.*, d.duration_htl_ord_week_7_pre,
                    d.duration_htl_ord_week_7_stddev, d.duration_htl_ord_week_7_per75,
                    d.duration_htl_ord_week_7_per50, d.duration_htl_ord_week_7_per25,
                    d.duration_htl_ord_week_7_numsord, d.duration_htl_ord_week_7_preord 
                from
                (
                    select 
                        a.*, b.duration_htl_ord_hour_7_pre,
                        b.duration_htl_ord_hour_7_stddev, b.duration_htl_ord_hour_7_per75,
                        b.duration_htl_ord_hour_7_per50, b.duration_htl_ord_hour_7_per25,
                        b.duration_htl_ord_hour_7_numsord, b.duration_htl_ord_hour_7_preord 
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
    )i 
    left join
    tmp_htlbidb.tmp_csy_supp_ord_hour_30 as j
    on i.supplierid = j.supplierid and i.orderdate = j.orderdate
)k
left join 
tmp_htlbidb.tmp_csy_supp_ord_month_30 as l
on k.supplierid = l.supplierid and k.orderdate = l.orderdate


问题：是保留房，订单确认客户时间很短（立即确认订单），但是酒店确认时间很长？
==========================================================================================================