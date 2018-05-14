--提取历史数据特征(过去一个月，过去一个星期)
--tmp_htlbidb.tmp_sgb_duration_positive
--不过滤时间
use tmp_htlbidb;
drop table if exists tmp_sgb_suppid_full;
create table tmp_sgb_suppid_full as
select
    a.supplierid, a.processtime as processtime_1, 
    b.duration, b.processtime as processtime_2, 
    b.process_hour, b.ord_hour
from
(select supplierid, processtime from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select supplierid, processtime, duration, orderdate, process_hour, ord_hour from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.supplierid = b.supplierid

==========================================================================================================

--一个星期
use tmp_htlbidb;
drop table if exists tmp_sgb_suppid_full_7;
create table tmp_sgb_suppid_full_7 as
select
	a.supplierid, a.processtime as processtime_1, 
	b.duration, b.processtime as processtime_2, 
	b.process_hour, b.ord_hour
from
(select supplierid, processtime from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select supplierid, processtime, duration, orderdate, process_hour, ord_hour from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.supplierid = b.supplierid
where b.processtime >= date_sub(a.processtime, 7) and b.processtime < to_date(a.processtime)

==================================================================================================================
--一个月
use tmp_htlbidb;
drop table if exists tmp_sgb_suppid_full_30;
create table tmp_sgb_suppid_full_30 as
select
    a.supplierid, a.processtime as processtime_1, 
    b.duration, b.processtime as processtime_2, 
    b.process_hour, b.ord_hour
from
(select supplierid, processtime from tmp_htlbidb.tmp_sgb_duration_positive)a
join
(select supplierid, processtime, duration, orderdate, process_hour, ord_hour from tmp_htlbidb.tmp_sgb_duration_positive)b
on a.supplierid = b.supplierid
where b.processtime >= date_sub(a.processtime, 30) and b.processtime < to_date(a.processtime)

==========================================================================================================