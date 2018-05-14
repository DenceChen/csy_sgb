--创建tmp_allcall
use tmp_htlbidb;
drop table if exists tmp_allcall_tmp;
create table tmp_allcall_tmp as
select
	b.dnis_no, b.time_quantum,
	b.nums_conn, b.nums_noconn,
	b.nums_conn/(b.nums_noconn+b.nums_conn) as con_rate,
	c.phone_call_nums
from
(
	select
		a.dnis_no, a.time_quantum,
		--统计每个电话在每个时段接通的次数与未接通的次数
		sum(case when isconn = 'yes' then 1 else 0 end) as nums_conn,
		sum(case when isconn = 'no' then 1 else 0 end) as nums_noconn
	from
	(
		select 
			maincid, orderid, dnis_no, time_in, 
			hour(time_in) as time_quantum, 
			(case when callduration > 0 then "yes" else "no" end) as isconn
		from OLAP_CallCenterDB.O_Call_013
		where "2016-05-31" <= d and d <= '${zdt.format("yyyy-MM-dd")}' and UPPER(calltype)="O" and orderid<>-1
	) as a 
	group by a.dnis_no, a.time_quantum
)b
join
(
  	select dnis_no, count(*) as phone_call_nums
  	from OLAP_CallCenterDB.O_Call_013
  	where "2016-05-31" <= d and d <= '${zdt.format("yyyy-MM-dd")}' and UPPER(calltype)="O" and orderid<>-1
  	group by dnis_no
)c 
on b.dnis_no=c.dnis_no

--根据dnis_no， time_quantum排序
set hive.mapred.mode=nonstrict;
use tmp_htlbidb;
drop table if exists tmp_allcall;
create table tmp_allcall as
select * from tmp_htlbidb.tmp_allcall_tmp
order by dnis_no, time_quantum
============================================================================================================
--创建tmp_allcall_full
use tmp_htlbidb;
drop table if exists tmp_allcall_full_tmp;
create table tmp_allcall_full_tmp as
select 
	a.dnis_no, a.time_quantum,
	(case when b.nums_conn is null then 0 else b.nums_conn end) as nums_conn,
	(case when b.nums_noconn is null then 0 else b.nums_noconn end) as nums_noconn,
	(case when b.con_rate is null then 0 else b.con_rate end) as con_rate,
	a.phone_call_nums
from
(
	--相当于之前的tmp_time_dnis
  	select c.dnis_no, d.time_quantum, c.phone_call_nums
	from
	(
		--相当于之前的tmp_allcall_dnis_no
		select distinct dnis_no, phone_call_nums, (0) as same from tmp_htlbidb.tmp_allcall
	)c
	left join
	(
	 	--相当于之前的tmp_allcall_24
		select distinct time_quantum, (0) as same from tmp_htlbidb.tmp_allcall
	)d
	on c.same == d.same
)a
left join
(
	select * from tmp_htlbidb.tmp_allcall
)b
on a.dnis_no == b.dnis_no and a.time_quantum == b.time_quantum


--根据dnis_no， time_quantum排序
set hive.mapred.mode=nonstrict;
use tmp_htlbidb;
drop table if exists tmp_allcall_full;
create table tmp_allcall_full as
select * from tmp_htlbidb.tmp_allcall_full_tmp
order by dnis_no, time_quantum
============================================================================================================
--创建tmp_allcall_finally
--order by
--set hive.mapred.mode=nonstrict;
--set hive.mapred.mode=strict;    -- 必须指定 limit 否则执行会报错。
use tmp_htlbidb;
drop table if exists tmp_allcall_finally;
create table tmp_allcall_finally as
select
	a.dnis_no, a.time_quantum, a.nums_conn, 
	a.nums_noconn, a.con_rate, a.phone_call_nums,
	--(
    --  case when 
    --  (a.con_rate >= 0.6 or (a.dnis_no = a.next_dnis and a.dnis_no = a.last_dnis and a.next_rate >= 0.6 and a.last_rate >=0.6)) 
    --  then 1 else 0 end
    --) as is_work
    if((a.con_rate >= 0.6 or (a.dnis_no = a.next_dnis and a.dnis_no = a.last_dnis and a.next_rate >= 0.6 and a.last_rate >=0.6)) , 1, 0) as is_work
from
(
	--模拟滑窗统计数据，窗口大小为3
	select
		dnis_no, time_quantum, nums_conn, nums_noconn, con_rate, phone_call_nums,
		LEAD(con_rate, 1) over(partition by dnis_no order by time_quantum) as next_rate,
		LAG(con_rate, 1) over(partition by dnis_no order by time_quantum) as last_rate,
		LEAD(dnis_no, 1) over(partition by dnis_no order by time_quantum) as next_dnis,
		LAG(dnis_no, 1) over(partition by dnis_no order by time_quantum) as last_dnis
	from tmp_htlbidb.tmp_allcall_full
)a
============================================================================================================























