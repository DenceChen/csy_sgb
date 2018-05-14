--dw_htlmaindb.currentfacthtlordersnap 酒店订单主表

--数据按时间被平均划分成两份
--通过第一部分数据构建统计模型,数据存储在tmp_htlbidb.tmp_allcall_part1中
use tmp_htlbidb;
drop table if exists tmp_allcall_part1;
create table tmp_allcall_part1 as
select
	b.dnis_no, b.time_quantum,
	b.nums_conn, b.nums_noconn,
	b.nums_conn/(b.nums_noconn+b.nums_conn) as con_rate,
	c.phone_call_nums
from
(
	select
		a.dnis_no, a.time_quantum,
		--统计每个电话在每个时段接通的次数与不接通的次数
		sum(case when isconn = 'yes' then 1 else 0 end) as nums_conn,
		sum(case when isconn = 'no' then 1 else 0 end) as nums_noconn
	from
	(
		select 
			maincid, orderid, dnis_no, time_in, 
			hour(time_in) as time_quantum, 
			(case when callduration>0 then "yes" else "no" end) as isconn
		from OLAP_CallCenterDB.O_Call_013
		where "2016-05-31"<=d and d <= "2017-05-31" and UPPER(calltype)="O" and orderid<>-1
	) as a 
	group by a.dnis_no, a.time_quantum
)b
left join
(
  	select dnis_no, count(*) as phone_call_nums
  	from OLAP_CallCenterDB.O_Call_013
  	where "2016-05-31"<=d and d <= "2017-05-31" and UPPER(calltype)="O" and orderid<>-1
  	group by dnis_no
)c on b.dnis_no=c.dnis_no

--tmp_allcall_part1与第二部分数据join，统计第二部分数据
use tmp_htlbidb;
drop table if exists tmp_allcall_valid;
create table tmp_allcall_valid as
select
	a.dnis_no, b.time_in,
	a.time_quantum,
	a.con_rate,
	b.isconn
from
(
  select * from tmp_htlbidb.tmp_allcall_part1
)a
join
(
  select 
  	  dnis_no, time_in, 
      hour(time_in) as time_quantum, 
      (case when callduration>0 then 1 else 0 end) as isconn
  from OLAP_CallCenterDB.O_Call_013
  where "2017-06-01"<=d and d <= "2018-04-09" and UPPER(calltype)="O" and orderid<>-1
)b
on a.dnis_no == b.dnis_no and a.time_quantum == b.time_quantum