------------------------------------------------------
--   统计每个电话在每个时段接通的次数与不接通的次数
------------------------------------------------------
select 
a.ani, a.time_quantum, 
b.connect_nums, a.call_nums, 
b.connect_nums/a.call_nums as con_rate,
c.phone_call_nums
from
(
	select ani, hour(datacreatetime) as time_quantum, count(*) as call_nums
	from ods_htl_htlorderprocessdb.htl_orderprocess_ccdesk_outcall
	where d <= "2018-03-26" and d >= "2018-03-20" group by ani, hour(datacreatetime)
) a
join
(
	select ani, hour(datacreatetime) as time_quantum, count(*) as connect_nums
	from ods_htl_htlorderprocessdb.htl_orderprocess_ccdesk_outcall
	where d <= "2018-03-26" and d >= "2018-03-20" and outcalltype='1' group by ani, hour(datacreatetime)
) b
on a.ani = b.ani and a.time_quantum = b.time_quantum
join
(
  	select ani, count(*) as phone_call_nums
  	from ods_htl_htlorderprocessdb.htl_orderprocess_ccdesk_outcall
  	where d <= "2018-03-26" and d >= "2018-03-20" group by ani
)c
on a.ani = c.ani
limit 100




-- 更简单的查询方式
select 
a.ani, a.time_quantum, 
a.nums_connect, a.nums_noconnect, 
a.nums_connect/(a.nums_noconnect+a.nums_connect) as con_rate,
b.phone_call_nums
from
(
  	select ani, hour(datacreatetime) as time_quantum,
		--统计每个电话在每个时段接通的次数与不接通的次数
		sum(case when outcalltype = '0' then 1 else 0 end) as nums_connect,
		sum(case when outcalltype = '1' then 1 else 0 end) as nums_noconnect
	from ods_htl_htlorderprocessdb.htl_orderprocess_ccdesk_outcall
	where d <= "2018-03-26" and d >= "2018-03-20" group by ani, hour(datacreatetime)
)a
join
(
  	select ani, count(*) as phone_call_nums
  	from ods_htl_htlorderprocessdb.htl_orderprocess_ccdesk_outcall
  	where d <= "2018-03-26" and d >= "2018-03-20" group by ani
)b
on a.ani = b.ani

--曹丹丹给我的数据
Select time_in as `呼出时间`,callduration as `通话时长`, dnis_no as `被叫号码` from OLAP_CallCenterDB.O_Call_013 where d=’2018-03-28’and UPPER(Calltype)=’O’
表分区为：日分区，增量分区（非全量）


--非strict模式下在使用order by的时候不需要使用limit
--zus 默认是strict模式的
--set hive.mapred.mode=nonstrict;
--set hive.mapred.mode=strict;
use tmp_htlbidb;
drop table if exists tmp_allcall;
create table tmp_allcall as
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
		where "2016-05-31"<=d and d <= "2018-03-27" and UPPER(calltype)="O" and orderid<>-1
	) as a 
	group by a.dnis_no, a.time_quantum
)b
left join
(
  	select dnis_no, count(*) as phone_call_nums
  	from OLAP_CallCenterDB.O_Call_013
  	where "2016-05-31"<=d and d <= "2018-03-27" and UPPER(calltype)="O" and orderid<>-1
  	group by dnis_no
)c on b.dnis_no=c.dnis_no
--ORDER BY b.dnis_no



use tmp_htlbidb;
drop table if exists tmp_allcall_24;
create table tmp_allcall_24 as
select distinct time_quantum, (0) as same from tmp_allcall

use tmp_htlbidb;
drop table if exists tmp_allcall_dnis_no;
create table tmp_allcall_dnis_no  as
select distinct dnis_no, (0) as same from tmp_allcall


--建立一张全交的表
use tmp_htlbidb;
drop table if exists tmp_time_dnis;
create table tmp_time_dnis as
select a.dnis_no, b.time_quantum
from
(
  select * from tmp_allcall_dnis_no
)a
left join
(
  select * from tmp_allcall_24
)b
on a.same == b.same


--扩展tmp_allcall表
use tmp_htlbidb;
drop table if exists tmp_allcall_full;
create table tmp_allcall_full as
select 
	a.dnis_no, a.time_quantum,
	b.nums_conn, b.nums_noconn,
	b.con_rate,b.phone_call_nums
from
(
  select * from tmp_time_dnis
)a
left join
(
  select * from tmp_allcall
)b
on a.dnis_no == b.dnis_no and a.time_quantum == b.time_quantum















