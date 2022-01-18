-- 1.   How many opens per receipt do we see on average?
-- a) Where receipts without open events count:
with count_open as
(select r.id, count(distinct o.id) as open_count
		from engagement.receipt_events r
		left join engagement.open_events o
			on o.receipt_id = r.id
		group by 1)

select avg(open_count)
from  count_open
-- b) Where receipts without open events don't count:
with count_open as
(select receipt_id, count(distinct id) as open_count
		from engagement.open_events
		group by 1)
		
select avg(open_count)
from  count_open
-- 2.   What is the average and median Transaction Amount values for receipts that are opened?
select  avg(trans_amt) as trans_amt_avg,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY trans_amt) as trans_amt_median
from engagement.receipt_events
where id in (select distinct receipt_id
			 from engagement.open_events)
-- 3.   Similarly, what is the average and median Transaction Amount values for receipts that are not opened?
select  avg(trans_amt) as trans_amt_avg,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY trans_amt) as trans_amt_median
from engagement.receipt_events
where id not in (select distinct receipt_id
			 from engagement.open_events)
-- 4.  Which brands see the most interaction?
select r.brand_id, count(*)
from engagement.open_events o
join engagement.receipt_events r
	on o.receipt_id = r.id
group by 1
order by 2 desc
-- 5.  Which email domains see the most interaction?
select  substring(email_address, position('@' in email_address) + 1,
				  case 
				  	when position('>' in email_address) > 0 
				  		then position('>' in email_address) - position('@' in email_address) - 1
				  else length(email_address) - position('@' in email_address)
				  end) as email_domain,
		count(*)
from engagement.open_events o
join engagement.receipt_events r
	on o.receipt_id = r.id
group by 1
order by 2 desc