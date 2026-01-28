with extract_dates as (
	select min(cd.extract_last_update_dtm) as extract_last_update_dtm_min
		,max(cd.extract_last_update_dtm) as extract_last_update_dtm_max
	from edp_stay_conformed.v_conf_checkout_curr ccc
	inner join edp_stay_checkout.v_checkout_detail cd 
		  on cd.stay_id = ccc.stay_id
		  and cd.checkout_detail_key = ccc.checkout_detail_key  
	where 1 = 1	
		and cd.arrival_dt <= '2025-06-14'
		and cd.departure_dt >= '2025-06-08'
		/*exclude cancels and no-shows*/
		and cd.prop_stay_status not in ('N', 'X')
)
, chk_stay_pop_1 as (
	select cd.stay_id
		,cd.prop_cd
		,cd.extract_last_update_dtm
		,cd.currency_cd as chckout_currency_cd
		,cd.arrival_dt
		,cd.departure_dt
	from edp_stay_conformed.v_conf_checkout_curr ccc
	inner join edp_stay_checkout.v_checkout_detail cd 
		  on cd.stay_id = ccc.stay_id
		  and cd.checkout_detail_key = ccc.checkout_detail_key  
	where 1 = 1	
		and cd.arrival_dt <= '2025-06-14'
		and cd.departure_dt >= '2025-06-08'
		/*exclude cancels and no-shows*/
		and cd.prop_stay_status not in ('N', 'X')
)
, chk_stay_pop_2 as (
	select c.stay_id
		,c.prop_cd
		,c.extract_last_update_dtm
		,c.chckout_currency_cd
		,datediff(day, c.arrival_dt, '2025-06-08') as stay_period_days_exclude_before
		,datediff(day, dateadd(day, 1, '2025-06-14'), c.departure_dt) as stay_period_days_exclude_after	
	from chk_stay_pop_1 c
	where 1 = 1
		and (
		c.departure_dt >= dateadd(day, 1, '2025-06-08')
		or (c.arrival_dt = '2025-06-08' and c.departure_dt = '2025-06-08' )
		)
)
, chk_stay_pop_3 as (
	select c.stay_id
		,c.prop_cd
		,c.extract_last_update_dtm
		,c.chckout_currency_cd
		,case when c.stay_period_days_exclude_before > 0 then c.stay_period_days_exclude_before else 0 end as stay_period_days_exclude_before
		,case when c.stay_period_days_exclude_after > 0 then c.stay_period_days_exclude_after else 0 end as stay_period_days_exclude_after
	from chk_stay_pop_2 c
)
, chk_bk as (
	select c.stay_id
		,c.prop_cd
		,c.extract_last_update_dtm
		,c.chckout_currency_cd		
		,c.stay_period_days_exclude_before + c.stay_period_days_exclude_after as stay_period_days_exclude
		,(s.room_cnt * (s.departure_dt - s.arrival_dt)) as room_nights		
	from chk_stay_pop_3 c
	left join edp_stay_conformed.v_conf_stay_curr_booking cscb
		on c.stay_id = cscb.stay_id
	inner join edp_stay_booking.v_stay s
		on cscb.stay_id = s.stay_id
		and cscb.stay_key = s.stay_key
)
, chk_dtl as (
	select c.stay_id
		,c.prop_cd
		,c.extract_last_update_dtm
		,c.chckout_currency_cd
		,c.room_nights
		,case when c.room_nights >= 1 then (1 - (c.stay_period_days_exclude / c.room_nights::float)) else 1 end as stay_period_inclusion_factor			
	from chk_bk c	
)
, primary_charge_category as (
	select ct.stay_id
		,ct.folio_id
		,ct.trans_id
		,max(ct.charge_category) as primary_charge_category
	from extract_dates ed 
	inner join edp_stay_checkout.v_checkout_transaction ct
		on ct.extract_last_update_dtm between ed.extract_last_update_dtm_min and ed.extract_last_update_dtm_max
	where 1 = 1
		and ct.charge_category <> ''		
		and ct.posting_type = 'P'
		and ct.trans_id is not null
		and ct.ledger_entry_amt <> 0
	group by ct.stay_id
		,ct.folio_id
		,ct.trans_id
)
, chk_trxn_1 as (
select ct.stay_id
	,pcc.primary_charge_category 
	,ct.charge_category 
	,ct.posting_type 
	,ct.trans_id 
	,ct.folio_id	
	,ct.trans_folio_id 
	,ct.orig_folio_id 		
	,ct.entry_type
	,ct.acctg_category 
	,ct.extract_last_update_dtm
	,ct.ledger_entry_amt
	from extract_dates ed 
	inner join edp_stay_checkout.v_checkout_transaction ct
		on ct.extract_last_update_dtm between ed.extract_last_update_dtm_min and ed.extract_last_update_dtm_max
	left join primary_charge_category pcc
		on ct.stay_id = pcc.stay_id 
		and ct.folio_id = pcc.folio_id 
		and ct.trans_id = pcc.trans_id 
	where 1 = 1
		and ct.charge_category in ('room')
		and ct.folio_id = ct.trans_folio_id 
		and ct.ledger_entry_amt <> 0
)
, chk_logic_1 as (
select c.prop_cd 
	,c.stay_id 
	,ct.charge_category 
	,c.chckout_currency_cd
	,c.room_nights
	,c.stay_period_inclusion_factor
	,sum(case when ct.primary_charge_category = ct.charge_category and ct.entry_type in ('CHARGE', 'ADJUST', 'ALLOWE') then ct.ledger_entry_amt else 0 end) as rev_filter_1
	,sum(case when ct.primary_charge_category = 'tax' and ct.charge_category = 'tax' and ct.entry_type in ('ALLOWE') then ct.ledger_entry_amt else 0 end) as rev_filter_2
	,sum(case when ct.charge_category = 'room' and ct.acctg_category = 'RX' then ct.ledger_entry_amt else 0 end) as rev_filter_3
from chk_dtl c
inner join chk_trxn_1 ct
	on c.stay_id = ct.stay_id
	and c.extract_last_update_dtm = ct.extract_last_update_dtm
group by c.prop_cd 
	,c.stay_id 
	,ct.charge_category 
	,c.chckout_currency_cd
	,c.room_nights
	,c.stay_period_inclusion_factor
)
, chk_logic_2 as (
	select c.prop_cd
		,c.stay_id
		,c.chckout_currency_cd
		,c.room_nights
		,c.stay_period_inclusion_factor
		,c.rev_filter_1 + c.rev_filter_2 + c.rev_filter_3 as chkout_room_lc_amt
	from chk_logic_1 c
	where 1 = 1
		and (c.rev_filter_1 + c.rev_filter_2 + c.rev_filter_3) <> 0
)
, plan_rates_cte as (
	select
		exchange_rate as plan_exchange_rate,
		rate_date,
		from_currency,
		to_currency,
		conversion_type
	from edp_rdm.v_pub_fx_rates 
	where conversion_type = 'Plan Exchange Rates'
		and to_currency = 'USD'
)
select c.prop_cd 	
	,round(sum(c.room_nights * c.stay_period_inclusion_factor), 0) as room_nights
	,round(sum(c.chkout_room_lc_amt * c.stay_period_inclusion_factor * rates_chk.plan_exchange_rate), 2) as room_rev_usd
from chk_logic_2 c
left join plan_rates_cte rates_chk
	on c.chckout_currency_cd = rates_chk.from_currency
group by c.prop_cd 	

