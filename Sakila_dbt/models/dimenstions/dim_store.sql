{{ config(post_hook='insert into {{this}}(store_id) VALUES (-1)') }}

with stg_store as (
	select *,
	  '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
	from store
),
staff as (
	select *
	from {{ ref('dim_staff')}}
),
address as (
	select *
	from address
),
city as (
	select *
	from city
),
country as (
	select *
	from country
)

select store.store_id,
		staff.staff_id,
		staff.first_name,
		staff.last_name,
		address.address_id,
		address.address as store_address,
		city.city_id,
		city.city,
		country.country_id,
		country.country,
		  '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from store
left join address
	on store.address_id = address.address_id
left join city 
	on city.city_id = address.city_id
left join country
	on country.country_id = city.country_id
left join staff
	on staff.staff_id = store.manager_staff_id