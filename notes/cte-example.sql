
create table person(name text, age int);


-- Variable assignment like name = 'guang'
-- Predicate name = 'guang' using CTE is like a semi join.
-- Cast/declaration of data type in 'guang'::text is necessary for Postgres.
with myname(name) as (select 'guang'::text) select * from person where name in (select * from myname);


-- doesn't work! recursive term cannot be in subquery
with recursive counted_purchase as (
     select * from purchase where id = 1 and year = 1988
     union all
     select * from purchase where id in (
     	    select max(id) + 1::int from counted_purchase
     ) and year = 1988
) select * from counted_purchase; 


-- doesn't work! aggregate function cannot be on recursive term
with recursive counted_purchase as (
     select * from purchase where id = 1 and year = 1988
     union all
     (with next_id(nid) as (
     	  select max(id) + 1::int from counted_purchase
     )
     select * from purchase, next_id where id = nid and year = 1988)
) select * from counted_purchase; 



with recursive counted_purchase as (
     select *, id + 1 as nid from purchase where id = 1 and year = 1988
     union all
     select purchase.*, purchase.id + 1 from purchase, counted_purchase where purchase.id = counted_purchase.nid and purchase.year = 1988
) select * from counted_purchase;


with recursive counted_purchase(nid) as (
     select id from purchase where id = 1 and year = 1988
     union all
     select nid + 1 from counted_purchase where exists (select id from purchase where id = nid + 1 and year = 1988)
)
select * from counted_purchase

with recursive counted_purchase(nid) as (
     select id from purchase where id = 1 and year = 1988
     union all
     select id from purchase where exists (select nid from counted_purchase where id = nid + 1) and year = purchase
)
select * from counted_purchase

with recursive counted_purchase(nid, nitem) as (
     select id, item from purchase where id = 1 and year = 1988
     union all
     select id, item from counted_purchase, purchase where id = nid + 1 and year = 1988
)
select * from counted_purchase
