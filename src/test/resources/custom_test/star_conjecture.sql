create temporary view vA as
  select 1 as a,
         2 as b,
         3 as c;


create temporary view vB as
  select 1 as d,
         2 as e,
         3 as f;

create temporary view vC as
  select *,
         4 as g
    from vB;

select vB.*,
       5 as z
  from vB join vC;
