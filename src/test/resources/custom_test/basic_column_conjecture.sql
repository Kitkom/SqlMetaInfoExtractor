create temporary view vA as
  select 1 as a,
         2 as b,
         3 as c;

create temporary view vB as
  select 1 as d,
         2 as e,
         3 as f;

select a + b + d as x,
       e + f as y,
       c,
       e,
       c+e
       from vA join vB join tA;