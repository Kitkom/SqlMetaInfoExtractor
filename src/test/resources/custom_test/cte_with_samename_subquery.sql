with a as (
select cA, cB, cC from tA)

select a.*, tB.cD from tB
  join  (select a.*, tC.cE from (select cA, cB from a ) a join tC) a