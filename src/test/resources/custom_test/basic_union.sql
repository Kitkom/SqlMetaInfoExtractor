select distinct * from (
  select cA as cZ from tA
      union all
  select cB + cC as cZ from tB
      union
  select cD from tC vA
      minus
  select cE from tD
  )