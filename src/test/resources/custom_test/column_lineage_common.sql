select
  cAlias as cA,
  cPlus1 + cPlus2 as cB,
  case when cCase = 2
       then "2"
       when cCase = cPlus1 + cPlus2
       then "Plus"
       else "Else"
   end as cC,
  dA.tA.cA as cD,
  dB.tB.*,
  cListQuery in (select * from dB.tB) as cE
  from dA.tA
  join dB.tB
