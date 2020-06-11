
insert into table target_table
select
  cAlias as cA,
  cPlus1 + cPlus2 as cB,
  case when cCase = 2
       then "2"
       when cCase = cPlus1 + cPlus2
       then cPlus2
       else "Else"
   end as cC,
  dA.tA.cA as cD,
  tB.*,
  cListQuery in (select tX, tY, tZ from dB.tB) as cE
  from dA.tA
  join (select tX, tY, tZ from dB.tB) tB
  join (select cAlias, cCase, cPlus1, cPlus2 from dC.tC) arias
  where cListQueryFilter in (select * from dC.tC)
