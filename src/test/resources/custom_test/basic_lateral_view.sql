  select f1, f2, c.t2, a.*
    from a
    join c
lateral view json_tuple(a.soj_context, 'bn', 'cat', 'catlvl') b as f1, f2, f3
lateral view explode(a.attr_vi) c as attr_vi
