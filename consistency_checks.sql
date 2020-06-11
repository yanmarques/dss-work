-- venda:vendedor
SELECT count(p.*) AS nr_par_previstas FROM vendedor v 
inner JOIN venda ON v.cd_vdd = venda.cd_vdd 
INNER JOIN parcela p ON (venda.cd_ven = p.cd_ven AND venda.cd_loj = p.cd_loj)
WHERE v.cd_vdd = 5 AND p.vl_pago IS NULL;

SELECT sum(p.vl_par) AS nr_par_previstas FROM vendedor v 
inner JOIN venda ON v.cd_vdd = venda.cd_vdd 
INNER JOIN parcela p ON (venda.cd_ven = p.cd_ven AND venda.cd_loj = p.cd_loj)
WHERE v.cd_vdd = 5 AND p.vl_pago IS NULL;

SELECT count(p.*) AS nr_par_pagas FROM vendedor v 
inner JOIN venda ON v.cd_vdd = venda.cd_vdd 
INNER JOIN parcela p ON (venda.cd_ven = p.cd_ven AND venda.cd_loj = p.cd_loj)
WHERE v.cd_vdd = 5 AND p.vl_pago IS NOT NULL;

SELECT count(p.*) AS nr_par_atrasadas FROM vendedor v 
inner JOIN venda ON v.cd_vdd = venda.cd_vdd 
INNER JOIN parcela p ON (venda.cd_ven = p.cd_ven AND venda.cd_loj = p.cd_loj)
WHERE v.cd_vdd = 5 AND p.vl_pago IS NOT NULL AND p.dt_vcto < p.dt_pagto;

-- venda:tempo
SELECT count(p.*) AS nr_par_pagas FROM parcela p
WHERE extract(month FROM p.dt_pagto) = 9
AND extract(year FROM p.dt_pagto) = 2006
and p.vl_pago IS NOT NULL;

SELECT count(p.*) AS nr_par_previstas FROM parcela p
WHERE extract(month FROM p.dt_vcto) = 9
AND extract(year FROM p.dt_vcto) = 2006
and p.vl_pago IS NULL;

SELECT count(p.*) AS nr_par_atrasadas FROM parcela p
WHERE extract(month FROM p.dt_pagto) = 9
AND extract(year FROM p.dt_pagto) = 2006
and p.vl_pago IS NOT NULL AND p.dt_vcto < p.dt_pagto;

SELECT sum(p.vl_par) AS vl_par_previstas FROM parcela p
WHERE extract(month FROM p.dt_vcto) = 9
AND extract(year FROM p.dt_vcto) = 2006
and p.vl_pago IS NULL;
