const sql = `
SELECT
  rsb.area AS area,
  rsb.lokasi AS lokasi,
  --   WSA SERVICE ------------------------------------
  (SELECT Jan FROM target_wsa WHERE kpi = 'service_a') AS tgt_service,

  CASE 
    WHEN svc_td.m01 = 0 OR svc_td.m01 IS NULL THEN '-'
    ELSE svc_td.m01
  END AS real_svc_td,
  
  CASE 
    WHEN svc_ys.m01 = 0 OR svc_ys.m01 IS NULL THEN '-'
    ELSE svc_ys.m01
  END AS real_svc_ys,

  
  ROUND(svc_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'service_a'), 0) * 100, 2) AS ach_service,
  CASE
    WHEN ROUND(svc_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'service_a'), 0) * 100, 2) < 100 THEN
      'not_comply'
    ELSE
      'comply'
  END AS service_comply,
  CASE
    WHEN svc_td.m01 < svc_ys.m01 THEN
      'down'
    WHEN svc_td.m01 > svc_ys.m01 THEN
      'up'
    ELSE
      'same'
  END AS growth_service,
  --   WSA SUGAR ------------------------------------
  (SELECT Jan FROM target_wsa WHERE kpi = 'sugar') AS tgt_sugar,
  CASE 
    WHEN sugar_td.m01 = 0 OR sugar_td.m01 IS NULL THEN '-'
    ELSE sugar_td.m01
  END AS real_sugar_td,
  
  CASE 
    WHEN sugar_ys.m01 = 0 OR sugar_ys.m01 IS NULL THEN '-'
    ELSE sugar_ys.m01
  END AS real_sugar_ys,

  ROUND(sugar_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'sugar'), 0) * 100, 2) AS ach_sugar,
  CASE
    WHEN ROUND(sugar_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'sugar'), 0) * 100, 2) < 100 THEN
      'not_comply'
    ELSE
      'comply'
  END AS sugar_comply,
  CASE
    WHEN sugar_td.m01 < sugar_ys.m01 THEN
      'down'
    WHEN sugar_td.m01 > sugar_ys.m01 THEN
      'up'
    ELSE
      'same'
  END AS growth_sugar,
  --   WSA TTR3 ------------------------------------
  (SELECT Jan FROM target_wsa WHERE kpi = 'ttr3_diamond') AS tgt_t3,
  CASE 
    WHEN t3_td.m01 = 0 OR t3_td.m01 IS NULL THEN '-'
    ELSE t3_td.m01
  END AS real_t3_td,
  
   CASE 
    WHEN t3_ys.m01 = 0 OR t3_ys.m01 IS NULL THEN '-'
    ELSE t3_ys.m01
  END AS real_t3_ys,


  CASE
    WHEN t3_td.m01 = 0.00 OR t3_td.m01 IS NULL THEN '-'
    ELSE ROUND(
      t3_td.m01 /
      NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'ttr3_diamond'), 0)
      * 100, 2
    )
  END AS ach_t3,

  CASE
    WHEN ROUND(t3_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 't3'), 0) * 100, 2) < 100 THEN
      'not_comply'
    ELSE
      'comply'
  END AS t3_comply,
  CASE
    WHEN t3_td.m01 < t3_ys.m01 THEN
      'down'
    WHEN t3_td.m01 > t3_ys.m01 THEN
      'up'
    ELSE
      'same'
  END AS growth_t3,

  --   WSA TTR6 ------------------------------------
  (SELECT Jan FROM target_wsa WHERE kpi = 'ttr6_platinum') AS tgt_t6,
  CASE 
    WHEN t6_td.m01 = 0 OR t6_td.m01 IS NULL THEN '-'
    ELSE t6_td.m01
  END AS real_t6_td,
  
   CASE 
    WHEN t6_ys.m01 = 0 OR t6_ys.m01 IS NULL THEN '-'
    ELSE t6_ys.m01
  END AS real_t6_ys,


  CASE
    WHEN t6_td.m01 = 0.00 OR t6_td.m01 IS NULL THEN '-'
    ELSE ROUND(
      t6_td.m01 /
      NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'ttr3_diamond'), 0)
      * 100, 2
    )
  END AS ach_t6,

  CASE
    WHEN ROUND(t6_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 't6'), 0) * 100, 2) < 100 THEN
      'not_comply'
    ELSE
      'comply'
  END AS t6_comply,
  CASE
    WHEN t6_td.m01 < t6_ys.m01 THEN
      'down'
    WHEN t6_td.m01 > t6_ys.m01 THEN
      'up'
    ELSE
      'same'
  END AS growth_t6,

   --   WSA TTR manja ------------------------------------
  (SELECT Jan FROM target_wsa WHERE kpi = 'ttrmanja') AS tgt_tm,
  CASE 
    WHEN tm_td.m01 = 0 OR tm_td.m01 IS NULL THEN '-'
    ELSE tm_td.m01
  END AS real_tm_td,
  
   CASE 
    WHEN tm_ys.m01 = 0 OR tm_ys.m01 IS NULL THEN '-'
    ELSE tm_ys.m01
  END AS real_tm_ys,


  CASE
    WHEN tm_td.m01 = 0.00 OR tm_td.m01 IS NULL THEN '-'
    ELSE ROUND(
      tm_td.m01 /
      NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'ttr3_diamond'), 0)
      * 100, 2
    )
  END AS ach_tm,

  CASE
    WHEN ROUND(tm_td.m01 / NULLIF((SELECT Jan FROM target_wsa WHERE kpi = 'tm'), 0) * 100, 2) < 100 THEN
      'not_comply'
    ELSE
      'comply'
  END AS tm_comply,
  CASE
    WHEN tm_td.m01 < tm_ys.m01 THEN
      'down'
    WHEN tm_td.m01 > tm_ys.m01 THEN
      'up'
    ELSE
      'same'
  END AS growth_tm
FROM
  region_sub_branch rsb

  -- ------------------ WSA SERVICE --------------------------
  LEFT JOIN wsa_service svc_td ON svc_td.witel = rsb.lokasi
  AND svc_td.tgl = CURDATE() AND svc_td.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  LEFT JOIN wsa_service svc_ys ON svc_ys.witel = rsb.lokasi
  AND svc_ys.tgl = CURDATE() - INTERVAL 1 DAY AND svc_ys.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')

  -- ------------------ WSA SUGAR --------------------------
  LEFT JOIN wsa_sugar sugar_td ON sugar_td.witel = rsb.lokasi 
  AND sugar_td.tgl = CURDATE() AND sugar_td.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  LEFT JOIN wsa_sugar sugar_ys ON sugar_ys.witel = rsb.lokasi
  AND sugar_ys.tgl = CURDATE() - INTERVAL 1 DAY AND sugar_ys.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  
-- ------------------ WSA TTR 3 JAM --------------------------
  LEFT JOIN wsa_ttr3 t3_td ON t3_td.witel = rsb.lokasi
  AND t3_td.tgl = CURDATE() AND t3_td.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  LEFT JOIN wsa_ttr3 t3_ys ON t3_ys.witel = rsb.lokasi AND t3_ys.tgl = CURDATE() - INTERVAL 1 DAY
  AND t3_ys.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')

  -- ------------------ WSA TTR 6 JAM --------------------------
  LEFT JOIN wsa_ttr6 t6_td ON t6_td.witel = rsb.lokasi
  AND t6_td.tgl = CURDATE() AND t6_td.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  LEFT JOIN wsa_ttr6 t6_ys ON t6_ys.witel = rsb.lokasi AND t6_ys.tgl = CURDATE() - INTERVAL 1 DAY
  AND t6_ys.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')

    -- ------------------ WSA TTR manja JAM --------------------------
  LEFT JOIN wsa_ttrmanja tm_td ON tm_td.witel = rsb.lokasi
  AND tm_td.tgl = CURDATE() AND tm_td.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  LEFT JOIN wsa_ttrmanja tm_ys ON tm_ys.witel = rsb.lokasi AND tm_ys.tgl = CURDATE() - INTERVAL 1 DAY
  AND tm_ys.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm')
  
  
  
  
  
  `;
