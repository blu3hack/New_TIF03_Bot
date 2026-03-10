WITH asr_ent AS (
    SELECT
      sc.reg AS area,
      sc.witel AS witel,
      
      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'tti_hsi') AS tgt_tti_hsi,
      ff_hsi.ttic AS tti_hsi,
      ROUND(ff_hsi.ttic / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'tti_hsi') * 100, 2) AS ach_tti_hsi,
      
      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ffg_hsi') AS tgt_ffg_hsi,
      ff_hsi.ffg AS ffg_hsi,
      ROUND(ff_hsi.ffg / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ffg_hsi') * 100, 2) AS ach_ffg_hsi,
      
      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttr_ffg_hsi') AS tgt_ttr_ffg_hsi,
      ff_hsi.ttr_ffg AS ttr_ffg_hsi,
      ROUND(ff_hsi.ttr_ffg / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttr_ffg_hsi') * 100, 2) AS ach_ttr_ffg_hsi,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ps_pi') AS tgt_ps_pi,
      ff_hsi.pspi AS ps_pi,
      ROUND(ff_hsi.pspi / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ps_pi') * 100, 2) AS ach_ps_pi,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ffg_non_hsi') AS tgt_ffg_non_hsi,
      ffg_non_hsi.comply AS ffg_non_hsi,
      ROUND(ffg_non_hsi.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ffg_non_hsi') * 100, 2) AS ach_ffg_non_hsi,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'prov_datin') AS tgt_prov_datin,
      provcomp.comply AS prov_datin,
      ROUND(provcomp.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'prov_datin') * 100, 2) AS ach_prov_datin,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'prov_wifi') AS tgt_prov_wifi,
      provcomp_wifi.comply AS prov_wifi,
      ROUND(provcomp_wifi.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'prov_wifi') * 100, 2) AS ach_prov_wifi,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttdc_siptrunk') AS tgt_ttdc_siptrunk,
      ttd_non_hsi.comply AS ttdc_siptrunk,
      ROUND(ttd_non_hsi.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttdc_siptrunk') * 100, 2) AS ach_ttdc_siptrunk,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttdc_wifi') AS tgt_ttdc_wifi,
      ttd_wifi.comply AS ttdc_wifi,
      ROUND(ttd_wifi.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttdc_wifi') * 100, 2) AS ach_ttdc_wifi,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttr_ffg_non_hsi') AS tgt_ttr_ffg_non_hsi,
      ttr_ffg_non_hsi.comply AS ttr_ffg_non_hsi,
      ROUND(ttr_ffg_non_hsi.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'ttr_ffg_non_hsi') * 100, 2) AS ach_ttr_ffg_non_hsi,

      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'unspec_gua_non_hsi') AS tgt_unspec_gua_non_hsi,
      unspec_guaranty.comply AS unspec_gua_non_hsi,
      ROUND(unspec_guaranty.comply / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'unspec_gua_non_hsi') * 100, 2) AS ach_unspec_gua_non_hsi,


      (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'unspec_gua_hsi') AS tgt_unspec_gua_hsi,
      ff_hsi.unspec AS unspec_gua_hsi,
      ROUND(ff_hsi.unspec / (SELECT feb FROM perf_tif.wisa_indikator WHERE kpi = 'unspec_gua_hsi') * 100, 2) AS ach_unspec_gua_hsi
      
      
      
      
      
    FROM
      perf_tif.sc_lokasi sc
      LEFT JOIN perf_tif.ff_hsi ff_hsi ON sc.witel = ff_hsi.lokasi AND ff_hsi.tgl = '2026-03-10'
      LEFT JOIN perf_tif.ffg_non_hsi ffg_non_hsi ON sc.witel = ffg_non_hsi.regional AND ffg_non_hsi.tgl = '2026-03-10' AND (
         ffg_non_hsi.jenis = 'area_ccm'
         OR ffg_non_hsi.regional LIKE '%TERRITORY%'
      )
      LEFT JOIN perf_tif.provcomp provcomp ON sc.witel = provcomp.regional AND provcomp.tgl = '2026-03-10' AND (
         provcomp.jenis = 'area_ccm'
         OR provcomp.regional LIKE '%TERRITORY%'
      )
      LEFT JOIN perf_tif.provcomp_wifi provcomp_wifi ON sc.witel = provcomp_wifi.regional AND provcomp_wifi.tgl = '2026-03-10' AND (
         provcomp_wifi.jenis = 'area_ccm'
         OR provcomp_wifi.regional LIKE '%TERRITORY%'
      )

      LEFT JOIN perf_tif.ttd_non_hsi ttd_non_hsi ON sc.witel = ttd_non_hsi.regional AND ttd_non_hsi.tgl = '2026-03-10' AND (
         ttd_non_hsi.jenis = 'area_ccm'
         OR ttd_non_hsi.regional LIKE '%TERRITORY%'
      )

      LEFT JOIN perf_tif.ttd_wifi ttd_wifi ON sc.witel = ttd_wifi.regional AND ttd_wifi.tgl = '2026-03-10' AND (
         ttd_wifi.jenis = 'area_ccm'
         OR ttd_wifi.regional LIKE '%TERRITORY%'
      )

      LEFT JOIN perf_tif.ttr_ffg_non_hsi ttr_ffg_non_hsi ON sc.witel = ttr_ffg_non_hsi.regional AND ttr_ffg_non_hsi.tgl = '2026-03-10' AND (
         ttr_ffg_non_hsi.jenis = 'area_ccm'
         OR ttr_ffg_non_hsi.regional LIKE '%TERRITORY%'
      )

      LEFT JOIN perf_tif.unspec_guaranty unspec_guaranty ON sc.witel = unspec_guaranty.regional AND unspec_guaranty.tgl = '2026-03-10' AND (
         unspec_guaranty.jenis = 'area_ccm'
         OR unspec_guaranty.regional LIKE '%TERRITORY%'
      )
    WHERE
      sc.reg IN ('tif', 'balnus_ccm', 'jatim_ccm', 'jateng_ccm', 'area_ccm')
)

SELECT * FROM asr_ent 

ORDER BY 
CASE
WHEN witel = 'TERRITORY 01' THEN 1
WHEN witel = 'TERRITORY 02' THEN 2
WHEN witel = 'TERRITORY 03' THEN 3
WHEN witel = 'BALI NUSRA' THEN 4
WHEN area = 'balnus_ccm' THEN 5
WHEN witel = 'JAWA TIMUR' THEN 6
WHEN area = 'jatim_ccm' THEN 7
WHEN witel = 'JATENG DIY' THEN 8
WHEN area = 'jateng_ccm' THEN 9
ELSE 1000
END
