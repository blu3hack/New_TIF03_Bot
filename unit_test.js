SELECT
  rcm.piloting,
  rcm.sto,

  kemt.tti_comply AS tti_com,
  kemt.tti_ps_ih AS tti_tot,
  ROUND((kemt.tti_comply / kemt.tti_ps_ih) * 100, 2) AS ttic,
  ROUND(
    ((kemt.tti_comply / kemt.tti_ps_ih) * 100) / 92.72 * 100
  ,2) AS ach_ttic,
  CASE WHEN ( ((kemt.tti_comply / kemt.tti_ps_ih) * 100) / 92.72 * 100 ) < 100 THEN 'not_comply' END AS tti_comply,

  kemt.ffg_comply AS ffg_com,
  kemt.ffg_jml_ps AS ffg_tot,
  ROUND((kemt.ffg_comply / kemt.ffg_jml_ps) * 100, 2) AS ffg,
  ROUND(
    ((kemt.ffg_comply / kemt.ffg_jml_ps) * 100) / 97.40 * 100
  ,2) AS ach_ffg,
  CASE WHEN ( ((kemt.ffg_comply / kemt.ffg_jml_ps) * 100) / 97.40 * 100 ) < 100 THEN 'not_comply' END AS ffg_comply,

  kemt.ttr_ffg_comply AS tfg_com,
  kemt.ttr_ffg_ggn_wsa AS tfg_tot,
  ROUND((kemt.ttr_ffg_comply / kemt.ttr_ffg_ggn_wsa) * 100, 2) AS tfg,
  ROUND(
    ((kemt.ttr_ffg_comply / kemt.ttr_ffg_ggn_wsa) * 100) / 80.81 * 100
  ,2) AS ach_tfg,
  CASE WHEN ( ((kemt.ttr_ffg_comply / kemt.ttr_ffg_ggn_wsa) * 100) / 80.81 * 100 ) < 100 THEN 'not_comply' END AS tfg_comply

FROM region_ccm rcm
LEFT JOIN kpi_endstate_monthly_tif kemt 
  ON kemt.lokasi = rcm.sto
WHERE rcm.piloting IS NOT NULL


UNION ALL 

SELECT
  rcm.piloting,
  'TOTAL' AS sto,

  SUM(kemt.tti_comply) AS tti_com,
  SUM(kemt.tti_ps_ih) AS tti_tot,
  ROUND((SUM(kemt.tti_comply) / SUM(kemt.tti_ps_ih)) * 100, 2) AS ttic,
  ROUND(
    ((kemt.tti_comply / kemt.tti_ps_ih) * 100) / 92.72 * 100
  ,2) AS ach_ttic,
  CASE WHEN ( ((SUM(kemt.tti_comply) / SUM(kemt.tti_ps_ih)) * 100) / 92.72 * 100 ) < 100 THEN 'not_comply' END AS tti_comply,

  SUM(kemt.ffg_comply) AS ffg_com,
  SUM(kemt.ffg_jml_ps) AS ffg_tot,
  ROUND((SUM(kemt.ffg_comply) / SUM(kemt.ffg_jml_ps)) * 100, 2) AS ffg,
  ROUND(
    ((kemt.ffg_comply / kemt.ffg_jml_ps) * 100) / 92.72 * 100
  ,2) AS ach_ffg,
  CASE WHEN ( ((SUM(kemt.ffg_comply) / SUM(kemt.ffg_jml_ps)) * 100) / 92.72 * 100 ) < 100 THEN 'not_comply' END AS ffg_comply,

  SUM(kemt.ttr_ffg_comply) AS tfg_com,
  SUM(kemt.ttr_ffg_ggn_wsa) AS tfg_tot,
  ROUND((SUM(kemt.ttr_ffg_comply) / SUM(kemt.ttr_ffg_ggn_wsa)) * 100, 2) AS tfg,
  ROUND(
    ((SUM(kemt.ttr_ffg_comply) / SUM(kemt.ttr_ffg_ggn_wsa)) * 100) / 92.72 * 100
  ,2) AS ach_tfg,
  CASE WHEN ( ((SUM(kemt.ttr_ffg_comply) / SUM(kemt.ttr_ffg_ggn_wsa)) * 100) / 92.72 * 100 ) < 100 THEN 'not_comply' END AS tfg_comply
  
FROM region_ccm rcm
LEFT JOIN kpi_endstate_monthly_tif kemt ON kemt.lokasi = rcm.sto
WHERE rcm.piloting IS NOT NULL
GROUP BY rcm.piloting










