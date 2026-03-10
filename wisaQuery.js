const mysql = require('mysql2/promise');
const fs = require('fs');
const { insertDate } = require('./currentDate.js');
const pool = require('./connection.js');

async function main() {
  // --- Query pertama ---
  const tgl = insertDate;
  const bulan = tgl.split('-')[1]; // "09"
  const bln = `m${bulan}`; // "${bln}"
  const month = parseInt(tgl.split('-')[1], 10); // 9
  const uic = 'operasional';

  console.log(bln);
  const sqltif = `
    SELECT 'ASR-ENT-Assurance Guarantee DATIN' AS kpi, sc_lokasi.witel AS lokasi, sugar_datin.jenis AS Area, sugar_datin.real AS Realisasi FROM sc_lokasi LEFT JOIN sugar_datin ON sc_lokasi.witel = sugar_datin.treg AND sugar_datin.tgl = '${tgl}' AND sugar_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Assurance Guarantee WiFi' AS kpi, sc_lokasi.witel AS lokasi, sugar_wifi.jenis AS Area, sugar_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN sugar_wifi ON sc_lokasi.witel = sugar_wifi.regional AND sugar_wifi.tgl = '${tgl}' AND sugar_wifi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Assurance Guarantee HSI' AS kpi, sc_lokasi.witel AS lokasi, hsi_sugar.jenis AS Area, hsi_sugar.real AS Realisasi FROM sc_lokasi LEFT JOIN hsi_sugar ON sc_lokasi.witel = hsi_sugar.treg AND hsi_sugar.tgl = '${tgl}' AND hsi_sugar.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-TTI Compliance HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.ttic AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-Fulfillment Guarantee HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-PS to PI Ratio HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.pspi AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-TTR Fulfillment Guarantee 3 jam HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.ttr_ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-WHF-TTI Compliance' AS kpi, sc_lokasi.witel AS lokasi, ff_ih.jenis AS Area, ff_ih.ttic AS Realisasi FROM sc_lokasi LEFT JOIN ff_ih ON sc_lokasi.witel = ff_ih.lokasi AND ff_ih.tgl = '${tgl}' AND ff_ih.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-WHF-Fulfillment Guarantee Compliance' AS kpi, sc_lokasi.witel AS lokasi, ff_ih.jenis AS Area, ff_ih.ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_ih ON sc_lokasi.witel = ff_ih.lokasi AND ff_ih.tgl = '${tgl}' AND ff_ih.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-WHF-TTR Fulfillment Guarantee Compliance' AS kpi, sc_lokasi.witel AS lokasi, ff_ih.jenis AS Area, ff_ih.ttr_ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_ih ON sc_lokasi.witel = ff_ih.lokasi AND ff_ih.tgl = '${tgl}' AND ff_ih.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-TTR Compliance K1 DATIN 1.5 Jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_datin.jenis AS Area, ttr_datin.k1 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_datin ON sc_lokasi.witel = ttr_datin.treg AND ttr_datin.tgl = '${tgl}' AND ttr_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-TTR Compliance K2 dan K1 Repair DATIN 3.6 Jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_datin.jenis AS Area, ttr_datin.k2 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_datin ON sc_lokasi.witel = ttr_datin.treg AND ttr_datin.tgl = '${tgl}' AND ttr_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-TTR Compliance K3 DATIN 7.2 Jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_datin.jenis AS Area, ttr_datin.k3 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_datin ON sc_lokasi.witel = ttr_datin.treg AND ttr_datin.tgl = '${tgl}' AND ttr_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-TTR Compliance WiFi' AS kpi, sc_lokasi.witel AS lokasi, ttr_wifi.jenis AS Area, ttr_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttr_wifi ON sc_lokasi.witel = ttr_wifi.regional AND ttr_wifi.tgl = '${tgl}' AND ttr_wifi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHF-Assurance Guarantee' AS kpi, sc_lokasi.witel AS lokasi, wsa_sugar.lokasi AS Area, wsa_sugar.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_sugar ON sc_lokasi.witel = wsa_sugar.witel AND wsa_sugar.tgl = '${tgl}' AND wsa_sugar.lokasi IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHF-TTR Comply 3H (D.V)' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttr3.lokasi AS Area, wsa_ttr3.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttr3 ON sc_lokasi.witel = wsa_ttr3.witel AND wsa_ttr3.tgl = '${tgl}' AND wsa_ttr3.lokasi IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHF-TTR Comply 6H (P)' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttr6.lokasi AS Area, wsa_ttr6.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttr6 ON sc_lokasi.witel = wsa_ttr6.witel AND wsa_ttr6.tgl = '${tgl}' AND wsa_ttr6.lokasi IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHF-TTR Comply 36H (Non HVC)' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttr36.lokasi AS Area, wsa_ttr36.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttr36 ON sc_lokasi.witel = wsa_ttr36.witel AND wsa_ttr36.tgl = '${tgl}' AND wsa_ttr36.lokasi IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHF-TTR Comply 3H Manja' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttrmanja.lokasi AS Area, wsa_ttrmanja.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttrmanja ON sc_lokasi.witel = wsa_ttrmanja.witel AND wsa_ttrmanja.tgl = '${tgl}' AND wsa_ttrmanja.lokasi IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Compliance-Time to Recover IndiBiz-4 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_indibiz.jenis AS Area, ttr_indibiz.real_1 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_indibiz ON sc_lokasi.witel = ttr_indibiz.treg AND ttr_indibiz.tgl = '${tgl}' AND ttr_indibiz.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Compliance-Time to Recover IndiBiz-24 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_indibiz.jenis AS Area, ttr_indibiz.real_2 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_indibiz ON sc_lokasi.witel = ttr_indibiz.treg AND ttr_indibiz.tgl = '${tgl}' AND ttr_indibiz.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Compliance-Time to Recover IndiHome Reseller-6 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_reseller.jenis AS Area, ttr_reseller.real_1 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_reseller ON sc_lokasi.witel = ttr_reseller.treg AND ttr_reseller.tgl = '${tgl}' AND ttr_reseller.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Compliance-Time to Recover IndiHome Reseller-36 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_reseller.jenis AS Area, ttr_reseller.real_2 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_reseller ON sc_lokasi.witel = ttr_reseller.treg AND ttr_reseller.tgl = '${tgl}' AND ttr_reseller.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-MTTR DWDM Protected' AS kpi, sc_lokasi.witel AS lokasi, ttr_dwdm.jenis AS Area, CASE WHEN ttr_dwdm.ach = 100 AND ttr_dwdm.real = 0 THEN '-' ELSE CAST(ttr_dwdm.real AS CHAR) END AS Realisasi FROM sc_lokasi LEFT JOIN ttr_dwdm ON sc_lokasi.witel = ttr_dwdm.treg AND ttr_dwdm.tgl = '${tgl}' AND ttr_dwdm.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-MTTR SIP TRUNK' AS kpi, sc_lokasi.witel AS lokasi, ttr_siptrunk.jenis AS Area, CASE WHEN ttr_siptrunk.ach = 100 AND ttr_siptrunk.real = 0 THEN '-' ELSE CAST(ttr_siptrunk.real AS CHAR) END AS Realisasi FROM sc_lokasi LEFT JOIN ttr_siptrunk ON sc_lokasi.witel = ttr_siptrunk.treg AND ttr_siptrunk.tgl = '${tgl}' AND ttr_siptrunk.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ENT-WiFi Revitalization' AS kpi, sc_lokasi.witel AS lokasi, wifi_revi.jenis AS Area, wifi_revi.comply AS Realisasi FROM sc_lokasi LEFT JOIN wifi_revi ON sc_lokasi.witel = wifi_revi.regional AND wifi_revi.tgl = '${tgl}' AND wifi_revi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-Fulfillment Guarantee Non HSI' AS kpi, sc_lokasi.witel AS lokasi, ffg_non_hsi.jenis AS Area, ffg_non_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ffg_non_hsi ON sc_lokasi.witel = ffg_non_hsi.regional AND ffg_non_hsi.tgl = '${tgl}' AND ffg_non_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-TTD Compliance Datin & SIP Trunk' AS kpi, sc_lokasi.witel AS lokasi, ttd_non_hsi.jenis AS Area, ttd_non_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttd_non_hsi ON sc_lokasi.witel = ttd_non_hsi.regional AND ttd_non_hsi.tgl = '${tgl}' AND ttd_non_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-TTR Fulfillment Guarantee 3 Jam Non HSI' AS kpi, sc_lokasi.witel AS lokasi, ttr_ffg_non_hsi.jenis AS Area, ttr_ffg_non_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttr_ffg_non_hsi ON sc_lokasi.witel = ttr_ffg_non_hsi.regional AND ttr_ffg_non_hsi.tgl = '${tgl}' AND ttr_ffg_non_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ALL-Service Availability-WiFi' AS kpi, sc_lokasi.witel AS lokasi, av_wifi_all.jenis AS Area, av_wifi_all.comply AS Realisasi FROM sc_lokasi LEFT JOIN av_wifi_all ON sc_lokasi.witel = av_wifi_all.regional AND av_wifi_all.tgl = '${tgl}' AND av_wifi_all.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT- Compliance Underspec Warranty Guarantee HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.unspec AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-TTD Compliance WIFI' AS kpi, sc_lokasi.witel AS lokasi, ttd_wifi.jenis AS Area, ttd_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttd_wifi ON sc_lokasi.witel = ttd_wifi.regional AND ttd_wifi.tgl = '${tgl}' AND ttd_wifi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-Provcomp Ratio  DATIN & SIP Trunk' AS kpi, sc_lokasi.witel AS lokasi, provcomp.jenis AS Area, provcomp.comply AS Realisasi FROM sc_lokasi LEFT JOIN provcomp ON sc_lokasi.witel = provcomp.regional AND provcomp.tgl = '${tgl}' AND provcomp.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT-Provcomp Ratio WiFi' AS kpi, sc_lokasi.witel AS lokasi, provcomp_wifi.jenis AS Area, provcomp_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN provcomp_wifi ON sc_lokasi.witel = provcomp_wifi.regional AND provcomp_wifi.tgl = '${tgl}' AND provcomp_wifi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'FFM-ENT- Compliance Underspec Warranty Guarantee Non HSI' AS kpi, sc_lokasi.witel AS lokasi, unspec_warranty.jenis AS Area, unspec_warranty.comply AS Realisasi FROM sc_lokasi LEFT JOIN unspec_warranty ON sc_lokasi.witel = unspec_warranty.regional AND unspec_warranty.tgl = '${tgl}' AND unspec_warranty.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHF-Service Availability' AS kpi, sc_lokasi.witel AS lokasi, wsa_service.lokasi AS Area, wsa_service.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_service ON sc_lokasi.witel = wsa_service.witel AND wsa_service.tgl = '${tgl}' AND wsa_service.lokasi IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Q gangguan - HSI' AS kpi, sc_lokasi.witel AS lokasi, q_hsi.jenis AS Area, q_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN q_hsi ON sc_lokasi.witel = q_hsi.regional AND q_hsi.tgl = '${tgl}' AND q_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-ENT-Q gangguan DATIN' AS kpi, sc_lokasi.witel AS lokasi, q_datin.jenis AS Area, q_datin.comply AS Realisasi FROM sc_lokasi LEFT JOIN q_datin ON sc_lokasi.witel = q_datin.regional AND q_datin.tgl = '${tgl}' AND q_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ENT-Underspec DATIN' AS kpi, sc_lokasi.witel AS lokasi, unspec_datin.jenis AS Area, unspec_datin.comply AS Realisasi FROM sc_lokasi LEFT JOIN unspec_datin ON sc_lokasi.witel = unspec_datin.regional AND unspec_datin.tgl = '${tgl}' AND unspec_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ENT-Underspec HSI EBIS' AS kpi, sc_lokasi.witel AS lokasi, unspec_hsi.jenis AS Area, unspec_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN unspec_hsi ON sc_lokasi.witel = unspec_hsi.regional AND unspec_hsi.tgl = '${tgl}' AND unspec_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ENT-% Closed SQM? EBIS Datin' AS kpi, sc_lokasi.witel AS lokasi, sqm_datin.jenis AS Area, sqm_datin.comply AS Realisasi FROM sc_lokasi LEFT JOIN sqm_datin ON sc_lokasi.witel = sqm_datin.regional AND sqm_datin.tgl = '${tgl}' AND sqm_datin.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ENT-% Closed SQM? HSI' AS kpi, sc_lokasi.witel AS lokasi, sqm_hsi.jenis AS Area, sqm_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN sqm_hsi ON sc_lokasi.witel = sqm_hsi.regional AND sqm_hsi.tgl = '${tgl}' AND sqm_hsi.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ONM-ENT-TTR Compliance Ticket Non-Numbering' AS kpi, sc_lokasi.witel AS lokasi, ttr_non_numbering.jenis AS Area, ttr_non_numbering.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttr_non_numbering ON sc_lokasi.witel = ttr_non_numbering.regional AND ttr_non_numbering.tgl = '${tgl}' AND ttr_non_numbering.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHM-MTTRi Critical Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.critical AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHM-MTTRi Low Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.low AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHM-MTTRi Major Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.major AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHM-MTTRi Minor Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.minor AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
    UNION ALL
    SELECT 'ASR-WHM-MTTRi Premium Site Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.premium AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('tif') WHERE sc_lokasi.reg IN ('tif') AND sc_lokasi.witel = 'TERRITORY 03'
  `;

  const sqlccm = `
      SELECT 'ASR-ENT-Assurance Guarantee DATIN' AS kpi, sc_lokasi.witel AS lokasi, sugar_datin.jenis AS Area, sugar_datin.real AS Realisasi FROM sc_lokasi LEFT JOIN sugar_datin ON sc_lokasi.witel = sugar_datin.treg AND sugar_datin.tgl = '${tgl}' AND sugar_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND sugar_datin.real IS NOT NULL
      UNION ALL

      SELECT 'ASR-ENT-Assurance Guarantee WiFi' AS kpi, sc_lokasi.witel AS lokasi, sugar_wifi.jenis AS Area, sugar_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN sugar_wifi ON sc_lokasi.witel = sugar_wifi.regional AND sugar_wifi.tgl = '${tgl}' AND sugar_wifi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND sugar_wifi.comply IS NOT NULL
      UNION ALL

      SELECT 'ASR-ENT-Assurance Guarantee HSI' AS kpi, sc_lokasi.witel AS lokasi, hsi_sugar.jenis AS Area, hsi_sugar.real AS Realisasi FROM sc_lokasi LEFT JOIN hsi_sugar ON sc_lokasi.witel = hsi_sugar.treg AND hsi_sugar.tgl = '${tgl}' AND hsi_sugar.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND hsi_sugar.real IS NOT NULL
      UNION ALL

      SELECT 'FFM-ENT-TTI Compliance HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.ttic AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_hsi.ttic IS NOT NULL
      UNION ALL

      SELECT 'FFM-ENT-Fulfillment Guarantee HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_hsi.ffg IS NOT NULL
      UNION ALL

      SELECT 'FFM-ENT-PS to PI Ratio HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.pspi AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_hsi.pspi IS NOT NULL
      UNION ALL

      SELECT 'FFM-ENT-TTR Fulfillment Guarantee 3 jam HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.ttr_ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_hsi.ttr_ffg IS NOT NULL
      UNION ALL

      SELECT 'FFM-WHF-TTI Compliance' AS kpi, sc_lokasi.witel AS lokasi, ff_ih.jenis AS Area, ff_ih.ttic AS Realisasi FROM sc_lokasi LEFT JOIN ff_ih ON sc_lokasi.witel = ff_ih.lokasi AND ff_ih.tgl = '${tgl}' AND ff_ih.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_ih.ttic IS NOT NULL
      UNION ALL

      SELECT 'FFM-WHF-Fulfillment Guarantee Compliance' AS kpi, sc_lokasi.witel AS lokasi, ff_ih.jenis AS Area, ff_ih.ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_ih ON sc_lokasi.witel = ff_ih.lokasi AND ff_ih.tgl = '${tgl}' AND ff_ih.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_ih.ffg IS NOT NULL
      UNION ALL

      SELECT 'FFM-WHF-TTR Fulfillment Guarantee Compliance' AS kpi, sc_lokasi.witel AS lokasi, ff_ih.jenis AS Area, ff_ih.ttr_ffg AS Realisasi FROM sc_lokasi LEFT JOIN ff_ih ON sc_lokasi.witel = ff_ih.lokasi AND ff_ih.tgl = '${tgl}' AND ff_ih.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_ih.ttr_ffg IS NOT NULL
      UNION ALL

      SELECT 'ASR-ENT-TTR Compliance K1 DATIN 1.5 Jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_datin.jenis AS Area, ttr_datin.k1 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_datin ON sc_lokasi.witel = ttr_datin.treg AND ttr_datin.tgl = '${tgl}' AND ttr_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_datin.k1 IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-TTR Compliance K2 dan K1 Repair DATIN 3.6 Jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_datin.jenis AS Area, ttr_datin.k2 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_datin ON sc_lokasi.witel = ttr_datin.treg AND ttr_datin.tgl = '${tgl}' AND ttr_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_datin.k2 IS NOT NULL
      UNION ALL 
      SELECT 'ASR-ENT-TTR Compliance K3 DATIN 7.2 Jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_datin.jenis AS Area, ttr_datin.k3 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_datin ON sc_lokasi.witel = ttr_datin.treg AND ttr_datin.tgl = '${tgl}' AND ttr_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_datin.k3 IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-TTR Compliance WiFi' AS kpi, sc_lokasi.witel AS lokasi, ttr_wifi.jenis AS Area, ttr_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttr_wifi ON sc_lokasi.witel = ttr_wifi.regional AND ttr_wifi.tgl = '${tgl}' AND ttr_wifi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_wifi.comply IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHF-Assurance Guarantee' AS kpi, sc_lokasi.witel AS lokasi, wsa_sugar.lokasi AS Area, wsa_sugar.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_sugar ON sc_lokasi.witel = wsa_sugar.witel AND wsa_sugar.tgl = '${tgl}' AND wsa_sugar.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wsa_sugar.${bln} IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHF-TTR Comply 3H (D.V)' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttr3.lokasi AS Area, wsa_ttr3.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttr3 ON sc_lokasi.witel = wsa_ttr3.witel AND wsa_ttr3.tgl = '${tgl}' AND wsa_ttr3.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wsa_ttr3.${bln} IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHF-TTR Comply 6H (P)' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttr6.lokasi AS Area, wsa_ttr6.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttr6 ON sc_lokasi.witel = wsa_ttr6.witel AND wsa_ttr6.tgl = '${tgl}' AND wsa_ttr6.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wsa_ttr6.${bln} IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHF-TTR Comply 36H (Non HVC)' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttr36.lokasi AS Area, wsa_ttr36.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttr36 ON sc_lokasi.witel = wsa_ttr36.witel AND wsa_ttr36.tgl = '${tgl}' AND wsa_ttr36.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wsa_ttr36.${bln} IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHF-TTR Comply 3H Manja' AS kpi, sc_lokasi.witel AS lokasi, wsa_ttrmanja.lokasi AS Area, wsa_ttrmanja.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_ttrmanja ON sc_lokasi.witel = wsa_ttrmanja.witel AND wsa_ttrmanja.tgl = '${tgl}' AND wsa_ttrmanja.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wsa_ttrmanja.${bln} IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-Compliance-Time to Recover IndiBiz-4 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_indibiz.jenis AS Area, ttr_indibiz.real_1 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_indibiz ON sc_lokasi.witel = ttr_indibiz.treg AND ttr_indibiz.tgl = '${tgl}' AND ttr_indibiz.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_indibiz.real_1 IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-Compliance-Time to Recover IndiBiz-24 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_indibiz.jenis AS Area, ttr_indibiz.real_2 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_indibiz ON sc_lokasi.witel = ttr_indibiz.treg AND ttr_indibiz.tgl = '${tgl}' AND ttr_indibiz.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_indibiz.real_2 IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-Compliance-Time to Recover IndiHome Reseller-6 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_reseller.jenis AS Area, ttr_reseller.real_1 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_reseller ON sc_lokasi.witel = ttr_reseller.treg AND ttr_reseller.tgl = '${tgl}' AND ttr_reseller.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_reseller.real_1 IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-Compliance-Time to Recover IndiHome Reseller-36 jam' AS kpi, sc_lokasi.witel AS lokasi, ttr_reseller.jenis AS Area, ttr_reseller.real_2 AS Realisasi FROM sc_lokasi LEFT JOIN ttr_reseller ON sc_lokasi.witel = ttr_reseller.treg AND ttr_reseller.tgl = '${tgl}' AND ttr_reseller.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_reseller.real_2 IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-MTTR DWDM Protected' AS kpi, sc_lokasi.witel AS lokasi, ttr_dwdm.jenis AS Area, CASE WHEN ttr_dwdm.ach = 100 AND ttr_dwdm.real = 0 THEN '-' ELSE CAST(ttr_dwdm.real AS CHAR) END AS Realisasi FROM sc_lokasi LEFT JOIN ttr_dwdm ON sc_lokasi.witel = ttr_dwdm.treg AND ttr_dwdm.tgl = '${tgl}' AND ttr_dwdm.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_dwdm.real IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-MTTR SIP TRUNK' AS kpi, sc_lokasi.witel AS lokasi, ttr_siptrunk.jenis AS Area, CASE WHEN ttr_siptrunk.ach = 100 AND ttr_siptrunk.real = 0 THEN '-' ELSE CAST(ttr_siptrunk.real AS CHAR) END AS Realisasi FROM sc_lokasi LEFT JOIN ttr_siptrunk ON sc_lokasi.witel = ttr_siptrunk.treg AND ttr_siptrunk.tgl = '${tgl}' AND ttr_siptrunk.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_siptrunk.real IS NOT NULL
      UNION ALL
      SELECT 'ONM-ENT-WiFi Revitalization' AS kpi, sc_lokasi.witel AS lokasi, wifi_revi.jenis AS Area, wifi_revi.comply AS Realisasi FROM sc_lokasi LEFT JOIN wifi_revi ON sc_lokasi.witel = wifi_revi.regional AND wifi_revi.tgl = '${tgl}' AND wifi_revi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wifi_revi.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT-Fulfillment Guarantee Non HSI' AS kpi, sc_lokasi.witel AS lokasi, ffg_non_hsi.jenis AS Area, ffg_non_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ffg_non_hsi ON sc_lokasi.witel = ffg_non_hsi.regional AND ffg_non_hsi.tgl = '${tgl}' AND ffg_non_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ffg_non_hsi.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT-TTD Compliance Datin & SIP Trunk' AS kpi, sc_lokasi.witel AS lokasi, ttd_non_hsi.jenis AS Area, ttd_non_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttd_non_hsi ON sc_lokasi.witel = ttd_non_hsi.regional AND ttd_non_hsi.tgl = '${tgl}' AND ttd_non_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttd_non_hsi.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT-TTR Fulfillment Guarantee 3 Jam Non HSI' AS kpi, sc_lokasi.witel AS lokasi, ttr_ffg_non_hsi.jenis AS Area, ttr_ffg_non_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttr_ffg_non_hsi ON sc_lokasi.witel = ttr_ffg_non_hsi.regional AND ttr_ffg_non_hsi.tgl = '${tgl}' AND ttr_ffg_non_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_ffg_non_hsi.comply IS NOT NULL
      UNION ALL
      SELECT 'ONM-ALL-Service Availability-WiFi' AS kpi, sc_lokasi.witel AS lokasi, av_wifi_all.jenis AS Area, av_wifi_all.comply AS Realisasi FROM sc_lokasi LEFT JOIN av_wifi_all ON sc_lokasi.witel = av_wifi_all.regional AND av_wifi_all.tgl = '${tgl}' AND av_wifi_all.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND av_wifi_all.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT- Compliance Underspec Warranty Guarantee HSI' AS kpi, sc_lokasi.witel AS lokasi, ff_hsi.jenis AS Area, ff_hsi.unspec AS Realisasi FROM sc_lokasi LEFT JOIN ff_hsi ON sc_lokasi.witel = ff_hsi.lokasi AND ff_hsi.tgl = '${tgl}' AND ff_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ff_hsi.unspec IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT-TTD Compliance WIFI' AS kpi, sc_lokasi.witel AS lokasi, ttd_wifi.jenis AS Area, ttd_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttd_wifi ON sc_lokasi.witel = ttd_wifi.regional AND ttd_wifi.tgl = '${tgl}' AND ttd_wifi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttd_wifi.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT-Provcomp Ratio  DATIN & SIP Trunk' AS kpi, sc_lokasi.witel AS lokasi, provcomp.jenis AS Area, provcomp.comply AS Realisasi FROM sc_lokasi LEFT JOIN provcomp ON sc_lokasi.witel = provcomp.regional AND provcomp.tgl = '${tgl}' AND provcomp.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND provcomp.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT-Provcomp Ratio WiFi' AS kpi, sc_lokasi.witel AS lokasi, provcomp_wifi.jenis AS Area, provcomp_wifi.comply AS Realisasi FROM sc_lokasi LEFT JOIN provcomp_wifi ON sc_lokasi.witel = provcomp_wifi.regional AND provcomp_wifi.tgl = '${tgl}' AND provcomp_wifi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND provcomp_wifi.comply IS NOT NULL
      UNION ALL
      SELECT 'FFM-ENT- Compliance Underspec Warranty Guarantee Non HSI' AS kpi, sc_lokasi.witel AS lokasi, unspec_warranty.jenis AS Area, unspec_warranty.comply AS Realisasi FROM sc_lokasi LEFT JOIN unspec_warranty ON sc_lokasi.witel = unspec_warranty.regional AND unspec_warranty.tgl = '${tgl}' AND unspec_warranty.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND unspec_warranty.comply IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHF-Service Availability' AS kpi, sc_lokasi.witel AS lokasi, wsa_service.lokasi AS Area, wsa_service.${bln} AS Realisasi FROM sc_lokasi LEFT JOIN wsa_service ON sc_lokasi.witel = wsa_service.witel AND wsa_service.tgl = '${tgl}' AND wsa_service.lokasi IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND wsa_service.${bln} IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-Q gangguan - HSI' AS kpi, sc_lokasi.witel AS lokasi, q_hsi.jenis AS Area, q_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN q_hsi ON sc_lokasi.witel = q_hsi.regional AND q_hsi.tgl = '${tgl}' AND q_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND q_hsi.comply IS NOT NULL
      UNION ALL
      SELECT 'ASR-ENT-Q gangguan DATIN' AS kpi, sc_lokasi.witel AS lokasi, q_datin.jenis AS Area, q_datin.comply AS Realisasi FROM sc_lokasi LEFT JOIN q_datin ON sc_lokasi.witel = q_datin.regional AND q_datin.tgl = '${tgl}' AND q_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND q_datin.comply IS NOT NULL
      UNION ALL
      SELECT 'ONM-ENT-Underspec HSI EBIS' AS kpi, sc_lokasi.witel AS lokasi, unspec_hsi.jenis AS Area, unspec_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN unspec_hsi ON sc_lokasi.witel = unspec_hsi.regional AND unspec_hsi.tgl = '${tgl}' AND unspec_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND unspec_hsi.comply IS NOT NULL
      UNION ALL
      SELECT 'ONM-ENT-Underspec DATIN' AS kpi, sc_lokasi.witel AS lokasi, unspec_datin.jenis AS Area, unspec_datin.comply AS Realisasi FROM sc_lokasi LEFT JOIN unspec_datin ON sc_lokasi.witel = unspec_datin.regional AND unspec_datin.tgl = '${tgl}' AND unspec_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND unspec_datin.comply IS NOT NULL
      UNION ALL
      SELECT 'ONM-ENT-% Closed SQM? EBIS Datin' AS kpi, sc_lokasi.witel AS lokasi, sqm_datin.jenis AS Area, sqm_datin.comply AS Realisasi FROM sc_lokasi LEFT JOIN sqm_datin ON sc_lokasi.witel = sqm_datin.regional AND sqm_datin.tgl = '${tgl}' AND sqm_datin.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND sqm_datin.comply IS NOT NULL
      UNION ALL
      SELECT 'ONM-ENT-% Closed SQM? HSI' AS kpi, sc_lokasi.witel AS lokasi, sqm_hsi.jenis AS Area, sqm_hsi.comply AS Realisasi FROM sc_lokasi LEFT JOIN sqm_hsi ON sc_lokasi.witel = sqm_hsi.regional AND sqm_hsi.tgl = '${tgl}' AND sqm_hsi.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND sqm_hsi.comply IS NOT NULL
      UNION ALL
      SELECT 'ONM-ENT-TTR Compliance Ticket Non-Numbering' AS kpi, sc_lokasi.witel AS lokasi, ttr_non_numbering.jenis AS Area, ttr_non_numbering.comply AS Realisasi FROM sc_lokasi LEFT JOIN ttr_non_numbering ON sc_lokasi.witel = ttr_non_numbering.regional AND ttr_non_numbering.tgl = '${tgl}' AND ttr_non_numbering.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND ttr_non_numbering.comply IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHM-MTTRi Critical Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.critical AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND mttr_mso.critical IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHM-MTTRi Low Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.low AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND mttr_mso.low IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHM-MTTRi Major Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.major AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND mttr_mso.major IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHM-MTTRi Minor Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.minor AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND mttr_mso.minor IS NOT NULL
      UNION ALL
      SELECT 'ASR-WHM-MTTRi Premium Site Compliance' AS kpi, sc_lokasi.witel AS lokasi, mttr_mso.jenis AS Area, mttr_mso.premium AS Realisasi FROM sc_lokasi LEFT JOIN mttr_mso ON sc_lokasi.witel = mttr_mso.regional AND mttr_mso.tgl = '${tgl}' AND mttr_mso.jenis IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') WHERE sc_lokasi.reg IN ('area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm') AND mttr_mso.premium IS NOT NULL
  `;

  const [tif] = await pool.execute(sqltif);
  const [ccm] = await pool.execute(sqlccm);

  function simpanCSV(dataRows, namaFile) {
    if (dataRows.length === 0) {
      console.log(`⚠️ Tidak ada data ditemukan untuk ${namaFile}`);
      return;
    }

    // Proses dan normalisasi setiap baris data
    dataRows = dataRows.map((row) => {
      let newRow;
      const lokasi_dis = ['D_BALI', 'D_MALANG', 'D_NUSRA', 'D_SEMARANG', 'D_SIDOARJO', 'D_SOLO', 'D_SURAMADU', 'D_YOGYAKARTA'];

      if (namaFile === 'tif.csv') {
        if (row.lokasi === 'NUSA TENGGARA') {
          newRow = { ...row, lokasi: 'D_NUSRA' };
        } else if (row.lokasi?.includes('TERRITORY')) {
          newRow = { ...row, lokasi: row.lokasi.replace('TERRITORY', 'TIF') };
        } else {
          newRow = { ...row, lokasi: `D_${row.lokasi}` };
        }

        if (lokasi_dis.includes(newRow.lokasi)) {
          newRow = { ...newRow, Area: 'district' };
        }
      } else if (namaFile === 'district.csv') {
        const region4 = ['KUDUS', 'MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SOLO', 'YOGYAKARTA'];
        const nasional = ['REGIONAL 01', 'REGIONAL 02', 'REGIONAL 03', 'REGIONAL 04', 'REGIONAL 05', 'REGIONAL 06', 'REGIONAL 07'];

        newRow = {
          ...row,
          Area: region4.includes(row.lokasi) ? 'witel' : nasional.includes(row.lokasi) ? 'nas' : 'witel',
        };

        if (row.lokasi.includes('SURABAYA')) {
          newRow.lokasi = row.lokasi.replace('SURABAYA', 'SBY');
        }
      } else {
        const area_ccm = ['BALI NUSRA', 'JATENG DIY', 'JAWA TIMUR'];
        const balnus_ccm = ['DENPASAR', 'FLORES', 'KUPANG', 'MATARAM'];
        const jateng_ccm = ['MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SURAKARTA', 'YOGYAKARTA'];
        const jatim_ccm = ['JEMBER', 'LAMONGAN', 'MADIUN', 'MALANG', 'SIDOARJO', 'SURABAYA'];

        if (area_ccm.includes(row.lokasi)) {
          newRow = { ...row, Area: 'area_ccm' };
        } else {
          newRow = { ...row, Area: 'branch' };
        }
      }

      // Pastikan Realisasi selalu ada (isi angka atau strip)
      if (row.Realisasi === null || row.Realisasi === undefined || row.Realisasi === '') {
        newRow.Realisasi = '-';
      }

      // Susun ulang kolom dan tambahkan insert_at
      return {
        kpi: newRow.kpi ?? '',
        lokasi: newRow.Area ?? '',
        Area: newRow.lokasi ?? '',
        Realisasi: newRow.Realisasi ?? '-',
        insert_at: insertDate,
        bulan: month,
        uic: uic,
      };
    });

    // Buat header CSV
    const headers = Object.keys(dataRows[0])
      .map((h) => `"${h.replace(/"/g, '""')}"`)
      .join(',');

    // Buat isi CSV
    const data = dataRows
      .map((row) =>
        Object.entries(row)
          .map(([key, val]) => {
            if (key === 'Realisasi') {
              if (['-', '', null, 0, '0', '0.0'].includes(val)) {
                return '"-"';
              }
              const num = parseFloat(val);
              return isNaN(num) ? '"-"' : num.toFixed(2);
            }

            if (key === 'bulan') {
              return month; // angka langsung
            }

            const safeVal = String(val).replace(/"/g, '""').replace(/\r?\n/g, ' ');
            return `"${safeVal}"`;
          })
          .join(','),
      )
      .join('\n');

    const csv = `${headers}\n${data}`;

    // Simpan file CSV
    const dir = 'loaded_file/msa_upload';
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(`${dir}/${namaFile}`, csv, 'utf-8');
    console.log(`✅ File ${namaFile} berhasil dibuat.`);
  }

  // Contoh pemakaian
  simpanCSV(tif, 'tif.csv');
  simpanCSV(ccm, 'ccm.csv');
  await pool.end();
}

main().catch(console.error);
