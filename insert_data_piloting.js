const connection = require('./connection');
const { insertDate } = require('./currentDate');

const currentDate = insertDate;
const yearMonth = currentDate.slice(0, 7).replace('-', '');
const likePeriode = `%${yearMonth}%`;

const dateObj = new Date(insertDate);
const month = String(dateObj.getMonth() + 1).padStart(2, '0');

console.log(month, yearMonth, likePeriode);

// Promisify query
function queryAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    connection.query(sql, params, (err, results) => {
      if (err) reject(err);
      else resolve(results);
    });
  });
}

// Fungsi hapus data berdasarkan tanggal hari ini
async function deleteExistingData() {
  // const tableForDelete = ['piloting_n2n_cx', 'piloting_n2n_cx_service_sugar', 'piloting_fulfillment_cx'];
  const tableForDelete = ['piloting_fulfillment_cx'];
  for (const table of tableForDelete) {
    const sql = `
      DELETE FROM ${table}
      WHERE periode = ?
    `;
    await queryAsync(sql, [yearMonth]);
  }
}

async function piloting_ttr() {
  console.log(currentDate, yearMonth, likePeriode);

  const sql = `
    INSERT INTO perf_tif.piloting_n2n_cx (periode, tgl, piloting, sto, com3, tot3, real_com3, ach_com3, comply_3, com6, tot6, real_com6, ach_com6, comply_6, com12, tot12, real_com12, ach_com12, comply_12, com24, tot24, real_com24, ach_com24, comply_24, com36, tot36, real_com36, ach_com36, comply_36)
    
    SELECT *
    FROM (
        /* ================= DETAIL PER STO ================= */
        SELECT
          ? AS periode,
          ? AS tgl,
          rcm.piloting AS piloting,
          dwc.STO AS sto,

          ---------------- COMPLY 3 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS com3,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS tot3,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2)  AS real_com3,
          
          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2) / 95.25) * 100 ,2) AS ach_com3,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2) / 95.25) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_3,

          ---------------- COMPLY 6 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS com6,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS tot6,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2)  AS real_com6,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2) / 95) * 100 ,2) AS ach_com6,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2) / 95) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_6,

          ---------------- COMPLY 12 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS com12,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS tot12,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2)  AS real_com12,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2) / 83) * 100 ,2) AS ach_com12,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2) / 83) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_12,

          ---------------- COMPLY 24 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS com24,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS tot24,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2)  AS real_com24,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2) / 99) * 100 ,2) AS ach_com24,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2) / 99) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_24,

          ---------------- COMPLY 36 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS com36,
          COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS tot36,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2)  AS real_com36,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2) / 99.5) * 100 ,2) AS ach_com36,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2) / 99.5) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_36

        FROM perf_tif.region_ccm rcm
        LEFT JOIN nonatero_download.Detil_WSA_CLOSE dwc
          ON dwc.STO = rcm.STO
          AND dwc.DPER LIKE ?
          AND dwc.IS_KPI_TTR = 1
        WHERE rcm.piloting IS NOT NULL
        GROUP BY rcm.piloting, dwc.STO

        UNION ALL

        /* ================= TOTAL PER PILOTING ================= */
        SELECT
          ? AS periode,
          ? AS tgl,
          rcm.piloting AS piloting,
          'TOTAL' AS sto,

          ---------------- COMPLY 3 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS com3,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS tot3,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2)  AS real_com3,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2) / 95.25) * 100 ,2) AS ach_com3,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2) / 95.25) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_3,

          ---------------- COMPLY 6 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS com6,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS tot6,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2)  AS real_com6,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2) / 95) * 100 ,2) AS ach_com6,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2) / 95) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_6,

          ---------------- COMPLY 12 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS com12,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS tot12,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2)  AS real_com12,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2) / 83) * 100 ,2) AS ach_com12,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2) / 83) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_12,

          ---------------- COMPLY 24 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS com24,
          COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS tot24,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2)  AS real_com24,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2) / 99) * 100 ,2) AS ach_com24,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2) / 99) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_24,

          ---------------- COMPLY 36 DAYS ----------------
          COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS com36,
          COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS tot36,
          ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2)  AS real_com36,

          ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2) / 99.5) * 100 ,2) AS ach_com36,

          CASE 
            WHEN (
              ROUND( (ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2) / 99.5) * 100 ,2)
            ) < 100 THEN 'not_comply'
          END AS comply_36

        FROM perf_tif.region_ccm rcm
        LEFT JOIN nonatero_download.Detil_WSA_CLOSE dwc
          ON dwc.STO = rcm.STO
          AND dwc.DPER LIKE ?
          AND dwc.IS_KPI_TTR = 1
        WHERE rcm.piloting IS NOT NULL
        GROUP BY rcm.piloting
    ) x
    ORDER BY
      piloting,
      CASE WHEN sto = 'TOTAL' THEN 2 ELSE 1 END,
      sto;
  `;

  try {
    await queryAsync(sql, [yearMonth, currentDate, likePeriode, yearMonth, currentDate, likePeriode]);

    console.log('Insert ke table piloting_n2n_cx berhasil:');
  } catch (err) {
    console.error('Error insert piloting:', err);
  }
}

async function piloting_service_sugar() {
  const sql = `
    INSERT INTO perf_tif.piloting_n2n_cx_service_sugar
    (periode, tgl, piloting, sto,
     service_lb, service_lg, service, ach_service, service_comp,
     sugar_lb, sugar_lg, sugar, ach_sugar, sugar_comp)

    SELECT *
    FROM (
      -- ================= DETAIL PER STO =================
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        rcm.sto,

        CAST(hslb.m${month} AS INT) AS service_lb,
        CAST(hslg.m${month} AS INT) AS service_lg,

        ROUND(
          (CAST(hslb.m${month} AS INT) - CAST(hslg.m${month} AS INT))
          / NULLIF(CAST(hslb.m${month} AS INT), 0) * 100, 2
        ) AS service,

        ROUND(
          (
            (CAST(hslb.m${month} AS INT) - CAST(hslg.m${month} AS INT))
            / NULLIF(CAST(hslb.m${month} AS INT), 0) * 100
          ) / 98.52 * 100, 2
        ) AS ach_service,

        CASE
          WHEN ROUND(
            (
              (CAST(hslb.m${month} AS INT) - CAST(hslg.m${month} AS INT))
              / NULLIF(CAST(hslb.m${month} AS INT), 0) * 100
            ) / 98.52 * 100, 2
          ) < 100 THEN 'not_comply'
        END AS service_comp,

        CAST(hsulb.m${month} AS INT) AS sugar_lb,
        CAST(hsulg.m${month} AS INT) AS sugar_lg,

        ROUND(
          (CAST(hsulb.m${month} AS INT) - CAST(hsulg.m${month} AS INT))
          / NULLIF(CAST(hsulb.m${month} AS INT), 0) * 100, 2
        ) AS sugar,

        ROUND(
          (
            (CAST(hsulb.m${month} AS INT) - CAST(hsulg.m${month} AS INT))
            / NULLIF(CAST(hsulb.m${month} AS INT), 0) * 100
          ) / 91.71 * 100, 2
        ) AS ach_sugar,

        CASE
          WHEN ROUND(
            (
              (CAST(hsulb.m${month} AS INT) - CAST(hsulg.m${month} AS INT))
              / NULLIF(CAST(hsulb.m${month} AS INT), 0) * 100
            ) / 91.71 * 100, 2
          ) < 100 THEN 'not_comply'
        END AS sugar_comp

      FROM perf_tif.region_ccm rcm
      LEFT JOIN perf_tif.helper_service_list_berbil hslb ON hslb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_service_list_gangguan hslg ON hslg.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_berbil hsulb ON hsulb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_gangguan hsulg ON hsulg.witel = rcm.sto
      WHERE rcm.piloting IS NOT NULL

      UNION ALL

      -- ================= TOTAL PER PILOTING =================
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        'TOTAL',

        SUM(CAST(hslb.m${month} AS INT)),
        SUM(CAST(hslg.m${month} AS INT)),

        ROUND(
          (SUM(CAST(hslb.m${month} AS INT)) - SUM(CAST(hslg.m${month} AS INT)))
          / NULLIF(SUM(CAST(hslb.m${month} AS INT)), 0) * 100, 2
        ),

        ROUND(
          (
            (SUM(CAST(hslb.m${month} AS INT)) - SUM(CAST(hslg.m${month} AS INT)))
            / NULLIF(SUM(CAST(hslb.m${month} AS INT)), 0) * 100
          ) / 98.52 * 100, 2
        ),

        CASE
          WHEN (
            (SUM(CAST(hslb.m${month} AS INT)) - SUM(CAST(hslg.m${month} AS INT)))
            / NULLIF(SUM(CAST(hslb.m${month} AS INT)), 0) * 100
          ) / 98.52 * 100 < 100 THEN 'not_comply'
        END,

        SUM(CAST(hsulb.m${month} AS INT)),
        SUM(CAST(hsulg.m${month} AS INT)),

        ROUND(
          (SUM(CAST(hsulb.m${month} AS INT)) - SUM(CAST(hsulg.m${month} AS INT)))
          / NULLIF(SUM(CAST(hsulb.m${month} AS INT)), 0) * 100, 2
        ),

        ROUND(
          (
            (SUM(CAST(hsulb.m${month} AS INT)) - SUM(CAST(hsulg.m${month} AS INT)))
            / NULLIF(SUM(CAST(hsulb.m${month} AS INT)), 0) * 100
          ) / 91.71 * 100, 2
        ),

        CASE
          WHEN (
            (SUM(CAST(hsulb.m${month} AS INT)) - SUM(CAST(hsulg.m${month} AS INT)))
            / NULLIF(SUM(CAST(hsulb.m${month} AS INT)), 0) * 100
          ) / 91.71 * 100 < 100 THEN 'not_comply'
        END

      FROM perf_tif.region_ccm rcm
      LEFT JOIN perf_tif.helper_service_list_berbil hslb ON hslb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_service_list_gangguan hslg ON hslg.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_berbil hsulb ON hsulb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_gangguan hsulg ON hsulg.witel = rcm.sto
      WHERE rcm.piloting IS NOT NULL
      GROUP BY rcm.piloting
    ) x
    ORDER BY piloting, CASE WHEN sto = 'TOTAL' THEN 2 ELSE 1 END, sto;
  `;

  const params = [
    yearMonth,
    currentDate, // detail
    yearMonth,
    currentDate, // total
  ];

  await queryAsync(sql, params);
  console.log('Insert piloting_n2n_cx_service_sugar berhasil');
}

async function piloting_fulfillment() {
  const sql = `
    INSERT INTO piloting_fulfillment_cx (periode, tgl, piloting,	sto,	tti_com,	tti_tot,	ttic,	ach_ttic,	ttic_comply,	ffg_com,	ffg_tot,	ffg,	ach_ffg,	ffg_comply,	tfg_com,	tfg_tot,	ttr_ffg,	ach_tfg,	tfg_comply)

    SELECT * FROM 
    (
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        rcm.sto,


        kemt.tti_comply AS tti_com,
        kemt.tti_ps_ih AS tti_tot,
        ROUND((kemt.tti_comply / kemt.tti_ps_ih) * 100, 2) AS ttic,
        ROUND(
          ((kemt.tti_comply / kemt.tti_ps_ih) * 100) / 92.72 * 100
        ,2) AS ach_ttic,
        CASE WHEN ( ((kemt.tti_comply / kemt.tti_ps_ih) * 100) / 92.72 * 100 ) < 100 THEN 'not_comply' END AS ttic_comply,

        kemt.ffg_comply AS ffg_com,
        kemt.ffg_jml_ps AS ffg_tot,
        ROUND((kemt.ffg_comply / kemt.ffg_jml_ps) * 100, 2) AS ffg,
        ROUND(
          ((kemt.ffg_comply / kemt.ffg_jml_ps) * 100) / 97.40 * 100
        ,2) AS ach_ffg,
        CASE WHEN ( ((kemt.ffg_comply / kemt.ffg_jml_ps) * 100) / 97.40 * 100 ) < 100 THEN 'not_comply' END AS ffg_comply,

        kemt.ttr_ffg_comply AS tfg_com,
        kemt.ttr_ffg_ggn_wsa AS tfg_tot,
        ROUND((kemt.ttr_ffg_comply / kemt.ttr_ffg_ggn_wsa) * 100, 2) AS ttr_ffg,
        ROUND(
          ((kemt.ttr_ffg_comply / kemt.ttr_ffg_ggn_wsa) * 100) / 80.81 * 100
        ,2) AS ach_tfg,
        CASE WHEN ( ((kemt.ttr_ffg_comply / kemt.ttr_ffg_ggn_wsa) * 100) / 80.81 * 100 ) < 100 THEN 'not_comply' END AS tfg_comply

      FROM region_ccm rcm
      LEFT JOIN helper_fulfillment kemt 
        ON kemt.lokasi = rcm.sto
      WHERE rcm.piloting IS NOT NULL


      UNION ALL 

      SELECT
        ? AS periode,
        ? AS tgl,
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
      LEFT JOIN helper_fulfillment kemt ON kemt.lokasi = rcm.sto
      WHERE rcm.piloting IS NOT NULL
      GROUP BY rcm.piloting

    ) X

    ORDER BY
    piloting,
    CASE WHEN sto = 'TOTAL' THEN 2 ELSE 1 END,
    sto;
  `;

  const params = [
    yearMonth,
    currentDate, // detail
    yearMonth,
    currentDate, // total
  ];

  await queryAsync(sql, params);
  console.log('Insert piloting_fulfillment_cx berhasil');
}

async function main() {
  try {
    // await deleteExistingData();
    // await piloting_ttr();
    await piloting_service_sugar();
    // await piloting_fulfillment();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    connection.end();
  }
}

main();
