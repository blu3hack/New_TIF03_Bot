const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('../currentDate');

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
  const tableForDelete = ['piloting_n2n_cx'];
  const currentDate = insertDate;
  for (const table of tableForDelete) {
    const sql = `
      DELETE FROM piloting_n2n_cx
      WHERE tgl = ?
    `;
    await queryAsync(sql, [currentDate]);
  }
}

async function piloting() {
  const currentDate = insertDate;
  const yearMonth = currentDate.slice(0, 7).replace('-', '');
  const likePeriode = `%${yearMonth}%`;

  const sql = `
    INSERT INTO perf_tif.piloting_n2n_cx (periode, tgl, piloting, sto, com3, tot3, real_com3, com6, tot6, real_com6, com12, tot12, real_com12, com24, tot24, real_com24, com36, tot36, real_com36)
    SELECT
      ? AS periode,
      ? AS tgl,
      rcm.piloting AS piloting,
      dwc.STO AS sto,

      COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS com3,
      COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS tot3,
      ROUND ((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) * 100 ) ,2)  AS real_com3,

      COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS com6,
      COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS tot6,
      ROUND ((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) /  COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) * 100 ) ,2)  AS real_com6,

      COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS com12,
      COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS tot12,
      ROUND ((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) /  COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) * 100 ) ,2)  AS real_com12,

      COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS com24,
      COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS tot24,
      ROUND ((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) /  COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) * 100 ) ,2)  AS real_com24,

      COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS com36,
      COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS tot36,
      ROUND ((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) /  COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) * 100 ) ,2)  AS real_com36

    FROM perf_tif.region_ccm rcm
    LEFT JOIN nonatero_download.Detil_WSA_CLOSE dwc
      ON dwc.STO = rcm.STO
      AND dwc.DPER LIKE ?
      AND dwc.IS_KPI_TTR = 1

    WHERE rcm.piloting IS NOT NULL

    GROUP BY
      rcm.piloting,
      dwc.STO;

  `;

  try {
    await queryAsync(sql, [yearMonth, currentDate, likePeriode]);
    console.log('Insert ke table piloting_n2n_cx berhasil:');
  } catch (err) {
    console.error('Error insert piloting:', err);
  }
}
async function main() {
  try {
    await deleteExistingData();
    await piloting();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    connection.end();
  }
}

main();
