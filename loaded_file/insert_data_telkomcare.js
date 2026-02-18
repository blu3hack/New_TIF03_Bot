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
  const tableForDelete = ['ttr_datin', 'sugar_datin', 'hsi_sugar', 'ttr_indibiz'];
  const currentDate = insertDate;
  const jenis_for_delete = ['balnus_ccm', 'jateng_ccm', 'jatim_ccm'];
  const inPlaceholders = jenis_for_delete.map(() => '?').join(',');
  for (const table of tableForDelete) {
    let colomnName = 'jenis';
    let tglName = 'tgl';
    const sql = `
      DELETE FROM ${table}
      WHERE ${tglName} = ?
      AND ${colomnName} IN (${inPlaceholders})
    `;
    await queryAsync(sql, [currentDate, ...jenis_for_delete]);
  }
}

async function ttr_datin() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ttr_datin (jenis, treg, tgl, k1, k2, k3)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      'nan' AS k1,
      
      ROUND (
        COUNT(
          CASE WHEN htd.KATEGORI = 'K2' AND htd.COMPLIANCE = 'COMPLY' THEN htd.INCIDENT END
        ) /
        COUNT(
          CASE WHEN htd.KATEGORI = 'K2' THEN htd.INCIDENT END
        ) * 100
      ,2) AS k2,
      
      ROUND (
        COUNT(
          CASE WHEN htd.KATEGORI = 'K3' AND htd.COMPLIANCE = 'COMPLY' THEN htd.INCIDENT END
        ) /
        COUNT(
          CASE WHEN htd.KATEGORI = 'K3' THEN htd.INCIDENT END
        ) * 100
      ,2) AS k3
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_ttr_datin htd ON htd.WORKZONE = rcm.sto   -- sesuaikan relasinya
    GROUP BY rcm.branch
  `;

  try {
    await queryAsync(sql, [currentDate]);
    console.log('Insert ke ttr_datin berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function sugar_datin() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO sugar_datin (jenis, treg, tgl, \`real\`)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLIANCE = 'COMPLY' THEN htd.INCIDENT END)
        / NULLIF(COUNT(htd.INCIDENT), 0) * 100
      , 2) AS \`real\`
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_sugar_datin htd ON htd.WORKZONE = rcm.sto
    GROUP BY rcm.branch, rsb.area
  `;

  try {
    await queryAsync(sql, [currentDate]);
    console.log('Insert ke sugar_datin berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function sugar_hsi() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO hsi_sugar (jenis, treg, tgl, \`real\`)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLIANCE = 'COMPLY' THEN htd.INCIDENT END)
        / NULLIF(COUNT(htd.INCIDENT), 0) * 100
      , 2) AS \`real\`
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_sugar_hsi htd ON htd.WORKZONE = rcm.sto
    GROUP BY rcm.branch, rsb.area
  `;

  try {
    await queryAsync(sql, [currentDate]);
    console.log('Insert ke sugar_hsi berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function ttr_indibiz() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ttr_indibiz (jenis, treg, tgl, real_1, real_2)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND (
        COUNT(CASE WHEN htd.COMPLY_4_E2E = 'COMPLY' THEN htd.INCIDENT END) / COUNT( htd.INCIDENT) * 100
      ,2) AS real_1,
      ROUND (
        COUNT(CASE WHEN htd.COMPLY_24_E2E = 'COMPLY' THEN htd.INCIDENT END) / COUNT( htd.INCIDENT) * 100
      ,2) AS real_2
      
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_ttr_indibiz htd ON htd.WORKZONE = rcm.sto   -- sesuaikan relasinya
    GROUP BY rcm.branch
  `;
  try {
    await queryAsync(sql, [currentDate]);
    console.log('Insert ke ttr_indibiz berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function ttr_reseller() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ttr_reseller (jenis, treg, tgl, real_1, real_2)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND (
        COUNT(CASE WHEN htd.COMPLY_6_E2E = 'COMPLY' THEN htd.INCIDENT END) / COUNT( htd.INCIDENT) * 100
      ,2) AS real_1,
      ROUND (
        COUNT(CASE WHEN htd.COMPLY_36_E2E = 'COMPLY' THEN htd.INCIDENT END) / COUNT( htd.INCIDENT) * 100
      ,2) AS real_2
      
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_ttr_reseller htd ON htd.WORKZONE = rcm.sto   -- sesuaikan relasinya
    GROUP BY rcm.branch
  `;
  try {
    await queryAsync(sql, [currentDate]);
    console.log('Insert ke ttr_reseller berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

// MAIN
async function main() {
  try {
    await deleteExistingData();
    await ttr_datin();
    await sugar_datin();
    await sugar_hsi();
    await ttr_indibiz();
    await ttr_reseller();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    connection.end();
  }
}
main();
