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
  const tableForDelete = ['mttr_mso'];
  const currentDate = insertDate;
  const jenis_for_delete = ['balnus_ccm', 'jateng_ccm', 'jatim_ccm', 'area_ccm'];
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

async function mtr_mso() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO mttr_mso (tgl, jenis, regional, critical, low, major, minor, premium)
    SELECT
      ? AS tgl,
      rsb.area AS jenis,
      rcm.branch AS regional,
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) = 0
          THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Critical' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) * 100
          , 2)
      END AS critical,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(COUNT(CASE WHEN hmm.site_down_severity = 'Low' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) * 100
          , 2)
      END AS low,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Major' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) * 100
          , 2)
      END AS major,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) = 0 THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Minor' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) * 100
          , 2)
      END AS minor,
      '-' AS premium

    FROM region_ccm rcm 
    LEFT JOIN helper_mttr_mso hmm on hmm.Workzone = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.branch
    GROUP BY
    rcm.branch

    UNION ALL

    SELECT
      ? AS tgl,
      rsb.area AS jenis,
      rcm.region AS regional,
      
        CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Critical' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) * 100
          , 2)
      END AS critical,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(COUNT(CASE WHEN hmm.site_down_severity = 'Low' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) * 100
          , 2)
      END AS low,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Major' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) * 100
          , 2)
      END AS major,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) = 0 THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Minor' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) * 100
          , 2)
      END AS minor,
      '-' AS premium

    FROM region_ccm rcm 
    LEFT JOIN helper_mttr_mso hmm on hmm.Workzone = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.region
    GROUP BY
    rcm.region

    UNION ALL 

    SELECT
      ? AS tgl,
      'tif' AS jenis,
      'TERRITORY 03' AS regional,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Critical' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) * 100
          , 2)
      END AS critical,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(COUNT(CASE WHEN hmm.site_down_severity = 'Low' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) * 100
          , 2)
      END AS low,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) = 0 
          THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Major' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) * 100
          , 2)
      END AS major,
      
      CASE 
          WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) = 0 THEN '-'
          ELSE
          ROUND(
              COUNT(CASE WHEN hmm.site_down_severity = 'Minor' AND hmm.cnop3_compliance = 1 THEN 1 END) / 
              COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) * 100
          , 2)
      END AS minor,
      '-' AS premium

    FROM helper_mttr_mso hmm
    WHERE
    hmm.Workzone IN (SELECT sto from region_ccm)
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke mttr_mso berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

// MAIN
async function main() {
  try {
    await deleteExistingData();
    await mtr_mso();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    connection.end();
  }
}
main();
