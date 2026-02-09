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
  const tableForDelete = [
    'ttr_datin',
    'ttd_non_hsi',
    'ttd_wifi',
    'ttr_ffg_non_hsi',
    'ttr_wifi',
    'unspec_datin',
    'unspec_hsi',
    'wifi_revi',
    'q_datin',
    'q_hsi',
    'ffg_non_hsi',
    'hsi_sugar',
    'mttr_mso',
    'ff_ih',
    'ff_hsi',
    'sugar_datin',
    'sugar_wifi',
    'ps_re',
    'ttr_ffg_download',
    'cnop_latency',
  ];

  const currentDate = insertDate;
  const jenis_for_delete = ['area_ccm', 'balnus_ccm', 'jateng_ccm', 'jatim_ccm'];

  const inPlaceholders = jenis_for_delete.map(() => '?').join(',');

  for (const table of tableForDelete) {
    let colomnName = 'jenis';
    let tglName = 'tgl';

    if (table === 'ttr_ffg_download' || table === 'cnop_latency') {
      colomnName = 'lokasi';
    } else if (table === 'ps_re') {
      colomnName = 'area';
    }

    if (table === 'cnop_latency') {
      tglName = 'insert_at';
    }

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
      'area_ccm' AS jenis,
      'BALI NUSRA' AS treg,
      tgl,
      'nan' AS k1,
      ROUND(AVG(NULLIF(k2, '-')), 2) AS k2,
      ROUND(AVG(NULLIF(k3, '-')), 2) AS k3
    FROM ttr_datin
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      'area_ccm' AS jenis,
      'JAWA TIMUR' AS treg,
      tgl,
      'nan' AS k1,
      ROUND(AVG(NULLIF(k2, '-')), 2) AS k2,
      ROUND(AVG(NULLIF(k3, '-')), 2) AS k3
    FROM ttr_datin
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN')
    GROUP BY tgl

    UNION ALL

    SELECT
      'area_ccm' AS jenis,
      'JATENG DIY' AS treg,
      tgl,
      'nan' AS k1,
      ROUND(AVG(NULLIF(k2, '-')), 2) AS k2,
      ROUND(AVG(NULLIF(k3, '-')), 2) AS k3
    FROM ttr_datin
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke ttr_datin berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function sugar_hsi() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO hsi_sugar (tgl, jenis, treg, \`real\`)

    SELECT
      tgl,
      'area_ccm',
      'BALI NUSRA',
      ROUND(AVG(NULLIF(\`real\`, '-')), 2)
    FROM hsi_sugar
    WHERE tgl = ?
      AND jenis = 'reg'
      AND treg IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm',
      'JAWA TIMUR',
      ROUND(AVG(NULLIF(\`real\`, '-')), 2)
    FROM hsi_sugar
    WHERE tgl = ?
      AND jenis = 'reg'
      AND treg IN (
        'MADIUN','MALANG','JEMBER','SIDOARJO',
        'SURABAYA SELATAN','SURABAYA UTARA',
        'MADURA','PASURUAN'
      )
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm',
      'JATENG DIY',
      ROUND(AVG(NULLIF(\`real\`, '-')), 2)
    FROM hsi_sugar
    WHERE tgl = ?
      AND jenis = 'reg'
      AND treg IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY tgl
  `;

  const sql2 = `
    INSERT INTO hsi_sugar (jenis, treg, tgl, \`real\`)
    SELECT
    rsb.area as jenis,
    rc.branch as treg,
    ? as tgl,
    ROUND( ( count(CASE WHEN KAT = 'non_gaul' THEN 1 END) / count(CASE WHEN KAT = 'total' THEN 1 END) * 100 ) ,2) as \`real\`
    FROM region_ccm rc
    LEFT JOIN helper_sugar_hsi hsh ON hsh.WORKZONE =  rc.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rc.branch
    GROUP BY rc.branch
    ORDER BY rsb.area, rc.branch
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    await queryAsync(sql2, [currentDate]);
    console.log('Insert ke table hsi_sugar berhasil:');
  } catch (err) {
    console.error('Error insert sugar_hsi:', err);
  }
}

async function sugar_datin() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO sugar_datin (jenis, treg, tgl, \`real\`)

    SELECT
      'area_ccm',
      'BALI NUSRA',
      tgl,
      ROUND(AVG(NULLIF(\`real\`, '-')), 2)
    FROM sugar_datin
    WHERE tgl = ?
      AND jenis = 'reg'
      AND treg IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      'area_ccm',
      'JAWA TIMUR',
      tgl,
      ROUND(AVG(NULLIF(\`real\`, '-')), 2)
    FROM sugar_datin
    WHERE tgl = ?
      AND jenis = 'reg'
      AND treg IN (
        'MADIUN','MALANG','JEMBER','SIDOARJO',
        'SURABAYA SELATAN','SURABAYA UTARA',
        'MADURA','PASURUAN'
      )
    GROUP BY tgl

    UNION ALL

    SELECT
      'area_ccm',
      'JATENG DIY',
      tgl,
      ROUND(AVG(NULLIF(\`real\`, '-')), 2)
    FROM sugar_datin
    WHERE tgl = ?
      AND jenis = 'reg'
      AND treg IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY tgl
  `;

  const sql2 = `
    INSERT INTO sugar_datin (jenis, treg, tgl, \`real\`)
    SELECT
    rsb.area as jenis,
    rc.branch as treg,
    ? as tgl,
    ROUND( ( count(CASE WHEN KAT = 'non_gaul' THEN 1 END) / count(CASE WHEN KAT = 'total' THEN 1 END) * 100 ) ,2) as \`real\`
    FROM region_ccm rc
    LEFT JOIN helper_sugar_datin hsd ON hsd.WORKZONE =  rc.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rc.branch
    GROUP BY rc.branch
    ORDER BY rsb.area, rc.branch
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    await queryAsync(sql2, [currentDate]);
    console.log('Insert ke table sugar datin berhasil:');
  } catch (err) {
    console.error('Error insert sugar datin:', err);
  }
}

// ========================= group area_ccm functions =========================

async function insert_ccm_group1(table) {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO ${table} (tgl, jenis, regional, comply)
    SELECT
      tgl,
      'area_ccm' AS jenis,
      'BALI NUSRA' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
    tgl,
      'area_ccm' AS jenis,
      'JAWA TIMUR' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN')
    GROUP BY tgl

    UNION ALL

    SELECT
    tgl,
      'area_ccm' AS jenis,
      'JATENG DIY' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log(`Insert ke table ${table} berhasil:`);
  } catch (err) {
    console.error('Error insert:', err);
  }
}

// MAIN
async function main() {
  try {
    await deleteExistingData();
    await ttr_datin();
    await sugar_hsi();
    await sugar_datin();
    await sugar_wifi();
    await mttr_mso();
    await ff_ih();
    await ttr_ffg_ccm();
    await ff_hsi();
    await ps_re();
    await cnop_latency();
    insert_ccm_group1('ttd_non_hsi');
    insert_ccm_group1('ttd_wifi');
    insert_ccm_group1('ttr_ffg_non_hsi');
    insert_ccm_group1('ttr_wifi');
    insert_ccm_group1('unspec_datin');
    insert_ccm_group1('unspec_hsi');
    insert_ccm_group1('wifi_revi');
    insert_ccm_group1('q_datin');
    insert_ccm_group1('q_hsi');
    insert_ccm_group1('ffg_non_hsi');
  } catch (err) {
    console.error('Error:', err);
  } finally {
    connection.end();
  }
}

main();
