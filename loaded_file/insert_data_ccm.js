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
    'ttr_indibiz',
    'ttr_reseller',
    'sqm_datin',
    'sqm_hsi',
  ];

  const currentDate = insertDate;
  const jenis_for_delete = ['area_ccm'];

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
    // await queryAsync(sql2, [currentDate]);
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
    // await queryAsync(sql2, [currentDate]);
    console.log('Insert ke table sugar datin berhasil:');
  } catch (err) {
    console.error('Error insert sugar datin:', err);
  }
}

async function mttr_mso() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO mttr_mso (tgl, jenis, regional, critical, low, major, minor, premium)

    SELECT
      tgl,
      'area_ccm' AS jenis,
      'BALI NUSRA' AS regional,
      ROUND(AVG(NULLIF(critical, '-')), 2) AS critical,
      ROUND(AVG(NULLIF(major, '-')), 2)   AS low,
      ROUND(AVG(NULLIF(major, '-')), 2)   AS major,
      ROUND(AVG(NULLIF(minor, '-')), 2)   AS minor,
      ROUND(AVG(NULLIF(premium, '-')), 2) AS premium
    FROM mttr_mso
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
      ROUND(AVG(NULLIF(critical, '-')), 2) AS critical,
      ROUND(AVG(NULLIF(major, '-')), 2)   AS low,
      ROUND(AVG(NULLIF(major, '-')), 2)   AS major,
      ROUND(AVG(NULLIF(minor, '-')), 2)   AS minor,
      ROUND(AVG(NULLIF(premium, '-')), 2) AS premium
    FROM mttr_mso
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
      ROUND(AVG(NULLIF(critical, '-')), 2) AS critical,
      ROUND(AVG(NULLIF(major, '-')), 2)   AS low,
      ROUND(AVG(NULLIF(major, '-')), 2)   AS major,
      ROUND(AVG(NULLIF(minor, '-')), 2)   AS minor,
      ROUND(AVG(NULLIF(premium, '-')), 2) AS premium
    FROM mttr_mso
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke mttr_mso berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function sugar_wifi() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO sugar_wifi (tgl, jenis, regional, comply)

    SELECT
      tgl,
      'area_ccm',
      'BALI NUSRA',
      ROUND(AVG(NULLIF(comply, '-')), 2)
    FROM sugar_wifi
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm',
      'JAWA TIMUR',
      ROUND(AVG(NULLIF(comply, '-')), 2)
    FROM sugar_wifi
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN (
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
      ROUND(AVG(NULLIF(comply, '-')), 2)
    FROM sugar_wifi
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table sugar wifi berhasil:');
  } catch (err) {
    console.error('Error insert sugar wifi:', err);
  }
}

async function ff_ih() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ff_ih (tgl, jenis, lokasi, ttic, ffg, ttr_ffg)

    SELECT
      ? ,
      'area_ccm' AS jenis,
      CASE
        WHEN lokasi = 'JATIM' THEN 'JAWA TIMUR'
        WHEN lokasi = 'BALI NUSRA' THEN 'BALI NUSRA'
        WHEN lokasi = 'JATENG-DIY' THEN 'JATENG DIY'
      END AS lokasi,
      tti_real As ttic,
      ffg_real As ffg,
      ttr_ffg_real As ttr_ffg
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('BALI NUSRA', 'JATENG-DIY', 'JATIM')
    GROUP BY lokasi

    UNION ALL

    SELECT
      ? ,
      'balnus_ccm' AS jenis,
      lokasi AS lokasi,
      tti_real As ttic,
      ffg_real As ffg,
      ttr_ffg_real As ttr_ffg
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('DENPASAR', 'FLORES', 'KUPANG', 'MATARAM')
    GROUP BY lokasi

    UNION ALL

    SELECT
      ? ,
      'jateng_ccm' AS jenis,
      lokasi AS lokasi,
      tti_real As ttic,
      ffg_real As ffg,
      ttr_ffg_real As ttr_ffg
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SURAKARTA', 'YOGYAKARTA')
    GROUP BY lokasi

    UNION ALL

    SELECT
      ? ,
      'jatim_ccm' AS jenis,
      lokasi AS lokasi,
      tti_real As ttic,
      ffg_real As ffg,
      ttr_ffg_real As ttr_ffg
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('JEMBER', 'LAMONGAN', 'MADIUN', 'MALANG', 'SIDOARJO', 'SURABAYA')
    GROUP BY lokasi
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate, currentDate]);
    console.log('Insert ke ff_ih berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function ttr_ffg_ccm() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ttr_ffg_download (area, lokasi, realisasi, tgl)

    SELECT
      CASE
        WHEN lokasi = 'JATIM' THEN 'JAWA TIMUR'
        WHEN lokasi = 'BALI NUSRA' THEN 'BALI NUSRA'
        WHEN lokasi = 'JATENG-DIY' THEN 'JATENG DIY'
      END AS area,
     
      'area_ccm' AS lokasi,
      ROUND(AVG(NULLIF(ttr_ffg_real, '-')), 2) As realisasi,
      ? As tgl
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('BALI NUSRA', 'JATENG-DIY', 'JATIM')
    GROUP BY lokasi

    UNION ALL

    SELECT
      lokasi AS area,
      'balnus_ccm' AS lokasi,
      ROUND(AVG(NULLIF(ttr_ffg_real, '-')), 2) As realisasi,
      ? As tgl
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('DENPASAR', 'FLORES', 'KUPANG', 'MATARAM')
    GROUP BY lokasi

    UNION ALL

    SELECT
      lokasi AS area,
      'jateng_ccm' AS lokasi,
      ROUND(AVG(NULLIF(ttr_ffg_real, '-')), 2) As realisasi,
      ? As tgl
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SURAKARTA', 'YOGYAKARTA')
    GROUP BY lokasi

    UNION ALL

    SELECT
      lokasi AS area,
      'jatim_ccm' AS lokasi,
      ROUND(AVG(NULLIF(ttr_ffg_real, '-')), 2) As realisasi,
      ? As tgl
    FROM kpi_endstate_monthly_ccm
    WHERE lokasi IN ('JEMBER', 'LAMONGAN', 'MADIUN', 'MALANG', 'SIDOARJO', 'SURABAYA')
    GROUP BY lokasi
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate, currentDate]);
    console.log('Insert ke ttr_ffg_download berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function ff_hsi() {
  const currentDate = insertDate;
  const branch = `
    INSERT INTO ff_hsi (tgl, jenis, lokasi, ttic, ffg, pspi, ttr_ffg, unspec)
    SELECT
      ? as tgl,
      CASE
        WHEN rc.branch IN ('DENPASAR', 'FLORES', 'KUPANG', 'MATARAM') THEN 'balnus_ccm'
        WHEN rc.branch IN ('MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SURAKARTA', 'YOGYAKARTA') THEN 'jateng_ccm'
        WHEN rc.branch IN ('JEMBER', 'LAMONGAN', 'MADIUN', 'MALANG', 'SIDOARJO', 'SURABAYA') THEN 'jatim_ccm'
      END AS jenis,
      rc.branch AS lokasi,
      ROUND(AVG( NULLIF(k.tti_real, 0)  ), 2) AS ttic,
      ROUND(AVG( NULLIF(k.ffg_real, 0)), 2) AS ffg,
      ROUND(AVG( NULLIF(k.ttr_ffg_real, 0) ), 2) AS ttr_ffg,
      ROUND(AVG( NULLIF(k.pspi_real, 0) ), 2) AS pspi,
      ROUND(AVG( NULLIF(k.unspec_real, 0) ), 2) AS unspec
    FROM kpi_msa_2025_reg k
    JOIN region_ccm rc ON k.lokasi = rc.sto

    GROUP BY rc.branch
    ORDER BY region, rc.branch, rc.sto;
  `;

  const region = `
    INSERT INTO ff_hsi (tgl, jenis, lokasi, ttic, ffg, pspi, ttr_ffg, unspec)
    SELECT
    ? as tgl,
    'area_ccm' AS jenis,
    rc.region AS lokasi,
    
    ROUND(AVG( NULLIF(k.tti_real, 0)  ), 2) AS ttic,
    ROUND(AVG( NULLIF(k.ffg_real, 0)), 2) AS ffg,
    ROUND(AVG( NULLIF(k.ttr_ffg_real, 0) ), 2) AS ttr_ffg,
    ROUND(AVG( NULLIF(k.pspi_real, 0) ), 2) AS pspi,
    ROUND(AVG( NULLIF(k.unspec_real, 0) ), 2) AS unspec
    FROM
      kpi_msa_2025_reg k
      
    JOIN region_ccm rc ON rc.sto = k.lokasi
    GROUP BY rc.region
  `;

  try {
    await queryAsync(branch, [currentDate]);
    await queryAsync(region, [currentDate]);
    console.log('Insert ke ff_hsi berhasil:');
  } catch (err) {
    console.error('Error insert:', err);
  }
}

async function ps_re() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO ps_re (lokasi, area, psre, tgl)

    SELECT
      'BALI NUSRA',
      'area_ccm',
      ROUND(AVG(NULLIF(psre, '-')), 2),
      tgl
    FROM ps_re
    WHERE tgl = ?
      AND area = 'reg'
      AND lokasi IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      'JAWA TIMUR',
      'area_ccm',
      ROUND(AVG(NULLIF(psre, '-')), 2),
      tgl
    FROM ps_re
    WHERE tgl = ?
      AND area = 'reg'
      AND lokasi IN (
        'MADIUN','MALANG','JEMBER','SIDOARJO',
        'SURABAYA SELATAN','SURABAYA UTARA',
        'MADURA','PASURUAN'
      )
    GROUP BY tgl

    UNION ALL

    SELECT
      'JATENG DIY',
      'area_ccm',
      ROUND(AVG(NULLIF(psre, '-')), 2),
      tgl
    FROM ps_re
    WHERE tgl = ?
      AND area = 'reg'
      AND lokasi IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table ps re berhasil:');
  } catch (err) {
    console.error('Error insert ps re:', err);
  }
}

async function cnop_latency() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO cnop_latency (kpi, lokasi, area, realisasi, insert_at)

    SELECT
      'ONM-WHM-Latency RAN to Core' AS kpi,
      'area_ccm' as lokasi,
      'BALI NUSRA' AS area,
      ROUND(AVG(NULLIF(realisasi, '-')), 2) as realisasi,
      insert_at
    FROM cnop_latency
    WHERE insert_at = ?
      AND lokasi = 'reg'
      AND area IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY insert_at

    UNION ALL

    SELECT
      'ONM-WHM-Latency RAN to Core' AS kpi,
      'area_ccm' as lokasi,
      'JAWA TIMUR' AS area,
      ROUND(AVG(NULLIF(realisasi, '-')), 2) as realisasi,
      insert_at
    FROM cnop_latency
    WHERE insert_at = ?
      AND lokasi = 'reg'
      AND area IN (
        'MADIUN','MALANG','JEMBER','SIDOARJO',
        'SURABAYA SELATAN','SURABAYA UTARA',
        'MADURA','PASURUAN'
      )
    GROUP BY insert_at

    UNION ALL

    SELECT
      'ONM-WHM-Latency RAN to Core' AS kpi,
      'area_ccm' AS lokasi,
      'JATENG DIY' AS area,
      ROUND(AVG(NULLIF(realisasi, '-')), 2) as realisasi,
      insert_at
    FROM cnop_latency
    WHERE insert_at = ?
      AND lokasi = 'reg'
      AND area IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY insert_at
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table cnop_latency berhasil:');
  } catch (err) {
    console.error('Error insert cnop_latency:', err);
  }
}

async function ttr_indibiz() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO ttr_indibiz (jenis, treg, tgl, real_1, real_2)
    SELECT
      'area_ccm' AS jenis,
      'BALI NUSRA' AS treg,
      tgl,
      ROUND(AVG(NULLIF(real_1, '-')), 2) AS real_1,
      ROUND(AVG(NULLIF(real_2, '-')), 2) AS real_2
    FROM ttr_indibiz
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
      ROUND(AVG(NULLIF(real_1, '-')), 2) AS real_1,
      ROUND(AVG(NULLIF(real_2, '-')), 2) AS real_2
    FROM ttr_indibiz
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('MADIUN', 'MALANG', 'JEMBER', 'SIDOARJO', 'SURABAYA SELATAN', 'SURABAYA UTARA', 'MADURA', 'PASURUAN')
    GROUP BY tgl

    UNION ALL

    SELECT
      'area_ccm' AS jenis,
      'JATENG DIY' AS treg,
      tgl,
      ROUND(AVG(NULLIF(real_1, '-')), 2) AS real_1,
      ROUND(AVG(NULLIF(real_2, '-')), 2) AS real_2
      
    FROM ttr_indibiz
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('KUDUS', 'MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SOLO', 'YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table ttr_indibiz berhasil:');
  } catch (err) {
    console.error('Error insert ttr_indibiz:', err);
  }
}

async function ttr_reseller() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO ttr_reseller (jenis, treg, tgl, real_1, real_2)
    SELECT
      'area_ccm' AS jenis,
      'BALI NUSRA' AS treg,
      tgl,
      ROUND(AVG(NULLIF(real_1, '-')), 2) AS real_1,
      ROUND(AVG(NULLIF(real_2, '-')), 2) AS real_2
    FROM ttr_reseller
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
      ROUND(AVG(NULLIF(real_1, '-')), 2) AS real_1,
      ROUND(AVG(NULLIF(real_2, '-')), 2) AS real_2
    FROM ttr_reseller
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('MADIUN', 'MALANG', 'JEMBER', 'SIDOARJO', 'SURABAYA SELATAN', 'SURABAYA UTARA', 'MADURA', 'PASURUAN')
    GROUP BY tgl

    UNION ALL

    SELECT
      'area_ccm' AS jenis,
      'JATENG DIY' AS treg,
      tgl,
      ROUND(AVG(NULLIF(real_1, '-')), 2) AS real_1,
      ROUND(AVG(NULLIF(real_2, '-')), 2) AS real_2
      
    FROM ttr_reseller
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND treg IN ('KUDUS', 'MAGELANG', 'PEKALONGAN', 'PURWOKERTO', 'SEMARANG', 'SOLO', 'YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table ttr_reseller berhasil:');
  } catch (err) {
    console.error('Error insert ttr_reseller:', err);
  }
}

async function sqm_datin() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO sqm_datin (tgl, jenis, regional, comply)
    SELECT
        tgl,
        'area_ccm' as jenis,
        'BALI NUSRA' AS regional,
          
        ROUND(AVG(NULLIF(comply, '-')), 2) as comply
        FROM sqm_datin
        WHERE tgl = ?
          AND jenis = 'reg'
          AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
        GROUP BY tgl

        UNION ALL

        SELECT
          tgl,
          'area_ccm' as jenis,
          'JAWA TIMUR' as regional,

          ROUND(AVG(NULLIF(comply, '-')), 2) as comply
        FROM sqm_datin
        WHERE tgl = ?
          AND jenis = 'reg'
          AND regional IN (
            'MADIUN','MALANG','JEMBER','SIDOARJO',
            'SURABAYA SELATAN','SURABAYA UTARA',
            'MADURA','PASURUAN'
          )
        GROUP BY tgl

        UNION ALL

        SELECT
          tgl,
          'area_ccm' as jenis,
          'JATENG DIY' as regional,
          
          ROUND(AVG(NULLIF(comply, '-')), 2) as comply
        FROM sqm_datin
        WHERE tgl = ?
          AND jenis = 'reg'
          AND regional IN (
            'KUDUS','MAGELANG','PEKALONGAN',
            'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
          )
        GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table ttr_reseller berhasil:');
  } catch (err) {
    console.error('Error insert ttr_reseller:', err);
  }
}

async function sqm_datin() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO sqm_hsi (tgl, jenis, regional, comply)
    SELECT
        tgl,
        'area_ccm' as jenis,
        'BALI NUSRA' AS regional,
          
        ROUND(AVG(NULLIF(comply, '-')), 2) as comply
        FROM sqm_hsi
        WHERE tgl = ?
          AND jenis = 'reg'
          AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
        GROUP BY tgl

        UNION ALL

        SELECT
          tgl,
          'area_ccm' as jenis,
          'JAWA TIMUR' as regional,

          ROUND(AVG(NULLIF(comply, '-')), 2) as comply
        FROM sqm_hsi
        WHERE tgl = ?
          AND jenis = 'reg'
          AND regional IN (
            'MADIUN','MALANG','JEMBER','SIDOARJO',
            'SURABAYA SELATAN','SURABAYA UTARA',
            'MADURA','PASURUAN'
          )
        GROUP BY tgl

        UNION ALL

        SELECT
          tgl,
          'area_ccm' as jenis,
          'JATENG DIY' as regional,
          
          ROUND(AVG(NULLIF(comply, '-')), 2) as comply
        FROM sqm_hsi
        WHERE tgl = ?
          AND jenis = 'reg'
          AND regional IN (
            'KUDUS','MAGELANG','PEKALONGAN',
            'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
          )
        GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table ttr_reseller berhasil:');
  } catch (err) {
    console.error('Error insert ttr_reseller:', err);
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
    await ttr_indibiz();
    await ttr_reseller();
    await sqm_datin();
    await sqm_hsi();
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
