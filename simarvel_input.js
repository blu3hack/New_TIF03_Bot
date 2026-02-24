const connection = require('./connection');
const { insertDate } = require('./currentDate');

// Hapus data lama
async function deleteExistingData() {
  const tableForDelete = ['mttr_mso'];
  const currentDate = insertDate;
  const jenis_for_delete = ['balnus_ccm', 'jateng_ccm', 'jatim_ccm', 'area_ccm'];
  const inPlaceholders = jenis_for_delete.map(() => '?').join(',');

  for (const table of tableForDelete) {
    const sql = `
      DELETE FROM ${table}
      WHERE tgl = ?
      AND jenis IN (${inPlaceholders})
    `;

    await connection.query(sql, [currentDate, ...jenis_for_delete]);
  }

  console.log('✅ Data lama berhasil dihapus');
}

async function mtr_mso() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO mttr_mso 
    (tgl, jenis, regional, critical, low, major, minor, premium)

    SELECT ?, rsb.area, rcm.branch,
    CASE 
      WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) = 0 
      THEN '-'
      ELSE ROUND(
        COUNT(CASE WHEN hmm.site_down_severity = 'Critical' AND hmm.cnop3_compliance = 1 THEN 1 END) /
        COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) * 100, 2)
    END,
    CASE 
      WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) = 0 
      THEN '-'
      ELSE ROUND(
        COUNT(CASE WHEN hmm.site_down_severity = 'Low' AND hmm.cnop3_compliance = 1 THEN 1 END) /
        COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) * 100, 2)
    END,
    CASE 
      WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) = 0 
      THEN '-'
      ELSE ROUND(
        COUNT(CASE WHEN hmm.site_down_severity = 'Major' AND hmm.cnop3_compliance = 1 THEN 1 END) /
        COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) * 100, 2)
    END,
    CASE 
      WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) = 0 
      THEN '-'
      ELSE ROUND(
        COUNT(CASE WHEN hmm.site_down_severity = 'Minor' AND hmm.cnop3_compliance = 1 THEN 1 END) /
        COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) * 100, 2)
    END,
    '-'
    FROM region_ccm rcm
    LEFT JOIN helper_mttr_mso hmm ON hmm.Workzone = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.branch
    GROUP BY rcm.branch

    UNION ALL

    SELECT
      ?,
      'area_ccm',
      rcm.region,
      CASE
        WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) = 0 THEN
          '-'
        ELSE
          ROUND(
            COUNT(CASE WHEN hmm.site_down_severity = 'Critical' AND hmm.cnop3_compliance = 1 THEN 1 END) / COUNT(CASE WHEN hmm.site_down_severity = 'Critical' THEN 1 END) * 100,
            2
          )
      END,
      CASE
        WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) = 0 THEN
          '-'
        ELSE
          ROUND(
            COUNT(CASE WHEN hmm.site_down_severity = 'Low' AND hmm.cnop3_compliance = 1 THEN 1 END) / COUNT(CASE WHEN hmm.site_down_severity = 'Low' THEN 1 END) * 100,
            2
          )
      END,
      CASE
        WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) = 0 THEN
          '-'
        ELSE
          ROUND(
            COUNT(CASE WHEN hmm.site_down_severity = 'Major' AND hmm.cnop3_compliance = 1 THEN 1 END) / COUNT(CASE WHEN hmm.site_down_severity = 'Major' THEN 1 END) * 100,
            2
          )
      END,
      CASE
        WHEN COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) = 0 THEN
          '-'
        ELSE
          ROUND(
            COUNT(CASE WHEN hmm.site_down_severity = 'Minor' AND hmm.cnop3_compliance = 1 THEN 1 END) / COUNT(CASE WHEN hmm.site_down_severity = 'Minor' THEN 1 END) * 100,
            2
          )
      END,
      '-'
    FROM
      region_ccm rcm
      LEFT JOIN helper_mttr_mso hmm ON hmm.Workzone = rcm.sto
      LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.branch
    GROUP BY
      rcm.region

    UNION ALL 

    SELECT
      ? as tgl,
      'tif' as jenis,
      'TERRITORY 01' AS regional,
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-1') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-1') THEN 1 END) * 100
      ,2) AS Critical,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-1') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-1') THEN 1 END) * 100
      ,2) AS Low,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-1') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-1') THEN 1 END) * 100
      ,2) AS Major,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-1') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-1') THEN 1 END) * 100
      ,2) AS Minor,
      '-' as Premium
    FROM
      helper_mttr_mso
      
    UNION ALL 

    SELECT
      ? as tgl,
      'tif' as jenis,
      'TERRITORY 02' AS regional,
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-2', 'REG-3') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-2', 'REG-3') THEN 1 END) * 100
      ,2) AS Critical,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-2', 'REG-3') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-2', 'REG-3') THEN 1 END) * 100
      ,2) AS Low,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-2', 'REG-3') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-2', 'REG-3') THEN 1 END) * 100
      ,2) AS Major,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-2', 'REG-3') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-2', 'REG-3') THEN 1 END) * 100
      ,2) AS Minor,
      '-' as Premium
    FROM
      helper_mttr_mso
      
      
    UNION ALL 

    SELECT
      ? as tgl,
      'tif' as jenis,
      'TERRITORY 03' AS regional,
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-4', 'REG-5') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-4', 'REG-5') THEN 1 END) * 100
      ,2) AS Critical,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-4', 'REG-5') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-4', 'REG-5') THEN 1 END) * 100
      ,2) AS Low,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-4', 'REG-5') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-4', 'REG-5') THEN 1 END) * 100
      ,2) AS Major,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-4', 'REG-5') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-4', 'REG-5') THEN 1 END) * 100
      ,2) AS Minor,
      '-' as Premium
    FROM
      helper_mttr_mso
      
    UNION ALL 

    SELECT
      ? as tgl,
      'tif' as jenis,
      'TERRITORY 04' AS regional,
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-6', 'REG-7') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Critical' AND Regional IN ('REG-6', 'REG-7') THEN 1 END) * 100
      ,2) AS Critical,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-6', 'REG-7') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Low' AND Regional IN ('REG-6', 'REG-7') THEN 1 END) * 100
      ,2) AS Low,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-6', 'REG-7') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Major' AND Regional IN ('REG-6', 'REG-7') THEN 1 END) * 100
      ,2) AS Major,
      
      ROUND (
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-6', 'REG-7') AND cnop3_compliance = 1 THEN 1 END) / 
        COUNT(CASE WHEN site_down_severity = 'Minor' AND Regional IN ('REG-6', 'REG-7') THEN 1 END) * 100
      ,2) AS Minor,
      '-' as Premium
    FROM
      helper_mttr_mso


  `;

  await connection.query(sql, [currentDate, currentDate, currentDate, currentDate, currentDate, currentDate]);
  console.log('✅ Insert ke mttr_mso berhasil');
}

// MAIN
async function main() {
  try {
    await deleteExistingData();
    await mtr_mso();
  } catch (err) {
    console.error('❌ Error:', err.message);
  } finally {
    await connection.end();
    console.log('🔌 Koneksi database ditutup');
    process.exit(0);
  }
}
main();
