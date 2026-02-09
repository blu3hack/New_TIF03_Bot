const connection = require('./connection');
const { insertDate } = require('./currentDate');

const currentDate = insertDate;
const yearMonth = currentDate.slice(0, 7).replace('-', '');
const likePeriode = `%${yearMonth}%`;
const year = Number(currentDate.slice(0, 4)); // 2026
const month = Number(currentDate.slice(5, 7)); // 1

console.log(year);
console.log(month);

// Promisify query
function queryAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    connection.query(sql, params, (err, results) => {
      if (err) reject(err);
      else resolve(results);
    });
  });
}

async function deleteExistingData() {
  const tables = ['nps_journey'];
  const sql = `
      DELETE FROM ${tables}
      WHERE periode = ?
    `;
  await queryAsync(sql, [yearMonth]);
}

async function piloting() {
  console.log(currentDate, yearMonth, likePeriode);

  const sql = `
    INSERT INTO nps_journey (journey, row_label, balinusra, jateng, jatim, total, lvl, periode)
    (
      -- DETAIL (reason)
      SELECT
        journey,
        CONCAT('   ', reason_desc) AS row_label,
        COUNT(CASE WHEN region = 'BALINUSRA' THEN 1 END) AS balinusra,
        COUNT(CASE WHEN region = 'JATENG' THEN 1 END) AS jateng,
        COUNT(CASE WHEN region = 'JATIM' THEN 1 END) AS jatim,
        
        (COUNT(CASE WHEN region = 'BALINUSRA' THEN 1 END) + COUNT(CASE WHEN region = 'JATENG' THEN 1 END) + COUNT(CASE WHEN region = 'JATIM' THEN 1 END)) as TOTAL,
        
        2 AS lvl,
        ${yearMonth} AS periode
        
      FROM nps_indihome
      WHERE 
        journey IN (
          'ACTIVATE',
          'get support',
          'use Indihome TV',
          'use Internet'
        )
        AND
        MONTH(STR_TO_DATE(survey_date, '%m/%d/%Y')) = ${month}
        AND YEAR(STR_TO_DATE(survey_date, '%m/%d/%Y')) = ${year}
      GROUP BY journey, reason_desc
    )

    UNION ALL

    (
      -- SUBTOTAL per JOURNEY
      SELECT
        journey,
        journey AS row_label,
        COUNT(CASE WHEN region = 'BALINUSRA' THEN 1 END) AS balinusra,
        COUNT(CASE WHEN region = 'JATENG' THEN 1 END) AS jateng,
        COUNT(CASE WHEN region = 'JATIM' THEN 1 END) AS jatim,
        (COUNT(CASE WHEN region = 'BALINUSRA' THEN 1 END) + COUNT(CASE WHEN region = 'JATENG' THEN 1 END) + COUNT(CASE WHEN region = 'JATIM' THEN 1 END)),
        1 AS lvl,
        ${yearMonth} AS periode
      FROM nps_indihome
      WHERE
        journey IN (
          'ACTIVATE',
          'get support',
          'use Indihome TV',
          'use Internet'
        )
        AND
        MONTH(STR_TO_DATE(survey_date, '%m/%d/%Y')) = ${month}
        AND YEAR(STR_TO_DATE(survey_date, '%m/%d/%Y')) = ${year}
      GROUP BY journey
    )

    UNION ALL

    (
      -- GRAND TOTAL
      SELECT
        'ZZZ' AS journey,
        'Grand Total' AS row_label,
        COUNT(CASE WHEN region = 'BALINUSRA' THEN 1 END) AS balinusra,
        COUNT(CASE WHEN region = 'JATENG' THEN 1 END) AS jateng,
        COUNT(CASE WHEN region = 'JATIM' THEN 1 END) AS jatim,
        (COUNT(CASE WHEN region = 'BALINUSRA' THEN 1 END) + COUNT(CASE WHEN region = 'JATENG' THEN 1 END) + COUNT(CASE WHEN region = 'JATIM' THEN 1 END)),
        3 AS lvl,
        ${yearMonth} AS periode
      FROM nps_indihome
      WHERE
          journey IN (
          'ACTIVATE',
          'get support',
          'use Indihome TV',
          'use Internet'
        )
        AND
        MONTH(STR_TO_DATE(survey_date, '%m/%d/%Y')) = ${month}
        AND YEAR(STR_TO_DATE(survey_date, '%m/%d/%Y')) = ${year}
    )

    ORDER BY
      journey,
      lvl,
      row_label;

  `;

  try {
    await queryAsync(sql, [yearMonth, currentDate, likePeriode, yearMonth, currentDate, likePeriode]);

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
