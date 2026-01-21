const fs = require('fs');
const cheerio = require('cheerio');
const path = require('path');

function convertXLSToCSV(fileName) {
  const filePath = `D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment/${fileName}.xls`;
  // Baca file sebagai teks
  let html = fs.readFileSync(filePath, 'utf8');

  // Bersihkan karakter tidak valid
  html = html.replace(/[\x00-\x09\x0B-\x1F]/g, '');

  // Load HTML
  const $ = cheerio.load(html);

  // Pastikan tabel ada
  const table = $('table');
  if (table.length === 0) {
    console.error('❌ Tidak ada <table> ditemukan dalam file!');
    process.exit();
  }

  let rows = [];

  // Loop baris
  table.find('tr').each((i, tr) => {
    let row = [];

    $(tr)
      .find('td, th')
      .each((j, cell) => {
        let text = $(cell).text().trim();
        text = text.replace(/\s+/g, ' ').trim(); // Normalisasi spasi
        row.push(text);
      });

    if (row.length > 0 && row.some((col) => col !== '')) {
      rows.push(row);
    }
  });

  let csvContent = rows
    .map((r) =>
      r
        .map(
          (col) => `"${col.replace(/"/g, '""')}"`, // escape double quotes
        )
        .join(','),
    )
    .join('\n');

  // Lokasi save CSV
  const csvPath = filePath.replace('.xls', '.csv');
  fs.writeFileSync(csvPath, csvContent);

  console.log(`file ${fileName}.csv berhasil disimpan dengan jumlah baris: ${rows.length}`);
}

// convertXLSToCSV('data_detail_202511');
// convertXLSToCSV('data_detail_202511 (1)');
// convertXLSToCSV('data_detail_202511 (2)');
// convertXLSToCSV('data_detail_202511 (3)');
// convertXLSToCSV('data_detail_202511 (4)');
// convertXLSToCSV('data_detail_202511 (5)');
// convertXLSToCSV('data_detail_202511 (6)');
// convertXLSToCSV('data_detail_202511 (7)');
// convertXLSToCSV('data_detail_202511 (8)');
// convertXLSToCSV('data_detail_202511 (9)');
// convertXLSToCSV('data_detail_202511 (10)');

// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-4'_''_'HSI24_-");
// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-4'_''_'HSI24_- (1)");
// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-5'_''_'HSI24_-");
// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-5'_''_'HSI24_- (1)");

// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-4'_''_'DATIN24_-");
// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-4'_''_'DATIN24_- (1)");
// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-5'_''_'DATIN24_-");
// convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-5'_''_'DATIN24_- (1)");

convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-4'_''_'HSI24_-");
convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-4'_''_'HSI24_- (1)");
convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-5'_''_'HSI24_-");
convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'REG-5'_''_'HSI24_- (1)");
