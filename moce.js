const fs = require('fs');
const path = require('path');

const source = 'C:/Users/L/Downloads/';
const destination = 'D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment';

// Pastikan folder tujuan ada
if (!fs.existsSync(destination)) {
  fs.mkdirSync(destination, { recursive: true });
}

fs.readdir(source, (err, files) => {
  if (err) return console.error('Gagal membaca folder:', err);

  files.forEach((file) => {
    if (file.includes('data_detail_') && (file.endsWith('.xlsx') || file.endsWith('.xls'))) {
      const oldPath = path.join(source, file);
      const newPath = path.join(destination, file);

      // Copy file
      const readStream = fs.createReadStream(oldPath);
      const writeStream = fs.createWriteStream(newPath);

      readStream.pipe(writeStream);

      writeStream.on('finish', () => {
        // Hapus file asal setelah berhasil dicopy
        fs.unlink(oldPath, (err) => {
          if (err) {
            console.error(`Gagal menghapus file asal ${file}:`, err);
          } else {
            console.log(`Berhasil memindahkan file: ${file}`);
          }
        });
      });

      writeStream.on('error', (err) => {
        console.error(`Gagal menulis file ${file}:`, err);
      });
    }
  });
});
