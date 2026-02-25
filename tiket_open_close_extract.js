const fs = require('fs');
const { exec } = require('child_process');
const path = require('path');
const { extractFull } = require('node-7z');
const pool = require('./connection'); // Menggunakan pool promise

(async () => {
  // 🔹 Helper: Cek apakah 7-Zip terinstall
  function check7zip() {
    return new Promise((resolve, reject) => {
      exec('7z', (error) => {
        if (error) {
          reject(new Error('7-Zip tidak ditemukan di PATH. Silakan instal 7-Zip.'));
        } else {
          resolve();
        }
      });
    });
  }

  // 🔹 Helper: Fungsi Ekstraksi (Dikonversi ke Promise agar bisa di-await)
  function extractFileAsync(zipFilePath, outputDir, password) {
    return new Promise((resolve, reject) => {
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      const extraction = extractFull(zipFilePath, outputDir, {
        password: password,
        $bin: '7z', // Memastikan menggunakan binary 7z
      });

      extraction.on('end', () => {
        console.log(`✅ Berhasil ekstrak: ${path.basename(zipFilePath)}`);
        resolve();
      });

      extraction.on('error', (err) => {
        console.error(`❌ Gagal ekstrak ${path.basename(zipFilePath)}:`, err);
        reject(err);
      });
    });
  }

  // 🔹 Fungsi Utama Ekstraksi
  async function extractAllFiles() {
    try {
      await check7zip();

      const tregs = ['reg4', 'reg5'];
      const types = ['open', 'close'];
      const sourceFolder = path.join(__dirname, 'file_download');
      const outputDir = path.join(__dirname, 'extracted');

      for (const treg of tregs) {
        for (const type of types) {
          const lookupKey = `${type}_${treg}`;

          // 1. Ambil OTP dari database
          const [rows] = await pool.query('SELECT otp FROM otp_for_extract WHERE message = ?', [lookupKey]);

          if (rows.length === 0) {
            console.warn(`⚠️ OTP tidak ditemukan untuk ${lookupKey}, dilewati.`);
            continue;
          }

          const password = rows[0].otp;

          // 2. Cari file di folder download
          if (!fs.existsSync(sourceFolder)) {
            console.error('❌ Folder file_download tidak ditemukan!');
            return;
          }

          const files = fs.readdirSync(sourceFolder);
          const fileName = files.find((f) => f.toLowerCase().includes(type) && f.toLowerCase().includes(treg) && f.endsWith('.zip'));

          if (!fileName) {
            console.warn(`⚠️ File ZIP untuk ${lookupKey} tidak ditemukan di folder.`);
            continue;
          }

          const zipFilePath = path.join(sourceFolder, fileName);

          // 3. Eksekusi Ekstraksi
          console.log(`📦 Mengekstrak ${fileName}...`);
          await extractFileAsync(zipFilePath, outputDir, password);
        }
      }

      console.log('🏁 Seluruh proses ekstraksi selesai!');
    } catch (error) {
      console.error(`💥 Terjadi kesalahan: ${error.message}`);
    } finally {
      await pool.end();
      console.log('🔒 Koneksi database ditutup.');
    }
  }

  // Jalankan
  await extractAllFiles();
})();
