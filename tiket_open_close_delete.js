const pool = require('./connection');
const fs = require('fs').promises;
const path = require('path');

/**
 * Fungsi untuk menghapus isi folder secara bersih
 */
async function clearFolder(folderPath) {
  try {
    // Gunakan path absolut agar aman dari mana pun skrip dijalankan
    const absolutePath = path.resolve(__dirname, folderPath);

    // Cek apakah folder ada
    await fs.access(absolutePath);

    const files = await fs.readdir(absolutePath);

    for (const file of files) {
      const filePath = path.join(absolutePath, file);
      const stats = await fs.stat(filePath);

      if (stats.isDirectory()) {
        await fs.rm(filePath, { recursive: true, force: true });
        console.log(`🗑️ Folder dihapus: ${file}`);
      } else {
        await fs.unlink(filePath);
        console.log(`📄 File dihapus: ${file}`);
      }
    }
    console.log(`✅ Isi folder ${folderPath} berhasil dikosongkan.`);
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.warn(`⚠️ Folder tidak ditemukan, dilewati: ${folderPath}`);
    } else {
      console.error(`❌ Gagal membersihkan ${folderPath}:`, err.message);
    }
  }
}

/**
 * Fungsi Utama Pembersihan
 */
async function runCleanup() {
  try {
    console.log('🔄 Memulai proses pembersihan sistem...');

    // 1. Kosongkan Tabel OTP
    // Menggunakan TRUNCATE biasanya lebih cepat daripada DELETE untuk mengosongkan tabel
    await pool.query('TRUNCATE TABLE perf_tif.otp_for_extract');
    console.log('✅ Tabel OTP telah di-reset (Truncated).');

    // 2. Daftar folder yang akan dikosongkan
    const foldersToClear = ['./extracted', './extracted-files', './file_download'];

    for (const folder of foldersToClear) {
      await clearFolder(folder);
    }

    console.log('🏁 Seluruh proses pembersihan selesai!');
  } catch (err) {
    console.error('💥 Terjadi kesalahan saat cleanup:', err.message);
  } finally {
    // Jangan pool.end() jika file ini di-require oleh file utama (Main Controller)
    // Gunakan pool.end() hanya jika file ini dijalankan secara mandiri (node cleanup.js)
    if (require.main === module) {
      await pool.end();
      console.log('🔒 Koneksi database ditutup.');
    }
  }
}

// Menjalankan fungsi jika file dipanggil langsung
if (require.main === module) {
  runCleanup();
}

// Mengekspor fungsi agar bisa dipakai di file utama (Main Controller)
module.exports = runCleanup;
