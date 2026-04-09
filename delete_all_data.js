const fs = require('fs/promises');
const path = require('path');

async function clearFolder(folderPath) {
  try {
    const files = await fs.readdir(folderPath);
    for (const file of files) {
      const filePath = path.join(folderPath, file);
      await fs.rm(filePath, { recursive: true, force: true });
    }
    console.log(`✅ Folder ${folderPath} berhasil dibersihkan`);
  } catch (err) {
    console.error(`❌ Gagal membersihkan ${folderPath}:`, err.message);
  }
}

async function main() {
  const baseDir = path.join(__dirname, 'loaded_file');
  const folders = ['asr_wifi', 'asr_datin', 'wsa', 'wsa_gamas', 'download_fulfillment', 'ff_non_hsi', 'wifi_revi', 'cnop', 'ps_re', 'msa_upload', 'unspec_datin', 'service_sugar', 'mttr_mso', 'valdat', 'qosmo'];
  for (const folder of folders) {
    const fullPath = path.join(baseDir, folder);
    await clearFolder(fullPath);
  }
  console.log('🎉 Semua folder di loaded_file selesai dibersihkan.');
  process.exit(0);
}
main();
