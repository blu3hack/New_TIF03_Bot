// loadfile.js
const { spawn } = require('child_process');

function runCommand(cmd, args = [], label = '', delay = 0) {
  return new Promise((resolve) => {
    if (label) console.log(label);

    const child = spawn(cmd, args, {
      cwd: process.cwd(),
      stdio: 'inherit',
      shell: true,
    });

    child.on('close', (code) => {
      console.log(`✅ Proses ${[cmd, ...args].join(' ')} selesai (exit code: ${code})`);
      if (delay > 0) {
        console.log(`⏳ Menunggu ${delay / 1000} detik...`);
        setTimeout(resolve, delay);
      } else {
        resolve();
      }
    });
  });
}

async function main() {
  console.log('Melakukan Proses Loaded filedata ke dalam Database');
  await new Promise((r) => setTimeout(r, 5000)); // delay 5 detik

  await runCommand('node', ['mttr_mso'], '\n========= Proses load data MTTR MSO =========', 5000);
  await runCommand('node', ['wsa_gamas'], '\n========= Proses load data wsa gamas =========', 5000);
  await runCommand('node', ['wsa_assurance'], '\n========= Proses load data wsa Assurance =========', 5000);
  await runCommand('node', ['assurance_strive'], '\n========= Proses load data Assurance Strive =========', 5000);
  // await runCommand('node', ['convert_fulfillment'], '\n========= Proses Convert xls ke csv =========', 5000);
  // await runCommand('node', ['download_fulfillment'], '\n========= Proses Collect data TTR FFG  =========', 5000);
  // await runCommand('node', ['insert_ttr_ffg_download'], '\n========= Proses Insert Data TTR FFG =========', 5000);

  console.log('\nEksekusi selesai. Tunggu sebentar sebelum menutup...');
  await new Promise((r) => setTimeout(r, 3000)); // delay 3 detik
}

main();
