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

  await runCommand('node', ['ps_re'], '\n========= Proses Pengambilan data di KPRO PS/RE via BOT =========', 5000);
  await runCommand('node', ['ps_re_input'], '\n========= Proses load data KPRO PS/RE =========', 5000);

  console.log('\nEksekusi selesai. Tunggu sebentar sebelum menutup...');
  await new Promise((r) => setTimeout(r, 3000)); // delay 3 detik
}

main();
