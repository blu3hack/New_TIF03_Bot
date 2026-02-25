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
  await runCommand('node', ['telkomcare'], '\n========= Proses Convert File Data =========', 5000);
  await runCommand('node', ['telkomcare_input'], '\n========= Proses Input File Data =========', 5000);
  await runCommand('node', ['telkomcare_convert'], '\n========= Proses Convert File Data =========', 5000);
  await runCommand('node', ['telkomcare_input_sugar_datin'], '\n========= Proses load data SUgar Datin =========', 5000);
  await runCommand('node', ['telkomcare_input_sugar_hsi'], '\n========= Proses load data Sugar HSI =========', 5000);
  await runCommand('node', ['telkomcare_input_ttr_datin'], '\n========= Proses load data TTR Datin =========', 5000);
  await runCommand('node', ['telkomcare_input_ttr_indibiz'], '\n========= Proses load data TTR INDIBIZ =========', 5000);
  await runCommand('node', ['telkomcare_input_ttr_reseller'], '\n========= Proses load data TTR RESELLER =========', 5000);
  await runCommand('node', ['telkomcare_input_all'], '\n========= Proses load data ALL TELKOMCARE =========', 5000);
  console.log('\nEksekusi selesai. Tunggu sebentar sebelum menutup...');
  await new Promise((r) => setTimeout(r, 3000)); // delay 3 detik
}

main();
