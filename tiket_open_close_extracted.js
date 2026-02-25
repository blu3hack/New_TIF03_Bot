const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

const folderPath = 'D:/Scrappers/Scrapper/extracted';
const open_tr4 = ['open', 'reg4']; // Bisa ditambah kata lain
const close_tr4 = ['close', 'reg4']; // Bisa ditambah kata lain
const open_tr5 = ['open', 'reg5']; // Bisa ditambah kata lain
const close_tr5 = ['close', 'reg5']; // Bisa ditambah kata lainv

const nameFile = [open_tr4, close_tr4, open_tr5, close_tr5];

for (i = 0; i < nameFile.length; i++) {
  function getMatchingFiles(folder, keywords) {
    try {
      const files = fs.readdirSync(folder); // Baca isi folder

      // Filter file berdasarkan keywords
      const matchingFiles = files.filter((file) => keywords.every((keyword) => file.includes(keyword)));

      return matchingFiles;
    } catch (error) {
      console.error('Error membaca folder:', error);
      return [];
    }
  }

  const result = getMatchingFiles(folderPath, nameFile[i]);
  const fileName = result.join('\n');
  const file = `${folderPath}/${fileName}`;
  const outputDir = 'D:/Scrappers/Scrapper/extracted-files';
  const command = `7z x "${file}" -o"${outputDir}" -tzip -y`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`Gagal mengekstrak ${path.basename(file)}:`, error.message);
      return;
    }
    console.log(`Berhasil mengekstrak: ${path.basename(file)}`);
  });
}
