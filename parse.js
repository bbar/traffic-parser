/**

NOTE: graceful-fs is used to avoide errors like EMFILE: too many open files

node parse.js \
--sources="/some/path/a.csv.gz /some/path/b.csv.gz /some/path/c.csv.gz /some/path/d.csv.gz" \
--destination="/some/path/data/parsed/intervals" \
--batch=5000

 */

const argv = require('yargs').argv
const { once } = require('events');
// const fs = require('fs');
const fs = require('graceful-fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');

function write(obj, destination) {
  try {
    for (const key in obj) {
      const entry = `${obj[key].join(os.EOL)}${os.EOL}`;
      fs.appendFile(path.join(...[destination, `${key}.csv`]), entry, (error) => {
        // if (error) throw error;
        if (error) console.log(error);
      });
    }
  } catch (error) {
    throw error;
  }
}

async function parse(source, destination, batch) {
  try {
    const fileStream = fs
      .createReadStream(source)
      .pipe(zlib.createGunzip());

    const rl = readline
      .createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

    let cnt = 0;
    let obj = {};

    rl
      .on('line', (line) => {
        cnt += 1;
        const [start, end, ...speeds] = line.split(',');

        speeds.forEach((speed, i) => {
          const entry = `${start},${end},${speed}`;
          if (obj[i]) {
            obj[i].push(entry);
          } else {
            obj[i] = [entry];
          }
        });

        if (cnt % batch === 0) {
          write(obj, destination)
          obj = {};
        }
      })
      .on('close', () => {
        write(obj, destination)
      });

    await once(rl, 'close');
  } catch (error) {
    throw error;
  }
}

(async () => {
  try {
    const start = Date.now();

    const {
      sources,
      destination,
      batch = 2000,
    } = argv;

    if (!sources) throw new Error('missing --sources');
    if (!destination) throw new Error('missing --destination');

    await fs.promises.mkdir(destination, { recursive: true });

    for (const source of sources.split(' ')) {
      await parse(source, destination, batch);
    }

    console.log(`Finished in ${Date.now() - start}ms`);
  } catch (error) {
    console.log(error);
  }
})();
