/**

NOTE: graceful-fs is used to avoide errors like EMFILE: too many open files

node parse.js \
--sources="/some/path/a.csv.gz /some/path/b.csv.gz /some/path/c.csv.gz /some/path/d.csv.gz" \
--destination="/some/path/data/parsed/intervals" \
--batch=5000

 */

const argv = require('yargs').argv;
const process = require('process');
const { once } = require('events');
// const fs = require('fs');
const fs = require('graceful-fs');
const util = require('util');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');

let linesParsed = 0;
let totalLines = 0;

function updateProgress(progress){
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Progress: ${progress}%`);
}

function write(obj, destination) {
  try {
    for (const key in obj) {
      const entry = `${obj[key].join(os.EOL)}${os.EOL}`;
      fs.appendFile(path.join(...[destination, `${key}.csv`]), entry, (error) => {
        // if (error) throw error;
        if (error) console.log(error);
      });
    }

    linesParsed += Object.keys(obj).length;
    updateProgress((linesParsed / totalLines).toFixed(2));
    // updateProgress((linesParsed / totalLines));
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

        if (batch === 0 || cnt % batch === 0) {
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
      batch = 5,
    } = argv;

    if (!sources) throw new Error('missing --sources');
    if (!destination) throw new Error('missing --destination');

    const _sources = sources.split(' ');

    await fs.promises.mkdir(destination, { recursive: true });
    const exec = util.promisify(require('child_process').exec);

    for (const source of _sources) {
      const { stdout } = await exec(`cat ${source} | wc -l`);
      totalLines += Number(stdout);
    }

    for (const source of _sources) {
      await parse(source, destination, batch);
    }

    process.stdout.write(`${os.EOL}Finished in ${Date.now() - start}ms${os.EOL}`);
  } catch (error) {
    console.log(error);
  }
})();
