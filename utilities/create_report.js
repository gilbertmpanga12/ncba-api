const stream = require('stream');
const papaparse = require('papaparse');
const fs = require('fs');

function writeToFile(record, outpathFile, hasFinished){
    let firstOutput = true;
    const writeStream = new stream.Writable({objectMode: true});
    const fileOutputStream = fs.createWriteStream(outpathFile);
    writeStream._write = (chunk, encoding, callback) => {
    const outputCsv = papaparse.unparse([chunk], {
        header: firstOutput
    });  
    fileOutputStream.write(outputCsv + "\n");
    firstOutput = false;
    callback();
    };

    if(hasFinished){
        console.log('I am done!!!!');
        fileOutputStream.end();
        writeStream.end();
    }
    

    return writeStream;
}


module.exports = writeToFile;
