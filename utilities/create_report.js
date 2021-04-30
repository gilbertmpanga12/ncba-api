const stream = require('stream');
const papaparse = require('papaparse');
const fs = require('fs');


function writeToFile(outpathFile, res, hasFinished){
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
    writeStream.on("finish", () => {
        console.log('I am done!!!! creating file');
        fileOutputStream.end();
        // res.status(200).send({csvUrl:})
        
    })
  
    return writeStream;
}


module.exports = writeToFile;
