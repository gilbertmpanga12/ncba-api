const stream = require('stream');

function validateJSONData(){
    const transformStream = new stream.Transform({objectMode: true});
    transformStream._transform = (chunk, encoding, callback) => {
        let data = chunk;
        data['_id'] = data['Customer Number'];
        transformStream.push(data);
        callback();
    }
    return transformStream;
}


module.exports = validateJSONData;