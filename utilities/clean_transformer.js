const stream = require('stream');
const moment = require('moment');
const e = require('cors');

function validateJSONData(){
    const transformStream = new stream.Transform({objectMode: true});
    transformStream._transform = (chunk, encoding, callback) => {
        let data = chunk;
        data['_id'] = data['Customer Number'];
        data['Loan Repaid Date'] = moment(data["Loan Repaid Date"]).format();
        data['Loan Start Date'] = moment(data["Loan Start Date"]).format();
        transformStream.push(data);
        callback();
        // try{
        //  if(csv_data["Customer Number"].trim() && 
        //   csv_data["Loan Reference"].trim() && 
        //   csv_data["Loan Repaid Date"].trim() 
        //   && csv_data["Loan Start Date"].trim()){
            
        // }else{
        //     const error_message = `Please check your csv file for missing 
        //     blank customer numbers and empty fields`;
        //     const error = new Error(error_message);
        //     callback(error);
        //     throw error;
        // }
        // }catch(e){
        //     const error_message = `Please check your csv file for missing cells or invalid fields entered`;
        //     const error = new Error(error_message);
        //     callback(error);
        //     throw error;
        
    }
    return transformStream;
}


module.exports = validateJSONData;