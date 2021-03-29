const firebase = require('firebase-admin');
const PdfPrinter = require('pdfmake');
const { Parser } = require('json2csv');
const expirydate = {action: 'read', expires: '03-09-2500'};
const {nanoid} = require('nanoid');
const { firestore } = require('firebase-admin');
const {logger} = require('../helpers/logger');


async function printPdf(fonts, docDefinition, res){
	try{
		let printer = new PdfPrinter(fonts);
		let pdfDoc = printer.createPdfKitDocument(docDefinition);
		const bucket = firebase.storage().bucket('wholesaleduuka-418f1.appspot.com');
		const gcsname = `${nanoid(10)}.pdf`;
		const file = bucket.file(gcsname);
		let stream = file.createWriteStream({
			metadata: {
				contentType: 'application/pdf'
			}
		});
	    pdfDoc.pipe(stream);
		pdfDoc.end();
		file.getSignedUrl(expirydate).then(url => {
			const pdfUrl = url[0];
			console.log(pdfUrl);
			res.status(200).json({pdfUrl: pdfUrl});
			
			});
	}catch(e){
		console.log('PDF CREATION ERROR', e);
		res.status(500).send({message: e});
	}
}

async function getWeeklyCsv(count, name, res){
	try{
		const results_array = [];
		const results = await firestore().collection(`${name}_week_${count}_original_details`).get();
		results.forEach(customer => {
			const customer_data = customer.data();
			results_array.push([customer_data['Customer Number'], customer_data['Loan Reference']]);
		});
		printCsv(results_array, res);
	}catch(e){
		logger.info(e);
	}
}

async function printCsv(fullReuslts, res){
  try{
	const json2csvParser = new Parser();
	const csv = json2csvParser.parse(fullReuslts);
	const bucket = firebase.storage().bucket('wholesaleduuka-418f1.appspot.com');
	  const gcsname = `${nanoid(10)}.csv`;
	const file = bucket.file(gcsname);
	file.save(csv, function(err){
	  if(err) throw err;
	  file.getSignedUrl(expirydate).then(url => {
		  console.log(url)
		res.status(200).json({csvUrl: url[0]});
	});
	});
	
  }catch(e){
	console.log('CSV CREATION ERROR', e);
		res.status(500).send({message: e});
  }

}





module.exports = {printPdf, printCsv, getWeeklyCsv};