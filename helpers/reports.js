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
			logger.info(pdfUrl);
			res.status(200).json({pdfUrl: pdfUrl});
			
			});
	}catch(e){
		logger.info('PDF CREATION ERROR', e);
		res.status(500).send({message: e});
	}
}

async function getWeeklyCsv(count, name, res){
	try{
		const results_array = [];
		const winners_array= [];
		const winners = await firestore().collection(`${name}_week_${count}_winners`).get();

		winners.forEach(winner => {
			const customer_data = winner.data();
			winners_array.push({
				"Customer Number": customer_data["Customer Number"],
				"Loan Reference": customer_data["Loan Reference"],
				"Loan Repaid Date": customer_data["Loan Repaid Date"],
				"Loan Start Date": customer_data["Loan Start Date"]
			  });
		});

		const results = await firestore().collection(`${name}_week_${count}_customer_details`).limit(80000).get();
		results.forEach(customer => {
			const customer_data = customer.data();
			results_array.push({
				"Customer Number": customer_data["Customer Number"],
				"Loan Reference": customer_data["Loan Reference"],
				"Loan Repaid Date": customer_data["Loan Repaid Date"],
				"Loan Start Date": customer_data["Loan Start Date"]
			  });// [customer_data['Customer Number'], customer_data['Loan Reference']]
		});

		results_array.push(...winners_array); // store back winners for that week
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
		logger.info(url)
		res.status(200).json({csvUrl: url[0]});
	});
	});
	
  }catch(e){
	logger.info('CSV CREATION ERROR', e);
		res.status(500).send({message: e});
  }

}





module.exports = {printPdf, printCsv, getWeeklyCsv};