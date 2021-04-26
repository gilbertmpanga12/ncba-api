const firebase = require('firebase-admin');
const PdfPrinter = require('pdfmake');
const { Parser } = require('json2csv');
const expirydate = {action: 'read', expires: '03-09-2500'};
const {nanoid} = require('nanoid');
const {logger} = require('../helpers/logger');
const writeToFile = require('../utilities/create_report');
const fs = require('fs');
const path = require('path');
const mergeFile = require("merge-files");
const { firestore } = require('firebase-admin');

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

 function getWeeklyCsv(count, name, res){
	let outputFile = path.join('./', `weeks/${name}_week_${count}.csv`);
	const createOutFile = fs.createWriteStream(outputFile);
	writeToFile(name, count).pipe(createOutFile).on("finish", () => {
		if(Number(count) > 1){
			const outputChildFile = path.join('./', `weeks/${name}_week_${count}_child.csv`);
			const createOutFileChild = fs.createWriteStream(outputChildFile);
			writeToFile(name, count).pipe(createOutFileChild).on("finish", () => {
				const mergedFilePath = `${name}_merged_week_${count}_diff_${count-1}`;
				fileMerge([outputFile, outputChildFile], mergedFilePath);
				return;
			});
		};
		const _uploadFile= await uploadFile(outputFile);
	});
}


async function fileMerge(inputPaths, outputFilePath){
	try{
		const operation = await mergeFile(inputPaths, outputFilePath);
		const _uploadFile= await uploadFile(outputFilePath);
		
	}catch(e){
		logger.info("Failed to merge files", e);
		throw e;
	}
}


async function uploadFile(outputFilePath){
	try{
		const uploadFile = await firebase.storage().bucket("wholesaleduuka-418f1.appspot.com")
		.upload(outputFilePath);
		const signUrl = await uploadFile[0].getSignedUrl(expirydate);
		const url = signUrl[0];
		// const storeInFirebase = await firestore().collection(``).doc(``).set({});
	}catch(e){
		logger.info("Failed to upload fiels to firebase", e);
		return e;
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