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
		const bucketName = 'wholesaleduuka-418f1.appspot.com';
		let printer = new PdfPrinter(fonts);
		let pdfDoc = printer.createPdfKitDocument(docDefinition);
		const bucket = firebase.storage().bucket(bucketName);
		
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
	var outputFile = path.join('./', `weeks/${name}_week_${count}.csv`);
	const createOutFile = fs.createWriteStream(outputFile);
	writeToFile(name, count).pipe(createOutFile).on("finish", async () => {
		if(Number(count) > 1){
			const outputChildFile = path.join('./', `weeks/${name}_week_${count}_child.csv`);
			const createOutFileChild = fs.createWriteStream(outputChildFile);
			writeToFile(name, (count - 1)).pipe(createOutFileChild).on("finish", () => {
				var mergedFilePath = path.join('./', `weeks/${name}_merged_week_${count}_diff_${count-1}.csv`);
				fileMerge([outputFile, outputChildFile], mergedFilePath, name, count, res);
			});
		}else{
			const _uploadFile= await uploadFile(outputFile, name, count, res);
		}
		
	});
}


async function fileMerge(inputPaths, outputFilePath, name, count, res){
	try{
		const operation = await mergeFile(inputPaths, outputFilePath);
		const _uploadFile= await uploadFile(outputFilePath, name, count, res);
		
	}catch(e){
		logger.info("Failed to merge files", e);
		throw e;
	}
}


async function uploadFile(outputFilePath, name, count, res){
	try{
		const uploadFile = await firebase.storage().bucket("wholesaleduuka-418f1.appspot.com")
		.upload(outputFilePath);
		const signUrl = await uploadFile[0].getSignedUrl(expirydate);
		const url = signUrl[0];
		res.status(200).send({csvUrl: url});
		const storeInFirebase = await firestore()
		.collection(`${name}_${count}_current_week_report`).doc(`${count}`).set({url: url},
		 {merge: true});
	}catch(e){
		logger.info("Failed to upload fiels to firebase", e);
		res.status(500).send({message: "Something went wrong while generating csv file"});
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


async function getLuck3Report(name, type, fonts, docDefinition ,res){
	try{
	  const pdfTable = docDefinition;
	  const winners = [];
	  const lucky3Winners = await firestore().collection(`${name}_winner3_details`).get();
	  lucky3Winners.forEach(winner => winners.push(winner.data()));
	  if(type === "csv"){
		const _saveFile= await printCsv(winners, res);
	  }else{
		const addItemsToPdfTable = winners.forEach(customer =>{
			pdfTable.content[1].table.body.push([customer['Customer Number'],customer['Loan Reference']]); 
		  });
		  printPdf(fonts, pdfTable, res);
	  }
	  
	}catch(e){
      res.status(500).send({message: "Failed to get lucky3 report " + e});
	}
  }




module.exports = {printPdf, printCsv, getWeeklyCsv, getLuck3Report};