const firebase = require('firebase-admin');
const PdfPrinter = require('pdfmake');
const { v4: uuidv4 } = require('uuid');
const { Parser } = require('json2csv');
const expirydate = {action: 'read', expires: '03-09-2500'};







async function printPdf(fonts, docDefinition, res, fullReuslts){
  const printer = new PdfPrinter(fonts);
	let pdfDoc = printer.createPdfKitDocument(docDefinition);
	
  const bucket = firebase.storage().bucket('wholesaleduuka-418f1.appspot.com');
	const gcsname = `${uuidv4()}.pdf`;
	const file = bucket.file(gcsname);
	const stream = file.createWriteStream({
		metadata: {
			contentType: 'application/pdf'
		}
	});
  pdfDoc.pipe(stream);
	stream.on('error', (err) => {
		console.log(err);
	});
	stream.on('finish', () => {
		file.getSignedUrl(expirydate).then(url => {
     const pdfUrl = url[0];
     res.status(200).json({pdfUrl: pdfUrl, csvUrl: url[0]});
		});
	});
	pdfDoc.end();
}

async function printCsv(fullReuslts, res){
  const json2csvParser = new Parser();
  const csv = json2csvParser.parse(fullReuslts);
  const bucket = firebase.storage().bucket('wholesaleduuka-418f1.appspot.com');
	const gcsname = `${uuidv4()}.csv`;
  const file = bucket.file(gcsname);
  file.save(csv, function(err){
    if(err) throw err;
    file.getSignedUrl(expirydate).then(url => {
      res.status(200).json({csvUrl: url[0]});
  });
  });
  

}





module.exports = {printPdf, printCsv};