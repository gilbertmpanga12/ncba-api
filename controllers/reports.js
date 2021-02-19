const {Router} = require('express');
const router = Router();
const {printCsv, printPdf} = require('../helpers/reports');

var fonts = {
	Roboto: {
		normal: 'controllers/fonts/ProximaSoft-Regular.ttf',
        bold: 'controllers/fonts/ProximaSoft-Regular.ttf',
        italics: 'controllers/fonts/ProximaSoft-Regular.ttf',
        bolditalics: 'controllers/fonts/ProximaSoft-Regular.ttf'
	}
 };

 var docDefinition = {
	content: [
		{text: 'T', style: 'subheader'},
		{
			layout: 'lightHorizontalLines',
			table: {
			 headerRows: 1,
			  widths: [ '*', 'auto', 100, '*', '*'],
	  
			  body: [
				[ 'customerId', 'points']
			  ]
			}
		  }
	],
	styles: {
		header: {
			fontSize: 18,
			bold: true,
			margin: [0, 0, 0, 10]
		},
		subheader: {
			fontSize: 16,
			bold: true,
			margin: [0, 10, 0, 5]
		},
		tableExample: {
			margin: [0, 5, 0, 15]
		},
		tableHeader: {
			bold: true,
			fontSize: 13,
			color: 'black'
		}
	},
	defaultStyle: {
		// alignment: 'justify'
    }};



router.post('/generate-pdf', async (req,res) => {
    const randomisedWinners = req.body;
	await printPdf(fonts, docDefinition, res, randomisedWinners);
    
});


router.post('/generate-csv', async (req,res) => {
    const randomisedWinners = req.body;
	await printCsv(fonts, docDefinition, res, randomisedWinners);
    
});



module.exports = router;