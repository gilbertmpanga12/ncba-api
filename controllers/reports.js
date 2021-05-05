const {Router} = require('express');
const router = Router();
const {printCsv, printPdf, getWeeklyCsv} = require('../helpers/reports');
const generateReport = require('../reports/generate_report');
const {getLuck3Report} = require('../helpers/reports');

var fonts = {
	Roboto: {
		normal: 'controllers/fonts/ProximaSoft-Regular.ttf',
        bold: 'controllers/fonts/ProximaSoft-Regular.ttf',
        italics: 'controllers/fonts/ProximaSoft-Regular.ttf',
        bolditalics: 'controllers/fonts/ProximaSoft-Regular.ttf'
	}
 };

 var docDefinition = {
	header: {
        margin:  [23,6,18,28],
        columns: [
            {
                image: 'controllers/fonts/logo.png',
				width: 100
			}
        ]
    },
	content: [
		{text: 'T', style: 'subheader'},
		{
			layout: 'lightHorizontalLines',
			table: {
			 headerRows: 1,
			  widths: [ '*', '*'],
	  
			  body: [
				[ 'Customer ID', 'Reference Id']
			  ]
			}
		  },
		  { text: ' ', alignment: 'left', fontSize: 18,  bold:true},
		  { text: ' ', alignment: 'left', fontSize: 18,  bold:true},
		  { text: ' ', alignment: 'left', fontSize: 18,  bold:true},
		  { text: ' ', alignment: 'left', fontSize: 18,  bold:true},
		  { text: 'Organization:', alignment: 'left', fontSize: 17, color: '#D1D5DB',  bold:true, lineHeight:1.5},
		  { text: 'Name:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Signature:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Date:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: '', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Organization:', alignment: 'left', fontSize: 17, color: '#D1D5DB',  bold:true, lineHeight:1.5},
		  { text: 'Name:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Signature:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Date:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: '', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Organization:', alignment: 'left', fontSize: 17, color: '#D1D5DB',  bold:true, lineHeight:1.5},
		  { text: 'Name:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Signature:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5},
		  { text: 'Date:', alignment: 'left', fontSize: 17, color: '#D1D5DB', bold:true, lineHeight:1.5}
		  
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
	docDefinition.content[1].table.body = [
		[ 'Customer ID', 'Reference ID']
	];
	docDefinition.content[0].text = 'Randomised 10 Lucky Winners for this week';
	randomisedWinners.forEach(customer => {
		docDefinition.content[1].table.body.push([customer['Customer Number'], customer['Loan Reference']]);
	});
	await printPdf(fonts, docDefinition, res);
    
});


router.post('/generate-csv', async (req,res) => {
    const randomisedWinners = req.body;
	await printCsv(randomisedWinners, res);
    
});

router.post('/generate-weekly-csv', async (req,res) => {
    const {count, name} = req.body;
	generateReport(name, count, res);
});


router.post('/get-lucky-3-report/:type', async (req, res, next) => {
    const {name} = req.body;
	const _docDefinition = Object.assign({}, docDefinition);
	const type = req.params['type'];
    if(type === "csv"){
		const _processReportsCsv = await getLuck3Report(name, type, fonts, _docDefinition, res);
		return;
	}
	_docDefinition.content[0].text = 'Randomised Lucky 3 Winners For Project '+ name;
	const _processReportsPdf = await getLuck3Report(name, type, fonts, _docDefinition, res);
});


module.exports = router;