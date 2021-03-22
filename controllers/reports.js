const {Router} = require('express');
const router = Router();
const {printCsv, printPdf, getWeeklyCsv} = require('../helpers/reports');

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
				[ 'Customer ID', 'Points']
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
	docDefinition.content[1].table.body = [
		[ 'Customer ID', 'Points']
	];
	docDefinition.content[0].text = 'Randomised 10 Lucky Winners';
	
	randomisedWinners.forEach(customer => {
		docDefinition.content[1].table.body.push(Object.values(customer));
	});
	await printPdf(fonts, docDefinition, res);
    
});


router.post('/generate-csv', async (req,res) => {
    const randomisedWinners = req.body;
	await printCsv(randomisedWinners, res);
    
});

router.post('/generate-weekly-csv', async (req,res) => {
    const {count, name} = req.body;
	getWeeklyCsv(count, name, res);
});



module.exports = router;