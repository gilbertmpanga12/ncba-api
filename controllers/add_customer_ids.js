const {Router} = require('express');
const router = Router();
const {ParallelIndividualWrites, RandomiseLuckyWinners, WriteCustomerDetails, 
    enterGrandDraw} = require('../helpers/parallel_add');

router.post('/add-customer-numbers', async (req, res, next) => {
    const customerIds = req.body['payload'];
    const name = req.body['name'];
    const count = req.body['count'];
    await ParallelIndividualWrites(customerIds, count, res, name);
});

router.post('/add-customer-details', async (req, res, next) => {
    const customerIds = req.body['payload'];
    const name = req.body['name'];
    const count = req.body['count'];
    await WriteCustomerDetails(customerIds, count, res, name);
});

router.get('/randomise-lucky-winners', async (req, res, next) => {
    const name = req.body['name'];
    const count = req.body['count'];
    await RandomiseLuckyWinners(name, count, res);
});

// router.post('/generate-raffle-project', async (req, res, next) => {
//     const uid = req.body['uid'];
//     const count = req.body['count'];
//     const name = req.body['name'];
//     await generateRaffleProject(uid, count, name , res);
// });


router.post('/enter-grand-draw', async (req, res, next) => {
    const uid = req.body['uid'];
    const name = req.body['name'];
    await enterGrandDraw(uid, name, res);
});


module.exports = router;