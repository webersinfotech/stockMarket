const express = require('express')
var bodyParser = require('body-parser');
// const ngrok = require('ngrok');
const app = express()
const knex = require('knex')({
    client: 'mysql',
    connection: {
        host:'google-account.cmlfk75xsv3h.ap-south-1.rds.amazonaws.com', 
        user: 'shahrushabh1996', 
        database: 'rapidTax',
        password: '11999966',
        ssl: 'Amazon RDS'
    }
});
const { uuid } = require('uuidv4');
const {attachOnDuplicateUpdate} = require('knex-on-duplicate-update');
attachOnDuplicateUpdate();

app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true}));

app.use(express.json())

app.post('/stockMarket', async (req, res) => {
    res.send('Data Received')
    const stocks = req.body.data
    let existingStocks = await knex.select('*').from('stockMarket')
    const stockMap = new Map()
    existingStocks.map((stock) => stockMap.set(stock.googleCode, stock.id))
    const insertData = []
    for (let stock of stocks) {
        const stockData = stockMap.get(stock.googleCode)
        stockMap.has(stock.googleCode) ? insertData.push({
            id: stockData,
            ...stock
        }) : insertData.push({
            id: uuid(),
            ...stock
        })
    }
    const result = await knex.insert(insertData)
    .into('stockMarket')
    .onDuplicateUpdate('price', 'priceopen', 'high', 'low', 'volume', 'marketcap', 'tradetime', 'datadelay', 'volumeavg', 'pe', 'eps', 'high52', 'low52', 'changeprice', 'beta', 'changepct', 'closeyest', 'shares', 'currency');
    console.log(result)
})

app.post('/currency', async (req, res) => {
    res.send('Data Received')
    const currencies = req.body.data
    let existingCurrencies = await knex.select('*').from('currencyConversation')
    const currencyMap = new Map()
    existingCurrencies.map((currency) => currencyMap.set(currency.googleCode, currency.id))
    const insertData = []
    for (let currency of currencies) {
        const currencyData = currencyMap.get(currency.googleCode)
        currencyMap.has(currency.googleCode) ? insertData.push({
            id: currencyData,
            ...currency
        }) : insertData.push({
            id: uuid(),
            ...currency
        })
    }
    const result = await knex.insert(insertData)
    .into('currencyConversation')
    .onDuplicateUpdate('rate')
    console.log(result)
})

app.listen(3000, async () => {
    console.log('Listening on 3000')
    // const url = await ngrok.connect(3000);
    // console.log(url)
})

// if (existingStocks.length) {
        //     await knex('stockMarket').where({id: existingStocks[0].id}).update(stock)
        // } else {
        //     await knex('stockMarket').insert({
        //         id: uuid(),
        //         ...stock
        //     })
        // }