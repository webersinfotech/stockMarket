const mutualfunds = require('./mutualfund_beautify.json')
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
const axios = require("axios");
const cluster = require('cluster');
const { performance } = require('perf_hooks');

(async () => {
    // const insertData = []
    // for (let mutualfund of mutualfunds) {
    //     insertData.push({
    //         id: uuid(),
    //         code: mutualfund.schemeCode,
    //         name: mutualfund.schemeName
    //     })
    // }
    // const data = await knex.insert(insertData)
    // .into('mutualFund')
    // const mutualfunds = await knex.select('id', 'code', 'fundHouse', 'schemeType', 'schemeCategory').from('mutualFund').limit(100)
    // for (let [index, mutualfund] of mutualfunds.entries()) {
    //     const { data: mfdata } = await axios.get(`https://api.mfapi.in/mf/${mutualfund.code}`)
    //     console.log(mutualfund.code, index)
    // }

    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        const connections = new Array(4).fill(0)

        for (let [index, connection] of connections.entries()) {
            await launchCluster()
        }

        async function launchCluster() {
            const clusterLaunch = cluster.fork()
            clusterLaunch.on('exit', async (worker, code, signal) => {
                console.log(`worker died`);
    
                await launchCluster()
            });
            await timeout(3000)
            return
        }
    } else {
        (async () => {
            let mutualfunds

            try {
                mutualfunds = await knex.select('id', 'code', 'schemeInfoStatus').from('mutualFund').where({
                    status: 'NOT STARTED'
                }).limit(200)

                console.log('mutualfunds Fetched')

                const mutualfundsIds = []

                mutualfunds.map((mutualfund) => mutualfundsIds.push(mutualfund.id))

                console.log('Mutual Fund ID array ready', mutualfundsIds)

                if (mutualfundsIds.length) {
                    await knex.raw(`UPDATE mutualFund SET status = ? WHERE id IN (?)`, ['PROCESSING', mutualfundsIds])
                }

                console.log('Updating status')
            } catch (err) {
                console.log(err)
            }

            for (let mutualfund of mutualfunds) {
                const t0 = performance.now()
                
                try {
                    const insertHistoricalRates = []

                    const { data: mfdata } = await axios.get(`https://api.mfapi.in/mf/${mutualfund.code}`)

                    if (mutualfund.schemeInfoStatus === 'NOT STARTED') {
                        await knex('mutualFund').where({
                            id: mutualfund.id
                        }).update({
                            fundHouse: mfdata.meta?.fund_house,
                            schemeType: mfdata.meta?.scheme_type,
                            schemeCategory: mfdata.meta?.scheme_category,
                            schemeInfoStatus: 'SUCCESS'
                        })
                    }

                    const existingHistorialData = await knex.select('id', 'date').from('mutualFundHistoricalRates').where({
                        mutualFundId: mutualfund.id
                    })

                    const existingHistorialDataMap = new Map()
                    existingHistorialData.map((rate) => existingHistorialDataMap.set(`${rate.date}`, undefined))
                    
                    for (let rate of mfdata.data) {
                        if (!existingHistorialDataMap.has(`${rate.date}`)) {
                            insertHistoricalRates.push({
                                id: uuid(),
                                mutualFundId: mutualfund.id,
                                date: rate.date.split("-").reverse().join("-"),
                                nav: parseFloat(rate.nav)
                            })
                        }
                    }

                    await knex.insert(insertHistoricalRates)
                    .into('mutualFundHistoricalRates')

                    await knex('mutualFund').where({
                        id: mutualfund.id
                    }).update({
                        status: 'SUCCESS'
                    })

                    const t1 = performance.now()
        
                    console.log(`${mutualfund.code} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
                } catch(err) {
                    console.log(err)

                    await knex('mutualFund').where({
                        id: mutualfund.id
                    }).update({
                        status: 'FAILED',
                        schemeInfoStatus: 'FAILED'
                    })

                    const t1 = performance.now()

                    console.log(`${mutualfund.code} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
                }
            }
        })()
    }
})()