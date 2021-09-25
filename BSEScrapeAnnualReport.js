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
const axios = require("axios");
const { uuid } = require('uuidv4');
const cluster = require('cluster');
const {attachOnDuplicateUpdate} = require('knex-on-duplicate-update');
const { performance } = require('perf_hooks');
attachOnDuplicateUpdate();

async function fetchAnnualReport(code) {
    var config = {
        method: 'get',
        url: `https://api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?scripcode=${code}`,
        headers: { 
          'authority': 'api.bseindia.com', 
          'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"', 
          'accept': 'application/json, text/plain, */*', 
          'sec-ch-ua-mobile': '?0', 
          'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36', 
          'sec-ch-ua-platform': '"macOS"', 
          'origin': 'https://www.bseindia.com', 
          'sec-fetch-site': 'same-site', 
          'sec-fetch-mode': 'cors', 
          'sec-fetch-dest': 'empty', 
          'referer': 'https://www.bseindia.com/', 
          'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8', 
          'if-modified-since': 'Mon, 20 Sep 2021 12:01:52 GMT'
        }
    };
    const { data } = await axios(config)
    return data
}

// (async () => {
//     let companies

//     try {
//         companies = await knex.select('id', 'code').from('stockMarket').where('googleCode', 'like', 'BOM:%').where({
//             reportStatus: 'NOT STARTED'
//         }).limit(1)

//         console.log('companies Fetched')

//         const companiesIds = []

//         companies.map((company) => companiesIds.push(company.id))

//         console.log('Companies ID array ready', companiesIds)

//         if (companiesIds.length) {
//             await knex.raw(`UPDATE stockMarket SET reportStatus = ? WHERE id IN (?)`, ['PROCESSING', companiesIds])
//         }

//         console.log('Updating status')
//     } catch (err) {
//         console.log(err)
//     }

//     for (let company of companies) {
//         const t0 = performance.now()

//         try {
//             const insertReports = []
            
//             let reports = await fetchAnnualReport(companies[0].code)
//             reports = reports.Table ? reports.Table : []

//             for (let report of reports) {
//                 let existingStocks = await knex.select('*').from('stockAnnualReports').where({
//                     code: company.code,
//                     year: parseInt(report.year)
//                 })

//                 if (existingStocks.length === 0) {
//                     insertReports.push({
//                         id: uuid(),
//                         code: company.code,
//                         year: parseInt(report.year),
//                         url: `https://www.bseindia.com/bseplus/AnnualReport/${company.code}/${report.file_name}`
//                     })
//                 }
//             }

//             await knex.insert(insertReports)
//             .into('stockAnnualReports')

//             await knex('stockMarket').where({
//                 code: company.code
//             }).update({
//                 reportStatus: 'SUCCESS'
//             })

//             const t1 = performance.now()

//             console.log(`${company.code} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
//         } catch (err) {
//             await knex('stockMarket').where({
//                 code: company.code
//             }).update({
//                 reportStatus: 'FAILED'
//             })
//             const t1 = performance.now()

//             console.log(`${company.code} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
//         }
//     }

//     process.exit()
// })()

(async () => {
    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        const connections = new Array(15).fill(0)

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
            let companies

            try {
                companies = await knex.select('id', 'code').from('stockMarket').where('googleCode', 'like', 'BOM:%').where({
                    reportStatus: 'NOT STARTED'
                }).limit(100)

                console.log('companies Fetched')

                const companiesIds = []

                companies.map((company) => companiesIds.push(company.id))

                console.log('Companies ID array ready', companiesIds)

                if (companiesIds.length) {
                    await knex.raw(`UPDATE stockMarket SET reportStatus = ? WHERE id IN (?)`, ['PROCESSING', companiesIds])
                }

                console.log('Updating status')
            } catch (err) {
                console.log(err)
            }
        
            for (let company of companies) {
                const t0 = performance.now()
        
                try {
                    const insertReports = []
                    
                    let reports = await fetchAnnualReport(company.code)
                    reports = reports.Table ? reports.Table : []
        
                    for (let report of reports) {
                        let existingStocks = await knex.select('*').from('stockAnnualReports').where({
                            code: company.code,
                            year: parseInt(report.year)
                        })
        
                        if (existingStocks.length === 0) {
                            insertReports.push({
                                id: uuid(),
                                code: company.code,
                                year: parseInt(report.year),
                                url: `https://www.bseindia.com/bseplus/AnnualReport/${company.code}/${report.file_name}`
                            })
                        }
                    }
        
                    await knex.insert(insertReports)
                    .into('stockAnnualReports')
        
                    await knex('stockMarket').where({
                        code: company.code
                    }).update({
                        reportStatus: 'SUCCESS'
                    })
        
                    const t1 = performance.now()
        
                    console.log(`${company.code} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
                } catch (err) {
                    await knex('stockMarket').where({
                        code: company.code
                    }).update({
                        reportStatus: 'FAILED'
                    })
                    const t1 = performance.now()
        
                    console.log(`${company.code} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
                }
            }

            process.exit()
        })()
    }
})()