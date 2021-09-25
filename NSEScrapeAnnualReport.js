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
// const axios = require("axios");
const { uuid } = require('uuidv4');
const cluster = require('cluster');
const {attachOnDuplicateUpdate} = require('knex-on-duplicate-update');
const { performance } = require('perf_hooks');
const { curly } = require('node-libcurl');
attachOnDuplicateUpdate();

async function fetchAnnualReport(code) {
    try {
        const { data } = await curly.get(`https://www.nseindia.com/api/annual-reports?index=equities&symbol=${code}`, {
            httpHeader: [
                'authority: www.nseindia.com',
                'sec-ch-ua: "Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
                'sec-ch-ua-mobile: ?0',
                'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
                'sec-ch-ua-platform: "macOS"',
                'accept: */*',
                'sec-fetch-site: same-origin',
                'sec-fetch-mode: cors',
                'sec-fetch-dest: empty',
                'referer: https://www.nseindia.com/companies-listing/corporate-filings-annual-reports',
                'accept-language: en-GB,en-US;q=0.9,en;q=0.8',
                'cookie: _ga=GA1.2.238773930.1630408397; _gid=GA1.2.521086446.1632216278; nseQuoteSymbols=[{"symbol":"RELIANCEP1","identifier":null,"type":"equity"},{"symbol":"SHAIVAL","identifier":null,"type":"equity"},{"symbol":"APOLLOHOSP","identifier":null,"type":"equity"},{"symbol":"APOLLOTYRE","identifier":null,"type":"equity"}]; nsit=WQ8Ih5bIycfcFvSxpSj631Vc; ak_bmsc=86D65006E3DF5E1260857D990345E7FA~000000000000000000000000000000~YAAQnYwsMcR9ggN8AQAAYvLTCw2AAZtlgje+1ScG3UgCcD4jdgoc5qv8vlQevgWhyi+hGOe5ZTQEvoJLaRuf4WPsGNu6eR4YBHci/aH+9BHCW1K+gXj9kdu5OJR1hWMG9Qh9Yw3EgT1UcI02M/3aiU27N8Mg8yds40gDC+SYWyvLfzk/VnUnJgeBojXINBdHQ5gzdsEZEvuranbB1kWj3knS7Hsr2B1whIPaQcCqWoy0q7M+1/0FdxY6HT+onNC2/OonE2NdpiKsh8hsVDu6jRRgEmaavSIhY2xMEOSQ5t6uuu+rhROc8cFxuvh3EMfBZPqit+vvcvV94blZcLUvylmVt0bu7Mk6ZHez5rB1CJ1CqF2aKOcZijimbPbW9xPyVbgNRX8JjCkg5AKwCDxA6bBCmDzlVtPYnqQYk6Qi7+99vpestoOZnje8N3xj7mfaftljiK4fUq2WkAIxnW4cO/Arad26T9nJJnhM3EOwmoTjpFFMNIRcEbKwNVQ25w==; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTYzMjI4NjE2MywiZXhwIjoxNjMyMjg5NzYzfQ.q1h5bhSt_F7ig_Ek18_nMmShzky_fx2SMjvC3juTg9w; RT="z=1&dm=nseindia.com&si=7b35f8fa-34a1-4dd8-8933-34d9ffc1eaf0&ss=ktv0y2oq&sl=0&tt=0&bcn=%2F%2F684d0d40.akstat.io%2F"; bm_sv=C07AA57743C759497E146C1528AD312C~b0nB6YtpcV2/xY9FrPyXzZGuHB5+V5+tmVoLpBqhAsMa3Ck1uRUEAZlfsT1zPtyw9KicF8kDBuSAH8eTDY9MATNmWXCg2vt3P9q4wB/fOZGFi88xZBXiOWmQDRKBLw40GDOUngZt1PVb86p9Z3MCa86zLTpSQO1vYZWnKn+1JPY=; bm_sv=C07AA57743C759497E146C1528AD312C~b0nB6YtpcV2/xY9FrPyXzZGuHB5+V5+tmVoLpBqhAsMa3Ck1uRUEAZlfsT1zPtyw9KicF8kDBuSAH8eTDY9MATNmWXCg2vt3P9q4wB/fOZHa85Ixr0wB9ytGKLf1mx8RanMFysGQr4LmVlpEOaUzv6QnXNNYezYUj/1XbgKrlBY=',
            ]
        })
        return data
    } catch (err) {
        console.log(err)
    }
}

(async () => {
    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        const connections = new Array(10).fill(0)

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
                companies = await knex.select('id', 'code').from('stockMarket').where('googleCode', 'like', 'NSE:%').where({
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
                    reports = reports.data ? reports.data : []

                    for (let report of reports) {
                        let existingStocks = await knex.select('*').from('stockAnnualReports').where({
                            code: company.code,
                            year: parseInt(report.toYr)
                        })

                        if (existingStocks.length === 0) {
                            insertReports.push({
                                id: uuid(),
                                code: company.code,
                                year: parseInt(report.toYr),
                                url: report.fileName
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
        })()
    }
})()
