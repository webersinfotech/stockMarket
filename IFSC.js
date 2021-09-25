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
const axios = require("axios")
const cheerio = require("cheerio")
const { uuid } = require('uuidv4')
const {attachOnDuplicateUpdate} = require('knex-on-duplicate-update')
const get = require("async-get-file")
const cluster = require('cluster');
const { performance } = require('perf_hooks');
attachOnDuplicateUpdate();

async function fetchHTML() {
    var config = {
        method: 'get',
        url: 'https://www.rbi.org.in/Scripts/bs_viewcontent.aspx?Id=2009',
        headers: { 
          'Connection': 'keep-alive', 
          'Cache-Control': 'max-age=0', 
          'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"', 
          'sec-ch-ua-mobile': '?0', 
          'sec-ch-ua-platform': '"macOS"', 
          'Upgrade-Insecure-Requests': '1', 
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36', 
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 
          'Sec-Fetch-Site': 'cross-site', 
          'Sec-Fetch-Mode': 'navigate', 
          'Sec-Fetch-User': '?1', 
          'Sec-Fetch-Dest': 'document', 
          'Referer': 'https://www.google.com/', 
          'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8', 
          'Cookie': 'IncPath=Includes1; _ga=GA1.3.251474673.1629458473; ASP.NET_SessionId=p24rurft144ougraujaatbbv; TS0178b498=01af024724332c18c1c213b50e2cd574315a7d8271a375668e1b19fac55c46736bdfcebd15ffaa365b61ea3fb3ed77fc812b351bf88e6035e9865d6973779683473bd03adc; _gid=GA1.3.1554145640.1632305084; TS71c5c044027=0874148b5fab200064da447a7d933e6af090aa8963f2e03343c646fc931ed6515555d8e8dc6d383908d8d10bdf113000c80dc04f591743999af3f0205844448c02a1e3863129565514a8b2e350565adcc92008da2ee83decd7cf5e8bd394065b; TS0178b498=01af024724b96b06d309c4ca2db48f3475fcfa968a5f85d89ec09867f5b9caea6853d452621ec60de213c5fb051915e9d40bc9f05c3c39d1ac63e2202df0123cd21647e040; TS71c5c044027=0874148b5fab2000d711b24a0f3bd8c33d8b322cea43d28bf7b580628143ad09f864f23068f15c1e0832f9b8f4113000c58bc74f026d0a43c3f8fc9c3e82bcacb13e2e90b3e58770dc185068884835fd21ace36676c67dea43c667586b693181'
        }
    };
    const { data } = await axios(config)
    return cheerio.load(data)
}

(async () => {
    // const $ = await fetchHTML()

    // let banks = []
    // $('.tablebg').eq(1).find('tr').each(function () {
    //     const bank = $(this).find('td').eq(1)

    //     banks.push({
    //         name: bank.text(),
    //         url: bank.find('a').attr('href')
    //     })
    // })

    // let existingBanks = await knex.select('*').from('banks')
    // const bankMap = new Map()
    // existingBanks.map((bank) => bankMap.set(bank.name, bank.id))
    // const insertData = []

    // for (let bank of banks) {
    //     const bankData = bankMap.get(bank.name)
    //     bankMap.has(bank.name) ? insertData.push({
    //         id: bankData,
    //         ...bank
    //     }) : insertData.push({
    //         id: uuid(),
    //         ...bank
    //     })
    // }
    // const result = await knex.insert(insertData)
    // .into('banks')
    // .onDuplicateUpdate('url');
    // console.log(result)

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
            let banks

            try {
                banks = await knex.select('id', 'name', 'url').from('banks').where({
                    status: 'NOT STARTED'
                }).limit(1)

                console.log('banks Fetched')

                const bankIds = []

                banks.map((bank) => bankIds.push(bank.id))

                console.log('Banks ID array ready', bankIds)

                if (bankIds.length) {
                    await knex.raw(`UPDATE banks SET status = ? WHERE id IN (?)`, ['PROCESSING', bankIds])
                }

                console.log('Updating status')
            } catch (err) {
                console.log(err)
            }

            for (let bank of banks) {
                const t0 = performance.now()

                try {
                    const options = {
                      directory: "./uploads",
                      filename: `${bank.id}.xlsx`
                    }

                    await get(bank.url, options);

                    await knex('banks').where({
                        id: bank.id
                    }).update({
                        status: 'SUCCESS'
                    })
                } catch (err) {
                    await knex('banks').where({
                        id: bank.id
                    }).update({
                        status: 'FAILED'
                    })

                    const t1 = performance.now()
        
                    console.log(`${bank.name} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
                }
            }

            process.exit()
        })()
    }
})()