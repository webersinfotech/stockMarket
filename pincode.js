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
const pincodes = require('./pincode.json')
const { uuid } = require('uuidv4');
const {attachOnDuplicateUpdate} = require('knex-on-duplicate-update');
attachOnDuplicateUpdate();

(async () => {
    const insertData = []
    const pincodeMap = new Map()

    const existingPincodes = await knex.select('id').from('pincode')
    existingPincodes.map((pincode) => pincodeMap.set(pincode.pincode, pincode))

    for (let [index, pincode] of pincodes.entries()) {
        console.log(index, '::: Processing')
        let existingPincode
        let id
        if (pincodeMap.has(pincode.Pincode)) {
            existingPincode = pincodeMap.get(pincode.Pincode)
            id = existingPincode.id
        }
        insertData.push({
            id: id || uuid(),
            circleName: pincode.CircleName,
            regionName: pincode.RegionName,
            divisionName: pincode.DivisionName,
            officeName: pincode.OfficeName,
            pincode: pincode.Pincode,
            officeType: pincode.OfficeType,
            delivery: pincode.Delivery,
            district: pincode.District,
            stateName: pincode.StateName,
            latitude: isNaN(parseFloat(pincode.Latitude)) ? 0 : parseFloat(pincode.Latitude),
            longitude: isNaN(parseFloat(pincode.Longitude)) ? 0 : parseFloat(pincode.Longitude)
        })
    }
    const result = await knex.insert(insertData)
    .into('pincode')
    .onDuplicateUpdate('circleName', 'regionName', 'divisionName', 'officeName', 'pincode', 'officeType', 'delivery', 'district', 'stateName', 'latitude', 'longitude')
    console.log(result)
})()