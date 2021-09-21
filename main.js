const { MongoClient } = require('mongodb');



const printCheapestSuburbs = async (client, country, market, maxNumber) => {

    const pipeline = [
        {
            '$match': {
                'bedrooms': 1,
                'address.country': country,
                'address.market': market,
                'address.suburb': {
                    '$exists': 1,
                    '$ne': ''
                }
            }
        },
        
        {
            '$group': {
                '_id': '$address.suburb',
                'averagePrice': {
                    '$avg': '$price'
                }
            }
        },

        {
            '$sort': {
                'averagePrice': 1
            }
        }
    ];

    const pipeline = [
        {
            $match: {
                cast: { $elemMatch: { $exists: true } },
                directors: { $elemMatch: { $exists: true } },
                writers: { $elemMatch: { $exists: true } }
            }
        },

        {
            '$project': {
                writers: {
                    $map: {
                        input: "$writers",
                        as: "writer",
                        in: {
                            $arrayElemAt: [
                                {
                                    $split: ["$$writer", " ("]
                                },
                                0
                            ]
                        }
                    }
                },
                cast: {
                    $map: {
                        input: "$cast",
                        as: "cast",
                        in: {
                            $arrayElemAt: [
                                {
                                    $split: ["$$cast", " ("]
                                },
                                0
                            ]
                        }
                    }
                },
                directors: {
                    $map: {
                        input: "$directors",
                        as: "directors",
                        in: {
                            $arrayElemAt: [
                                {
                                    $split: ["$$directors", " ("]
                                },
                                0
                            ]
                        }
                    }
                }
                

            },
            
            
        },
        {
            '$project': {
                'commonPeopleSize': {
                    '$size': { '$setIntersection': ["$writers", "$cast", "$directors"] }
                }
            }
        }
        ,
        {
            '$match': {
                'commonPeopleSize': {
                    '$ne': 0
                },
                
            }
        }
        
    ]

        ;
    const aggCursor = client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .aggregate(pipeline);
    await aggCursor.forEach(airbnbListing => {
        console.log(`${airbnbListing._id}: ${airbnbListing.averagePrice}`);
    });

}

async function main() {

    const uri = "mongodb+srv://shreyash:shreyash@cluster0.s3sjv.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";


    const client = new MongoClient(uri);

    try {

        await client.connect();
        await printCheapestSuburbs(client, "Australia", "Sydney", 10);
    }
    catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }


}


async function createDatabase(client, newListing) {

    const result = await client.db("sample_airbnb").collection("listingsAndReviews").insertOne(newListing);
    console.log(`listing created ${result.insertedId}`);

}
async function createMultipleEntries(client, newListings) {

    const result = await client.db("sample_airbnb").collection("listingsAndReviews").insertMany(newListings);
    console.log(`listings created ${result.insertedIds['0']}`);
    console.log(result.insertedIds);
}

async function updateListingByName(client, nameOfListing, updatedListing) {
    const result = await client.db("sample_airbnb").collection("listingsAndReviews").updateOne({
        name: nameOfListing

    }, {
        $set: updatedListing
    }, { upsert: true });

    console.log(`${result.matchedCount} doc matched`);
    console.log(`${result.modifiedCount} docs updated`);

}


async function updateAllListingsToHavePropertyType(client) {

    const result = await client.db("sample_airbnb").collection("listingsAndReviews").updateMany({
        property_type: { $exists: false }
    }, {
        $set: { property_type: "Unknown" }
    });

    console.log(`${result.matchedCount} doc matched`);
    console.log(`${result.modifiedCount} docs updated`);

}
async function deleteListing(client, nameOfListing) {
    const result = await client.db("sample_airbnb").collection("listingsAndReviews").deleteOne({ name: nameOfListing });


}

async function deletedListingsBeforeADate(client, date) {
    const result = await client.db("sample_airbnb").collection('listingsAndReviews').deleteMany({ "last_scrapped": { $lt: date } });

    console.log(`${result.deletedCount} document(s) were deleted`);

}
async function findListingsMinBedroomsMostRecentReviews(client, {
    minimunBedrooms = 0,
    minimumBathrooms = 0,
    maximumResults = Number.MAX_SAFE_INTEGER
} = {}) {

    const cursor = await client.db("sample_airbnb").collection("listingsAndReviews").find(
        {
            bedrooms: { $gte: minimunBedrooms },
            bathrooms: { $gte: minimumBathrooms }
        }).sort({ last_review: -1 })
        .limit(maximumResults);


    const result = await cursor.toArray();

    if (result.length > 0) {
        result.forEach((result, i) => {
            date = new Date(result.last_review).toDateString();


            console.log();

            console.log(`${i + 1}, name: ${result.name}`);


        })
    }

}

async function findOneListingByName(client, nameOfListing) {
    const result = await client.db("sample_airbnb").collection("listingsAndReviews").findOne(
        { name: nameOfListing }
    )
    if (result) {
        console.log(result);

    }
    else {
        console.log("No Listing");
    }
}


async function listDatabses(client) {
    const databasesList = await client.db().admin().listDatabases();

    console.log("Databases");

    databasesList.databases.forEach(db => { console.log(`Databases ${db.name}`); }
    )
}


main().catch(console.error);