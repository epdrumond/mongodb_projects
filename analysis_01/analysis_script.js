// Identify 10 most frequent cuisines -------------------------------
db.restaurants.aggregate( 
    { $group: { _id: "$cuisine", total_restaurants: { $count: {} } } }, 
    { $sort: { total_restaurants: -1 } }, 
    { $limit: 10 }
)

// Get last grade distribution --------------------------------------
db.restaurants.aggregate(
    { $project: { last_evaluation: { $slice: ["$grades", 1] } } }, 
    { $project: { last_grade: { $arrayElemAt: ["$last_evaluation.grade", 0] } } },
    { $group: { _id: "$last_grade" , total_restaurants: { $count: {} } } },
    { $sort: {_id: 1} }
)

// Share of graded restaurants by cuisine ---------------------------
db.restaurants.aggregate(
    { $project: { cuisine: 1, last_evaluation: { $slice: ["$grades", 1] } } }, 
    { $project: { cuisine: 1, last_grade: { $arrayElemAt: ["$last_evaluation.grade", 0] } } },
    { $project: {
        cuisine: 1,
        is_graded: {
            $switch: {
                branches: [
                        { case: { $in: ["$last_grade", ["A", "B", "C"]] }, then: 1 },
                        { case: { $eq: ["$last_grade", "Not Yet Graded"] }, then: 0 },
                        { case: { $eq: ["$last_grade", null] }, then: 0 }
                    ],
                default: 0
            }
        }
    } 
    },
    { $group: {
        _id: "$cuisine", 
        total_restaurants: { $count: {} } ,
        graded_restaurants: { $sum: "$is_graded" }        
    }
    },
    { $project: {
        _id: 1,
        total_restaurants: 1,
        pct_graded_restaurants: { $round: { $divide: [ { $multiply: [100, "$graded_restaurants"] }, "$total_restaurants" ] } }        
    } 
    },    
    { $sort: { total_restaurants: -1 } },
    { $limit: 10 }
)

// Compute cuisine average of a transformed numerical score ---------
db.restaurants.aggregate(
    { $project: { cuisine: 1, last_evaluation: { $slice: ["$grades", 1] } } }, 
    { $project: { cuisine: 1, last_grade: { $arrayElemAt: ["$last_evaluation.grade", 0] } } },
    { $match: { last_grade: { $in: [ "A", "B", "C" ] } } },
    { $project: {
            cuisine: 1,
            last_grade_num: {
                $switch: {
                    branches: [
                        { case: { $eq: ["$last_grade", "A"] }, then: 3 },
                        { case: { $eq: ["$last_grade", "B"] }, then: 2 },
                        { case: { $eq: ["$last_grade", "C"] }, then: 1 }
                    ]
                }
            }
        } 
    },
    { $group: {  _id: "$cuisine", average_score:  { $avg: "$last_grade_num" }, total_restaurants: { $count: {} } } },
    { $sort: { total_restaurants: -1 } },
    { $limit: 10 },
    { $project: { _id: 1, total_restaurants: 1, avg_score: { $round: ["$average_score", 3] } } }
)

// Neighborhoods with the best restaurants --------------------------
db.restaurants.aggregate(
    { $match: { borough: { $ne: "Missing" } } },
    { $project: { borough: 1, last_evaluation: { $slice: ["$grades", 1] } } }, 
    { $project: { borough: 1, last_grade: { $arrayElemAt: ["$last_evaluation.grade", 0] } } },
    { $match: { last_grade: { $in: [ "A", "B", "C" ] } } },
    { $project: {
        borough: 1,
            last_grade_num: {
                $switch: {
                    branches: [
                        { case: { $eq: ["$last_grade", "A"] }, then: 3 },
                        { case: { $eq: ["$last_grade", "B"] }, then: 2 },
                        { case: { $eq: ["$last_grade", "C"] }, then: 1 }
                    ]
                }
            }
        } 
    },
    { $group: {  _id: "$borough", average_score:  { $avg: "$last_grade_num" } } },
    { $sort: { average_score: -1 } },
    { $limit: 3 },
    { $project: { _id: 1, avg_score: { $round: ["$average_score", 3] } } }
)