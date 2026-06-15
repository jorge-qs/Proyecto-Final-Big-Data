// Ejecutar con: mongosh "mongodb://root:root@localhost:27017/yelp" scripts/mongo_indexes.js

const db = db.getSiblingDB("yelp");

// businesses
db.businesses.createIndex({ city: 1 });
db.businesses.createIndex({ state: 1 });
db.businesses.createIndex({ categories: 1 });   // multikey
db.businesses.createIndex({ stars: -1 });

// reviews
db.reviews.createIndex({ business_id: 1 });
db.reviews.createIndex({ user_id: 1 });
db.reviews.createIndex({ date: 1 });
db.reviews.createIndex({ stars: -1 });
db.reviews.createIndex({ business_id: 1, date: 1 });

// users
db.users.createIndex({ review_count: -1 });

// tips
db.tips.createIndex({ business_id: 1 });
db.tips.createIndex({ user_id: 1 });

print("Índices creados correctamente.");
