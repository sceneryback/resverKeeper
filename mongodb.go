package resverKeeper

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

type Mongodb struct {
	coll *mongo.Collection
}

func NewMongodb(url, database, collName string) (*Mongodb, error) {
	var db Mongodb

	clientOptions := options.Client().ApplyURI(url)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Printf("failed to connect mongodb: %s", err)
		return nil, err
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Printf("failed to ping mongodb: %s", err)
		return nil, err
	}

	db.coll = client.Database(database).Collection(collName)

	return &db, nil
}

func (m *Mongodb) CreateVersionStore(storeName string) error {
	return nil
}

func (m *Mongodb) InitializeVersion(identifier string) (int, error) {
	_, err := m.coll.InsertOne(context.TODO(), bson.M{"identifier": identifier, "version": 1})
	if err != nil {
		log.Printf("failed to insert version: %s", err)
		return 0, err
	}
	return 1, nil
}

func (m *Mongodb) GetVersion(identifier string) (int, error) {
	var result struct {
		Version int
	}
	err := m.coll.FindOne(context.TODO(), bson.M{"identifier": identifier}).Decode(&result)
	if err != nil {
		log.Printf("failed to find identifier version: %s", err)
		return 0, err
	}
	return result.Version, nil
}

func (m *Mongodb) IncreaseVersion(identifier string) (int, error) {
	_, err := m.coll.UpdateOne(context.TODO(), bson.M{"identifier": identifier}, bson.M{"$inc": bson.M{"version": 1}})
	if err != nil {
		log.Printf("failed to increase version: %s", err)
		return 0, err
	}
	return m.GetVersion(identifier)
}
