//go:build mongomatrix

package mongoadapter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestRealMongoCompatibility(t *testing.T) {
	address := os.Getenv("MONGO_MATRIX_ADDR")
	if address == "" {
		t.Skip("MONGO_MATRIX_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	ready := make(chan string, 1)
	events := make(chan observation.Interaction, 200)
	upstream := config.Upstream{
		ID: "mongo-real", Name: "MongoDB " + os.Getenv("MONGO_MATRIX_VERSION"), Protocol: "mongodb", ListenAddr: "127.0.0.1:0", Target: address, Enabled: true,
		MongoDB: &config.MongoDBOptions{
			ListenerUsername: "portscope_listener", ListenerPassword: "listener-secret", ListenerAuthSource: "admin",
			UpstreamUsername: "portscope_upstream", UpstreamPassword: "upstream-secret", UpstreamAuthSource: "admin",
		},
	}
	probe, err := openUpstream(ctx, upstream)
	if err != nil {
		t.Fatalf("open authenticated upstream: %v", err)
	}
	_ = probe.connection.Close()
	go func() {
		_ = New().Run(ctx, upstream, testSink{events: events}, func(address string) { ready <- address })
	}()
	proxyAddress := <-ready
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + proxyAddress + "/?directConnection=true").SetAuth(options.Credential{Username: "portscope_listener", Password: "listener-secret", AuthSource: "admin"}).SetServerSelectionTimeout(10 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal(err)
	}
	collection := client.Database("portscope_matrix").Collection("documents")
	_ = collection.Drop(ctx)
	if _, err := collection.InsertMany(ctx, []any{bson.D{{Key: "kind", Value: "book"}, {Key: "rank", Value: 1}}, bson.D{{Key: "kind", Value: "book"}, {Key: "rank", Value: 2}}}); err != nil {
		t.Fatal(err)
	}
	cursor, err := collection.Find(ctx, bson.D{{Key: "kind", Value: "book"}}, options.Find().SetSort(bson.D{{Key: "rank", Value: 1}}))
	if err != nil {
		t.Fatal(err)
	}
	var documents []bson.M
	if err := cursor.All(ctx, &documents); err != nil || len(documents) != 2 {
		t.Fatalf("find returned %d documents: %v", len(documents), err)
	}
	if result, err := collection.UpdateOne(ctx, bson.D{{Key: "rank", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "checked", Value: true}}}}); err != nil || result.ModifiedCount != 1 {
		t.Fatalf("update result = %#v, %v", result, err)
	}
	if result, err := collection.DeleteOne(ctx, bson.D{{Key: "rank", Value: 2}}); err != nil || result.DeletedCount != 1 {
		t.Fatalf("delete result = %#v, %v", result, err)
	}
	observed := waitForOperations(t, events, "INSERT portscope_matrix.documents", "FIND portscope_matrix.documents", "UPDATE portscope_matrix.documents", "DELETE portscope_matrix.documents")
	if observed["INSERT portscope_matrix.documents"].Request.Kind != "json" || observed["FIND portscope_matrix.documents"].Attributes["documents"] != "2" {
		t.Fatalf("real MongoDB observations = %#v", observed)
	}
}
