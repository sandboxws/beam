/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.bson.BsonDocument;
import org.bson.Document;

/** Builds a MongoDB AggregateIterable object. */
public class AggregationQueryBuilder
    implements SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> {

  private List<BsonDocument> mongoDbPipeline;

  public AggregationQueryBuilder withMongoDbPipeline(List<BsonDocument> mongoDbPipeline) {
    this.mongoDbPipeline = mongoDbPipeline;
    return this;
  }

  @Override
  public MongoCursor<Document> apply(MongoCollection<Document> collection) {
    return collection.aggregate(mongoDbPipeline).iterator();
  }

  public static AggregationQueryBuilder create() {
    return new AggregationQueryBuilder();
  }
}
