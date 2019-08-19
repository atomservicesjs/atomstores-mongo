import { IEventStores } from "atomservicescore";
import { expect } from "chai";
import { MongoClient } from "mongodb";
import { createEventStores } from "./createEventStores";
import { IEventStoresConnector } from "./IEventStoresConnector";

let client: MongoClient;
let connector: IEventStoresConnector;
const dbName = "TestEventStores";

describe("createEventStores.ts tests", () => {
  before(async () => {
    const uri = "mongodb://localhost:27017";
    client = await MongoClient.connect(uri, { useNewUrlParser: true });

    connector = {
      connect: async (scope, type) => client.db(dbName).collection(scope),
    };
  });

  describe("#createEventStores()", () => {
    it("expect to create an instance of EventStores", () => {
      // arranges

      // acts
      const stores = createEventStores(connector);

      // asserts
      expect(stores).not.to.equal(undefined);
      expect(stores).not.to.equal(null);
    });
  });

  describe("#EventStores", () => {
    let stores: IEventStores;

    describe("#EventStores.queryByID()", () => {
      const scope = "TestScope";

      after(async () => {
        await client.db(dbName).collection(scope).deleteMany({});
      });

      it("expect to query an event by eventID", async () => {
        // arranges
        const createdAt = new Date();
        const eventID = "A1234567890";
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);
        const expected = {
          _createdAt: createdAt,
          _id: eventID,
          _version: 1,
          aggregateID,
          name,
          type,
        };
        await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID,
          _version: 1,
          aggregateID,
          name,
          type,
        });

        // acts
        const result = await stores.queryByID(scope, type, eventID);

        // asserts
        expect(result).to.deep.equal(expected);
      });
    });

    describe("#EventStores.queryCurrentVersion()", () => {
      const scope = "TestScope";

      afterEach(async () => {
        await client.db(dbName).collection(scope).deleteMany({});
      });

      it("expect to query a current version: 0 as default", async () => {
        // arranges
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);
        const expected = {
          aggregateID,
          type,
          version: 0,
        };

        // acts
        const result = await stores.queryCurrentVersion(scope, type, aggregateID);

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query a current version, #1", async () => {
        // arranges
        const createdAt = new Date();
        const eventID = "A1234567890";
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);
        await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID,
          _version: 1,
          aggregateID,
          name,
          type,
        });
        const expected = {
          aggregateID,
          type,
          version: 1,
        };

        // acts
        const result = await stores.queryCurrentVersion(scope, type, aggregateID);

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query a current version, #2", async () => {
        // arranges
        const createdAt = new Date();
        const eventID1 = "A1234567890";
        const eventID2 = "B1234567890";
        const eventID3 = "C1234567890";
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);
        await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID1,
          _version: 1,
          aggregateID,
          name,
          type,
        });
        await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID2,
          _version: 2,
          aggregateID,
          name,
          type,
        });
        await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID3,
          _version: 3,
          aggregateID,
          name,
          type,
        });
        const expected = {
          aggregateID,
          type,
          version: 3,
        };

        // acts
        const result = await stores.queryCurrentVersion(scope, type, aggregateID);

        // asserts
        expect(result).to.deep.equal(expected);
      });
    });

    describe("#EventStores.queryEventsByAggregateID()", () => {
      const scope = "TestScope";

      afterEach(async () => {
        await client.db(dbName).collection(scope).deleteMany({});
      });

      it("expect to query an array of events by aggregateID, #1", async () => {
        // arranges
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);
        const expected: any[] = [];

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID);

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query an array of events by aggregateID, #2", async () => {
        // arranges
        const createdAt = new Date();
        const eventID = "X1234567890";
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);
        await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID,
          _version: 1,
          aggregateID,
          name,
          type,
        });
        const expected = [{
          _createdAt: createdAt,
          _id: eventID,
          _version: 1,
          aggregateID,
          name,
          type,
        }];

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID);

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query an array of events by aggregateID with options, #1", async () => {
        // arranges
        const type = "TestType";
        const aggregateID = "1234567890A";
        stores = createEventStores(connector);
        const expected: any[] = [];

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID, { initialVersion: 0 });

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query an array of events by aggregateID with options, #2", async () => {
        // arranges
        const type = "TestType";
        const aggregateID = "1234567890A";
        stores = createEventStores(connector);
        const expected: any[] = [];

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID, { initialVersion: 0, limit: 10 });

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query an array of events by aggregateID with options, #3", async () => {
        // arranges
        const createdAt = new Date();
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890A";
        stores = createEventStores(connector);
        const ps: any[] = [];
        const expected: any[] = [];

        for (let i = 1; i <= 10; i++) {
          const eventID = `event-${i}`;
          const event = {
            _createdAt: createdAt,
            _id: eventID,
            _version: i,
            aggregateID,
            name,
            type,
          };
          expected.push(event);
          ps.push(stores.storeEvent(scope, event));
        }

        await Promise.all(ps);

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID, { initialVersion: 0, limit: 10 });

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query an array of events by aggregateID with options, #4", async () => {
        // arranges
        const createdAt = new Date();
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890A";
        stores = createEventStores(connector);
        const ps: any[] = [];
        const expected: any[] = [];

        for (let i = 1; i <= 10; i++) {
          const eventID = `event-${i}`;
          const event = {
            _createdAt: createdAt,
            _id: eventID,
            _version: i,
            aggregateID,
            name,
            type,
          };
          if (i >= 5) {
            expected.push(event);
          }
          ps.push(stores.storeEvent(scope, event));
        }

        await Promise.all(ps);

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID, { initialVersion: 5 });

        // asserts
        expect(result).to.deep.equal(expected);
      });

      it("expect to query an array of events by aggregateID with options, #5", async () => {
        // arranges
        const createdAt = new Date();
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890A";
        stores = createEventStores(connector);
        const ps: any[] = [];
        const expected: any[] = [];
        const initialVersion = 5;
        const limit = 2;

        for (let i = 1; i <= 10; i++) {
          const eventID = `event-${i}`;
          const event = {
            _createdAt: createdAt,
            _id: eventID,
            _version: i,
            aggregateID,
            name,
            type,
          };

          if (i >= initialVersion && expected.length < limit) {
            expected.push(event);
          }

          ps.push(stores.storeEvent(scope, event));
        }

        await Promise.all(ps);

        // acts
        const result = await stores.queryEventsByAggregateID(scope, type, aggregateID, { initialVersion, limit });

        // asserts
        expect(result.length).to.equal(limit);
        expect(result).to.deep.equal(expected);
      });
    });

    describe("#EventStores.storeEvent()", () => {
      const scope = "TestScope";

      after(async () => {
        await client.db(dbName).collection(scope).deleteMany({});
      });

      it("expect to store an event into stores", async () => {
        // arranges
        const createdAt = new Date();
        const eventID = "A1234567890";
        const name = "TestEventName";
        const type = "TestType";
        const aggregateID = "1234567890";
        stores = createEventStores(connector);

        // acts
        const result = await stores.storeEvent(scope, {
          _createdAt: createdAt,
          _id: eventID,
          _version: 1,
          aggregateID,
          name,
          type,
        });

        // asserts
        expect(result).to.equal(undefined);
      });
    });

    describe("#EventStores.storeEvents()", () => {
      const scope = "TestScope";

      after(async () => {
        await client.db(dbName).collection(scope).deleteMany({});
      });

      it("expect to store an array of events into stores", async () => {
        // arranges
        const createdAt = new Date();
        const name = "TestEventName";
        const type1 = "TestType01";
        const type2 = "TestType02";
        const aggregateIDA = "1234567890A";
        const aggregateIDB = "1234567890B";
        stores = createEventStores(connector);
        const events01: any[] = [];
        const events02: any[] = [];

        for (let i = 1; i <= 10; i++) {
          const eventID = `eventA-${i}`;
          events01.push({
            _createdAt: createdAt,
            _id: eventID,
            _version: i,
            aggregateID: aggregateIDA,
            name,
            type: type1,
          });
        }

        for (let i = 1; i <= 5; i++) {
          const eventID = `eventB-${i}`;
          events02.push({
            _createdAt: createdAt,
            _id: eventID,
            _version: i,
            aggregateID: aggregateIDB,
            name,
            type: type2,
          });
        }

        // acts
        await stores.storeEvents(scope, [...events01, ...events02]);

        // asserts
        const result01 = await stores.queryEventsByAggregateID(scope, type1, aggregateIDA);
        const result02 = await stores.queryEventsByAggregateID(scope, type2, aggregateIDB);

        expect(result01).to.deep.equal(events01);
        expect(result02).to.deep.equal(events02);
      });
    });
  });
});
