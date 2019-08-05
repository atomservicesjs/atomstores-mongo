import { IEventStores } from "atomservicescore";
import { IEventStoresConnector } from "./IEventStoresConnector";

export const createEventStores = (connector: IEventStoresConnector): IEventStores => ((Connector): IEventStores => {
  const stores: IEventStores = {
    queryByID: async (scope, type, eventID) => {
      const collection = await Connector.connect(scope, type);

      return collection.findOne({ _id: eventID });
    },
    queryCurrentVersion: async (scope, type, aggregateID) => {
      const collection = await Connector.connect(scope, type);
      const result = await collection.aggregate([
        { $match: { aggregateID } },
        { $sort: { _version: -1 } },
        { $limit: 1 },
      ]).toArray();

      if (result.length > 0) {
        return {
          aggregateID,
          type,
          version: result[0]._version,
        };
      } else {
        return {
          aggregateID,
          type,
          version: 0,
        };
      }
    },
    queryEventsByAggregateID: async (scope, type, aggregateID, options) => {
      const collection = await Connector.connect(scope, type);

      if (options) {
        const { initialVersion, limit } = options;
        const aggregate: any[] = [];

        const $match: {
          _version?: { $gte: number };
          aggregateID: string;
        } = { aggregateID };

        if (initialVersion) {
          $match._version = { $gte: initialVersion };
        }

        const $sort = { _version: 1 };

        aggregate.push({ $match });
        aggregate.push({ $sort });

        if (limit) {
          const $limit = limit;
          aggregate.push({ $limit });
        }

        return collection.aggregate(aggregate).toArray();
      } else {
        return collection.find({ aggregateID }).toArray();
      }
    },
    storeEvent: async (scope, event) => {
      const collection = await Connector.connect(scope, event.type);

      await collection.insertOne(event);
    },
    storeEvents: async (scope, events) => {
      const reducedEvents = events.reduce((result, each) => {
        if (!result[each.type]) {
          result[each.type] = [];
        }

        result[each.type].push(each);

        return result;
      }, {} as { [type: string]: any[]; });

      const types = Object.keys(reducedEvents);
      const ps = types.map(async (type) => {
        const collection = await Connector.connect(scope, type);

        return collection.insertMany(reducedEvents[type]);
      });

      await Promise.all(ps);
    },
  };

  Object.freeze(stores);

  return stores;
})(connector);
