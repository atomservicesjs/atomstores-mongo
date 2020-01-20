import { IStateAccess } from "atomservicescore";
import { Collection } from "mongodb";

import { createStateCursor } from "./core/createStateCursor";

export const connectStateAccess = (collection: Collection): IStateAccess => ((COLLECTION): IStateAccess => {
  const StateAccess: IStateAccess = {
    count: () => COLLECTION.count(),
    fetchAggregates: async () => {
      const cursor = COLLECTION.find();
      return createStateCursor.fromFind(cursor);
    },
    queryByAggregateID: (aggregateID) =>
      COLLECTION.findOne({ _id: aggregateID }),
    queryCurrentVersion: async (aggregateID) => {
      const item = await COLLECTION.findOne({ _id: aggregateID });
      const version = item ? item._version : 0;

      return {
        aggregateID,
        version,
      };
    },
  };

  Object.freeze(StateAccess);

  return StateAccess;
})(collection);
