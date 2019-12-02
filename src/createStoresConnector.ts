import { MongoClient } from "mongodb";
import { IEventStoresConnector } from "./IEventStoresConnector";

interface IStoresConnectorOptions {
  uri?: string;
}

export const createStoresConnector = (options: IStoresConnectorOptions = {}): IEventStoresConnector => ((Options): IEventStoresConnector => {
  const { uri = "mongodb://localhost:27017" } = Options;
  const Client = new MongoClient(
    uri,
    {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    },
  );

  return {
    connect: async (scope, type) => {
      await Client.connect();

      return Client.db(`${scope.toLowerCase()}-events`).collection(type);
    },
  };
})(options);
