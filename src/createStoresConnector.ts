import { MongoClient, MongoClientOptions } from "mongodb";
import { IEventStoresConnector } from "./IEventStoresConnector";

interface IStoresConnectorConfigs {
  uri?: string;
  options?: any;
  resolve?: (values: { scope: string; type: string; }) => {
    database: string;
    collection: string;
  };
}

const DEFINED = {
  options: {
    useUnifiedTopology: true,
  } as MongoClientOptions,
  resolve: (values: { scope: string; type: string; }) => ({
    collection: values.type,
    database: `${values.scope.toLowerCase()}-events`,
  }),
  uri: "mongodb://localhost:27017",
};

export const createStoresConnector = (configs: IStoresConnectorConfigs = {}): IEventStoresConnector => ((Configs): IEventStoresConnector => {
  const {
    uri = DEFINED.uri,
    resolve = DEFINED.resolve,
  } = Configs;

  const options = Object.assign({}, DEFINED.options, Configs.options || {});

  const Client = new MongoClient(uri, options);

  return {
    connect: async (scope, type) => {
      await Client.connect();
      const { collection, database } = resolve({ scope, type });

      return Client.db(database).collection(collection);
    },
  };
})(configs);
