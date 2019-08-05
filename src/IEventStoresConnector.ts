import * as MongoDB from "mongodb";

export interface IEventStoresConnector {
  connect: (scope: string, type: string) => Promise<MongoDB.Collection>;
}
