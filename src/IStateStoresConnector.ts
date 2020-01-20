import { Collection } from "mongodb";

export interface IStateStoresConnector {
  connect: (scope: string, type: string) => Promise<Collection>;
}
