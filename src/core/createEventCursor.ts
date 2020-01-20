import { IEventStoresCursor } from "atomservicescore";
import { AggregationCursor, Cursor } from "mongodb";

export const createEventCursor = {
  fromAggregation: (cursor: AggregationCursor): IEventStoresCursor => ({
    hasNext: () => cursor.hasNext(),
    next: () => cursor.next(),
    toArray: () => cursor.toArray(),
  }),
  fromFind: (cursor: Cursor): IEventStoresCursor => ({
    hasNext: () => cursor.hasNext(),
    next: () => cursor.next(),
    toArray: () => cursor.toArray(),
  }),
};

Object.freeze(createEventCursor);
