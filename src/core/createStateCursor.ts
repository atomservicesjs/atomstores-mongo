import { IStateAccessCursor } from "atomservicescore";
import { Cursor } from "mongodb";

export const createStateCursor = {
  fromFind: (cursor: Cursor): IStateAccessCursor => ({
    hasNext: () => cursor.hasNext(),
    next: () => cursor.next(),
    toArray: () => cursor.toArray(),
  }),
};

Object.freeze(createStateCursor);
