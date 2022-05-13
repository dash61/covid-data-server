

// Helper fn to return true if an object is empty.
export function detectEmptyObject(obj: Object): boolean {
  if (obj // 👈 null and undefined check
    && Object.keys(obj).length === 0
    && Object.getPrototypeOf(obj) === Object.prototype) {
      return true;
  }
  return false;
}

export function ISODate(dateStart: string) {
  return new Date(dateStart);
}
