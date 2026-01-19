


export function isValidUUID(id: string): boolean {
  if (!id || typeof id !== 'string') return false;
  
  
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return uuidRegex.test(id);
}


export function validateUserId(userId: string | undefined | null): string {
  if (!userId) {
    throw new Error('User ID is required');
  }
  if (!isValidUUID(userId)) {
    throw new Error('Invalid user ID format');
  }
  return userId;
}


export function validateUUID(id: string | undefined | null, paramName: string = 'ID'): string {
  if (!id) {
    throw new Error(`${paramName} is required`);
  }
  if (!isValidUUID(id)) {
    throw new Error(`Invalid ${paramName} format`);
  }
  return id;
}

export function sanitizeFilterValue(value: string | number): string {
  
  const str = String(value);
  
  return str.replace(/[;,()]/g, '');
}

