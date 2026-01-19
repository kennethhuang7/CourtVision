import { supabase } from './supabase';
import { validateUserId } from './security';

const PROFILE_PICTURES_BUCKET = 'profile-pictures';
const BANNERS_BUCKET = 'banners';
const GROUP_PICTURES_BUCKET = 'group-pictures';


export async function uploadProfilePicture(file: File, userId: string): Promise<string> {
  
  const validatedUserId = validateUserId(userId);

  
  const fileParts = file.name.toLowerCase().split('.');
  if (fileParts.length < 2) {
    throw new Error('File must have an extension');
  }
  const fileExt = fileParts[fileParts.length - 1];
  
  
  if (!ALLOWED_IMAGE_EXTENSIONS.includes(fileExt)) {
    throw new Error(`File extension .${fileExt} is not allowed. Allowed: ${ALLOWED_IMAGE_EXTENSIONS.join(', ')}`);
  }
  
  const fileName = `${validatedUserId}-${Date.now()}.${fileExt}`;
  
  const filePath = `${validatedUserId}/${fileName}`;

  
  const { error: uploadError } = await supabase.storage
    .from(PROFILE_PICTURES_BUCKET)
    .upload(filePath, file, {
      cacheControl: '3600',
      upsert: false,
    });

  if (uploadError) {
    throw uploadError;
  }

  
  const { data } = supabase.storage
    .from(PROFILE_PICTURES_BUCKET)
    .getPublicUrl(filePath);

  return data.publicUrl;
}


export async function uploadBanner(file: File, userId: string): Promise<string> {
  
  const validatedUserId = validateUserId(userId);

  
  const fileParts = file.name.toLowerCase().split('.');
  if (fileParts.length < 2) {
    throw new Error('File must have an extension');
  }
  const fileExt = fileParts[fileParts.length - 1];
  
  
  if (!ALLOWED_IMAGE_EXTENSIONS.includes(fileExt)) {
    throw new Error(`File extension .${fileExt} is not allowed. Allowed: ${ALLOWED_IMAGE_EXTENSIONS.join(', ')}`);
  }
  
  const fileName = `${validatedUserId}-${Date.now()}.${fileExt}`;
  
  const filePath = `${validatedUserId}/${fileName}`;

  
  const { error: uploadError } = await supabase.storage
    .from(BANNERS_BUCKET)
    .upload(filePath, file, {
      cacheControl: '3600',
      upsert: false,
    });

  if (uploadError) {
    throw uploadError;
  }

  
  const { data } = supabase.storage
    .from(BANNERS_BUCKET)
    .getPublicUrl(filePath);

  return data.publicUrl;
}


export async function deleteProfilePicture(url: string): Promise<void> {
  
  
  const urlParts = url.split('/');
  const publicIndex = urlParts.findIndex(part => part === 'public');
  if (publicIndex === -1) {
    throw new Error('Invalid profile picture URL');
  }
  
  const filePath = urlParts.slice(publicIndex + 1).join('/');

  const { error } = await supabase.storage
    .from(PROFILE_PICTURES_BUCKET)
    .remove([filePath]);

  if (error) {
    throw error;
  }
}


export async function deleteBanner(url: string): Promise<void> {
  
  
  const urlParts = url.split('/');
  const publicIndex = urlParts.findIndex(part => part === 'public');
  if (publicIndex === -1) {
    throw new Error('Invalid banner URL');
  }
  
  const filePath = urlParts.slice(publicIndex + 1).join('/');

  const { error } = await supabase.storage
    .from(BANNERS_BUCKET)
    .remove([filePath]);

  if (error) {
    throw error;
  }
}


export async function uploadGroupPicture(file: File, userId: string): Promise<string> {
  
  const validatedUserId = validateUserId(userId);

  
  const fileParts = file.name.toLowerCase().split('.');
  if (fileParts.length < 2) {
    throw new Error('File must have an extension');
  }
  const fileExt = fileParts[fileParts.length - 1];
  
  
  if (!ALLOWED_IMAGE_EXTENSIONS.includes(fileExt)) {
    throw new Error(`File extension .${fileExt} is not allowed. Allowed: ${ALLOWED_IMAGE_EXTENSIONS.join(', ')}`);
  }
  
  const fileName = `${validatedUserId}-${Date.now()}.${fileExt}`;
  
  const filePath = `${validatedUserId}/${fileName}`;

  
  const { error: uploadError } = await supabase.storage
    .from(GROUP_PICTURES_BUCKET)
    .upload(filePath, file, {
      cacheControl: '3600',
      upsert: false,
    });

  if (uploadError) {
    throw uploadError;
  }

  
  const { data } = supabase.storage
    .from(GROUP_PICTURES_BUCKET)
    .getPublicUrl(filePath);

  return data.publicUrl;
}


export async function deleteGroupPicture(url: string): Promise<void> {
  
  
  const urlParts = url.split('/');
  const publicIndex = urlParts.findIndex(part => part === 'public');
  if (publicIndex === -1) {
    throw new Error('Invalid group picture URL');
  }
  
  const filePath = urlParts.slice(publicIndex + 1).join('/');

  const { error } = await supabase.storage
    .from(GROUP_PICTURES_BUCKET)
    .remove([filePath]);

  if (error) {
    throw error;
  }
}


const ALLOWED_IMAGE_EXTENSIONS = ['jpg', 'jpeg', 'png', 'webp', 'gif'];


const FILE_SIGNATURES: Record<string, number[][]> = {
  'image/jpeg': [[0xFF, 0xD8, 0xFF]],
  'image/png': [[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]],
  'image/webp': [[0x52, 0x49, 0x46, 0x46], [0x57, 0x45, 0x42, 0x50]], 
  'image/gif': [[0x47, 0x49, 0x46, 0x38, 0x37, 0x61], [0x47, 0x49, 0x46, 0x38, 0x39, 0x61]], 
};


function validateFileExtension(filename: string): boolean {
  const parts = filename.toLowerCase().split('.');
  if (parts.length < 2) return false;
  const ext = parts[parts.length - 1];
  return ALLOWED_IMAGE_EXTENSIONS.includes(ext);
}


async function validateFileSignature(file: File): Promise<boolean> {
  return new Promise((resolve) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      if (!e.target?.result) {
        resolve(false);
        return;
      }
      
      const arrayBuffer = e.target.result as ArrayBuffer;
      const bytes = new Uint8Array(arrayBuffer.slice(0, 12)); 
      
      
      for (const [mimeType, signatures] of Object.entries(FILE_SIGNATURES)) {
        if (file.type === mimeType) {
          for (const signature of signatures) {
            let matches = true;
            for (let i = 0; i < signature.length; i++) {
              if (bytes[i] !== signature[i]) {
                matches = false;
                break;
              }
            }
            if (matches) {
              resolve(true);
              return;
            }
          }
        }
      }
      
      resolve(false);
    };
    reader.onerror = () => resolve(false);
    reader.readAsArrayBuffer(file.slice(0, 12));
  });
}


export async function validateImageFile(file: File, maxSizeMB: number = 5): Promise<string | null> {
  
  if (!file.type.startsWith('image/')) {
    return 'File must be an image';
  }
  
  
  if (!validateFileExtension(file.name)) {
    return `File extension not allowed. Allowed extensions: ${ALLOWED_IMAGE_EXTENSIONS.join(', ')}`;
  }
  
  
  const maxSizeBytes = maxSizeMB * 1024 * 1024;
  if (file.size > maxSizeBytes) {
    return `File size must be less than ${maxSizeMB}MB`;
  }
  
  
  const isValidSignature = await validateFileSignature(file);
  if (!isValidSignature) {
    return 'File does not appear to be a valid image file';
  }

  return null;
}

