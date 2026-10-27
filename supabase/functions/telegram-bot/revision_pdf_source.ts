export type SupabaseStorageObjectRef = {
  bucket: string;
  path: string;
};

export function parseSupabaseStorageObjectUrl(
  fileUrl: string,
  supabaseUrl: string,
): SupabaseStorageObjectRef | null {
  let url: URL;
  let base: URL;

  try {
    url = new URL(fileUrl);
    base = new URL(supabaseUrl);
  } catch {
    return null;
  }

  if (url.origin !== base.origin) return null;

  const prefixes = [
    "/storage/v1/object/sign/",
    "/storage/v1/object/public/",
    "/storage/v1/object/authenticated/",
  ];

  const prefix = prefixes.find((candidate) => url.pathname.startsWith(candidate));
  if (!prefix) return null;

  const objectPart = url.pathname.slice(prefix.length);
  const slash = objectPart.indexOf("/");
  if (slash <= 0 || slash === objectPart.length - 1) return null;

  const bucket = decodeURIComponent(objectPart.slice(0, slash));
  const path = objectPart
    .slice(slash + 1)
    .split("/")
    .map((segment) => decodeURIComponent(segment))
    .join("/");

  return bucket && path ? { bucket, path } : null;
}
