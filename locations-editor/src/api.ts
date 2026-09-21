export class ApiError extends Error {
  status: number;
  recoveryUrl?: string;
  constructor(status: number, message: string, recoveryUrl?: string) {
    super(message);
    this.status = status;
    this.recoveryUrl = recoveryUrl;
  }
}
export async function api<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, { credentials: "same-origin", ...options });
  if (response.status === 204) return undefined as T;
  const data = await response.json();
  if (!response.ok)
    throw new ApiError(
      response.status,
      data.message || "The request failed. Try again.",
      data.recoveryUrl,
    );
  return data;
}
