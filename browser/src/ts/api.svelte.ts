import { State, API_URL } from "./store.svelte";
import { getCache, setCache } from './cache.js';

// NEW: accept optional age filter (0|1|2|'all') and return the new folders→users→ages shape
type AgeMini = { count: number; size: number; disk: number; mtime: number };
// Accept optional age filter (0 | 1 | 2 | 'all') and pass it through.
type ScannedFile = { path: string; size: number; modified: string; owner: string };
export type RawFolder = { path: string; users: Record<string, Record<'0'|'1'|'2', AgeMini>> };


class Api {
  private baseUrl = `${API_URL}/`;
  public error: string = "";

  private async request<T>(
    endpoint: string = "",
    method: "GET" | "POST" | "PUT" | "DELETE" = "GET",
    body?: unknown,
    use_cache?: boolean
  ): Promise<T | null> {
    try {
      this.error = "";

      const options: RequestInit = {
        method,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${State.token}`,
        },
      };

      if (body) 
        options.body = JSON.stringify(body);

      if (use_cache) {        
        let data = await getCache(endpoint);
        if (data) {
          //console.log("Data from cache:", endpoint);
          return data    
        }
      }
      const url = `${this.baseUrl}${endpoint}`
      console.log('fetching url:', url)

      const response = await fetch(url, options);
      const data: T = await response.json();
      if (!response.ok) {
        if (response.status === 401) {
          console.log('authentication failed.')
        }
        this.error = (data as any).detail || "Unknown API error";
        console.log(`API Error: ${this.error}`);
        return null;
      }

      // const data = await fetchWithIndexedDB(`${this.baseUrl}${endpoint}`, options);
      await setCache(endpoint, data)
          
      return data;

    } catch (err) {
      console.log("Fetch error:", err);
      this.error = "API: Error in fetching data.";
      return null;

    } finally {

    }
  }

  async getUsers(): Promise<string[]> {
    let result = await this.request<string[]>('users', "GET", undefined, true)
    return result ?? []
  }

  async getFolders(path: string, users: string[], age?: number): Promise<RawFolder[]> {
    const pathParam = `path=${encodeURIComponent(path)}`
    const userParam = users.length > 0 ? `&users=${encodeURIComponent(users.join(','))}` : '';
    const ageParam = age !== undefined && age !== -1 ? `&age=${age}` : ''
    const url = `folders?${pathParam}${userParam}${ageParam}`
    let result = await this.request<RawFolder[]>(url, "GET", undefined, true)
    return result ?? []
  }

  async getFiles(path: string, users: string[], age?: number): Promise<ScannedFile[]> {
    const pathParam = `path=${encodeURIComponent(path)}`
    const userParam = users.length > 0 ? `&users=${encodeURIComponent(users.join(','))}` : ''
    const ageParam = age !== undefined && age !== -1 ? `&age=${age}` : ''
    const url = `files?${pathParam}${userParam}${ageParam}`
    let result = await this.request<ScannedFile[]>(url, "GET", undefined, true)
    return result ?? []
  }

  // async createItem(Item: Partial<Item>): Promise<Item | null> {
  //   return await this.request<Item>("Items", "POST", Item);
  // }

  // async updateItem(id: number, Item: Partial<Item>): Promise<Item | null> {
  //   return await this.request<Item>(`Items/${id}`, "PUT", Item);
  // }

  // async deleteItem(id: number): Promise<boolean> {
  //   return (await this.request<{ success: boolean }>(`Items/${id}`, "DELETE"))?.success ?? false;
  // }

}

export const api = new Api();

