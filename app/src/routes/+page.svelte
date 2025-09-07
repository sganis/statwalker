<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";
  import { getParent, formatBytes, capitalize } from "../js/util";

  import { listen } from '@tauri-apps/api/event';

  type Progress = {
    current: number;
    total: number;
  };

  listen<Progress>('progress', (event) => {
    progress_current = event.payload.current
    progress_total = event.payload.total
    if (progress_total > 0)
      progress_percent = Math.round(progress_current/progress_total * 100)
  })

  let path = $state("/");
  let files = $state<any[]>([]);
  let loading = $state(false);
  let initializing = $state(true)
  let progress_current = $state(0)
  let progress_total = $state(0)
  let progress_percent = $state(0)
  let history = $state<string[]>([path]);
  let histIdx = $state(0);
  type SortKey = "disk" | "size" | "count";
  let sortBy = $state<SortKey>("disk");
  let sortOpen = $state(false);
  let selectedUser = $state(-1)
  let users = $state([])

  // robust number coercion
  const toNum = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };

  // Always DESC sort
  const sortedFiles = $derived.by(() => {
    const key = sortBy;
    const arr = files ? [...files] : [];
    return arr.sort((a: any, b: any) => toNum(b?.[key]) - toNum(a?.[key]));
  });

  // --- sort-aware max + pct (use .by to track sortBy/files) ---
  let maxMetric = $derived.by(() => {
    const key = sortBy;
    const vals = files?.map(f => toNum(f?.[key])) ?? [];
    const max = Math.max(0, ...vals);
    return max > 0 ? max : 1;          // avoid divide-by-zero
  });

  const pct = (n: any) => {
    const x = toNum(n);
    const p = (x / maxMetric) * 100;
    const clamped = Math.max(0, Math.min(100, p));
    return Math.round(clamped * 10) / 10; // 1 decimal
  };

  const metricValue = (file: any) => toNum(file?.[sortBy]);

  // right-side label reflecting current sort
  function rightValue(file: any) {
    switch (sortBy) {
      case "disk":  return formatBytes(toNum(file?.disk));
      case "size":  return formatBytes(toNum(file?.size));
      case "count": return toNum(file?.count).toLocaleString();
    }
  }

  // ---- single-flight fetch (unchanged) ----
  function createDoItAgain<T extends any[]>(fn: (...args: T) => Promise<void>) {
    let running = false;
    let nextArgs: T | null = null;
    return async (...args: T) => {
      nextArgs = args;
      if (running) return;
      running = true;
      try {
        while (nextArgs) {
          const argsNow = nextArgs;
          nextArgs = null;
          await fn(...(argsNow as T));
        }
      } finally { running = false; }
    };
  }

  const fetchFiles = createDoItAgain(async (p: string) => {
    loading = true;
    try {
      let data = ''
      if (selectedUser > -1) {
        data = await invoke<string>("get_files_memory_user", { path: p, uid: Number(selectedUser) });
      } else {
        data = await invoke<string>("get_files_memory", { path: p });
      }
      console.log(data)
      files = JSON.parse(data)
    } finally {
      loading = false;
    }
  });

  function pushHistory(p: string) {
    if (history[histIdx] === p) return;
    history = history.slice(0, histIdx + 1);
    history.push(p);
    histIdx = history.length - 1;
  }

  function chooseSort(key: SortKey) { sortBy = key; sortOpen = false; }
  function navigateTo(p: string) { path = p; pushHistory(p); fetchFiles(p); }
  function refresh() { fetchFiles(path); }
  function goUp() { const parent = getParent(path); navigateTo(parent); }
  function goBack() { if (histIdx > 0) { histIdx -= 1; path = history[histIdx]; fetchFiles(path); } }
  function goForward() { if (histIdx < history.length - 1) { histIdx += 1; path = history[histIdx]; fetchFiles(path); } }
  function onPathKeydown(e: KeyboardEvent) { if (e.key === "Enter") navigateTo(path); }
  async function scan() { 
    let db = "/Users/san/dev/statwalker/rs/mac.agg.csv"
    try{
      initializing = true
      users = await invoke("load_db", { path: db });
      console.log(users)
      initializing = false
    } catch(e) {
      console.log(e)
    }
    fetchFiles(path);   
  }
  

  onMount(async () => { 
    // let db = "/Users/san/dev/statwalker/rs/mac.agg.csv"
    // try{
    //   initializing = true
    //   users = await invoke("load_db", { path: db });
    //   console.log(users)
    //   initializing = false
    // } catch(e) {
    //   console.log(e)
    // }
    // fetchFiles(path); 
  })

</script>


<div class="flex flex-col h-screen min-h-0 gap-2 p-2">
  <div class="flex gap-2 items-center relative">
    <button class="btn" onclick={goBack} disabled={histIdx === 0}>
      <span class="material-symbols-outlined">arrow_back_ios</span>
    </button>
    <button class="btn" onclick={goForward} disabled={histIdx >= history.length - 1}>
      <span class="material-symbols-outlined">arrow_forward_ios</span>
    </button>
    <button class="btn" onclick={goUp} disabled={getParent(path) === path}>
      <span class="material-symbols-outlined">arrow_upward</span>
    </button>
    <button class="btn" onclick={refresh}>
      <div class="flex items-center gap-2">
        <span class="material-symbols-outlined">refresh</span>
      </div>
    </button>

    <!-- Sort dropdown -->
    <div class="relative">
      <button class="btn w-24" onclick={() => (sortOpen = !sortOpen)}>
        <div class="flex items-center gap-2">
          <span class="material-symbols-outlined">sort</span>
          {capitalize(sortBy)}
        </div>
      </button>
      {#if sortOpen}
        <div class="flex flex-col divide-y divide-gray-500 absolute w-24 rounded border
           border-gray-500  bg-gray-800 shadow-lg z-20 overflow-hidden mt-1">
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" 
            onclick={() => chooseSort("disk")}>
            By Disk
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700 border-trasparent" 
            onclick={() => chooseSort("size")}>
            By Size
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" 
            onclick={() => chooseSort("count")}>
            By Count
          </button>
        </div>
      {/if}
    </div>
    <select bind:value={selectedUser} onchange={refresh}>
      <option value={-1}>All Users</option>
      {#each Object.entries(users) as [uid, username]}
      <option value={uid}>{username}</option>
      {/each}
    </select>    
    <button class="btn" onclick={scan}>
      <div class="flex items-center gap-2">
        <span class="material-symbols-outlined">scan</span>
      </div>
    </button>
  </div>
  <input
      bind:value={path}
      placeholder="Path..."
      class="grow"
      onkeydown={onPathKeydown}
      aria-busy={loading}
    />
  {#if initializing}
    <div class="flex flex-col w-full h-full items-center justify-between font-mono">
      <div class="w-full bg-gray-700 rounded-full h-1">
        <div 
          class="bg-orange-500 h-1 rounded-full transition-all duration-300"
          style="width: {progress_percent}%">
        </div>        
      </div>
      <div class="flex flex-col justify-center grow  items-center w-64">
          <div class="flex w-full justify-between">
            <div class="">Progress:</div>
            <div class="">{progress_percent}%</div>
          </div>   
          <div class="flex w-full justify-between">
            <div class="">Loaded files:</div>
            <div class="">{progress_current}</div>
          </div>   
          <div class="flex w-full justify-between">
            <div class="">Total:</div>
            <div class="">{progress_total}</div>
          </div>   
      
        </div>
    </div>
  {:else if loading}
    <!-- Skeleton Loader (UI stays interactive) -->
    <div class="flex flex-col gap-2 overflow-y-auto">
      {#each Array(6) as _, i}
        <div class="relative p-3 bg-gray-800 border border-gray-600 rounded-lg animate-pulse min-h-16 h-16">
          <div class="flex items-center justify-between gap-4">
            <div class="h-4 bg-gray-700 rounded w-3/4"></div>
            <div class="h-3 bg-gray-700 rounded w-12"></div>
          </div>
          <div class="flex gap-2 mt-2">
            <div class="h-3 bg-gray-700 rounded w-16"></div>
            <div class="h-3 bg-gray-700 rounded w-20"></div>
            <div class="h-3 bg-gray-700 rounded w-24"></div>
          </div>
        </div>
      {/each}
    </div>
  {:else}
    <div class="flex flex-col gap-2 overflow-y-auto transition-opacity duration-200 p-4">
      {#each sortedFiles as file}
        <div
          class="relative p-3 cursor-pointer hover:opacity-95 bg-gray-700 border border-gray-600 rounded-lg overflow-hidden min-h-16 h-16"
          onclick={() => navigateTo(file.path)}
        >
          <div
      class="absolute left-0 top-0 bottom-0 bg-orange-500/90 transition-[width] duration-300 z-0 pointer-events-none"
      style="width: {pct(metricValue(file))}%"
    />
          <div class="relative z-10">
            <div class="flex items-center justify-between gap-4">
              <p class="font-medium truncate text-white">{file.path}</p>
              <span class="text-xs text-gray-300 tabular-nums">{rightValue(file)}</span>
            </div>
            <p class="text-xs text-gray-300 mt-1">
              Files: {file.count} • 
              Size: {formatBytes(file.size)} •
              Disk: {formatBytes(file.disk)} •
              Modified: {new Date(file.modified * 1000).toLocaleString()}
            </p>
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>
