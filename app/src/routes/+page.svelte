<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";
  import { getParent, formatBytes } from "../js/util";

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
      const raw: string = await invoke("get_files_memory", { path: p });
      files = JSON.parse(raw);               // keep raw; we sort in derived
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

  onMount(async () => { 
    let db = "/Users/san/dev/statwalker/rs/mac.agg.csv"
    try{
      initializing = true
      await invoke("load_db", { path: db });
      initializing = false
    } catch(e) {
      console.log(e)
    }
    fetchFiles(path); 
  })

</script>


<div class="flex flex-col h-screen min-h-0 gap-2">
  <div class="flex gap-2 items-center relative">
    <button onclick={goBack} disabled={histIdx === 0}>◀ Back</button>
    <button onclick={goForward} disabled={histIdx >= history.length - 1}>Forward ▶</button>
    <button onclick={goUp} disabled={getParent(path) === path}>Up</button>
    <button onclick={refresh}>Refresh</button>

    <!-- Sort dropdown -->
    <div class="relative">
      <button onclick={() => (sortOpen = !sortOpen)}>
        Sort: {sortBy.toUpperCase()} ▾
      </button>
      {#if sortOpen}
        <div class="absolute mt-1 w-40 rounded-md border border-gray-600 bg-gray-800 shadow-lg z-20">
          <button class="block w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("disk")}>
            Disk
          </button>
          <button class="block w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("size")}>
            Size
          </button>
          <button class="block w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("count")}>
            Files
          </button>
        </div>
      {/if}
    </div>
    <input
      bind:value={path}
      placeholder="Path..."
      class="grow"
      onkeydown={onPathKeydown}
      aria-busy={loading}
    />
  </div>

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
    <div class="flex flex-col gap-2 overflow-y-auto p-4">
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
