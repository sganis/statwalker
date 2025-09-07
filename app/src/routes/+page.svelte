<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";
  import { getParent, formatBytes } from "../js/util";

  let path = $state("/");
  let files = $state<any[]>([]);
  let loading = $state(false);

  // History state
  let history = $state<string[]>([path]);
  let histIdx = $state(0);

  // For progress bars
  let maxSize = $derived(files?.length ? Math.max(...files.map(i => i.disk || 0)) : 1);
  const pct = (n: number) => Math.max(0, Math.min(100, Math.round((+n / maxSize) * 1000) / 10));

  // ---- sorting ----
  type SortKey = "disk" | "size" | "count";
  let sortBy = $state<SortKey>("disk");
  let sortDir = $state<"asc" | "desc">("desc");
  let sortOpen = $state(false);

  const sortedFiles = $derived.by(() => {
    const arr = files ? [...files] : [];
    const key: SortKey = sortBy;
    let sf = arr.sort((a: any, b: any) => {
      const av = Number(a?.[key] ?? 0);
      const bv = Number(b?.[key] ?? 0);
      return (av - bv) * -1; // always desc
    })
    console.log("Sorted files", sf);
    return sf;
  });

  // -------- Single-flight "do-it-again" ----------
  function createDoItAgain<T extends any[]>(fn: (...args: T) => Promise<void>) {
    let running = false;
    let nextArgs: T | null = null;

    return async (...args: T) => {
      // Always capture the latest intent
      nextArgs = args;
      if (running) return; // We will run once current completes
      running = true;
      try {
        while (nextArgs) {
          const argsNow = nextArgs;
          nextArgs = null;
          await fn(...(argsNow as T));
        }
      } finally {
        running = false;
      }
    };
  }

  // Actual fetcher wrapped with do-it-again
  const fetchFiles = createDoItAgain(async (p: string) => {
    loading = true; // keep true across the loop
    try {
      const raw: string = await invoke("get_files", { path: p });
      const list = JSON.parse(raw).sort((a: any, b: any) => b.disk - a.disk);
      files = list;
    } finally {
      loading = false; // turns false only after the final run in the loop
    }
  });

  function pushHistory(p: string) {
    if (history[histIdx] === p) return;
    history = history.slice(0, histIdx + 1);
    history.push(p);
    histIdx = history.length - 1;
  }

  // Navigate normally: update path & history, then queue fetch
  async function navigateTo(p: string) {
    path = p;          // reflect immediately in UI
    pushHistory(p);    // record history move
    // Queue the latest path; if a fetch is running, this won't start a new trip,
    // it will only schedule a single "do it again" with the newest path.
    fetchFiles(p);
  }

  // Refresh queues current path (same single-flight semantics)
  function refresh() {
    fetchFiles(path);
  }

  function goUp() {
    const parent = getParent(path);
    navigateTo(parent); // queue (don’t block UI)
  }

  // Back/Forward traverse history but DO NOT push a new entry
  function goBack() {
    if (histIdx > 0) {
      histIdx -= 1;
      path = history[histIdx];
      fetchFiles(path); // queue latest; single-flight handles coalescing
    }
  }

  function goForward() {
    if (histIdx < history.length - 1) {
      histIdx += 1;
      path = history[histIdx];
      fetchFiles(path); // queue latest; single-flight handles coalescing
    }
  }

  function onPathKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") navigateTo(path);
  }

  // ---- sort UI helpers ----
  function chooseSort(key: SortKey) {
    sortBy = key;
    sortOpen = false;
  }

  // right-side formatter based on current sort
  function rightValue(file: any) {
    switch (sortBy) {
      case "disk": return formatBytes(Number(file?.disk ?? 0));
      case "size": return formatBytes(Number(file?.size ?? 0));
      case "count": return (Number(file?.count ?? 0)).toLocaleString();
    }
  }

  onMount(() => {
    fetchFiles(path); // initial load
  });
</script>

<div class="flex flex-col h-screen min-h-0 gap-2">
  <div class="flex gap-2 items-center relative">
    <button on:click={goBack} disabled={histIdx === 0}>◀ Back</button>
    <button on:click={goForward} disabled={histIdx >= history.length - 1}>Forward ▶</button>
    <button on:click={goUp} disabled={getParent(path) === path}>Up</button>
    <button on:click={refresh}>Refresh</button>

    <!-- Sort dropdown -->
    <div class="relative">
      <button on:click={() => (sortOpen = !sortOpen)}>
        Sort: {sortBy.toUpperCase()} ▾
      </button>
      {#if sortOpen}
        <div class="absolute mt-1 w-40 rounded-md border border-gray-600 bg-gray-800 shadow-lg z-20">
          <button class="block w-full text-left px-3 py-2 hover:bg-gray-700" on:click={() => chooseSort("disk")}>
            Disk
          </button>
          <button class="block w-full text-left px-3 py-2 hover:bg-gray-700" on:click={() => chooseSort("size")}>
            Size
          </button>
          <button class="block w-full text-left px-3 py-2 hover:bg-gray-700" on:click={() => chooseSort("count")}>
            Files
          </button>
        </div>
      {/if}
    </div>
    <input
      bind:value={path}
      placeholder="Path..."
      class="grow"
      on:keydown={onPathKeydown}
      aria-busy={loading}
    />
  </div>

  {#if loading}
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
          on:click={() => navigateTo(file.path)}
        >
          <div class="absolute inset-0 bg-orange-500/90 transition-all duration-300"
               style="width: {pct(file.disk)}%; z-index: 0;"></div>
          <div class="relative z-10">
            <div class="flex items-center justify-between gap-4">
              <p class="font-medium truncate text-white">{file.path}</p>
              <span class="text-xs text-gray-300 tabular-nums">{rightValue(file)}</span>
            </div>
            <p class="text-xs text-gray-300 mt-1">
              Files: {file.count} • 
              Disk Usage: {formatBytes(file.disk)} •
              Last Modified: {new Date(file.modified * 1000).toLocaleString()}
            </p>
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>
