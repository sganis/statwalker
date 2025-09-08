<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";
  import { getParent, formatBytes, capitalize, COLORS} from "../js/util";

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

  // Regular file item (existing)
  type FileItem = {
    path: string;
    count: number;
    size: number;
    disk: number;
    modified: number;
    users: number[];
  };

  // Stacked file item (new)
  type UserStatsJson = {
    username: string;
    count: number;
    size: number;
    disk: number;
  };

  type FileItemStacked = {
    path: string;
    total_count: number;
    total_size: number;
    total_disk: number;
    modified: number;
    users: Record<number, UserStatsJson>;
  };

  let path = $state("/");
  let files = $state<any[]>([]);
  let loading = $state(false);
  let initializing = $state(false)
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
  let userColors = $state(new Map<string, string>());

  // Generate consistent colors for users
  function generateUserColors(users: any) {
    const colorMap = new Map();
    Object.keys(users).forEach((uid, index) => {
      colorMap.set(uid, COLORS[index % COLORS.length]);
    });
    return colorMap;
  }

  type Tip = {
    show: boolean;
    x: number;
    y: number;
    username?: string;
    value?: string;
    percent?: number;
  };
  let tip = $state<{ show: boolean; x: number; y: number; username?: string; value?: string; percent?: number; }>({
    show: false, x: 0, y: 0
  });

  function showTip(e: MouseEvent, userData: UserStatsJson, percent: number) {
    tip = {
      show: true,
      x: e.clientX,
      y: e.clientY,
      username: userData.username,
      value: rightValueForUser(userData),
      percent: Math.round(percent * 10) / 10,
    };
  }

  function moveTip(e: MouseEvent) {
    if (!tip.show) return;
    tip = { ...tip, x: e.clientX, y: e.clientY };
  }

  function hideTip() { tip = { show: false, x: 0, y: 0 }; }


  // robust number coercion
  const toNum = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };

  // Always DESC sort
  const sortedFiles = $derived.by(() => {
    const key = sortBy;
    const arr = files ? [...files] : [];
    return arr.sort((a: any, b: any) => {
      let aVal, bVal;
      
      switch(key) {
          case "disk": aVal = toNum(a?.total_disk); bVal = toNum(b?.total_disk); break;
          case "size": aVal = toNum(a?.total_size); bVal = toNum(b?.total_size); break;
          case "count": aVal = toNum(a?.total_count); bVal = toNum(b?.total_count); break;
      } 
      return bVal - aVal;
    })
  })

  // --- sort-aware max + pct (use .by to track sortBy/files) ---
  let maxMetric = $derived.by(() => {
    const key = sortBy;
    const vals = files?.map(f => {
        switch(key) {
          case "disk": return toNum(f?.total_disk);
          case "size": return toNum(f?.total_size);
          case "count": return toNum(f?.total_count);
        }      
    }) ?? [];
    const max = Math.max(0, ...vals);
    return max > 0 ? max : 1;          // avoid divide-by-zero
  });

  const pct = (n: any) => {
    const x = toNum(n);
    const p = (x / maxMetric) * 100;
    const clamped = Math.max(0, Math.min(100, p));
    return Math.round(clamped * 10) / 10; // 1 decimal
  };

  const metricValue = (file: any) => {
      switch(sortBy) {
        case "disk": return toNum(file?.total_disk);
        case "size": return toNum(file?.total_size);
        case "count": return toNum(file?.total_count);
      }
  };

  // right-side label reflecting current sort
  function rightValue(file: any) {
      switch (sortBy) {
        case "disk":  return formatBytes(toNum(file?.total_disk));
        case "size":  return formatBytes(toNum(file?.total_size));
        case "count": return toNum(file?.total_count).toLocaleString();
      }
  }

  // Helper function for user-specific right values
  function rightValueForUser(userData: UserStatsJson) {
    switch (sortBy) {
      case "disk":  return formatBytes(toNum(userData?.disk));
      case "size":  return formatBytes(toNum(userData?.size));
      case "count": return toNum(userData?.count).toLocaleString();
    }
  }

  const userMetricFor = (ud: any) =>
    sortBy === "disk" ? Number(ud.disk)
  : sortBy === "size" ? Number(ud.size)
  : Number(ud.count);

  // per-file sorter
  function sortedUserEntries(file: any) {
    return Object.entries(file?.users ?? {})
      .sort(([, a], [, b]) => userMetricFor(a) - userMetricFor(b)); // small → large
  }

  // Build an aggregate "file" for the current path: sums of all visible children
  function aggregatePathTotals(filesArr: any[], p: string): FileItemStacked {
    let total_count = 0, total_size = 0, total_disk = 0, modified = 0;
    const aggUsers: Record<string, UserStatsJson> = {};

    for (const f of filesArr ?? []) {
      total_count += toNum(f?.total_count);
      total_size  += toNum(f?.total_size);
      total_disk  += toNum(f?.total_disk);
      modified     = Math.max(modified, toNum(f?.modified));

      const u = f?.users ?? {};
      for (const [uid, data] of Object.entries(u)) {
        const key = String(uid);
        const prev = aggUsers[key] ?? {
          username: (users && (users as any)[key]) || (data as any)?.username || key,
          count: 0, size: 0, disk: 0,
        };
        aggUsers[key] = {
          username: prev.username,
          count: prev.count + toNum((data as any)?.count),
          size:  prev.size  + toNum((data as any)?.size),
          disk:  prev.disk  + toNum((data as any)?.disk),
        };
      }
    }

    return {
      path: p,
      total_count,
      total_size,
      total_disk,
      modified,
      users: aggUsers as any,
    };
  }

  // Recompute whenever `files` or `path` changes
  const pathTotals = $derived.by(() => aggregatePathTotals(files, path));

  // ---- single-flight fetch (updated for stacked view) ----
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
      // Determine user filter based on current UI state
      let userFilter: number[] = [];
      
      if (selectedUser > -1) {
        userFilter = [Number(selectedUser)];
      } 
      // Always call the same function, just with different filters
      const data = await invoke<string>("get_files", { 
        path: p, 
        userFilter 
      });
      
      files = JSON.parse(data);
      console.log('files:', $state.snapshot(files))
      
      // Generate user colors when we have user data
      if (users) {
        userColors = generateUserColors(users);
      }
    } finally {
      loading = false;
    }
  });

// The rest of your code stays exactly the same!
// No need for multiple invoke calls or complex branching logic
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
    await scan()
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

    <!-- Hide user selector in stacked mode -->
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
  <div class="flex">
    <input
        bind:value={path}
        placeholder="Path..."
        class="grow"
        onkeydown={onPathKeydown}
        aria-busy={loading}
      />
  </div>

  <!-- Path total header item -->
  <div class="">
    <div class="relative px-2 bg-gray-700 border border-gray-600 rounded overflow-hidden">
      <!-- Stacked bar for TOTAL of current path (full width) -->
      <div class="absolute left-0 top-0 bottom-0 flex z-0" style="width: 100%">
        {#each sortedUserEntries(pathTotals) as [uid, userData] (uid)}
          {@const userMetric = sortBy === "disk" ? userData.disk :
                              sortBy === "size" ? userData.size :
                              userData.count}
          {@const totalMetric = sortBy === "disk" ? pathTotals.total_disk :
                                sortBy === "size" ? pathTotals.total_size :
                                pathTotals.total_count}
          {@const userPercent = totalMetric > 0 ? (userMetric / totalMetric) * 100 : 0}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <div class="h-full transition-all duration-300 min-w-[0.5px] hover:opacity-90"
              style="width: {userPercent}%; background-color: {userColors.get(uid) || '#666'};"
              onmouseenter={(e) => showTip(e, userData, userPercent)}
              onmousemove={moveTip}
              onmouseleave={hideTip}
              aria-label={`${userData.username}: ${rightValueForUser(userData)}`}>
          </div>
        {/each}
      </div>
      <!-- Foreground content -->
      <div class="relative z-10 pointer-events-none">
        <div class="flex items-center justify-end">
        <p class="text-xs">
          Files: {pathTotals.total_count} •
          Modified: {pathTotals.modified ? new Date(pathTotals.modified * 1000).toLocaleDateString() : "—"} •
          Size: {formatBytes(pathTotals.total_size)} •
          Disk: {formatBytes(pathTotals.total_disk)} 
          
        </p>
        </div>
      </div>
    </div>
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
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="relative p-2 cursor-pointer hover:opacity-95 bg-gray-700 border border-gray-600 rounded-lg overflow-hidden min-h-16 h-16"
          onclick={() => navigateTo(file.path)}
        >
            <!-- Stacked bar background -->
            <div class="absolute left-0 top-0 bottom-0 flex z-0" 
                 style="width: {pct(metricValue(file))}%">
              {#each sortedUserEntries(file) as [uid, userData]}
                {@const userMetric = sortBy === "disk" ? userData.disk : 
                                     sortBy === "size" ? userData.size : 
                                     userData.count}
                {@const totalMetric = sortBy === "disk" ? file.total_disk : 
                                      sortBy === "size" ? file.total_size : 
                                      file.total_count}
                {@const userPercent = totalMetric > 0 ? (userMetric / totalMetric) * 100 : 0}
                <div class="h-full transition-all duration-300 min-w-[0.5px] hover:opacity-90" 
                    style="width: {userPercent}%; background-color: {userColors.get(uid) || '#666'};"
                    onmouseenter={(e) => showTip(e, userData, userPercent)}
                    onmousemove={moveTip}
                    onmouseleave={hideTip}
                    aria-label={`${userData.username}: ${rightValueForUser(userData)}`}
                  >
                </div>
              {/each}
            </div>
          
          <div class="flex flex-col gap-2 relative z-10 pointer-events-none">
            <div class="flex items-center justify-between gap-4">
              <div class="w-full overflow-hidden text-ellipsis whitespace-nowrap 
                text-left [direction:rtl]">
                <span class="[direction:ltr]">{file.path}</span>
              </div>
              <span class="text-nowrap font-bold">{rightValue(file)}</span>
            </div>
            <div class="flex justify-end">
              <p class="text-xs text-gray-300">
                Files: {file.total_count} • 
                Size: {formatBytes(file.total_size)} •
                Disk: {formatBytes(file.total_disk)} •
                Modified: {new Date(file.modified * 1000).toLocaleDateString()}
              </p>
            </div>
            
          </div>
        </div>
      {/each}
    </div>

    <!-- Add a legend for stacked mode -->
    <!-- {#if userColors.size > 0}
    <div class="flex flex-wrap gap-2 p-2 bg-gray-800 rounded-lg mt-2">
      <span class="text-xs text-gray-300">Users:</span>
      {#each Object.entries(users) as [uid, username]}
        <div class="flex items-center gap-1">
          <div class="w-3 h-3 rounded" style="background-color: {userColors.get(uid)}"></div>
          <span class="text-xs text-gray-300">{username}</span>
        </div>
      {/each}
    </div>
    {/if} -->
  {/if}
  <div class="grow"></div>
</div>


{#if tip.show}
  <div
    class="fixed z-50 pointer-events-none"
    style="
      left: {tip.x}px;
      top: {tip.y}px;
      transform: translate(-50%, calc(-100% - 10px));  /* center & above pointer */
    "
  >
    <div class="relative rounded-xl border border-white/10 bg-black/90 text-white shadow-xl px-3 py-2">
      <div class="flex items-center justify-between gap-3">
        <div class="font-medium text-sm truncate max-w-[180px]">{tip.username}</div>
        <div class="text-xs opacity-80">{tip.percent}%</div>
      </div>
      <div class="text-xs opacity-90 mt-1">{tip.value}</div>

      <!-- arrow pointing down to the cursor -->
      <div class="absolute left-1/2 top-full -translate-x-1/2 mt-[-4px]">
        <div class="w-2 h-2 rotate-45 bg-black/90 border border-white/10 border-l-0 border-t-0"></div>
      </div>
    </div>
  </div>
{/if}
