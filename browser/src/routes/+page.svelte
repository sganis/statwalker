<script lang="ts">
  import { onMount } from "svelte";
  import { getParent, formatBytes, capitalize, COLORS } from "../js/util";
  import { api } from "../js/api.svelte";
  import { API_URL } from "../js/store.svelte";
  import Svelecte from 'svelecte'

  // ---------- Types from the NEW backend shape (bytes) ----------

  // Per-user stats embedded in a folder row (sizes in BYTES now)
  type UserStatsJson = {
    username: string;
    count: number;
    size: number; // bytes
    disk: number; // bytes
  };

  // Folder row from /api/folders (index)
  type FileItem = {
    path: string;
    total_count: number;
    total_size: number; // bytes
    total_disk: number; // bytes
    modified: string;   // ISO date string (e.g., "2025-09-09")
    users: Record<string, UserStatsJson>; // keyed by username
  };

  // Scanned file from /api/files
  type ScannedFile = {
    path: string;
    size: number;      // bytes
    modified: string;  // ISO date string
    owner: string;     // username
  };

  // ---------- State ----------

  let path = $state("/");
  let folders = $state<FileItem[]>([]);
  let files = $state<ScannedFile[]>([]);
  let loading = $state(false);
  let initializing = $state(false);
  let progress_current = $state(0);
  let progress_total = $state(0);
  let progress_percent = $state(0);
  let history = $state<string[]>([path]);
  let histIdx = $state(0);

  type SortKey = "disk" | "size" | "count";
  let sortBy = $state<SortKey>("disk");
  let sortOpen = $state(false);

  // selection is by username (not uid). Empty string = "All users"
  let selectedUser = $state<string>("All Users");

  // /api/users returns a simple string[]
  let users = $state<string[]>([]);
  let userColors = $state(new Map<string, string>()); // cache: username -> color

  // ---------- Helpers ----------

  function displayPath(p: string): string {
    if (!p) return "/";
    let s = p.replace(/\\/g, "/");
    if (s !== "/") s = s.replace(/\/+$/, "");
    if (!s.startsWith("/")) s = "/" + s;
    return s || "/";
  }

  // Seed colors for known users (optional)
  function seedUserColors(usernames: string[]) {
    usernames.forEach((uname, index) => {
      if (!userColors.has(uname)) {
        userColors.set(uname, COLORS[index % COLORS.length]);
      }
    });
  }

  // Deterministic color for any username (stable + cached)
  function colorForUsername(uname: string): string {
    const cached = userColors.get(uname);
    if (cached) return cached;
    let h = 0;
    for (let i = 0; i < uname.length; i++) {
      h = (h * 31 + uname.charCodeAt(i)) >>> 0;
    }
    const color = COLORS[h % COLORS.length];
    userColors.set(uname, color);
    return color;
  }

  type Tip = {
    show: boolean;
    x: number;
    y: number;
    username?: string;
    value?: string;
    percent?: number;
  };
  let tip = $state<Tip>({ show: false, x: 0, y: 0 });

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
  function hideTip() {
    tip = { show: false, x: 0, y: 0 };
  }

  const toNum = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };

  // ---------- Sorting (Folders) ----------

  const sortedfolders = $derived.by(() => {
    const key = sortBy;
    const arr = folders ? [...folders] : [];
    return arr.sort((a: any, b: any) => {
      let aVal, bVal;
      switch (key) {
        case "disk":
          aVal = toNum(a?.total_disk);
          bVal = toNum(b?.total_disk);
          break;
        case "size":
          aVal = toNum(a?.total_size);
          bVal = toNum(b?.total_size);
          break;
        case "count":
          aVal = toNum(a?.total_count);
          bVal = toNum(b?.total_count);
          break;
      }
      return bVal - aVal;
    });
  });

  // Folder progress bar max
  let maxMetric = $derived.by(() => {
    const key = sortBy;
    const vals =
      folders?.map((f) => {
        switch (key) {
          case "disk":
            return toNum(f?.total_disk);
          case "size":
            return toNum(f?.total_size);
          case "count":
            return toNum(f?.total_count);
        }
      }) ?? [];
    const max = Math.max(0, ...vals);
    return max > 0 ? max : 1;
  });

  const pct = (n: any) => {
    const x = toNum(n);
    const p = (x / maxMetric) * 100;
    const clamped = Math.max(0, Math.min(100, p));
    return Math.round(clamped * 10) / 10;
  };

  const metricValue = (file: FileItem) => {
    switch (sortBy) {
      case "disk":
        return toNum(file?.total_disk);
      case "size":
        return toNum(file?.total_size);
      case "count":
        return toNum(file?.total_count);
    }
  };

  // Right label (folders) – sizes already in BYTES
  function rightValue(file: FileItem) {
    switch (sortBy) {
      case "disk":
        return formatBytes(toNum(file?.total_disk));
      case "size":
        return formatBytes(toNum(file?.total_size));
      case "count":
        return toNum(file?.total_count).toLocaleString();
    }
  }

  // Per-user right label – sizes already in BYTES
  function rightValueForUser(userData: UserStatsJson) {
    switch (sortBy) {
      case "disk":
        return formatBytes(toNum(userData?.disk));
      case "size":
        return formatBytes(toNum(userData?.size));
      case "count":
        return toNum(userData?.count).toLocaleString();
    }
  }

  const userMetricFor = (ud: UserStatsJson) =>
    sortBy === "disk" ? Number(ud.disk) : sortBy === "size" ? Number(ud.size) : Number(ud.count);

  function sortedUserEntries(file: FileItem) {
    return Object.entries(file?.users ?? {}).sort(([, a], [, b]) => userMetricFor(a) - userMetricFor(b));
  }

  // Build an aggregate "file" for the current path: sums of all visible children (bytes)
  function aggregatePathTotals(foldersArr: FileItem[], p: string): FileItem {
    let total_count = 0;
    let total_size = 0;
    let total_disk = 0;
    let modified = ""; // ISO string; keep the latest lexicographically
    const aggUsers: Record<string, UserStatsJson> = {};

    for (const f of foldersArr ?? []) {
      total_count += toNum(f?.total_count);
      total_size += toNum(f?.total_size);
      total_disk += toNum(f?.total_disk);
      if (typeof f?.modified === "string" && f.modified) {
        if (!modified || f.modified > modified) modified = f.modified;
      }

      const u = f?.users ?? {};
      for (const [uname, data] of Object.entries(u)) {
        const key = String(uname);
        const prev =
          aggUsers[key] ?? {
            username: (data as any)?.username || key,
            count: 0,
            size: 0,
            disk: 0,
          };
        aggUsers[key] = {
          username: prev.username,
          count: prev.count + toNum((data as any)?.count),
          size: prev.size + toNum((data as any)?.size),
          disk: prev.disk + toNum((data as any)?.disk),
        };
      }
    }

    return {
      path: p,
      total_count,
      total_size,
      total_disk,
      modified,
      users: aggUsers,
    };
  }

  const pathTotals = $derived.by(() => aggregatePathTotals(folders, path));

  // ---------- Sorting (Files) ----------
  function fileMetricValue(f: ScannedFile) {
    switch (sortBy) {
      case "disk":
      case "size":
        return toNum(f.size); // bytes
      case "count":
        return 1;
    }
  }

  const sortedfiles = $derived.by(() => {
    const arr = files ? [...files] : [];
    return arr.sort((a, b) => fileMetricValue(b) - fileMetricValue(a));
  });

  const maxFileMetric = $derived.by(() => {
    const vals = files?.map((f) => fileMetricValue(f)) ?? [];
    const max = Math.max(0, ...vals);
    return max > 0 ? max : 1;
  });
  const filePct = (f: ScannedFile) => Math.round((fileMetricValue(f) / maxFileMetric) * 1000) / 10;

  function rightValueFile(f: ScannedFile) {
    switch (sortBy) {
      case "disk":
      case "size":
        return formatBytes(toNum(f.size)); // bytes
      case "count":
        return "1";
    }
  }

  // ---- single-flight fetch (usernames) ----
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
      } finally {
        running = false;
      }
    };
  }

  const fetchFolders = createDoItAgain(async (_p: string) => {
    loading = true;
    try {
      const userFilter: string[] = selectedUser ==='All Users'? []: [selectedUser];
      folders = await api.getFolders(path, userFilter);
      files = await api.getFiles(path, userFilter); // files now include owner + ISO modified
      // Seed colors for the known users list, but bars use colorForUsername() anyway
      if (users && users.length > 0) seedUserColors(users);
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

  function chooseSort(key: SortKey) {
    sortBy = key;
    sortOpen = false;
  }
  function navigateTo(p: string) {
    path = displayPath(p);
    pushHistory(path);
    fetchFolders(path);
  }
  function refresh() {
    fetchFolders(path);
  }
  function goUp() {
    const parent = getParent(path);
    navigateTo(parent);
  }
  function goBack() {
    if (histIdx > 0) {
      histIdx -= 1;
      path = history[histIdx];
      fetchFolders(path);
    }
  }
  function goForward() {
    if (histIdx < history.length - 1) {
      histIdx += 1;
      path = history[histIdx];
      fetchFolders(path);
    }
  }
  function onPathKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") navigateTo(path);
  }

  onMount(async () => {
    console.log("api url:", API_URL);
    users = await api.getUsers(); // array of usernames
    users.splice(0,0,"All Users")
    selectedUser = 'All Users'
    seedUserColors(users);
    await refresh();
  });
</script>

<div class="flex flex-col h-screen min-h-0 gap-2 p-2">
  <div class="flex gap-2 items-center relative">
    <button class="btn" onclick={goBack} disabled={histIdx === 0}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_back_ios</span>
      </div>
    </button>
    <button class="btn" onclick={goForward} disabled={histIdx >= history.length - 1}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_forward_ios</span>
      </div>
    </button>
    <button class="btn" onclick={goUp} disabled={getParent(path) === path}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_upward</span>
      </div>
    </button>
    <button class="btn" onclick={refresh}>
      <div class="flex items-center">
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
        <div
          class="flex flex-col divide-y divide-gray-500 absolute w-24 rounded border
           border-gray-500 bg-gray-800 shadow-lg z-20 overflow-hidden mt-1"
        >
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("disk")}>
            By Disk
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("size")}>
            By Size
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("count")}>
            By Count
          </button>
        </div>
      {/if}
    </div>

    <!-- username selector -->
    <!-- <select bind:value={selectedUser} onchange={refresh} class="min-w-40">
      <option value="">All Users</option>
      {#each users as uname}
        <option value={uname}>{uname}</option>
      {/each}
    </select> -->
    <Svelecte  bind:value={selectedUser} options={users} onChange={refresh}
       class="z-20 min-w-20 h-10 border rounded border-gray-600 bg-gray-800 text-white"
    />
  </div>

  <div class="flex">
    <input bind:value={path} placeholder="Path..." class="grow" onkeydown={onPathKeydown} aria-busy={loading} />
  </div>

  <!-- Path total header item -->
  <div>
    <div class="relative px-2 bg-gray-700 border border-gray-600 rounded overflow-hidden">
      <!-- Stacked bar for TOTAL of current path (full width) -->
      <div class="absolute left-0 top-0 bottom-0 flex z-0" style="width: 100%">
        {#each sortedUserEntries(pathTotals) as [uname, userData] (uname)}
          {@const userMetric = sortBy === "disk" ? userData.disk : sortBy === "size" ? userData.size : userData.count}
          {@const totalMetric =
            sortBy === "disk" ? pathTotals.total_disk : sortBy === "size" ? pathTotals.total_size : pathTotals.total_count}
          {@const userPercent = totalMetric > 0 ? (userMetric / totalMetric) * 100 : 0}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <div
            class="h-full transition-all duration-300 min-w-[0.5px] hover:opacity-90"
            style="width: {userPercent}%; background-color: {colorForUsername(uname)};"
            onmouseenter={(e) => showTip(e, userData, userPercent)}
            onmousemove={moveTip}
            onmouseleave={hideTip}
            aria-label={`${userData.username}: ${rightValueForUser(userData)}`}
          ></div>
        {/each}
      </div>
      <!-- Foreground content -->
      <div class="relative z-10 pointer-events-none">
        <div class="flex items-center justify-end">
          <p class="text-xs">
            folders: {pathTotals.total_count} • Modified:
            {pathTotals.modified || "—"} • Size:
            {formatBytes(pathTotals.total_size)} • Disk: {formatBytes(pathTotals.total_disk)}
          </p>
        </div>
      </div>
    </div>
  </div>

  {#if initializing}
    <div class="flex flex-col w-full h-full items-center justify-between font-mono">
      <div class="w-full bg-gray-700 rounded-full h-1">
        <div class="bg-orange-500 h-1 rounded-full transition-all duration-300" style="width: {progress_percent}%"></div>
      </div>
      <div class="flex flex-col justify-center grow items-center w-64">
        <div class="flex w-full justify-between">
          <div>Progress:</div>
          <div>{progress_percent}%</div>
        </div>
        <div class="flex w-full justify-between">
          <div>Loaded folders:</div>
          <div>{progress_current}</div>
        </div>
        <div class="flex w-full justify-between">
          <div>Total:</div>
          <div>{progress_total}</div>
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
      <!-- Folders -->
      {#each sortedfolders as file}
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="relative px-2 py-1 cursor-pointer hover:opacity-95 bg-gray-700 border border-gray-600 rounded-lg overflow-hidden min-h-16"
          onclick={() => navigateTo(file.path)}
        >
          <!-- Stacked bar background -->
          <div class="absolute left-0 top-0 bottom-0 flex z-0" style="width: {pct(metricValue(file))}%">
            {#each sortedUserEntries(file) as [uname, userData]}
              {@const userMetric = sortBy === "disk" ? userData.disk : sortBy === "size" ? userData.size : userData.count}
              {@const totalMetric = sortBy === "disk" ? file.total_disk : sortBy === "size" ? file.total_size : file.total_count}
              {@const userPercent = totalMetric > 0 ? (userMetric / totalMetric) * 100 : 0}
              <div
                class="h-full transition-all duration-300 min-w-[0.5px] hover:opacity-90"
                style="width: {userPercent}%; background-color: {colorForUsername(uname)};"
                onmouseenter={(e) => showTip(e, userData, userPercent)}
                onmousemove={moveTip}
                onmouseleave={hideTip}
                aria-label={`${userData.username}: ${rightValueForUser(userData)}`}
              ></div>
            {/each}
          </div>

          <div class="flex flex-col gap-2 relative z-10 pointer-events-none">
            <div class="flex items-center justify-between gap-4">
              <div class="w-full overflow-hidden text-ellipsis whitespace-nowrap">
                <span>{file.path}</span>
              </div>
              <span class="text-nowrap font-bold">{rightValue(file)}</span>
            </div>
            <div class="flex justify-end">
              <p class="text-xs text-gray-300">
                folders: {file.total_count} • Size: {formatBytes(file.total_size)} • Disk:
                {formatBytes(file.total_disk)} • Modified: {file.modified || "—"}
              </p>
            </div>
          </div>
        </div>
      {/each}

      <!-- Files (after folders) -->
      {#each sortedfiles as f}
        {@const color = colorForUsername(f.owner)}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="flex">
          <span class="material-symbols-outlined text-4xl">subdirectory_arrow_right</span>
          <div class="flex grow relative px-2 py-1 bg-gray-700 border-gray-600 rounded overflow-hidden text-xs">
            <div class="flex flex-col w-full">
              <div class="absolute left-0 top-0 bottom-0 z-0 opacity-60" style="width: {filePct(f)}%; background-color: {color};"></div>
              <div class="relative z-10 flex items-center justify-between gap-1">
                <div class="w-full overflow-hidden text-ellipsis whitespace-nowrap">{f.path}</div>
                <div class="flex items-center gap-4 text-sm font-semibold text-nowrap">{rightValueFile(f)}</div>
              </div>
              <div class="relative z-10 flex justify-end text-gray-300">
                Size: {formatBytes(f.size)} • Owner: {f.owner} • Modified: {f.modified}
              </div>
            </div>
          </div>
        </div>
      {/each}
    </div>
  {/if}
  <div class="grow"></div>
</div>

{#if tip.show}
  <div
    class="fixed z-50 pointer-events-none"
    style="
      left: {tip.x}px;
      top: {tip.y}px;
      transform: translate(-50%, calc(-100% - 10px));
    "
  >
    <div class="relative rounded-xl border border-white/10 bg-black/90 text-white shadow-xl px-3 py-2">
      <div class="flex items-center justify-between gap-3">
        <div class="font-medium text-sm truncate max-w-[180px]">{tip.username}</div>
        <div class="text-xs opacity-80">{tip.percent}%</div>
      </div>
      <div class="text-xs opacity-90 mt-1">{tip.value}</div>
      <div class="absolute left-1/2 top-full -translate-x-1/2 mt-[-4px]">
        <div class="w-2 h-2 rotate-45 bg-black/90 border border-white/10 border-l-0 border-t-0"></div>
      </div>
    </div>
  </div>
{/if}
