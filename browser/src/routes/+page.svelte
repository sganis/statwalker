<script lang="ts">
  import { onMount } from "svelte";
  import { getParent, humanTime, humanCount, formatBytes, COLORS } from "../js/util";
  import { api } from "../js/api.svelte";
  import { API_URL } from "../js/store.svelte";
  import Svelecte, { addRenderer } from 'svelecte';
  
  //#region colors
  function colorRenderer(item, _isSelection, _inputValue) {
    const base = "width:16px;height:16px;border:1px solid white;border-radius:3px;flex:none;";
    const a = COLORS[0];
    const b = COLORS[1] ?? a;
    const c = COLORS[2] ?? b;
    const d = COLORS[3] ?? c;

    const bg =
      item.user === "All Users"
        ? `background: linear-gradient(90deg, ${a} 0%, ${b} 33%, ${c} 66%, ${d} 100%);`
        : `background: ${item.color};`;
      return `<div class="flex gap-2 items-center">
                <div class="border border-gray-400 rounded"
                  style="${base}${bg}"></div>
                <div>${item.user}</div>              
              </div>`
  }
  addRenderer('color', colorRenderer);

  // Seed colors for known users (optional)
  function seedUserColors(usernames: string[]) {
    usernames.forEach((uname, index) => {
      if (!userColors.has(uname)) {
        userColors.set(uname, COLORS[index % COLORS.length]);
      }
    })
    userDropdown = Array.from(userColors.entries()).map(([user, color]) => ({user,color}))
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
  //#endregion

  // ---------- NEW backend shape (folders → users → ages) ----------
  type AgeMini = {
    count: number;
    size: number;  // bytes
    disk: number;  // bytes
    mtime: number; // unix seconds
  };

  type RawFolder = {
    path: string;
    // username -> "0"/"1"/"2" -> AgeMini
    users: Record<string, Record<string, AgeMini>>;
  };

  // ---------- Types used by existing UI (we'll derive these) ----------
  type UserStatsJson = {
    username: string;
    count: number;
    size: number; // bytes
    disk: number; // bytes
    mtime: number;
  };

  type FileItem = {
    path: string;
    total_count: number;
    total_size: number; // bytes
    total_disk: number; // bytes
    modified: number;   // unix time
    users: Record<string, UserStatsJson>; // keyed by username (aggregated across ages)
  };

  // Scanned file from /api/files
  type ScannedFile = {
    path: string;
    size: number;      // bytes
    modified: number;  // unix
    owner: string;     // username
  }

  // ===== Age Filter =====
  type AgeFilter = 'all' | 0 | 1 | 2;
  let ageFilter = $state<AgeFilter>('all');
  let ageOpen = $state(false);
  const AGE_LABELS: Record<AgeFilter, string> = {
    all: "All Ages",
    0: "Recents",
    1: "Not too old",
    2: "Old files",
  }

  function displayAgeLabel(a: AgeFilter) { return AGE_LABELS[a]; }
  function chooseAge(a: AgeFilter) {
    ageFilter = a;
    ageOpen = false;
    refresh();
  }

  // ---------- derive legacy FileItem from RawFolder (apply ageFilter) ----------
  function transformFolders(raw: RawFolder[], filter: AgeFilter): FileItem[] {
    // Ages to include
    const ages: string[] = filter === 'all' ? ["0","1","2"] : [String(filter)];

    return (raw ?? [])
      .map((rf) => {
        const usersAgg: Record<string, UserStatsJson> = {};
        let total_count = 0;
        let total_size  = 0;
        let total_disk  = 0;
        let max_mtime   = 0;

        const userEntries = Object.entries(rf.users ?? {});
        for (const [uname, agesMap] of userEntries) {
          let u_count = 0, u_size = 0, u_disk = 0, u_mtime = 0;

          for (const a of ages) {
            const s = agesMap?.[a];
            if (!s) continue;
            // coerce defensively
            const c = Number(s.count ?? 0);
            const sz = Number(s.size ?? 0);
            const dk = Number(s.disk ?? 0);
            const mt = Number(s.mtime ?? 0);

            u_count += Number.isFinite(c) ? c : 0;
            u_size  += Number.isFinite(sz) ? sz : 0;
            u_disk  += Number.isFinite(dk) ? dk : 0;
            if (Number.isFinite(mt) && mt > u_mtime) u_mtime = mt;
          }

          if (u_count || u_size || u_disk) {
            usersAgg[uname] = {
              username: uname,
              count: u_count,
              size:  u_size,
              disk:  u_disk,
              mtime: u_mtime,
            };
            total_count += u_count;
            total_size  += u_size;
            total_disk  += u_disk;
            if (u_mtime > max_mtime) max_mtime = u_mtime;
          }
        }

        return {
          path: rf.path,
          total_count,
          total_size,
          total_disk,
          modified: max_mtime,
          users: usersAgg,
        };
      })
      // hide folders with no data after filter
      .filter(f => Object.keys(f.users).length > 0);
  }

  //#region state
  let folders = $state<FileItem[]>([]);
  let files = $state<ScannedFile[]>([]);
  let loading = $state(false);
  let initializing = $state(false);
  let progress_current = $state(0);
  let progress_total = $state(0);
  let progress_percent = $state(0);
  let history = $state<string[]>(['/']);
  let histIdx = $state(0);

  type SortKey = "disk" | "count";
  let sortBy = $state<SortKey>("disk");
  let sortOpen = $state(false);

  // selection is by username (not uid). Empty string = "All users"
  let selectedUser = $state<string>("All Users");

  // /api/users returns a simple string[]
  let users = $state<string[]>([]);
  let userColors = $state(new Map<string, string>()); // cache: username -> color
  let userDropdown = $state<{user:string;color:string}[]>([]);

  // Your Svelte 5 component
  let pathInput = $state();
  let path = $state('/');
  let fullPath = $state('');
  let isEditing = $state(false);
  //#endregion

  // Function to set path programmatically (use this instead of directly setting path)
  function setPath(newPath) {
    const displayedPath = displayPath(newPath);
    fullPath = displayedPath;

    if (!isEditing) {
      path = truncatePathFromStart(displayedPath);
    } else {
      path = displayedPath;
    }
  }

  // Function to truncate path from the beginning
  function truncatePathFromStart(inputPath, maxLength = 50) {
    if (!inputPath || inputPath.length <= maxLength) return inputPath;

    const parts = inputPath.split('/');
    let result = parts[parts.length - 1]; // Start with filename

    // Add directories from the end until we approach maxLength
    for (let i = parts.length - 2; i >= 0; i--) {
      const potential = parts[i] + '/' + result;
      if (('...' + potential).length > maxLength) break;
      result = potential;
    }

    return '...' + result;
  }

  function onPathFocus() {
    isEditing = true;
    if (fullPath) {
      path = fullPath;
    }
  }

  function onPathBlur() {
    isEditing = false;
    if (path && !path.startsWith('...')) {
      fullPath = path;
    }
    if (fullPath) {
      path = truncatePathFromStart(fullPath);
    }
  }

  // ---------- Helpers ----------
  function displayPath(p: string): string {
    if (!p) return "/";
    let s = p.replace(/\\/g, "/");
    if (s !== "/") s = s.replace(/\/+$/, "");
    if (!s.startsWith("/")) s = "/" + s;
    return s || "/";
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

  // === Tooltip clamping additions ===
  let bubbleEl: HTMLDivElement | null = $state(null);
  const MARGIN = 8;
  const ARROW_GAP = 10;

  function clampToViewport(rawX: number, rawY: number) {
    const ww = window.innerWidth;
    const wh = window.innerHeight;

    const w = bubbleEl?.offsetWidth ?? 200;
    const h = bubbleEl?.offsetHeight ?? 60;

    const halfW = w / 2;

    const minX = MARGIN + halfW;
    const maxX = ww - MARGIN - halfW;

    const minY = MARGIN + h + ARROW_GAP;
    const maxY = wh - MARGIN;

    return {
      x: Math.min(maxX, Math.max(minX, rawX)),
      y: Math.min(maxY, Math.max(minY, rawY)),
    };
  }

  function showTip(e: MouseEvent, userData: UserStatsJson, percent: number) {
    const { x, y } = clampToViewport(e.clientX, e.clientY);
    tip = {
      show: true,
      x,
      y,
      username: userData.username,
      value: rightValueForUser(userData),
      percent: Math.round(percent * 10) / 10,
    };
  }
  function moveTip(e: MouseEvent) {
    if (!tip.show) return;
    const { x, y } = clampToViewport(e.clientX, e.clientY);
    tip = { ...tip, x, y };
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
  const displaySortBy = (key: string) => {
    switch (key) {
      case "disk":
        return 'Disk Usage'
      case "count":
        return 'Total Files'
      default:
        return 'Disk Usage'
    }
  }
  const metricValue = (file: FileItem) => {
    switch (sortBy) {
      case "disk":
        return toNum(file?.total_disk);
      case "count":
        return toNum(file?.total_count);
    }
  }

  // Right label (folders) – sizes already in BYTES
  function rightValue(file: FileItem) {
    switch (sortBy) {
      case "disk":
        return formatBytes(toNum(file?.total_disk));
      case "count":
        return toNum(file?.total_count).toLocaleString();
    }
  }

  // Per-user right label – sizes already in BYTES
  function rightValueForUser(userData: UserStatsJson) {
    switch (sortBy) {
      case "disk":
        return formatBytes(toNum(userData?.disk));
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
    let modified = 0; 
    const aggUsers: Record<string, UserStatsJson> = {};

    for (const f of foldersArr ?? []) {
      total_count += toNum(f?.total_count);
      total_size  += toNum(f?.total_size);
      total_disk  += toNum(f?.total_disk);
      if (f.modified > modified) 
        modified = f.modified;
      
      const u = f?.users ?? {};
      for (const [uname, data] of Object.entries(u)) {
        const d = data as UserStatsJson;
        const prev = aggUsers[uname] ?? { username: uname, count: 0, size: 0, disk: 0, mtime: 0 };
        aggUsers[uname] = {
          username: uname,
          count: prev.count + toNum(d.count),
          size:  prev.size  + toNum(d.size),
          disk:  prev.disk  + toNum(d.disk),
          mtime: Math.max(prev.mtime, toNum(d.mtime)),
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
      const userFilter: string[] = selectedUser === 'All Users' ? [] : [selectedUser];

      // Pass age filter to both endpoints
      const raw: RawFolder[] = await api.getFolders(_p, userFilter, ageFilter);
      folders = transformFolders(raw, ageFilter);

      files = await api.getFiles(_p, userFilter, ageFilter);
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
  function navigateTo(p) {
    setPath(p);
    pushHistory(fullPath || path);
    fetchFolders(fullPath || path);
  }
  function refresh() {
    fetchFolders(fullPath || path);
  }
  function goHome() {
    navigateTo('/');
  }
  function goUp() {
    const parent = getParent(fullPath || path);
    navigateTo(parent);
  }
  function goBack() {
    if (histIdx > 0) {
      histIdx -= 1;
      setPath(history[histIdx]);
      fetchFolders(history[histIdx]);
    }
  }
  function goForward() {
    if (histIdx < history.length - 1) {
      histIdx += 1;
      setPath(history[histIdx]);
      fetchFolders(history[histIdx]);
    }
  }
  function onPathKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      navigateTo(fullPath || path);
    }
  }
  function userChanged() {
    if (!selectedUser)
      selectedUser = 'All Users'
    refresh()
  }

  onMount(async () => {
    console.log("api url:", API_URL);
    users = await api.getUsers(); // array of usernames
    users.splice(0,0,"All Users")
    selectedUser = 'All Users'
    seedUserColors(users);

    // Initialize the path truncation
    fullPath = path;
    path = truncatePathFromStart(path);

    await refresh();
  });
</script>

<div class="flex flex-col h-screen min-h-0 gap-2 p-2">
  <div class="flex gap-2 items-center relative">
    <button class="btn" onclick={goHome} title="Go to Root Folder" disabled={histIdx === 0}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">home</span>
      </div>
    </button>
    <button class="btn" onclick={goBack} title="Go Back" disabled={histIdx === 0}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_back_ios</span>
      </div>
    </button>
    <button class="btn" onclick={goForward} title="Go Forward" disabled={histIdx >= history.length - 1}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_forward_ios</span>
      </div>
    </button>
    <button class="btn" onclick={goUp} title="Go Up" disabled={getParent(path) === path}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_upward</span>
      </div>
    </button>
    <!-- <button class="btn"  onclick={refresh}>
      <div class="flex items-center">
        <span class="material-symbols-outlined">refresh</span>
      </div>
    </button> -->

    <!-- Sort dropdown -->
    <div class="relative">
      <button class="btn w-36" onclick={() => (sortOpen = !sortOpen)}>
        <div class="flex items-center gap-2">
          <span class="material-symbols-outlined">sort</span>
          {displaySortBy(sortBy)}
        </div>
      </button>
      {#if sortOpen}
        <div
          class="flex flex-col divide-y divide-gray-500 absolute w-36 rounded border
           border-gray-500 bg-gray-800 shadow-lg z-20 overflow-hidden mt-0.5"
        >
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700 text-nowrap" onclick={() => chooseSort("disk")}>
            By Disk Usage
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseSort("count")}>
            By Total Files
          </button>
        </div>
      {/if}
    </div>

    <!-- NEW: Age filter dropdown -->
    <div class="relative">
      <button class="btn w-36" onclick={() => (ageOpen = !ageOpen)}>
        <div class="flex items-center gap-2">
          <span class="material-symbols-outlined">schedule</span>
          {displayAgeLabel(ageFilter)}
        </div>
      </button>
      {#if ageOpen}
        <div
          class="flex flex-col divide-y divide-gray-500 absolute w-48 rounded border
           border-gray-500 bg-gray-800 shadow-lg z-20 overflow-hidden mt-0.5"
        >
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseAge('all')}>
            All Ages
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseAge(0)}>
            Recents (2 months)
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseAge(1)}>
            Not too old (2 years)
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseAge(2)}>
            Old files
          </button>
        </div>
      {/if}
    </div>

    <Svelecte
      bind:value={selectedUser} 
      options={userDropdown}
      valueField="user" 
      renderer="color"
      on:change={userChanged}
      class="z-20 min-w-40 h-10 border rounded border-gray-600 bg-gray-800 text-white"
    />
  </div>
  <div class="flex">
    <input 
    bind:this={pathInput}
    bind:value={path} placeholder="Path..." 
    class="w-full truncate text-left"
    onkeydown={onPathKeydown} 
    onblur={onPathBlur}
    onfocus={onPathFocus}
    disabled={loading} />
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
            {humanCount(pathTotals.total_count)} Files 
            • Changed {humanTime(pathTotals.modified)} 
            • {formatBytes(pathTotals.total_disk)}
            {#if formatBytes(pathTotals.total_size) !== formatBytes(pathTotals.total_disk)}
              ({formatBytes(pathTotals.total_size)} on size)
            {/if}          
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
              {@const totalMetric =
                sortBy === "disk" ? file.total_disk : sortBy === "size" ? file.total_size : file.total_count}
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
                {humanCount(file.total_count)} Files 
                • Changed {humanTime(file.modified)} 
                • {formatBytes(file.total_disk)} 
                {#if formatBytes(file.total_size) !== formatBytes(file.total_disk)}
                  ({formatBytes(file.total_size)} on size)
                {/if}          
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
          <div class="flex grow relative px-2 py-1 bg-gray-700 border border-gray-600 rounded overflow-hidden text-xs">
            <div class="flex flex-col w-full">
              <div class="absolute left-0 top-0 bottom-0 z-0 opacity-60" style="width: {filePct(f)}%; background-color: {color};"></div>
              <div class="relative z-10 flex items-center justify-between gap-1">
                <div class="w-full overflow-hidden text-ellipsis whitespace-nowrap">{f.path}</div>
                <div class="flex items-center gap-4 text-sm font-semibold text-nowrap">{rightValueFile(f)}</div>
              </div>
              <div class="relative z-10 flex justify-between text-gray-300">
                <div class="">{f.owner}</div>
                <div class="">Changed {humanTime(f.modified)}</div>
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
    <div
      bind:this={bubbleEl}
      class="relative rounded-xl border border-white/10 bg-black/90 text-white shadow-xl px-3 py-2"
    >
      <div class="flex items-center justify-center">
        <div class="font-medium text-sm truncate max-w-[180px]">{tip.username}</div>        
      </div>
      <div class="flex gap-2 items-center justify-between text-xs opacity-90">
        <div class="text-nowrap">{tip.value}</div>
        <div class="">{tip.percent}%</div>
      </div>
      <div class="absolute left-1/2 top-full -translate-x-1/2 mt-[-4px]">
        <div class="w-2 h-2 rotate-45 bg-black/90 border border-white/10 border-l-0 border-t-0"></div>
      </div>
    </div>
  </div>
{/if}
