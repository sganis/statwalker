<script lang="ts">
  import { onMount } from "svelte";
  import { SvelteMap } from 'svelte/reactivity';
  import { 
    getParent, humanTime, humanCount, 
    formatBytes, getOptimalColors, COLORS,
  } from "../ts/util";
  import { api } from "../ts/api.svelte";
  import { API_URL, State } from "../ts/store.svelte";
  import Svelecte, { addRenderer } from 'svelecte';
  import ColorPicker from 'svelte-awesome-color-picker';
  import PickerButton from '../lib/PickerButton.svelte';
  import PickerWrapper from '../lib/PickerWrapper.svelte';

  //#region state
  let allColors: string[] = []
  let showColorPicker = false;
  let path = $state('/');
  let fullPath = $state('/');
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
  let selectedUser = $state("All Users");
  let selectedUserColor = $state('')
  let users = $state<string[]>([]);
  let userColors = $state(new SvelteMap<string, string>()); // cache: username -> color
  let userDropdown = $state<{user:string;color:string}[]>([]);
  let pathInput = $state();
  let isEditing = $state(false);

  //#endregion

  //#region colors

  function colorRenderer(item, _isSelection, _inputValue) {
    const icon_base = "width:16px;height:16px;border:1px solid white;border-radius:3px;flex:none;";
    const a = COLORS[0];
    const b = COLORS[1] ?? a;
    const c = COLORS[2] ?? b;
    const d = COLORS[3] ?? c;

    const icon_bg =
      item.user === "All Users"
        ? `linear-gradient(90deg, ${a} 0%, ${b} 33%, ${c} 66%, ${d} 100%)`
        : _isSelection ? selectedUserColor : item.color;
    const user_css = !State.isAdmin ? "text-gray-400" : ''
    return `<div class="flex gap-2 items-center">
                <div class="border border-gray-500 rounded"
                    style="${icon_base} background: ${icon_bg};">
                </div>
                <div class="${user_css}">${item.user}</div>              
            </div>`    
  }
  addRenderer('color', colorRenderer);

  // Seed colors for known users (optional)
  function createUserDropdown(usernames: string[]) {
    usernames.forEach((uname, index) => {
      if (!userColors.has(uname)) {
        userColors.set(uname, allColors[index % allColors.length]);
      }
    })
    userColors.set('All Users', '')
    userDropdown = Array.from(userColors.entries()).map(([user, color]) => ({user,color}))
  }

  // Deterministic color for any username (stable + cached)
  // function colorForUsername(uname: string): string {
  //   const cached = userColors.get(uname);
  //   if (cached) return cached;
  //   let h = 0;
  //   for (let i = 0; i < uname.length; i++) {
  //     h = (h * 31 + uname.charCodeAt(i)) >>> 0;
  //   }
  //   const color = allColors[h % allColors.length];
  //   userColors.set(uname, color);
  //   return color;
  // }
  //#endregion

  //#region types
  type Age = {
    count: number;
    disk: number;  // bytes
    atime: number; // unix seconds
    mtime: number; // unix seconds
  }
  type RawFolder = {
    path: string;
    // username -> "0"/"1"/"2" -> Age
    users: Record<string, Record<string, Age>>;
  }
  type UserStatsJson = {
    username: string;
    count: number;
    disk: number; // bytes
    atime: number;
    mtime: number;
  }
  type FileItem = {
    path: string;
    total_count: number;
    total_disk: number; // bytes
    accessed: number;   // unix time
    modified: number;   // unix time
    users: Record<string, UserStatsJson>; // keyed by username (aggregated across ages)
  }
  // Scanned file from /api/files
  type ScannedFile = {
    path: string;
    size: number;      // bytes
    accessed: number;  // unix
    modified: number;  // unix
    owner: string;     // username
  }
  //#endregion

  //#region age filter
  type AgeFilter = -1 | 0 | 1 | 2;
  let ageFilter = $state<AgeFilter>(-1);
  let ageOpen = $state(false);
  const AGE_LABELS: Record<AgeFilter, string> = {
    '-1': "Any Time",
    0: "Recent",
    1: "Not too old",
    2: "Old files",
  }

  function displayAgeLabel(a: AgeFilter) { 
    return AGE_LABELS[a]; 
  }

  function chooseAge(a: AgeFilter) {
    ageFilter = a;
    ageOpen = false;
    refresh();
  }
  //#endregion

  //#region tooltip
  type Tip = {
    show: boolean;
    x: number;
    y: number;
    username?: string;
    value?: string;
    percent?: number;
  }
  let tip = $state<Tip>({ show: false, x: 0, y: 0 });

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
    cancelHide(); // keep it open while interacting
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
    cancelHide(); // moving over the target keeps it alive
    const { x, y } = clampToViewport(e.clientX, e.clientY);
    tip = { ...tip, x, y };
  }
  function hideTip() {
    cancelHide();
    tip = { show: false, x: 0, y: 0 };
  }

  const HIDE_DELAY = 1200; // ms — tweak to taste
  let hideTimer: number | null = $state(null);

  function scheduleHide(ms = HIDE_DELAY) {
    if (hideTimer) clearTimeout(hideTimer);
    hideTimer = window.setTimeout(() => {
      tip = { show: false, x: 0, y: 0 };
      hideTimer = null;
    }, ms);
  }

  function cancelHide() {
    if (hideTimer) {
      clearTimeout(hideTimer);
      hideTimer = null;
    }
  }
  //#endregion

  //#region folders and files bars

  function transformFolders(raw: RawFolder[], filter: AgeFilter): FileItem[] {
    // Ages to include
    const ages: string[] = filter === -1 ? ["0","1","2"] : [String(filter)];

    return (raw ?? [])
      .map((rf) => {
        const usersAgg: Record<string, UserStatsJson> = {};
        let total_count = 0;
        let total_disk  = 0;
        let max_atime   = 0;
        let max_mtime   = 0;

        const userEntries = Object.entries(rf.users ?? {});
        for (const [uname, agesMap] of userEntries) {
          let u_count = 0, u_disk = 0, u_atime = 0, u_mtime = 0;

          for (const a of ages) {
            const s = agesMap?.[a];
            if (!s) 
              continue;
            // coerce defensively
            const c = Number(s.count ?? 0);
            const dk = Number(s.disk ?? 0);
            const at = Number(s.mtime ?? 0);
            const mt = Number(s.mtime ?? 0);

            u_count += Number.isFinite(c) ? c : 0;
            u_disk  += Number.isFinite(dk) ? dk : 0;
            if (Number.isFinite(at) && at > u_atime) 
              u_atime = at;
            if (Number.isFinite(mt) && mt > u_mtime) 
              u_mtime = mt;
          }

          if (u_count || u_disk) {
            usersAgg[uname] = {
              username: uname,
              count: u_count,
              disk:  u_disk,
              atime: u_atime,
              mtime: u_mtime,
            };
            total_count += u_count;
            total_disk  += u_disk;
            if (u_atime > max_atime) 
              max_atime = u_atime;
            if (u_mtime > max_mtime) 
              max_mtime = u_mtime;
          }
        }

        return {
          path: rf.path,
          total_count,
          total_disk,
          accessed: max_atime,
          modified: max_mtime,
          users: usersAgg,
        };
      })
      // hide folders with no data after filter
      .filter(f => Object.keys(f.users).length > 0);
  }
  const toNum = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }
  // ---------- Sorting (Folders) ----------
  const sortedFolders = $derived.by(() => {
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
  })
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
  })
  const pct = (n: any) => {
    const x = toNum(n);
    const p = (x / maxMetric) * 100;
    const clamped = Math.max(0, Math.min(100, p));
    return Math.round(clamped * 10) / 10;
  }
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
  function clickOutside(
    node: HTMLElement,
    cb: (() => void) | { close: () => void }
  ) {
    let close: (() => void) | undefined =
      typeof cb === "function" ? cb : cb?.close;

    const onPointerDown = (e: PointerEvent) => {
      if (!node.contains(e.target as Node)) close?.();
    };

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" || e.key === "Esc") close?.();
    };

    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown, true);

    return {
      update(next: typeof cb) {
        close = typeof next === "function" ? next : next?.close;
      },
      destroy() {
        document.removeEventListener("pointerdown", onPointerDown, true);
        document.removeEventListener("keydown", onKeyDown, true);
      },
    };
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
  const userMetricFor = (ud: UserStatsJson) => sortBy === "disk" ? Number(ud.disk) : Number(ud.count);
  function sortedUserEntries(file: FileItem) {
    return Object.entries(file?.users ?? {}).sort(([, a], [, b]) => userMetricFor(a) - userMetricFor(b));
  }

  function aggregatePathTotals(foldersArr: FileItem[], filesArr: ScannedFile[], p: string): FileItem {
    let total_count = 0;
    let total_disk = 0;
    let accessed = 0; 
    let modified = 0; 
    const aggUsers: Record<string, UserStatsJson> = {};

    // Aggregate folders (existing logic)
    for (const f of foldersArr ?? []) {
      total_count += toNum(f?.total_count);
      total_disk  += toNum(f?.total_disk);
      if (f.accessed > accessed) 
        accessed = f.accessed;
      if (f.modified > modified) 
        modified = f.modified;
      
      const u = f?.users ?? {};
      for (const [uname, data] of Object.entries(u)) {
        const d = data as UserStatsJson;
        const prev = aggUsers[uname] ?? { 
          username: uname, count: 0, disk: 0, atime: 0, mtime: 0 };
        aggUsers[uname] = {
          username: uname,
          count: prev.count + toNum(d.count),
          disk:  prev.disk  + toNum(d.disk),
          atime: Math.max(prev.atime, toNum(d.atime)),
          mtime: Math.max(prev.mtime, toNum(d.mtime)),
        };
      }
    }

    // Aggregate files (new logic)
    for (const file of filesArr ?? []) {
      total_count += 1;  // Each file counts as 1
      total_disk += toNum(file?.size);
      if (file.accessed > accessed) 
        accessed = file.accessed;
      if (file.modified > modified) 
        modified = file.modified;
      
      // Aggregate user stats for this file
      const owner = file.owner;
      if (owner) {
        const prev = aggUsers[owner] ?? { 
          username: owner, count: 0, disk: 0, atime: 0, mtime: 0 };
        aggUsers[owner] = {
          username: owner,
          count: prev.count + 1,
          disk: prev.disk + toNum(file.size),
          atime: Math.max(prev.atime, toNum(file.accessed)),
          mtime: Math.max(prev.mtime, toNum(file.modified)),
        };
      }
    }

    return {
      path: p,
      total_count,
      total_disk,
      accessed,
      modified,
      users: aggUsers,
    };
  }

  const pathTotals = $derived.by(() => aggregatePathTotals(folders, files, path));
  // ---------- Sorting (Files) ----------
  function fileMetricValue(f: ScannedFile) {
    switch (sortBy) {
      case "disk":
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
    // Use the parent folder's total as the maximum for file bars
    const parentTotal = sortBy === "disk" ? pathTotals.total_disk : pathTotals.total_count;
    return parentTotal > 0 ? parentTotal : 1;
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
  //#endregion

  //#region fetch data
  // single-flight fetch
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
    } finally {
      loading = false;
    }
  })
  //#endregion

  //#region menus

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
  function onUserChanged() {
    console.log('selected user:',selectedUser)
    selectedUserColor = userColors.get(selectedUser) ?? '#000000'
    // if (!selectedUser || selectedUser===null) {
    //   selectedUser = 'All Users'
    // }
    refresh()
  }

  //#endregion

  //#region path

	let copyFeedbackVisible = $state(false);

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
  function displayPath(p: string): string {
    if (!p) return "/";
    let s = p.replace(/\\/g, "/");
    if (s !== "/") s = s.replace(/\/+$/, "");
    if (!s.startsWith("/")) s = "/" + s;
    return s || "/";
  }
  function onPathKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      navigateTo(fullPath || path);
    }
  }

  // Function to copy text from input/textarea
	async function copyText(e) {
    let element = e.currentTarget
    try {
      const textToCopy = element.value !== undefined 
      ? element.value 
      : (element.textContent || element.innerText || '')
      await navigator.clipboard.writeText(textToCopy)
      showCopyFeedback()
    } catch (err) {
      console.error('Copy failed:', err);
    }
	}

	// Show copy feedback notification
	function showCopyFeedback() {
		copyFeedbackVisible = true;
		setTimeout(() => {
			copyFeedbackVisible = false;
		}, 2000);
	}

  //#endregion

  onMount(async () => {
    console.log("api url:", API_URL)
    users = await api.getUsers()
    console.log("Users:", $state.snapshot(users))
    allColors = getOptimalColors(users.length)
    console.log("Colors:", allColors)
    createUserDropdown(users);    
    
    if (State.isAdmin) {
      selectedUser = 'All Users'
    } else {
      selectedUser = State.username
    }
    
    fullPath = path;
    path = truncatePathFromStart(path);
    refresh();
  })
  
</script>

<div class="flex flex-col h-screen min-h-0 gap-2 p-2">
  <div class="flex gap-2 items-center relative select-none">
    <button class="btn" onclick={goHome} title="Go to Root Folder" disabled={histIdx === 0 || fullPath === '/'}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">home</span>
      </div>
    </button>
    <button class="btn" onclick={goBack} title="Go Back" disabled={histIdx === 0}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_back</span>
      </div>
    </button>
    <button class="btn" onclick={goForward} title="Go Forward" disabled={histIdx >= history.length - 1}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_forward</span>
      </div>
    </button>
    <button class="btn" onclick={goUp} title="Go Up" disabled={getParent(path) === path}>
      <div class="flex items-center">
      <span class="material-symbols-outlined">arrow_upward</span>
      </div>
    </button>

    <!-- Sort dropdown -->
    <div class="relative" use:clickOutside={() => (sortOpen = false)}>
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

    <!-- Age filter dropdown -->
    <div class="relative"  use:clickOutside={() => (ageOpen = false)}>
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
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseAge(-1)}>
            Any Time
          </button>
          <button class="w-full text-left px-3 py-2 hover:bg-gray-700" onclick={() => chooseAge(0)}>
            Recent (2 months)
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
      disabled={!State.isAdmin}
      bind:value={selectedUser} 
      options={userDropdown}
      name="user-select"
      valueField="user"
      renderer="color"
      highlightFirstItem={false}
      onChange={onUserChanged}
      closeAfterSelect={true}
      deselectMode="native"
      virtualList={true}
      class="z-20 min-w-40 h-10 border rounded
       border-gray-500 bg-gray-800 text-white"
    />
    {#if !selectedUser || selectedUser === 'All Users'}
      <button class="btn" disabled={true}>
        <div class="flex items-center">
          <span class="material-symbols-outlined">colors</span>
        </div>
      </button>
    {:else}
      <ColorPicker 
        bind:hex={selectedUserColor} 
        components={{ input: PickerButton,  wrapper: PickerWrapper }}
        label="Change User Color"
        onInput={(e)=>{
          userColors.set(selectedUser, selectedUserColor)
          userDropdown = Array.from(userColors.entries()).map(([user, color]) => ({user,color}))
        }}
      />    
    {/if}
  </div>
  <div class="flex">
    <input 
      bind:this={pathInput}
      bind:value={path} placeholder="Path..." 
      class="w-full truncate text-left cursor-pointer"
      onkeydown={onPathKeydown} 
      onblur={onPathBlur}
      onfocus={onPathFocus}
      onclick={(e)=>copyText(e)}
      autocorrect="off" 
      spellcheck="false"
      autocomplete="off"
      autocapitalize="none"
      disabled={loading} />
  </div>
  
  <!-- Path total header item -->
  <div class="">
    <div class="relative px-2 bg-gray-700 border border-gray-500 rounded overflow-hidden">
      <!-- Total bar background -->
      <div class="absolute left-0 top-0 bottom-0 flex z-0" style="width: 100%">
        {#each sortedUserEntries(pathTotals) as [uname, userData] (uname)}
          {@const userMetric = sortBy === "disk" ? userData.disk : userData.count}
          {@const totalMetric =
            sortBy === "disk" ? pathTotals.total_disk :  pathTotals.total_count}
          {@const userPercent = totalMetric > 0 ? (userMetric / totalMetric) * 100 : 0}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <div
            class="h-full transition-all duration-300 min-w-[0.5px] hover:opacity-90"
            style="width: {userPercent}%; background-color: {userColors.get(uname)};"
            onmouseenter={(e) => showTip(e, userData, userPercent)}
            onmousemove={moveTip}
            onmouseleave={hideTip}
            aria-label={`${userData.username}: ${rightValueForUser(userData)}`}
          ></div>
        {/each}
      </div>
      <!-- Total bar foreground -->
      <div class="relative z-10 p-1 pointer-events-none">        
        <!-- <div class="" ondblclick={copyPath} role="textbox">{path}</div> -->
        <div class="flex items-center justify-end">
          <p class="">
            {humanCount(pathTotals.total_count)} Files 
            • Changed {humanTime(pathTotals.modified)} 
            • {formatBytes(pathTotals.total_disk)}                  
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
        <div class="relative p-3 bg-gray-800 border border-gray-500 rounded-lg animate-pulse min-h-16 h-16">
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
      {#each sortedFolders as folder}
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="relative px-2 py-1 cursor-pointer hover:opacity-95 bg-gray-700 border border-gray-500 rounded-lg overflow-hidden min-h-16"
          onclick={() => navigateTo(folder.path)}
        >
          <!-- Folder bar background -->
          <div class="absolute left-0 top-0 bottom-0 flex z-0" style="width: {pct(metricValue(folder))}%">
            {#each sortedUserEntries(folder) as [uname, userData]}
              {@const userMetric = sortBy === "disk" ? userData.disk : userData.count}
              {@const totalMetric =
                sortBy === "disk" ? folder.total_disk :folder.total_count}
              {@const userPercent = totalMetric > 0 ? (userMetric / totalMetric) * 100 : 0}
              <div
                class="h-full transition-all duration-300 min-w-[0.5px] hover:opacity-90"
                style="width: {userPercent}%; background-color: {userColors.get(uname)};"
                onmouseenter={(e) => showTip(e, userData, userPercent)}
                onmousemove={moveTip}
                onmouseleave={hideTip}
                aria-label={`${userData.username}: ${rightValueForUser(userData)}`}
              ></div>
            {/each}
          </div>
          <!-- Folder bar foreground -->
          <div class="relative flex flex-col gap-2 z-10 pointer-events-none">
            <div class="flex items-center justify-between gap-4">
              <div class="w-full overflow-hidden text-ellipsis whitespace-nowrap">
                <div>{folder.path}</div>
              </div>
              <span class="text-nowrap font-bold">{rightValue(folder)}</span>
            </div>
            <div class="flex justify-end">
              <p class="text-sm">
                {humanCount(folder.total_count)} Files 
                • Updated {humanTime(folder.modified)} 
                {#if humanTime(folder.accessed) > humanTime(folder.modified)}
                • Last file read {humanTime(folder.accessed)} 
                {/if}                      
              </p>
            </div>
          </div>
        </div>
      {/each}

      <!-- Files (after folders) -->
      {#each sortedfiles as f}
        {@const color = userColors.get(f.owner)}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="flex">
          <span class="material-symbols-outlined text-4xl">subdirectory_arrow_right</span>
          <div class="relative flex grow px-2 py-1 bg-gray-700 border border-gray-500 rounded overflow-hidden text-xs">
            <div class="flex flex-col w-full">
              <!-- File bar background -->
              <div class="absolute left-0 top-0 bottom-0 z-0 opacity-60" 
                style="width: {filePct(f)}%; background-color: {color};">
              </div>
              <div class="relative z-10 flex items-center justify-between gap-2">
                <div class="w-full overflow-hidden">
                  <!-- svelte-ignore a11y_click_events_have_key_events -->
                  <span class="cursor-pointer text-ellipsis text-nowrap"
                    onclick={(e)=>copyText(e)}>
                    {f.path}
                  </span>                  
                </div>
                <div class="flex items-center gap-4 text-sm font-semibold text-nowrap">
                  {rightValueFile(f)}
                </div>
              </div>
              <div class="relative z-10 flex justify-between">
                <div class="">{f.owner}</div>
                <div class="">
                  Updated {humanTime(f.modified)} • Read {humanTime(f.accessed)}
                </div>
              </div>
            </div>
          </div>
        </div>
      {/each}
    </div>
  {/if}
  <div class="grow"></div>
</div>

<!-- Tooltip -->
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

<!-- Feedback notification -->
{#if copyFeedbackVisible}
  <div 
    class="fixed top-1 inset-x-0 mx-auto w-max bg-emerald-600 text-white px-4 py-1 
      rounded-lg font-medium shadow-lg z-50 transform transition-transform duration-300 
      ease-[cubic-bezier(0.68,-0.55,0.265,1.55)]"
    class:translate-x-full={!copyFeedbackVisible}
    class:translate-x-0={copyFeedbackVisible}
  >
    Path copied!
  </div>
{/if}