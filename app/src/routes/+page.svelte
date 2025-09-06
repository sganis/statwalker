<script lang="ts">

  import { invoke } from "@tauri-apps/api/core";
  import { onMount, untrack } from "svelte";

  let path = $state("c:\\Dev");
  let files = $state([]);
  let loading = $state(false);
  let maxSize = $derived(files?.length ? Math.max(...files?.map(i => i.size || 0)) : 1);
  const pct = (n: number) => Math.max(0, Math.min(100, Math.round((+n / maxSize) * 1000) / 10));

  // $effect(() => {
  //   if (path) {
  //     untrack(async () => {
  //       let f = JSON.parse(await getFiles(path))
  //       files = f.sort((a, b) => b.size - a.size);
  //     })
  //   }
  // })
  onMount(() => {
    //refresh();
  })

  async function getFiles(path: string) {
    let f: string = await invoke("get_files", { path });
    files = JSON.parse(f).sort((a, b) => b.size - a.size);
    console.log("files", files);
  }

  function normalizeSeps(p) {
    // Use platform separator detection if needed.
    // Here we normalize to backslashes on Windows, slashes otherwise.
    if (navigator.userAgent.includes("Windows")) {
      return p.replace(/\//g, "\\");
    } else {
      return p.replace(/\\/g, "/");
    }
  }

  function isRootDir(p) {
    if (navigator.userAgent.includes("Windows")) {
      // Drive root like C:\ or UNC root like \\server\share
      if (/^[a-zA-Z]:\\?$/.test(p)) return true;
      if (/^\\\\[^\\]+\\[^\\]+\\?$/.test(p)) return true;
      return false;
    } else {
      return p === "/";
    }
  }
  async function refresh() {
      await getFiles(path)
  }
  async function goToPath(p) {
    path = p;
    await refresh()
  }
  async function goUp() {
    let s = normalizeSeps(path.trim());

    // Accept "C:" as root
    if (navigator.userAgent.includes("Windows") && /^[a-zA-Z]:$/.test(s)) {
      s += "\\";
    }

    if (isRootDir(s)) {
      return s;
    }

    // Trim trailing separators
    s = s.replace(/[\\/]+$/, "");

    // Split by either slash or backslash
    const parts = s.split(/[\\/]/);

    if (parts.length <= 1) {
      return ".";
    }

    parts.pop();
    let parent = parts.join(navigator.userAgent.includes("Windows") ? "\\" : "/");

    // Ensure drive roots look like "C:\" not "C:"
    if (navigator.userAgent.includes("Windows") && /^[a-zA-Z]:$/.test(parent)) {
      parent += "\\";
    }

    if (!parent) return navigator.userAgent.includes("Windows") ? "C:\\" : "/";

    path = parent
    await refresh()
  }


</script>

<div class="flex flex-col h-screen min-h-0 gap-2">
  <div class="flex gap-2">
    <button onclick={goUp}>Up</button>
    <button onclick={refresh}>Refresh</button>
    <input bind:value={path} placeholder="Path..." class="grow" />
  </div>
  {#if loading}
    <p>loading...</p>
  {:else}
  <div class="flex flex-col gap-2 overflow-y-auto
    transition-opacity duration-200 pe-4" 
    >
    {#each files as file}
      <!-- svelte-ignore a11y_click_events_have_key_events -->
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <div class="relative p-3 cursor-pointer hover:opacity-95 bg-gray-800 
        border border-gray-600"
        style="
         --w: {pct(file.size)}%;
    --alpha: 0.9;
    /* paint only the filled part; the rest shows bg-black */
    background-image: linear-gradient(
      90deg,
      rgba(59,130,246,var(--alpha)),
      rgba(59,130,246,var(--alpha))
    );
    background-size: var(--w) 100%;
    background-repeat: no-repeat;
    background-position: 0 0;
    border-radius: 0.5rem;
        " 
        onclick={()=>goToPath(file.path)}>
        <div class="flex items-center justify-between gap-4">
        <p class="font-medium truncate">{file.path}</p>
        <span class="text-xs text-gray-500 tabular-nums">{pct(file.size)}%</span>
      </div>
      <p class="text-xs text-gray-600 mt-1">
        Files: {file.count} • Size: {file.size.toLocaleString()} bytes •
        Modified: {new Date(file.modified * 1000).toLocaleString()}
      </p>
      <!-- {#if file.users?.length}
        <p class="text-[11px] text-gray-500 mt-0.5">Users: {file.users.join(", ")}</p>
      {/if} -->
      </div>      
    {/each}
  </div>
{/if}
</div>