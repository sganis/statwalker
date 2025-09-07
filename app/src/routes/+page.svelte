<script lang="ts">

  import { invoke } from "@tauri-apps/api/core";
  import { onMount, untrack } from "svelte";
  import { getParent, formatBytes } from "../js/util";
  //let path = $state("c:\\Dev");
  let path = $state("/");
  let files = $state([]);
  let loading = $state(false);
  let maxSize = $derived(files?.length ? Math.max(...files?.map(i => i.disk || 0)) : 1);
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
    refresh();
  })

  async function getFiles(path: string) {
    let f: string = await invoke("get_files", { path });
    files = JSON.parse(f).sort((a, b) => b.disk - a.disk);
    console.log("files", files);
  }

  async function refresh() {
    loading = true;
    await getFiles(path)
    loading = false;
  }
  async function goToPath(p) {
    path = p;
    await refresh()
  }


// Refactored goUp function using getParent
async function goUp() {
  const parent = getParent(path);
  path = parent;
  await refresh();
}

</script>

<div class="flex flex-col h-screen min-h-0 gap-2">
  <div class="flex gap-2">
    <button onclick={goUp} disabled={loading}>Up</button>
    <button onclick={refresh}>Refresh</button>
    <input bind:value={path} placeholder="Path..." class="grow" disabled={loading} />
  </div>
  {#if loading}
    <!-- Skeleton Loader -->
    <div class="flex flex-col gap-2 overflow-y-auto p-4">
      {#each Array(6) as _, i}
        <div class="relative p-3 bg-gray-800 border border-gray-600 rounded-lg 
          animate-pulse min-h-16 h-16" >
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
    {#each files as file}
      <!-- svelte-ignore a11y_click_events_have_key_events -->
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <div class="relative p-3 cursor-pointer hover:opacity-95
       bg-gray-700 border border-gray-600 rounded-lg overflow-hidden min-h-16 h-16"
        onclick={()=>goToPath(file.path)} disalbed={loading}>
        <!-- Orange progress bar background -->
        <div class="absolute inset-0 bg-orange-500/90 transition-all duration-300"
             style="width: {pct(file.disk)}%; z-index: 0;"></div>
        
        <!-- Content overlay -->
        <div class="relative z-10">
          <div class="flex items-center justify-between gap-4">
            <p class="font-medium truncate text-white">
              {file.path}
            </p>
            <span class="text-xs text-gray-300 tabular-nums">
              {formatBytes(file.disk)}
            </span>
          </div>
          <p class="text-xs text-gray-300 mt-1">
            Files: {file.count} • Size: {file.disk.toLocaleString()} bytes •
            Modified: {new Date(file.modified * 1000).toLocaleString()}
          </p>
        </div>
      </div>      
    {/each}
  </div>
{/if}
</div>