<script>
  let { children } = $props();
  import "../app.css";
  import {State} from "../ts/store.svelte"
</script>

<div class="flex flex-col h-screen min-h-0 overflow-hidden">
  <div class="flex items-center justify-between p-4 text-xl  border-b border-gray-500 text-gray-200 select-none">
    <div class="">
      Statwalker
    </div>
    <div class="grow"></div>
    <div class="text-sm">
      {State.message}
    </div>
  </div>
  <div class="flex flex-col h-full overflow-auto p-2 bg-[var(--color)]">
    {@render children()}
  </div>
    
</div>
