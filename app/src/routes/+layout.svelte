<script>
  let { children } = $props();
  import "../app.css";
</script>

<div class="flex flex-col h-screen min-h-0 overflow-hidden">
  <div class="p-4 text-xl font-bold">Statwalker</div>
  <div class="flex flex-col h-full overflow-auto p-2 bg-[var(--color)]">
    {@render children()}
  </div>
    
</div>
