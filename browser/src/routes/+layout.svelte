<script>
  let { children } = $props();
  import "../app.css";
  import logo from '../assets/disk_usage.svg'
</script>

<div class="flex flex-col h-screen min-h-0 overflow-hidden">
  <div class="flex items-center justify-between p-4 text-xl  border-b border-gray-500 text-gray-200 select-none">
    <div class="flex gap-2 items-center">
      <div><img src={logo} width=28 alt="logo"></div>
      <div class="">Statwalker 2.0</div>      
    </div>
    <div class="grow"></div>
  </div>
  <div class="flex flex-col h-full overflow-auto p-2 bg-[var(--color)]">
    {@render children()}
  </div>
    
</div>
