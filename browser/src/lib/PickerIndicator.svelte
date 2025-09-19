<script lang="ts">
  // API from the docs
  export let pos: { x: number; y: number };
</script>

<div
  class="pointer-events-none absolute z-30 -translate-x-1/2 -translate-y-1/2
         w-3 h-3 rounded-full bg-white ring-2 ring-black/80
         shadow-[0_0_0_2px_#fff] mix-blend-difference"
  style="left:{pos.x}%; top:{pos.y}%;"
></div>