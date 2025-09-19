<script lang="ts">
  interface Props {
    wrapper: HTMLElement | undefined;
    isOpen: boolean;
    isDialog: boolean;
    children: import('svelte').Snippet;
  }
  let { wrapper = $bindable(), isOpen, isDialog, children }: Props = $props();
</script>

<div
  bind:this={wrapper}
  role={isDialog ? 'dialog' : undefined}
  aria-label="color picker"
  class={`w-max rounded border p-2  overflow-visible
          ${isOpen ? 'inline-block' : 'hidden'}
          ${isDialog ? 'absolute -mt-2.5 -ml-0.5 z-20' : 'relative'}
          bg-[var(--cp-bg-color)] border-[var(--cp-border-color,black)]`}
>
  {@render children()}
</div>
