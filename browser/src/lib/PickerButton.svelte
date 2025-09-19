<script lang="ts">
  interface Props {
		/** DOM element of the label wrapper */
		labelElement: HTMLLabelElement | undefined;
		/** hex color */
		hex: string | null;
		/** input label */
		label: string;
		/** input name, useful in a native form */
		name?: string | undefined;
		/** directionality left to right, or right to left*/
		dir: 'ltr' | 'rtl';
	}

	let { labelElement = $bindable(), hex, label, name = undefined, dir }: Props = $props();

</script>

<label
  bind:this={labelElement}
  aria-label={label}
  dir={dir}
  class="btn inline-flex items-center gap-1 cursor-pointer select-none"
  title={label}
>
  <span class="material-symbols-outlined">colors</span>
  <input type="hidden" {name} value={hex ?? ''} />
</label>
