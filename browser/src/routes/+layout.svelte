<script>
  // keep your imports
  import "../app.css";
  import logo from '../assets/disk_usage.svg';
  import Login from '../lib/Login.svelte';
  import { State, initialState} from '../ts/store.svelte';

  let { children } = $props()
  let authed = $derived(Boolean(State.token) && (!State.expiresAt || Date.now() < State.expiresAt))

  // keep authed in sync with State (when Login writes State, this flips)
  $effect(() => {
    authed = Boolean(State.token) && (!State.expiresAt || Date.now() < State.expiresAt);
  });

  function logout(silent = false) {
    State.logout()
    localStorage.removeItem('state');
    // optional: redirect to login (hash-based in your app)
    if (!silent) location.href = '/#/login';
  }

  function onLogout() {
    logout();
    authed = false;
  }
</script>

<div class="flex flex-col h-screen min-h-0 overflow-hidden">
  <div class="flex items-center justify-between p-4 text-xl border-b border-gray-500 text-gray-200 select-none">
    <div class="flex gap-2 items-center">
      <div><img src={logo} width={28} alt="logo" /></div>
      <div>Statwalker 2.0</div>
    </div>

    <div class="grow"></div>

    <!-- NEW: small auth status / logout -->
    {#if authed}
      <div class="flex items-center gap-3 text-sm">
        <span class="opacity-80">{State.username}</span>
        {#if State.isAdmin}
          <span class="px-2 py-1 rounded bg-emerald-600 text-white">Admin</span>
        {/if}
        <button class="px-3 py-1 rounded bg-gray-700 hover:bg-gray-600" 
          onclick={onLogout}>
          Logout
        </button>
      </div>
    {/if}
  </div>

  <div class="flex flex-col h-full overflow-hidden p-2 bg-[var(--color)]">
    {#if authed}
      {@render children()}
    {:else}
      <Login />
    {/if}
  </div>
</div>
