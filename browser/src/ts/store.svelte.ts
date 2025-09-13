export const API_URL = `${import.meta.env.VITE_PUBLIC_BASE_URL}api`;

export const initialState = {
  username: "",
  token: "",
  isAdmin: false,
  expiresAt: null
}

const localState = localStorage.getItem("state");
const appState = localState ? JSON.parse(localState) : initialState;

export const State = $state({
  username: appState.username,
  token: appState.token,
  isAdmin: appState.isAdmin,
  expiresAt: appState.expiresAt,
  logout: () => {
    Object.assign(State, initialState);
    try { 
      localStorage.removeItem("state"); } catch {}
    }
})
