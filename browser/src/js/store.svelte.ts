export const API_URL = `${import.meta.env.VITE_PUBLIC_BASE_URL}api`;

const initialState = {
  username: "",
  token: "",
  profile: {}
};
const localState = localStorage.getItem("state");
const appState = localState ? JSON.parse(localState) : initialState;

export const State = $state({
  username: appState.username,
  token: appState.token,
  profile: appState.profile,
  searchText: '',
  currentWellbore: {wellbore:'WELLBORE'},
  currentWellId: '',
  wellboreList: [],
  currentWorkflowId: '',
  currentWorkflow: {},
  
  currentWell: {},

  // wellbores list
  wellboresRightPanel: 'map',
  wellboresMainPanelVisibility: true,
  wellboresRightPanelVisibility: true,
  wellboresRightPanelExpanded: false,
  // wellbore item
  wellboreItemRightPanel: 'list',
  wellboreItemMainPanelVisibility: true,
  wellboreItemRightPanelVisibility: true,
  wellboreItemRightPanelExpanded: false,
  // workflow 
  workflowMainPanelVisibility: true,
  nodePropertiesVisibility: false,
  workflowNavigation: ['Workflows'],
  // isUser: function() {
  //   return this.profile.role ==='AP_USER' 
  //     || this.profile.role ==='AP_ENGINEER'
  //     || this.profile.role ==='AP_ADMIN'
  // },
  isEngineer: function() {
    return this.profile.role ==='AP_ENGINEER'
      || this.profile.role ==='AP_ADMIN'
  },
  isAdmin: function() {
    return this.profile.role ==='AP_ADMIN'
  }
})
