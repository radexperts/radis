function selectiveTransferForm() {
  const config = getAditConfig();
  const STORAGE_KEY = "selectiveTransferForm-" + config.user_id;
  const ADVANCED_OPTIONS_COLLAPSED_KEY = "advancedOptionsCollapsed";

  function loadState() {
    let state = {};
    try {
      state = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "{}");
      if (typeof state !== "object") state = {};
    } catch (error) {
      console.error(`Invalid state from local storage: ${error}`);
    }
    return state;
  }

  function updateState(key, value) {
    const state = loadState();
    state[key] = value;
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }

  return {
    isDestinationFolder: false,

    init: function (formEl) {
      this.formEl = formEl;

      const advancedOptionsEl = this.formEl.querySelector("#advanced_options");
      advancedOptionsEl.addEventListener("hide.bs.collapse", function () {
        updateState(ADVANCED_OPTIONS_COLLAPSED_KEY, true);
      });
      advancedOptionsEl.addEventListener("show.bs.collapse", function () {
        updateState(ADVANCED_OPTIONS_COLLAPSED_KEY, false);
      });

      this._restoreState();
    },
    _restoreState: function () {
      const state = loadState();

      if (ADVANCED_OPTIONS_COLLAPSED_KEY in state) {
        const advancedOptionsEl =
          this.formEl.querySelector("#advanced_options");
        if (state[ADVANCED_OPTIONS_COLLAPSED_KEY])
          advancedOptionsEl.collapse("hide");
        else advancedOptionsEl.collapse("show");
      }
    },
    onStartTransfer: function (event) {
      const formEl = this.formEl;
      const buttonEl = event.currentTarget;
      buttonEl.style.pointerEvents = "none";

      function disableTransferButton() {
        // We can only disable the button after the message was send as otherwise
        // htmx won't send the message.
        buttonEl.disabled = true;
        formEl.removeEventListener("htmx:wsAfterSend", disableTransferButton);
      }
      formEl.addEventListener("htmx:wsAfterSend", disableTransferButton);
    },
  };
}
