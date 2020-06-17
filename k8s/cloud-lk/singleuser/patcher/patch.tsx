(function(that, categories) {
  function card(text, onclick, iconUrl) {
    return <div class="jp-LauncherCard" style={{
        width: 'calc(2 * var(--jp-private-launcher-card-size))',
        height: 'calc(2 * var(--jp-private-launcher-card-size))',
        background: '#004165',
        border: 'none',
      }} onClick={onclick}>
        <div class="jp-LauncherCard-icon" style={{
          height: 'calc(2 * var(--jp-private-launcher-card-icon-height))',
        }}>
          <img src={ iconUrl } class="jp-Launcher-kernelIcon" style={{ width: 'auto' }}></img>
        </div>
        <div class="jp-LauncherCard-label">
          <p style={{
            color: 'white',
            fontWeight: 'bold',
            fontSize: '16px',
            lineHeight: '16px',
            overflow: 'visible',
          }}>
            {text}
          </p>
        </div>
      </div>;
  }
  function gui() {
    window.open('LYNXKITE_ADDRESS/signedUsernameLogin?token=LYNXKITE_SIGNED_TOKEN_URL', '_blank');
  }
  function welcome() {
    that._commands.execute('docmanager:open', { path:'Welcome.ipynb' });
  }

  return <div>
    <h1>Your LynxKite demo instance is ready!</h1>
    <p>
    This instance will be suspended if unused for an hour.
    You can come back later by visiting <a
      style={{ textDecoration: 'underline' }}
      href="https://demo.lynxkite.com/">demo.lynxkite.com</a>.
    Instances unused for more than a week may be deleted.
    For more information and our tutorials see the <a
      style={{ textDecoration: 'underline' }}
      href="https://lynxkite.com/docs/latest">LynxKite documentation</a>.</p>
    <div class="jp-Launcher-cardContainer">
      { card('Use LynxKite’s graphical interface', gui, 'https://lynxkite.com/favicon.png') }
      { card('Use LynxKite’s Python API', welcome, categories.Notebook[0].kernelIconUrl) }
    </div>
  </div>;
})(this, e)
