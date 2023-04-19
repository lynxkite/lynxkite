// This file lists the dependencies so Vite can find them.
import 'angular-hotkeys/build/hotkeys.min.css';
import 'bootstrap/dist/css/bootstrap.css';
import '@fortawesome/fontawesome-free/css/all.css';
import 'typeface-exo-2/index.css';
import 'bootstrap-tourist/bootstrap-tourist.css';
// We could use a dynamic import if not for https://github.com/vitejs/vite/issues/3924.
// import.meta.glob('./styles/*.css', {eager: true});
import './styles/alert.css';
import './styles/backup.css';
import './styles/cleaner.css';
import './styles/context-menu.css';
import './styles/delta.css';
import './styles/drop-themes.css';
import './styles/entry-selector.css';
import './styles/file-parameter.css';
import './styles/find-in-page-box.css';
import './styles/flat-toolbar.css';
import './styles/graph.css';
import './styles/graph-view.css';
import './styles/help.css';
import './styles/histogram.css';
import './styles/inline-input.css';
import './styles/item-name-and-menu.css';
import './styles/logs.css';
import './styles/model-details.css';
import './styles/model-parameter.css';
import './styles/operation-parameters.css';
import './styles/operation-selector.css';
import './styles/parameters-parameter.css';
import './styles/project-graph.css';
import './styles/project-state.css';
import './styles/spark-status.css';
import './styles/splash.css';
import './styles/state-view.css';
import './styles/table-browser.css';
import './styles/table-state-view.css';
import './styles/user-menu.css';
import './styles/value.css';
import './styles/wizard.css';
import './styles/workspace.css';

// Bootstrap 3 looks for a global "jQuery", so we set it and delay the import.
import './scripts/util/jq-global';
import 'bootstrap';
// LynxKite code.
import.meta.glob('./scripts/**/*.js', {eager: true});
