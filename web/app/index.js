// This file lists the dependencies so Vite can find them.
import 'angular-hotkeys/build/hotkeys.min.css';
import 'bootstrap/dist/css/bootstrap.css';
import '@fortawesome/fontawesome-free/css/all.css';
import 'typeface-exo-2/index.css';
import 'bootstrap-tourist/bootstrap-tourist.css';
import.meta.glob('./styles/*.css', {eager: true});

// Bootstrap 3 looks for a global "jQuery", so we set it and delay the import.
import './scripts/util/jq-global';
import 'bootstrap';
// LynxKite code.
import.meta.glob('./scripts/**/*.js', {eager: true});
